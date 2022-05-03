package database

import (
	"context"
	"time"

	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/SkynetLabs/skyd/skymodules"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	// ErrInvalidSkylink is returned when a client call supplies an invalid
	// skylink hash.
	ErrInvalidSkylink = errors.New("invalid skylink")
	// ErrSkylinkExists is returned when we try to create a skylink that already
	// exists.
	ErrSkylinkExists = errors.New("skylink already exists")
	// ErrSkylinkNoExist is returned when we try to get a skylink that doesn't
	// exist.
	ErrSkylinkNoExist = errors.New("skylink does not exist")

	// LockDuration defines the duration of a database lock. We lock skylinks
	// while we are trying to pin them to a new server. The goal is to only
	// allow a single server to pin a given skylink at a time.
	LockDuration = 24 * time.Hour
)

type (
	// Skylink represents a skylink object in the DB.
	// The Unpin field instructs all servers, currently pinning this skylink,
	// that there are no users pinning and the servers should unpin it as well.
	Skylink struct {
		ID          primitive.ObjectID `bson:"_id,omitempty"`
		Skylink     string             `bson:"skylink"`
		Servers     []string           `bson:"servers"`
		Unpin       bool               `bson:"unpin"`
		LockedBy    string             `bson:"locked_by"`
		LockExpires time.Time          `bson:"lock_expires"`
	}
)

// CreateSkylink inserts a new skylink into the DB. Returns an error if it
// already exists.
func (db *DB) CreateSkylink(ctx context.Context, skylink string, server string) (Skylink, error) {
	sl, err := CanonicalSkylink(skylink)
	if err != nil {
		return Skylink{}, ErrInvalidSkylink
	}
	if server == "" {
		return Skylink{}, errors.New("invalid server name")
	}
	s := Skylink{
		Skylink: sl,
		Servers: []string{server},
	}
	ir, err := db.staticDB.Collection(collSkylinks).InsertOne(ctx, s)
	if mongo.IsDuplicateKeyError(err) {
		return Skylink{}, ErrSkylinkExists
	}
	if err != nil {
		return Skylink{}, err
	}
	s.ID = ir.InsertedID.(primitive.ObjectID)
	return s, nil
}

// FindSkylink fetches a skylink from the DB.
func (db *DB) FindSkylink(ctx context.Context, skylink string) (Skylink, error) {
	sl, err := CanonicalSkylink(skylink)
	if err != nil {
		return Skylink{}, ErrInvalidSkylink
	}
	sr := db.staticDB.Collection(collSkylinks).FindOne(ctx, bson.M{"skylink": sl})
	if sr.Err() == mongo.ErrNoDocuments {
		return Skylink{}, ErrSkylinkNoExist
	}
	if sr.Err() != nil {
		return Skylink{}, sr.Err()
	}
	s := Skylink{}
	err = sr.Decode(&s)
	if err != nil {
		return Skylink{}, err
	}
	return s, nil
}

// MarkPinned marks a skylink as pinned (or no longer unpinned), meaning
// that Pinner should make sure it's pinned by the minimum number of servers.
func (db *DB) MarkPinned(ctx context.Context, skylink string) error {
	sl, err := CanonicalSkylink(skylink)
	if err != nil {
		return ErrInvalidSkylink
	}
	filter := bson.M{"skylink": sl}
	update := bson.M{"$set": bson.M{"unpin": false}}
	opts := options.Update().SetUpsert(true)
	_, err = db.staticDB.Collection(collSkylinks).UpdateOne(ctx, filter, update, opts)
	return err
}

// MarkUnpinned marks a skylink as unpinned, meaning that all servers
// should stop pinning it.
func (db *DB) MarkUnpinned(ctx context.Context, skylink string) error {
	sl, err := CanonicalSkylink(skylink)
	if err != nil {
		return ErrInvalidSkylink
	}
	filter := bson.M{"skylink": sl}
	update := bson.M{"$set": bson.M{"unpin": true}}
	opts := options.Update().SetUpsert(true)
	_, err = db.staticDB.Collection(collSkylinks).UpdateOne(ctx, filter, update, opts)
	return err
}

// AddServerForSkylink adds a new server to the list of servers known to be pinning
// this skylink. If the skylink does not already exist in the database it will
// be inserted. This operation is idempotent.
//
// The `markPinned` flag sets the `unpin` field of the skylink to false when
// raised but it doesn't set it to false when not raised. The reason for that is
// that it accommodates a specific use case - adding a server to the list of
// pinners of a given skylink will set the unpin field to false is we are doing
// that because we know that a user is pinning it but not so if we are running
// a server sweep and documenting which skylinks are pinned by this server.
func (db *DB) AddServerForSkylink(ctx context.Context, skylink string, server string, markPinned bool) error {
	sl, err := CanonicalSkylink(skylink)
	if err != nil {
		return ErrInvalidSkylink
	}
	filter := bson.M{"skylink": sl}
	var update bson.M
	if markPinned {
		update = bson.M{
			"$addToSet": bson.M{"servers": server},
			"$set":      bson.M{"unpin": false},
		}
	} else {
		update = bson.M{"$addToSet": bson.M{"servers": server}}
	}
	opts := options.Update().SetUpsert(true)
	_, err = db.staticDB.Collection(collSkylinks).UpdateOne(ctx, filter, update, opts)
	return err
}

// RemoveServerFromSkylink removes a server to the list of servers known to be
// pinning this skylink. If the skylink does not exist in the database it will
// not be inserted.
func (db *DB) RemoveServerFromSkylink(ctx context.Context, skylink string, server string) error {
	sl, err := CanonicalSkylink(skylink)
	if err != nil {
		return ErrInvalidSkylink
	}
	filter := bson.M{
		"skylink": sl,
		"servers": server,
	}
	update := bson.M{"$pull": bson.M{"servers": server}}
	_, err = db.staticDB.Collection(collSkylinks).UpdateOne(ctx, filter, update)
	return err
}

// CanonicalSkylink validates the given string as a skylink and returns its
// canonical form (base64), regardless of the input format, e.g. base32.
func CanonicalSkylink(sl string) (string, error) {
	var s skymodules.Skylink
	err := s.LoadString(sl)
	if err != nil {
		return "", err
	}
	return s.String(), nil
}
