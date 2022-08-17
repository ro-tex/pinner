package database

import (
	"context"
	"github.com/skynetlabs/pinner/lib"
	"time"

	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/SkynetLabs/skyd/skymodules"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	// maxNumFailedAttempts defines the maximum number of failed attempts at
	// which we would still attempt to pin a skylink.
	maxNumFailedAttempts = 5
	// maxNumSkylinksToProcess defines the maximum number of skylinks we want to
	// process in one batch. We need this in order to stay within Mongo's limits
	// for request size.
	maxNumSkylinksToProcess = 1000
)

var (
	// ErrInvalidSkylink is returned when a client call supplies an invalid
	// skylink hash.
	ErrInvalidSkylink = errors.New("invalid skylink")
	// ErrSkylinkExists is returned when we try to create a skylink that already
	// exists.
	ErrSkylinkExists = errors.New("skylink already exists")
	// ErrSkylinkNotExist is returned when we try to get a skylink that doesn't
	// exist.
	ErrSkylinkNotExist = errors.New("skylink does not exist")
	// ErrNoSkylinksLocked is returned when we try to lock underpinned skylinks
	// for pinning but we fail to do so.
	ErrNoSkylinksLocked = errors.New("no skylinks locked")
	// ErrNoUnderpinnedSkylinks is returned when all skylinks in the database
	// are either sufficiently pinned or pinned by the local server.
	ErrNoUnderpinnedSkylinks = errors.New("no underpinned skylinks found")
	// LockDuration defines the duration of a database lock. We lock skylinks
	// while we are trying to pin them to a new server. The goal is to only
	// allow a single server to pin a given skylink at a time.
	LockDuration = 7 * time.Hour
)

type (
	// SkylinkOnly is a helper type which we can use when we want to grab a
	// skylink from the DB.s
	SkylinkOnly struct {
		Skylink string `bson:"skylink"`
	}
	// Skylink represents a skylink object in the DB.
	Skylink struct {
		ID      primitive.ObjectID `bson:"_id,omitempty"`
		Skylink string             `bson:"skylink"`
		Servers []string           `bson:"servers"`
		// Pinned tells us that at least one user is actively pinning this
		// skylink and we want to keep it alive. If Pinned is false then all
		// servers should actively unpin the skylink and stop paying for it.
		// This is not yet implemented.
		Pinned         bool      `bson:"pinned"`
		LockedBy       string    `bson:"locked_by"`
		LockExpires    time.Time `bson:"lock_expires"`
		FailedAttempts uint      `bson:"failed_attempts,omitempty"`
	}
)

// CreateSkylink inserts a new skylink into the DB. Returns an error if it
// already exists.
func (db *DB) CreateSkylink(ctx context.Context, skylink skymodules.Skylink, server string) (Skylink, error) {
	if server == "" {
		return Skylink{}, errors.New("invalid server name")
	}
	s := Skylink{
		Skylink: skylink.String(),
		Servers: []string{server},
		Pinned:  true,
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

// DeleteSkylink removes the skylink from the database.
func (db *DB) DeleteSkylink(ctx context.Context, skylink skymodules.Skylink) error {
	filter := bson.M{"skylink": skylink.String()}
	dr, err := db.staticDB.Collection(collSkylinks).DeleteOne(ctx, filter)
	if err == mongo.ErrNoDocuments || dr.DeletedCount == 0 {
		return ErrSkylinkNotExist
	}
	return nil
}

// FindSkylink fetches a skylink from the DB.
func (db *DB) FindSkylink(ctx context.Context, skylink skymodules.Skylink) (Skylink, error) {
	sr := db.staticDB.Collection(collSkylinks).FindOne(ctx, bson.M{"skylink": skylink.String()})
	if sr.Err() == mongo.ErrNoDocuments {
		return Skylink{}, ErrSkylinkNotExist
	}
	if sr.Err() != nil {
		return Skylink{}, sr.Err()
	}
	s := Skylink{}
	err := sr.Decode(&s)
	if err != nil {
		return Skylink{}, err
	}
	return s, nil
}

// MarkFailedAttempt notes that we failed to pin this skylink.
func (db *DB) MarkFailedAttempt(ctx context.Context, skylink skymodules.Skylink) error {
	db.staticLogger.Tracef("Entering MarkFailedAttempt. Skylink: '%s'", skylink)
	defer db.staticLogger.Tracef("Exiting  MarkFailedAttempt. Skylink: '%s'", skylink)
	filter := bson.M{"skylink": skylink.String()}
	update := bson.M{"$inc": bson.M{"failed_attempts": 1}}
	_, err := db.staticDB.Collection(collSkylinks).UpdateOne(ctx, filter, update)
	return err
}

// MarkPinned marks a skylink as pinned (or no longer unpinned), meaning
// that Pinner should make sure it's pinned by the minimum number of servers.
func (db *DB) MarkPinned(ctx context.Context, skylink skymodules.Skylink) error {
	db.staticLogger.Tracef("Entering MarkPinned. Skylink: '%s'", skylink)
	defer db.staticLogger.Tracef("Exiting  MarkPinned. Skylink: '%s'", skylink)
	filter := bson.M{"skylink": skylink.String()}
	update := bson.M{"$set": bson.M{"pinned": true}}
	opts := options.Update().SetUpsert(true)
	_, err := db.staticDB.Collection(collSkylinks).UpdateOne(ctx, filter, update, opts)
	return err
}

// MarkUnpinned marks a skylink as unpinned, meaning that all servers
// should stop pinning it.
func (db *DB) MarkUnpinned(ctx context.Context, skylink skymodules.Skylink) error {
	db.staticLogger.Tracef("Entering MarkUnpinned. Skylink: '%s'", skylink)
	defer db.staticLogger.Tracef("Exiting  MarkUnpinned. Skylink: '%s'", skylink)
	filter := bson.M{"skylink": skylink.String()}
	update := bson.M{"$set": bson.M{"pinned": false}}
	opts := options.Update().SetUpsert(true)
	_, err := db.staticDB.Collection(collSkylinks).UpdateOne(ctx, filter, update, opts)
	return err
}

// AddServerForSkylinks adds a new server to the list of servers known to be
// pinning these skylinks. If a skylink does not already exist in the database
// it will be inserted. This operation is idempotent.
//
// The `markPinned` flag sets the `unpin` field of a skylink to false when
// raised but it doesn't set it to false when not raised. The reason for that is
// that it accommodates a specific use case - adding a server to the list of
// pinners of a given skylink will set the pinned field to true is we are doing
// that because we know that a user is pinning it but not so if we are running
// a server sweep and documenting which skylinks are pinned by this server.
func (db *DB) AddServerForSkylinks(ctx context.Context, skylinks []string, server string, markPinned bool) error {
	db.staticLogger.Tracef("Entering AddServerForSkylinks. Number of skylinks: %d, server: '%s'", len(skylinks), server)
	defer db.staticLogger.Tracef("Exiting  AddServerForSkylinks. Number of skylinks: %d, server: '%s'", len(skylinks), server)

	var update bson.M
	if markPinned {
		update = bson.M{
			"$addToSet": bson.M{"servers": server},
			"$set":      bson.M{"pinned": true},
		}
	} else {
		update = bson.M{"$addToSet": bson.M{"servers": server}}
	}

	var sls []string
	for len(skylinks) > 0 {
		if len(skylinks) > maxNumSkylinksToProcess {
			sls, skylinks = skylinks[:maxNumSkylinksToProcess], skylinks[maxNumSkylinksToProcess:]
		} else {
			sls, skylinks = skylinks[:], []string{}
		}

		// Make sure the skylink records exist.
		recs := make([]interface{}, len(sls))
		for idx, sl := range sls {
			recs[idx] = SkylinkOnly{Skylink: sl}
		}
		insOps := options.InsertMany().SetOrdered(false)
		_, err := db.staticDB.Collection(collSkylinks).InsertMany(ctx, recs, insOps)
		if err != nil && !mongo.IsDuplicateKeyError(err) {
			return err
		}

		// Update the skylink records.
		filter := bson.M{"skylink": bson.M{"$in": sls}}
		_, err = db.staticDB.Collection(collSkylinks).UpdateMany(ctx, filter, update)
		if err != nil {
			db.staticLogger.Debugf("Failed to add server '%s' for skylinks '%v'. Error: '%v'", server, sls, err)
			return err
		}
	}
	return nil
}

// RemoveServer removes the server as pinner from all skylinks in the database.
// Returns the number of skylinks from which the server was removed as pinner.
func (db *DB) RemoveServer(ctx context.Context, server string) (int64, error) {
	filter := bson.M{"servers": server}
	update := bson.M{"$pull": bson.M{"servers": server}}
	ur, err := db.staticDB.Collection(collSkylinks).UpdateMany(ctx, filter, update)
	if errors.Contains(err, mongo.ErrNoDocuments) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return ur.ModifiedCount, nil
}

// RemoveServerFromSkylinks removes a server from the list of servers known to
// be pinning these skylinks. If a skylink does not exist in the database it
// will not be inserted.
func (db *DB) RemoveServerFromSkylinks(ctx context.Context, skylinks []string, server string) error {
	db.staticLogger.Tracef("Entering RemoveServerFromSkylinks. Number of skylinks: %d, server: '%s'", len(skylinks), server)
	defer db.staticLogger.Tracef("Exiting  RemoveServerFromSkylinks. Number of skylinks: %d, server: '%s'", len(skylinks), server)

	update := bson.M{"$pull": bson.M{"servers": server}}
	var sls []string
	for len(skylinks) > 0 {
		if len(skylinks) > maxNumSkylinksToProcess {
			sls, skylinks = skylinks[:maxNumSkylinksToProcess], skylinks[maxNumSkylinksToProcess:]
		} else {
			sls, skylinks = skylinks[:], []string{}
		}

		filter := bson.M{
			"skylink": bson.M{"$in": sls},
			"servers": server,
		}
		_, err := db.staticDB.Collection(collSkylinks).UpdateMany(ctx, filter, update)
		if err != nil {
			return err
		}
	}
	return nil
}

// FindAndLockUnderpinned fetches and locks a single underpinned skylink
// from the database. The method selects only skylinks which are not pinned by
// the given server.
//
// The MongoDB query is this:
// db.getCollection('skylinks').find({
//     "pinned": { "$ne": false }},
//     "$expr": { "$lt": [{ "$size": "$servers" }, 2 ]},
//     "servers": { "$nin": [ "ro-tex.siasky.ivo.NOPE" ]},
//     "$or": [
//         { "lock_expires" : { "$exists": false }},
//         { "lock_expires" : { "$lt": new Date() }}
//     ]
// })
func (db *DB) FindAndLockUnderpinned(ctx context.Context, server string, skipSkylinks []string, minPinners int) (skymodules.Skylink, error) {
	if skipSkylinks == nil {
		skipSkylinks = make([]string, 0)
	}
	filter := bson.M{
		// We use pinned != false because pinned == true is the default but it's
		// possible that we've missed setting that somewhere.
		"pinned": bson.M{"$ne": false},
		// Pinned by fewer than the minimum number of servers.
		"$expr": bson.M{"$lt": bson.A{bson.M{"$size": "$servers"}, minPinners}},
		// Not pinned by the given server.
		"servers": bson.M{"$nin": bson.A{server}},
		// Skip all skylinks in the skipSkylinks list.
		"skylink": bson.M{"$nin": skipSkylinks},
		// Unlocked.
		"$or": bson.A{
			bson.M{"lock_expires": bson.M{"$exists": false}},
			bson.M{"lock_expires": bson.M{"$lt": lib.Now()}},
		},
		// We use "not greater than X" because that also covers the case where
		// the field is not set.
		"failed_attempts": bson.M{"$not": bson.M{"$gt": maxNumFailedAttempts}},
	}
	update := bson.M{
		"$set": bson.M{
			"locked_by":    server,
			"lock_expires": lib.Now().Add(LockDuration),
		},
	}
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	sr := db.staticDB.Collection(collSkylinks).FindOneAndUpdate(ctx, filter, update, opts)
	if sr.Err() == mongo.ErrNoDocuments {
		return skymodules.Skylink{}, ErrNoUnderpinnedSkylinks
	}
	if sr.Err() != nil {
		return skymodules.Skylink{}, sr.Err()
	}
	var result SkylinkOnly
	err := sr.Decode(&result)
	if err != nil {
		return skymodules.Skylink{}, errors.AddContext(err, "failed to decode result")
	}
	return SkylinkFromString(result.Skylink)
}

// ServersForSkylink returns a list of servers pinning the given skylink
// according to the database.
func (db *DB) ServersForSkylink(ctx context.Context, skylink skymodules.Skylink) ([]string, error) {
	sr := db.staticDB.Collection(collSkylinks).FindOne(ctx, bson.M{"skylink": skylink.String()})
	if sr.Err() == mongo.ErrNoDocuments {
		return []string{}, ErrSkylinkNotExist
	}
	if sr.Err() != nil {
		return nil, sr.Err()
	}
	var result struct {
		Servers []string `bson:"servers"`
	}
	err := sr.Decode(&result)
	if err != nil {
		return nil, errors.AddContext(err, "failed to decode results")
	}
	return result.Servers, nil
}

// SkylinksForServer returns a list of skylinks pinned by the given server
// according to the database. Note that this list doesn't necessarily match the
// list of skylink the server is actually pinning, it's the list the database
// knows of.
func (db *DB) SkylinksForServer(ctx context.Context, server string) ([]string, error) {
	c, err := db.staticDB.Collection(collSkylinks).Find(ctx, bson.M{"servers": server})
	if err != nil {
		return nil, err
	}
	if c.RemainingBatchLength() == 0 {
		return nil, mongo.ErrNoDocuments
	}
	var results []SkylinkOnly
	err = c.All(ctx, &results)
	if err != nil {
		return nil, errors.AddContext(err, "failed to decode results")
	}
	skylinks := make([]string, len(results))
	for k, v := range results {
		skylinks[k] = v.Skylink
	}
	return skylinks, nil
}

// UnlockSkylink removes the lock on the skylink put while we're trying to pin
// it to a new server.
func (db *DB) UnlockSkylink(ctx context.Context, skylink skymodules.Skylink, server string) error {
	db.staticLogger.Tracef("Entering UnlockSkylink. Skylink: '%s', server: '%s'", skylink, server)
	defer db.staticLogger.Tracef("Exiting  UnlockSkylink. Skylink: '%s', server: '%s'", skylink, server)
	filter := bson.M{
		"skylink":   skylink.String(),
		"locked_by": server,
	}
	update := bson.M{
		"$set": bson.M{
			"locked_by":    "",
			"lock_expires": time.Time{},
		},
	}
	ur, err := db.staticDB.Collection(collSkylinks).UpdateOne(ctx, filter, update)
	if errors.Contains(err, mongo.ErrNoDocuments) || ur.ModifiedCount == 0 {
		return ErrNoSkylinksLocked
	}
	return err
}

// IsNoSkylinksNeedPinning returns true when the given error indicates that
// there are no more skylinks that need to be pinned by the current server.
func IsNoSkylinksNeedPinning(err error) bool {
	return errors.Contains(err, ErrNoUnderpinnedSkylinks)
}

// SkylinkFromString converts a string to a Skylink.
func SkylinkFromString(s string) (skymodules.Skylink, error) {
	var sl skymodules.Skylink
	err := sl.LoadString(s)
	if err != nil {
		return skymodules.Skylink{}, err
	}
	return sl, nil
}
