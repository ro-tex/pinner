package database

import (
	"context"
	"fmt"
	"time"

	"github.com/skynetlabs/pinner/logger"
	"gitlab.com/NebulousLabs/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const (
	// MongoDefaultTimeout is our default timeout for database operations.
	MongoDefaultTimeout = 30 * time.Second
)

var (
	// ErrCtxFailedToConnect is the context we add to an error when we fail to
	// connect to the db.
	ErrCtxFailedToConnect = "failed to connect to the db"

	// dbName defines the name of the database this service uses
	dbName = "pinner"
	// collConfig defines the name of the collection which will hold the
	// cluster-wide service configuration.
	collConfig = "configuration"
	// collServerLoad defines the name of the collection which will hold
	// information about each server's load in terms of amount of data it's
	// pinning
	collServerLoad = "server_load"
	// collSkylinks defines the name of the collection which will hold
	// information about skylinks
	collSkylinks = "skylinks"
)

type (
	// DB holds a connection to the database, as well as helpful shortcuts to
	// collections and utilities.
	DB struct {
		staticCtx    context.Context
		staticDB     *mongo.Database
		staticLogger logger.Logger
	}

	// DBCredentials is a helper struct that binds together all values needed for
	// establishing a DB connection.
	DBCredentials struct {
		User     string
		Password string
		Host     string
		Port     string
	}

	// Hello is a selection of the information returned by MongoDB as response
	// to db.hello(), i.e. this is some basic information about the DB node.
	Hello struct {
		Hosts              []string  `bson:"hosts" json:"hosts"`
		SetName            string    `bson:"setName" json:"setName"`
		IsWriteablePrimary bool      `bson:"isWriteablePrimary" json:"isWriteablePrimary"`
		Secondary          bool      `bson:"secondary" json:"secondary"`
		Primary            string    `bson:"primary" json:"primary"`
		Me                 string    `bson:"me" json:"me"`
		LocalTime          time.Time `bson:"localTime" json:"localTime"`
		OK                 float32   `bson:"ok" json:"OK"`
	}
)

// New creates a new database connection.
func New(ctx context.Context, creds DBCredentials, logger logger.Logger) (*DB, error) {
	return NewCustomDB(ctx, dbName, creds, logger)
}

// NewCustomDB creates a new database connection to a database with a custom name.
func NewCustomDB(ctx context.Context, dbName string, creds DBCredentials, logger logger.Logger) (*DB, error) {
	if ctx == nil {
		return nil, errors.New("invalid context provided")
	}
	if logger == nil {
		return nil, errors.New("invalid logger provided")
	}

	auth := options.Credential{
		Username: creds.User,
		Password: creds.Password,
	}
	opts := options.Client().
		ApplyURI(fmt.Sprintf("mongodb://%s:%s/", creds.Host, creds.Port)).
		SetAuth(auth).
		SetReadConcern(readconcern.Local()).
		SetReadPreference(readpref.Nearest()).
		SetWriteConcern(writeconcern.New(writeconcern.WMajority(), writeconcern.WTimeout(30*time.Second))).
		SetCompressors([]string{"zstd", "zlib", "snappy"})
	c, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, errors.AddContext(err, ErrCtxFailedToConnect)
	}
	db := c.Database(dbName)
	err = ensureDBSchema(ctx, db, logger)
	if err != nil {
		return nil, err
	}
	return &DB{
		staticCtx:    ctx,
		staticDB:     db,
		staticLogger: logger,
	}, nil
}

// ConfigValue returns a cluster-wide configuration value, stored in the
// database.
func (db *DB) ConfigValue(ctx context.Context, key string) (string, error) {
	sr := db.staticDB.Collection(collConfig).FindOne(ctx, bson.M{"key": key})
	if sr.Err() != nil {
		return "", sr.Err()
	}
	var result = struct {
		Value string
	}{}
	err := sr.Decode(&result)
	if err != nil {
		return "", err
	}
	return result.Value, nil
}

// Disconnect closes the connection to the database in an orderly fashion.
func (db *DB) Disconnect(ctx context.Context) error {
	return db.staticDB.Client().Disconnect(ctx)
}

// NumberSessionsInProgress returns the number of sessions that have been
// started for this client but have not been closed (i.e. EndSession has not
// been called).
func (db *DB) NumberSessionsInProgress() int {
	return db.staticDB.Client().NumberSessionsInProgress()
}

// Ping sends a ping command to verify that the client can connect to the DB and
// specifically to the primary.
func (db *DB) Ping(ctx context.Context) error {
	return db.staticDB.Client().Ping(ctx, readpref.Primary())
}

// Hello returns some status information about the local DB node.
func (db *DB) Hello(ctx context.Context) (*Hello, error) {
	sr := db.staticDB.RunCommand(ctx, bson.M{"hello": 1})
	if sr.Err() != nil {
		return nil, sr.Err()
	}
	var res Hello
	err := sr.Decode(&res)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

// SetConfigValue updates a cluster-wide configuration value, stored in the
// database.
func (db *DB) SetConfigValue(ctx context.Context, key, value string) error {
	opts := options.Update().SetUpsert(true)
	filter := bson.M{"key": key}
	update := bson.M{
		"$set": bson.M{
			"key":   key,
			"value": value,
		},
	}
	_, err := db.staticDB.Collection(collConfig).UpdateOne(ctx, filter, update, opts)
	return err
}

// ensureDBSchema checks that we have all collections and indexes we need and
// creates them if needed.
// See https://docs.mongodb.com/manual/indexes/
// See https://docs.mongodb.com/manual/core/index-unique/
func ensureDBSchema(ctx context.Context, db *mongo.Database, log logger.Logger) error {
	for collName, models := range schema() {
		coll, err := ensureCollection(ctx, db, collName)
		if err != nil {
			return err
		}
		iv := coll.Indexes()
		names, err := iv.CreateMany(ctx, models)
		if err != nil {
			return errors.AddContext(err, "failed to create indexes")
		}
		log.Debugf("Ensured index exists: %v", names)
	}
	return nil
}

// ensureCollection gets the given collection from the
// database and creates it if it doesn't exist.
func ensureCollection(ctx context.Context, db *mongo.Database, collName string) (*mongo.Collection, error) {
	coll := db.Collection(collName)
	if coll == nil {
		err := db.CreateCollection(ctx, collName)
		if err != nil {
			return nil, err
		}
		coll = db.Collection(collName)
		if coll == nil {
			return nil, errors.New("failed to create collection " + collName)
		}
	}
	return coll, nil
}
