package database

import (
	"context"
	"gitlab.com/NebulousLabs/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	// ErrServerLoadNotFound is returned when we don't have a record for the
	// server's load.
	ErrServerLoadNotFound = errors.New("server load not found")
)

// ServerLoad returns the server load stored in the database.
func (db *DB) ServerLoad(ctx context.Context, server string) (int64, error) {
	filter := bson.M{"server_name": server}
	sr := db.staticDB.Collection(collServerLoad).FindOne(ctx, filter)
	if sr.Err() == mongo.ErrNoDocuments {
		return 0, ErrServerLoadNotFound
	}
	if sr.Err() != nil {
		return 0, sr.Err()
	}
	res := struct {
		Load int64
	}{}
	err := sr.Decode(&res)
	if err != nil {
		return 0, err
	}
	return res.Load, nil
}

// ServerLoadPosition returns the position of the server in the list of servers
// based on its load. It also returns the total number of servers.
func (db *DB) ServerLoadPosition(ctx context.Context, server string) (int, int, error) {
	// Fetch all server loads, order by load in descending order.
	opts := &options.FindOptions{
		Sort: bson.M{"load": -1},
	}
	c, err := db.staticDB.Collection(collServerLoad).Find(ctx, bson.M{}, opts)
	if err != nil {
		return 0, 0, err
	}
	found := false
	position := 0
	cur := struct {
		ServerName string `bson:"server_name"`
		Load       int64  `bson:"load"`
	}{}
	// Loop over all server loads until we find the one we need.
	for c.Next(ctx) {
		err = c.Decode(&cur)
		if err != nil {
			return 0, 0, err
		}
		if cur.ServerName == server {
			found = true
			break
		}
		position++
	}
	if found {
		return position, position + 1 + c.RemainingBatchLength(), nil
	}
	return 0, 0, ErrServerLoadNotFound
}

// SetServerLoad stores the load of this server in the database.
func (db *DB) SetServerLoad(ctx context.Context, server string, load int64) error {
	if server == "" {
		return errors.New("invalid server name")
	}
	filter := bson.M{"server_name": server}
	update := bson.M{"$set": bson.M{
		"server_name": server,
		"load":        load,
	}}
	opts := options.Update().SetUpsert(true)
	_, err := db.staticDB.Collection(collServerLoad).UpdateOne(ctx, filter, update, opts)
	return err
}
