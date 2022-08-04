package database

import (
	"encoding/hex"
	"github.com/skynetlabs/pinner/database"
	"github.com/skynetlabs/pinner/test"
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/fastrand"
	"math"
	"testing"
)

// TestServerLoad ensures that we can correctly set and get the load of servers,
// as well as fetch their correct positions in the list of servers ordered by
// load.
func TestServerLoad(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	ctx, cancel := test.Context()
	defer cancel()
	db, err := test.NewDatabase(ctx, t.Name())
	if err != nil {
		t.Fatal(err)
	}

	server1 := hex.EncodeToString(fastrand.Bytes(16))
	server2 := hex.EncodeToString(fastrand.Bytes(16))
	load1 := int64(fastrand.Intn(math.MaxInt))
	load2 := int64(fastrand.Intn(math.MaxInt))
	if load1 == load2 {
		load2++
	}
	var pos1, pos2 int
	if load1 > load2 {
		pos1 = 0
		pos2 = 1
	} else {
		pos1 = 1
		pos2 = 0
	}

	// Get the load of a non-existent server. Expect ErrServerLoadNotFound.
	_, err = db.ServerLoad(ctx, "non-existent")
	if !errors.Contains(err, database.ErrServerLoadNotFound) {
		t.Fatalf("Expected '%s', got '%v'", database.ErrServerLoadNotFound, err)
	}
	// Set the server load of two servers.
	err = db.SetServerLoad(ctx, server1, load1)
	if err != nil {
		t.Fatal(err)
	}
	err = db.SetServerLoad(ctx, server2, load2)
	if err != nil {
		t.Fatal(err)
	}
	// Get the loads.
	l1, err := db.ServerLoad(ctx, server1)
	if err != nil {
		t.Fatal(err)
	}
	if l1 != load1 {
		t.Fatalf("Expected load of %d, got %d", load1, l1)
	}
	l2, err := db.ServerLoad(ctx, server2)
	if err != nil {
		t.Fatal(err)
	}
	if l2 != load2 {
		t.Fatalf("Expected load of %d, got %d", load2, l2)
	}
	// Get the positions of the two servers.
	p1, total, err := db.ServerLoadPosition(ctx, server1)
	if err != nil {
		t.Fatal(err)
	}
	if p1 != pos1 || total != 2 {
		t.Fatalf("Expected position %d of %d, got %d of %d", pos1, 2, p1, total)
	}
	p2, total, err := db.ServerLoadPosition(ctx, server2)
	if err != nil {
		t.Fatal(err)
	}
	if p2 != pos2 || total != 2 {
		t.Fatalf("Expected position %d of %d, got %d of %d", pos2, 2, p2, total)
	}
}
