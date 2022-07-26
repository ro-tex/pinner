package sweeper

import (
	"context"
	"github.com/skynetlabs/pinner/skyd"
	"github.com/skynetlabs/pinner/sweeper"
	"github.com/skynetlabs/pinner/test"
	"testing"
	"time"
)

// TestSweeper ensures that Sweeper properly scans skyd and updates the DB.
func TestSweeper(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	ctx := context.Background()
	db, err := test.NewDatabase(ctx, t.Name())
	if err != nil {
		t.Fatal(err)
	}
	skydc := skyd.NewSkydClientMock()
	serverName := t.Name()
	logger := test.NewDiscardLogger()

	// Ensure there are no skylinks pinned by this server, according to the DB.
	sls, err := db.SkylinksForServer(ctx, serverName)
	if err != nil {
		t.Fatal(err)
	}
	if len(sls) > 0 {
		t.Fatalf("Expected no skylinks marked as pinned by this server, got %d", len(sls))
	}

	testSweepPeriod := 100 * time.Millisecond
	swpr := sweeper.New(db, skydc, serverName, logger)
	swpr.UpdateSchedule(testSweepPeriod)
	defer swpr.Close()

	// Wait for the sweep to come and go through.
	// Rebuilding the cache will take 100ms.
	time.Sleep(300 * time.Millisecond)

	// Ensure the sweep passed and there are skylinks in the DB marked as pinned
	// by this server.
	sls, err = db.SkylinksForServer(ctx, serverName)
	if err != nil {
		t.Fatal(err)
	}
	// Grab the skylinks available in the mock. We expect to see these in the DB.
	mockSkylinks := skydc.Skylinks()
	if len(sls) != len(mockSkylinks) {
		t.Fatalf("Expected %d skylinks, got %d", len(mockSkylinks), len(sls))
	}
	// Ensure all skylinks are there.
	for _, s := range mockSkylinks {
		if !test.Contains(sls, s) {
			t.Fatalf("Missing skylink '%s'", s)
		}
	}
}
