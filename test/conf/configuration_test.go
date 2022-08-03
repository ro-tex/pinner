package conf

import (
	"context"
	"github.com/skynetlabs/pinner/conf"
	"github.com/skynetlabs/pinner/database"
	"github.com/skynetlabs/pinner/test"
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/fastrand"
	"go.mongodb.org/mongo-driver/mongo"
	"strconv"
	"testing"
	"time"
)

// TestConfig ensures that configuration functions work correctly.
func TestConfig(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	tests := map[string]func(t *testing.T, db *database.DB){
		"DryRun":     testDryRun,
		"MinPinners": testMinPinners,
		"NextScan":   testNextScan,
	}

	// Creating a DB is relatively expensive and since it's unnecessary we'll
	// reuse the same DB instance.
	db, err := test.NewDatabase(context.Background(), t.Name())
	if err != nil {
		t.Fatal(err)
	}

	for name, tt := range tests {
		t.Run(name, curryDB(tt, db))
	}
}

// curryDB transforms a test function with a DB parameter into a regular test
// function.
func curryDB(fn func(t *testing.T, db *database.DB), db *database.DB) func(t *testing.T) {
	return func(t *testing.T) {
		fn(t, db)
	}
}

// testDryRun ensures DryRun functions correctly.
func testDryRun(t *testing.T, db *database.DB) {
	ctx, cancel := test.Context()
	defer cancel()
	// Check value before setting, expect false.
	val, err := conf.DryRun(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if val {
		t.Fatalf("Expected %t, got %t", false, val)
	}
	err = db.SetConfigValue(ctx, conf.ConfDryRun, "true")
	if err != nil {
		t.Fatal(err)
	}
	val, err = conf.DryRun(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if !val {
		t.Fatalf("Expected %t, got %t", true, val)
	}
	err = db.SetConfigValue(ctx, conf.ConfDryRun, "false")
	if err != nil {
		t.Fatal(err)
	}
	val, err = conf.DryRun(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if val {
		t.Fatalf("Expected %t, got %t", false, val)
	}
	err = db.SetConfigValue(ctx, conf.ConfDryRun, "THIS IS NOT A VALID BOOLEAN!")
	if err != nil {
		t.Fatal(err)
	}
	_, err = conf.DryRun(ctx, db)
	if err == nil {
		t.Fatalf("Expected this to fail.")
	}
}

// testMinPinners ensures MinPinners functions correctly.
func testMinPinners(t *testing.T, db *database.DB) {
	ctx, cancel := test.Context()
	defer cancel()
	val, err := conf.MinPinners(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	// Make sure we get the default value.
	if val != 1 {
		t.Fatalf("Expected %d, got %d", 1, val)
	}
	// Set min pinners to a valid value [1;10].
	mp := 1 + fastrand.Intn(9)
	err = db.SetConfigValue(ctx, conf.ConfMinPinners, strconv.Itoa(mp))
	if err != nil {
		t.Fatal(err)
	}
	val, err = conf.MinPinners(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if val != mp {
		t.Fatalf("Expected %d, got %d", mp, val)
	}

	// expectBuildCritical fetches the value of MinPinners and fails the test if
	// it doesn't detect a panic.
	expectBuildCritical := func() {
		err = func() (err error) {
			defer func() {
				if e := recover(); e != nil {
					err = nil
				} else {
					err = errors.New("expected a panic but didn't get it")
				}
			}()
			_, _ = conf.MinPinners(ctx, db)
			return
		}()
		if err != nil {
			t.Fatalf("Expected a build.Critical but didn't get it.")
		}
	}

	// Set to less than the minimum.
	err = db.SetConfigValue(ctx, conf.ConfMinPinners, "0")
	if err != nil {
		t.Fatal(err)
	}
	expectBuildCritical()
	// Set to less than the minimum and negative.
	err = db.SetConfigValue(ctx, conf.ConfMinPinners, "-5")
	if err != nil {
		t.Fatal(err)
	}
	expectBuildCritical() // Set to more than the maximum.
	err = db.SetConfigValue(ctx, conf.ConfMinPinners, "100")
	if err != nil {
		t.Fatal(err)
	}
	expectBuildCritical()
}

// testNextScan ensures NextScan and SetNextScan function correctly.
func testNextScan(t *testing.T, db *database.DB) {
	ctx, cancel := test.Context()
	defer cancel()
	logger := test.NewDiscardLogger()
	// Ensure that there is no value in the DB.
	_, err := db.ConfigValue(ctx, conf.ConfNextScan)
	if !errors.Contains(err, mongo.ErrNoDocuments) {
		t.Fatalf("Expected '%s', got '%s'", mongo.ErrNoDocuments, err)
	}
	// Get the NextScan value, expect this to set the default offset.
	t0, err := conf.NextScan(ctx, db, logger)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	tolerance := 5 * time.Second
	if t0.After(now.Add(tolerance)) || t0.Before(now.Add(-1*tolerance)) {
		t.Fatalf("Expected roughly '%s', got '%s'", now.Add(conf.DefaultNextScanOffset), t0)
	}
	// Try to set an invalid value.
	err = conf.SetNextScan(ctx, db, time.Now().UTC().Add(-1*time.Hour))
	if !errors.Contains(err, conf.ErrTimeTooSoon) {
		t.Fatalf("Expected '%s', got '%s'", conf.ErrTimeTooSoon, err)
	}
	// Set a valid value.
	// Truncate to second because RFC3339 doesn't represent less than that.
	t1 := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	err = conf.SetNextScan(ctx, db, t1)
	if err != nil {
		t.Fatal(err)
	}
	// Verify the value is set correctly.
	t2, err := conf.NextScan(ctx, db, logger)
	if err != nil {
		t.Fatal(err)
	}
	if !t2.Equal(t1) {
		t.Fatalf("Expected '%s', got '%s'", t1, t2)
	}
}
