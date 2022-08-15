package workers

import (
	"context"
	"encoding/hex"
	"github.com/skynetlabs/pinner/lib"
	"gitlab.com/NebulousLabs/fastrand"
	"testing"
	"time"

	"github.com/skynetlabs/pinner/conf"
	"github.com/skynetlabs/pinner/database"
	"github.com/skynetlabs/pinner/skyd"
	"github.com/skynetlabs/pinner/test"
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/SkynetLabs/skyd/build"
	"gitlab.com/SkynetLabs/skyd/skymodules"
	"go.sia.tech/siad/modules"
)

const (
	// cyclesToWait establishes a common number of sleepBetweenScans cycles we
	// should wait until we consider that a file has been or hasn't been picked
	// by the scanner.
	cyclesToWait = 3
)

// TestScannerDryRun ensures that dry_run works as expected.
func TestScannerDryRun(t *testing.T) {
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
	// Set dry_run: true.
	err = db.SetConfigValue(ctx, conf.ConfDryRun, "true")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.SetConfigValue(ctx, conf.ConfDryRun, "false")
		if err != nil {
			t.Fatal(err)
		}
	}()

	cfg, err := test.LoadTestConfig()
	if err != nil {
		t.Fatal(err)
	}
	skydcm := skyd.NewSkydClientMock()
	serverName := t.Name()
	s := NewScanner(db, test.NewDiscardLogger(), cfg.MinPinners, serverName, cfg.SleepBetweenScans, skydcm)
	err = s.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if e := s.Close(); e != nil {
			t.Error(errors.AddContext(e, "failed to close threadgroup"))
		}
	}()

	// Trigger a pin event.
	//
	// Add a skylink from the name of a different server.
	sl := test.RandomSkylink()
	otherServer := "other server"
	_, err = db.CreateSkylink(ctx, sl, otherServer)
	if err != nil {
		t.Fatal(err)
	}
	// Sleep for a while, giving a chance to the scanner to pick the skylink up.
	time.Sleep(cyclesToWait * s.SleepBetweenScans())
	// Make sure the skylink isn't pinned on the local (mock) skyd.
	if skydcm.IsPinning(sl.String()) {
		t.Fatal("We didn't expect skyd to be pinning this.")
	}
	// Remove the other server, making the file underpinned.
	err = db.RemoveServerFromSkylinks(ctx, []string{sl.String()}, otherServer)
	if err != nil {
		t.Fatal(err)
	}

	// Wait - the skylink should not be picked up and pinned on the local skyd.
	time.Sleep(cyclesToWait * s.SleepBetweenScans())

	// Verify skyd doesn't have the pin.
	//
	// Make sure the skylink is not pinned on the local (mock) skyd.
	if skydcm.IsPinning(sl.String()) {
		t.Fatal("We did not expect skyd to be pinning this.")
	}

	// Turn off dry run.
	err = db.SetConfigValue(ctx, conf.ConfDryRun, "false")
	if err != nil {
		t.Fatal(err)
	}

	// Wait for the skylink should be picked up and pinned on the local skyd.
	err = build.Retry(cyclesToWait, s.SleepBetweenScans(), func() error {
		// Make sure the skylink is pinned on the local (mock) skyd.
		if !skydcm.IsPinning(sl.String()) {
			return errors.New("we expected skyd to be pinning this")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestScanner_calculateSleep ensures that staticEstimateTimeToFull returns what
// we expect.
func TestScanner_calculateSleep(t *testing.T) {
	tests := map[string]struct {
		dataSize      uint64
		expectedSleep time.Duration
	}{
		"small file": {
			1 << 20, // 1 MB
			3 * time.Second,
		},
		"5 MB": {
			1 << 20 * 5, // 5 MB
			3 * time.Second,
		},
		"50 MB": {
			1 << 20 * 50, // 50 MB
			7 * time.Second,
		},
		"500 MB": {
			1 << 20 * 500, // 500 MB
			48 * time.Second,
		},
		"5 GB": {
			1 << 30 * 5, // 5 GB
			480 * time.Second,
		},
	}

	skydMock := skyd.NewSkydClientMock()
	s := Scanner{
		staticSkydClient: skydMock,
	}
	skylink := test.RandomSkylink()

	for tname, tt := range tests {
		// Prepare the mock.
		meta := skymodules.SkyfileMetadata{Length: tt.dataSize}
		skydMock.SetMetadata(skylink.String(), meta, nil)

		sleep := s.staticEstimateTimeToFull(skylink)
		if sleep != tt.expectedSleep {
			t.Errorf("%s: expected %ds, got %ds", tname, tt.expectedSleep/time.Second, sleep/time.Second)
		}
	}
}

// TestScannerSuite ensures that Scanner does its job.
func TestScannerSuite(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	// This is an entire test suite, so it's possible for it to run well beyond
	// our default timeout for a single test. That's why we use a context with
	// no timeout.
	ctx := context.Background()
	db, err := test.NewDatabase(ctx, t.Name())
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := test.LoadTestConfig()
	if err != nil {
		t.Fatal(err)
	}

	tests := map[string]func(t *testing.T, db *database.DB, cfg conf.Config, skydcm *skyd.ClientMock){
		"Base":                   testBase,
		"SleepForOrUntilStopped": testSleepForOrUntilStopped,
		"EstimateTimeToFull":     testEstimateTimeToFull,
		"WaitUntilHealthy":       testWaitUntilHealthy,
	}

	skydcm := skyd.NewSkydClientMock()
	for name, tt := range tests {
		t.Run(name, curryTest(tt, db, cfg, skydcm))
	}
}

// curryTest transforms a custom test function into a standard test function.
func curryTest(fn func(t *testing.T, db *database.DB, cfg conf.Config, skydcm *skyd.ClientMock), db *database.DB, cfg conf.Config, skydcm *skyd.ClientMock) func(t *testing.T) {
	return func(t *testing.T) {
		fn(t, db, cfg, skydcm)
	}
}

// testBase ensures that Scanner works as expected in the general case.
func testBase(t *testing.T, db *database.DB, cfg conf.Config, skydcm *skyd.ClientMock) {
	s := NewScanner(db, test.NewDiscardLogger(), cfg.MinPinners, t.Name(), cfg.SleepBetweenScans, skydcm)
	err := s.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if e := s.Close(); e != nil {
			t.Error(errors.AddContext(e, "failed to close threadgroup"))
		}
	}()

	ctx, cancel := test.Context()
	defer cancel()
	// Add a skylink from the name of a different server.
	sl := test.RandomSkylink()
	otherServer := "other server"
	_, err = db.CreateSkylink(ctx, sl, otherServer)
	if err != nil {
		t.Fatal(err)
	}

	// Sleep for a while, giving a chance to the scanner to pick the skylink up.
	time.Sleep(cyclesToWait * s.SleepBetweenScans())
	// Make sure the skylink isn't pinned on the local (mock) skyd.
	if skydcm.IsPinning(sl.String()) {
		t.Fatal("We didn't expect skyd to be pinning this.")
	}
	// Remove the other server, making the file underpinned.
	err = db.RemoveServerFromSkylinks(ctx, []string{sl.String()}, otherServer)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for the skylink should be picked up and pinned on the local skyd.
	err = build.Retry(cyclesToWait, s.SleepBetweenScans(), func() error {
		// Make sure the skylink is pinned on the local (mock) skyd.
		if !skydcm.IsPinning(sl.String()) {
			return errors.New("we expected skyd to be pinning this")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// testSleepForOrUntilStopped ensures that staticSleepForOrUntilStopped
// functions properly.
func testSleepForOrUntilStopped(t *testing.T, db *database.DB, cfg conf.Config, skydcm *skyd.ClientMock) {
	s := NewScanner(db, test.NewDiscardLogger(), cfg.MinPinners, t.Name(), cfg.SleepBetweenScans, skydcm)
	// Sleep for 10ms, expect false.
	stopped := s.staticSleepForOrUntilStopped(10 * time.Millisecond)
	if stopped {
		t.Fatal("Unexpected.")
	}
	// Schedule a stop in 10ms.
	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = s.Close()
	}()
	// Sleep for 100ms, expect to be stopped in 10ms.
	t0 := lib.Now()
	stopped = s.staticSleepForOrUntilStopped(100 * time.Millisecond)
	if !stopped {
		t.Fatal("Unexpected")
	}
	// Expect current time to be t0 + 10ms. Give 5ms tolerance.
	if lib.Now().After(t0.Add(15 * time.Millisecond)) {
		t.Fatalf("Expected to sleep for about 10ms, slept for %d ms", lib.Now().Sub(t0).Milliseconds())
	}
}

// testEstimateTimeToFull ensures that staticEstimateTimeToFull functions
// correctly.
func testEstimateTimeToFull(t *testing.T, db *database.DB, cfg conf.Config, skydcm *skyd.ClientMock) {
	s := NewScanner(db, test.NewDiscardLogger(), cfg.MinPinners, t.Name(), cfg.SleepBetweenScans, skydcm)

	chunk := 10 * modules.SectorSizeStandard
	oneChunkTime := time.Duration((1*chunk*fanoutRedundancy+(baseSectorRedundancy-1)*modules.SectorSize)/assumedUploadSpeedInBytes) * time.Second
	twoChunkTime := time.Duration((2*chunk*fanoutRedundancy+(baseSectorRedundancy-1)*modules.SectorSize)/assumedUploadSpeedInBytes) * time.Second

	tests := map[string]struct {
		size    uint64
		err     error
		expTime time.Duration
	}{
		"error": {
			err:     errors.New("error while fetching metadata"),
			expTime: SleepBetweenPins, // 1ms
		},
		"zero": {
			size:    0,            // defaults to one chunk
			expTime: oneChunkTime, // 3s
		},
		"one": {
			size:    1024,         // rounds up to one chunk
			expTime: oneChunkTime, // 3s
		},
		"two": {
			size:    1024 + chunk, // rounds up to two chunks
			expTime: twoChunkTime, // 7s
		},
	}

	sl := test.RandomSkylink()
	for name, tt := range tests {
		// Set the size.
		skydcm.SetMetadata(sl.String(), skymodules.SkyfileMetadata{Length: tt.size}, tt.err)
		// Get the time.
		estTime := s.staticEstimateTimeToFull(sl)
		if estTime != tt.expTime {
			t.Errorf("Test '%s': expected %s, got %s", name, tt.expTime, estTime)
		}
	}
}

// testWaitUntilHealthy ensures that staticWaitUntilHealthy functions correctly.
func testWaitUntilHealthy(t *testing.T, db *database.DB, cfg conf.Config, skydcm *skyd.ClientMock) {
	s := NewScanner(db, test.NewDiscardLogger(), cfg.MinPinners, t.Name(), cfg.SleepBetweenScans, skydcm)

	sl := test.RandomSkylink()
	sp, err := sl.SiaPath()
	if err != nil {
		t.Fatal(err)
	}
	// Set health to "unhealthy".
	skydcm.SetHealth(sp, 0.99)

	// Wait for the file to become healthy.
	// Expect this to hit the deadline after 6s.
	t0 := lib.Now()
	s.staticWaitUntilHealthy(sl, sp)
	t1 := lib.Now()
	// Expect the time difference to be around 6s. Add 5ms tolerance.
	if t0.Add(6*time.Second + 5*time.Millisecond).Before(t1) {
		t.Fatalf("Expected to wait for 6s, waited for %d ms", t1.Sub(t0).Milliseconds())
	}

	// Try again. This time we'll mark the skylink as healthy after 100ms.
	go func() {
		time.Sleep(100 * time.Millisecond)
		skydcm.SetHealth(sp, 0)
	}()
	t0 = lib.Now()
	s.staticWaitUntilHealthy(sl, sp)
	t1 = lib.Now()
	// Expect the time difference to be around 100ms. Add 50ms tolerance.
	if t0.Add(150 * time.Millisecond).Before(t1) {
		t.Fatalf("Expected to wait for 100ms, waited for %d ms", t1.Sub(t0).Milliseconds())
	}

	// Set the metadata fetch to error out. Expect this to take ~2ms.
	skydcm.SetMetadata(sl.String(), skymodules.SkyfileMetadata{}, errors.New("metadata error"))
	t0 = lib.Now()
	s.staticWaitUntilHealthy(sl, sp)
	t1 = lib.Now()
	// Expect the time difference to be around 2ms. Add 2ms tolerance.
	if t0.Add(4 * time.Millisecond).Before(t1) {
		t.Fatalf("Expected to wait for 2ms, waited for %d ms", t1.Sub(t0).Milliseconds())
	}
}

// TestFindAndPinOneUnderpinnedSkylink ensures that
// managedFindAndPinOneUnderpinnedSkylink functions correctly.
func TestFindAndPinOneUnderpinnedSkylink(t *testing.T) {
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
	cfg, err := test.LoadTestConfig()
	if err != nil {
		t.Fatal(err)
	}
	skydcm := skyd.NewSkydClientMock()
	serverName := t.Name()
	s := NewScanner(db, test.NewDiscardLogger(), cfg.MinPinners, serverName, cfg.SleepBetweenScans, skydcm)

	sl := test.RandomSkylink()

	// Look for underpinned skylinks in the empty DB.
	_, _, _, err = s.managedFindAndPinOneUnderpinnedSkylink()
	if !database.IsNoSkylinksNeedPinning(err) {
		t.Fatalf("Expected '%v', got '%v'", database.ErrNoUnderpinnedSkylinks, err)
	}

	// Add an underpinned skylink.
	_, err = db.CreateSkylink(ctx, sl, serverName)
	if err != nil {
		t.Fatal(err)
	}
	err = db.RemoveServerFromSkylinks(ctx, []string{sl.String()}, serverName)
	if err != nil {
		t.Fatal(err)
	}
	// Ensure the skylink is not pinned by skyd.
	if skydcm.IsPinning(sl.String()) {
		t.Fatal("Expected the skylink to not be pinned, yet.")
	}
	sl1, _, _, err := s.managedFindAndPinOneUnderpinnedSkylink()
	if err != nil {
		t.Fatal(err)
	}
	if !sl1.Equals(sl) {
		t.Fatalf("Expected '%s', got '%s'", sl.String(), sl1.String())
	}
	// Check if it's pinned by skyd.
	if !skydcm.IsPinning(sl.String()) {
		t.Fatal("Expected the skylink to be pinned.")
	}
	// Check if that is reflected in the DB.
	sls, err := db.SkylinksForServer(ctx, serverName)
	if err != nil {
		t.Fatal(err)
	}
	if !test.Contains(sls, sl.String()) {
		t.Fatalf("Expected to find '%s' among the skylinks pinned by this server, got '%v'", sl.String(), sls)
	}

	// We'll unmark the skylink as pinned by this server and we'll get it pinned
	// again. We expect the skyd client to return an ErrSkylinkAlreadyPinned
	// and then the scanner to add the server as a pinner of the skylink.
	err = db.RemoveServerFromSkylinks(ctx, []string{sl.String()}, serverName)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, err = s.managedFindAndPinOneUnderpinnedSkylink()
	if err != nil {
		t.Fatal(err)
	}
	sls, err = db.SkylinksForServer(ctx, serverName)
	if err != nil {
		t.Fatal(err)
	}
	if !test.Contains(sls, sl.String()) {
		t.Fatalf("Expected to find '%s' among the skylinks pinned by this server, got '%v'", sl.String(), sls)
	}

	// Make sure we handle blocked skylinks correctly.
	blockedSl := test.RandomSkylink()
	_, err = db.CreateSkylink(ctx, blockedSl, serverName)
	if err != nil {
		t.Fatal(err)
	}
	// Make sure the skylink is in the DB.
	_, err = db.FindSkylink(ctx, blockedSl)
	if err != nil {
		t.Fatal(err)
	}
	err = db.RemoveServerFromSkylinks(ctx, []string{blockedSl.String()}, serverName)
	if err != nil {
		t.Fatal(err)
	}
	skydcm.SetPinError(skyd.ErrSkylinkIsBlocked)
	_, _, _, err = s.managedFindAndPinOneUnderpinnedSkylink()
	if err != nil {
		t.Fatal(err)
	}
	sls, err = db.SkylinksForServer(ctx, serverName)
	if err != nil {
		t.Fatal(err)
	}
	if test.Contains(sls, blockedSl.String()) {
		t.Fatalf("Expected to NOT find '%s' among the skylinks pinned by this server, got '%v'", blockedSl.String(), sls)
	}
	// Make sure the skylink is gone from the DB.
	_, err = db.FindSkylink(ctx, blockedSl)
	if !errors.Contains(err, database.ErrSkylinkNotExist) {
		t.Fatalf("Expected '%s', got '%v'", database.ErrSkylinkNotExist, err)
	}
}

// TestEligibleToPin makes sure that we can follow our eligibility rules:
// - always eligible if below the hard limit
// - always eligible if last
// - eligible if in the last X%
func TestEligibleToPin(t *testing.T) {
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
	cfg, err := test.LoadTestConfig()
	if err != nil {
		t.Fatal(err)
	}
	skydcm := skyd.NewSkydClientMock()
	s := NewScanner(db, test.NewDiscardLogger(), cfg.MinPinners, cfg.ServerName, cfg.SleepBetweenScans, skydcm)

	// Set the load levels for three other servers. The last one will be empty.
	err1 := s.staticDB.SetServerLoad(ctx, "server1", 30*int64(AlwaysPinThreshold))
	err2 := s.staticDB.SetServerLoad(ctx, "server2", 20*int64(AlwaysPinThreshold))
	err3 := s.staticDB.SetServerLoad(ctx, "server3", 0)
	if err = errors.Compose(err1, err2, err3); err != nil {
		t.Fatal(err)
	}

	// Check eligibility for a server that's not in the database.
	// Expect an error.
	_, err = s.staticEligibleToPin(ctx)
	if !errors.Contains(err, database.ErrServerLoadNotFound) {
		t.Fatalf("Expected '%v', got '%v'", database.ErrServerLoadNotFound, err)
	}
	// Set the server load level to a low level but not last.
	// Bottom 50% but not bottom 30%. Still, below the hard limit, so we expect
	// to be eligible.
	err = s.staticDB.SetServerLoad(ctx, cfg.ServerName, int64(AlwaysPinThreshold)/2)
	if err != nil {
		t.Fatal(err)
	}
	eligible, err := s.staticEligibleToPin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !eligible {
		t.Fatal("Expected to be eligible, wasn't.")
	}
	// Set the load level above the hard limit and above 30%.
	// Expect not eligible.
	err = s.staticDB.SetServerLoad(ctx, cfg.ServerName, 3*int64(AlwaysPinThreshold))
	if err != nil {
		t.Fatal(err)
	}
	eligible, err = s.staticEligibleToPin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if eligible {
		t.Fatal("Expected to not be eligible, was.")
	}
	// Bump the load level of server3, so out current server is left last.
	err = s.staticDB.SetServerLoad(ctx, "server3", 5*int64(AlwaysPinThreshold))
	if err != nil {
		t.Fatal(err)
	}
	eligible, err = s.staticEligibleToPin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !eligible {
		t.Fatal("Expected to be eligible, wasn't.")
	}
	// Add one more server above our server, so we're not last but we're in the
	// bottom 30%
	err = s.staticDB.SetServerLoad(ctx, "server4", 6*int64(AlwaysPinThreshold))
	if err != nil {
		t.Fatal(err)
	}
	eligible, err = s.staticEligibleToPin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !eligible {
		t.Fatal("Expected to be eligible, wasn't.")
	}
}

// TestScannerObeysLimit ensures that the scanner won't try to pin underpinned
// skylinks once the server is not eligible.
func TestScannerObeysLimit(t *testing.T) {
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
	cfg, err := test.LoadTestConfig()
	if err != nil {
		t.Fatal(err)
	}
	skydcm := skyd.NewSkydClientMock()
	s := NewScanner(db, test.NewDiscardLogger(), cfg.MinPinners, cfg.ServerName, cfg.SleepBetweenScans, skydcm)
	err = s.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if e := s.Close(); e != nil {
			t.Error(errors.AddContext(e, "failed to close threadgroup"))
		}
	}()

	// TEST: Eligible, expect to pin.
	// We have just one server, so we're eligible.
	err = db.SetServerLoad(ctx, cfg.ServerName, int64(AlwaysPinThreshold)/2)
	if err != nil {
		t.Fatal(err)
	}
	// Add an underpinned skylink.
	sl1, err := addUnderpinned(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	// Wait for the scanner to run and make sure we've pinned this skylink.
	err = build.Retry(cyclesToWait, sleepBetweenScans, func() error {
		// Make sure the skylink is pinned on the local (mock) skyd.
		if !skydcm.IsPinning(sl1.String()) {
			return errors.New("we expected skyd to be pinning this")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// TEST: Not eligible, expect not to pin.
	// Add another server with zero load. Set our load to be above the hard
	// limit. This will put us in the top 50%, so we should not pin.
	// Set the load levels for four other servers. The last one will be empty.
	err = s.staticDB.SetServerLoad(ctx, "server4", 0)
	if err != nil {
		t.Fatal(err)
	}
	err = db.SetServerLoad(ctx, cfg.ServerName, 2*int64(AlwaysPinThreshold))
	if err != nil {
		t.Fatal(err)
	}
	// Add an underpinned skylink.
	sl2, err := addUnderpinned(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	// Wait for the scanner to run and make sure we are not pinning this skylink.
	expErr := errors.New("expected error")
	err = build.Retry(cyclesToWait, sleepBetweenScans, func() error {
		if !skydcm.IsPinning(sl2.String()) {
			return expErr
		}
		return nil
	})
	if err != expErr {
		t.Fatalf("Expected '%v', got '%v'", expErr, err)
	}
}

// addUnderpinned is a helper that adds a random skylink to the DB and then
// removes its pinner, so it becomes underpinned.
func addUnderpinned(ctx context.Context, db *database.DB) (skymodules.Skylink, error) {
	sl := test.RandomSkylink()
	randSrv := hex.EncodeToString(fastrand.Bytes(16))
	_, err := db.CreateSkylink(ctx, sl, randSrv)
	if err != nil {
		return skymodules.Skylink{}, err
	}
	// Make it underpinned.
	err = db.RemoveServerFromSkylinks(ctx, []string{sl.String()}, randSrv)
	if err != nil {
		return skymodules.Skylink{}, err
	}
	return sl, nil
}
