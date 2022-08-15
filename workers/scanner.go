package workers

import (
	"context"
	"fmt"
	"github.com/skynetlabs/pinner/lib"
	"strings"
	"sync"
	"time"

	"github.com/skynetlabs/pinner/conf"
	"github.com/skynetlabs/pinner/database"
	"github.com/skynetlabs/pinner/logger"
	"github.com/skynetlabs/pinner/skyd"
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/fastrand"
	"gitlab.com/NebulousLabs/threadgroup"
	"gitlab.com/SkynetLabs/skyd/build"
	"gitlab.com/SkynetLabs/skyd/skymodules"
	"go.sia.tech/siad/modules"
)

/**
 PHASE 1: <DONE>
 - scan the DB once a day for underpinned files
 - when you find an underpinned file that's not currently locked
	- lock it
	- pin it locally and add the current server to its list
	- unlock it

 PHASE 2: <DONE>
 - calculate server load by getting the total size of files pinned by each server
 - only pin underpinned files if the current server is in the lowest 30% of servers, otherwise exit before scanning further
 - always pin if this server is lowest in the list (covers setups with up to 3 servers)

 PHASE 3: <TODO>
 - add a second scanner which looks for skylinks which should be unpinned and unpins them from the local skyd.
*/

// Handy constants used to improve readability.
const (
	assumedUploadSpeedInBytes = 1 << 30 / 4 / 8 // 25% of 1Gbps in bytes
	baseSectorRedundancy      = 10
	fanoutRedundancy          = 3
)

var (
	// AlwaysPinThreshold sets a limit on the contract data of the server. If
	// the server is below that limit, it will repin underpinned files even if it
	// is not in the bottom X% in the cluster.
	AlwaysPinThreshold = build.Select(
		build.Var{
			Standard: 50 * database.TiB,
			Dev:      1 * database.MiB,
			Testing:  1 * database.MiB,
		}).(int)
	// PinningRangeThresholdPercent defines the cutoff line in the list of
	// servers, ordered by how much data they are pinning, below which a server
	// will pin underpinned skylinks.
	PinningRangeThresholdPercent = 0.30
	// SleepBetweenPins defines how long we'll sleep between pinning files.
	// We want to add this sleep in order to prevent a single server from
	// grabbing all underpinned files and overloading itself. We also want to
	// allow for some time for the newly pinned files to reach full redundancy
	// before we pin more files.
	SleepBetweenPins = build.Select(
		build.Var{
			Standard: 10 * time.Second,
			Dev:      time.Second,
			Testing:  time.Millisecond,
		}).(time.Duration)
	// SleepBetweenHealthChecks defines the wait time between calls to skyd to
	// check the current health of a given file.
	SleepBetweenHealthChecks = build.Select(
		build.Var{
			Standard: 5 * time.Second,
			Dev:      time.Second,
			Testing:  time.Millisecond,
		}).(time.Duration)

	// printPinningStatisticsPeriod defines how often we print intermediate
	// statistics while pinning underpinned files.
	printPinningStatisticsPeriod = build.Select(build.Var{
		Standard: 10 * time.Minute,
		Dev:      10 * time.Second,
		Testing:  500 * time.Millisecond,
	}).(time.Duration)

	// sleepBetweenScans defines how often we'll scan the DB for underpinned
	// skylinks.
	// Needs to be at least twice as long as conf.SleepBetweenChecksForScan.
	sleepBetweenScans = build.Select(build.Var{
		// In production we want to use a prime number of hours, so we can
		// de-sync the scan and the sweeps.
		Standard: 19 * time.Hour,
		Dev:      2 * conf.SleepBetweenChecksForScan,
		Testing:  2 * conf.SleepBetweenChecksForScan,
	}).(time.Duration)
	// sleepVariationFactor defines how much the sleep between scans will
	// vary between executions. It represents percent.
	sleepVariationFactor = 0.1
)

type (
	// Scanner is a background worker that periodically scans the database for
	// underpinned skylinks. Once an underpinned skylink is found (and it's not
	// being pinned by the local server already), Scanner pins it to the local
	// skyd.
	Scanner struct {
		staticDB                *database.DB
		staticLogger            logger.Logger
		staticServerName        string
		staticSkydClient        skyd.Client
		staticSleepBetweenScans time.Duration
		staticTG                *threadgroup.ThreadGroup

		dryRun     bool
		minPinners int
		// skipSkylinks is a list of skylinks which we want to skip during this
		// scan. These might be skylinks which errored out or blocked skylinks.
		skipSkylinks []string
		mu           sync.Mutex
	}
)

// NewScanner creates a new Scanner instance.
func NewScanner(db *database.DB, logger logger.Logger, minPinners int, serverName string, customSleepBetweenScans time.Duration, skydClient skyd.Client) *Scanner {
	sleep := customSleepBetweenScans
	if sleep == 0 {
		sleep = sleepBetweenScans
	}
	return &Scanner{
		staticDB:                db,
		staticLogger:            logger,
		staticServerName:        serverName,
		staticSkydClient:        skydClient,
		staticSleepBetweenScans: sleep,
		staticTG:                &threadgroup.ThreadGroup{},

		minPinners: minPinners,
	}
}

// Close stops the background worker thread.
func (s *Scanner) Close() error {
	return s.staticTG.Stop()
}

// SleepBetweenScans defines how often we'll scan the DB for underpinned
// skylinks. The returned value varies by +/-sleepVariationFactor and it's
// centered on sleepBetweenScans.
func (s *Scanner) SleepBetweenScans() time.Duration {
	variation := int(float64(s.staticSleepBetweenScans) * sleepVariationFactor)
	upper := int(s.staticSleepBetweenScans) + variation
	lower := int(s.staticSleepBetweenScans) - variation
	rng := upper - lower
	return time.Duration(fastrand.Intn(rng) + lower)
}

// Start launches the background worker thread that scans the DB for underpinned
// skylinks.
func (s *Scanner) Start() error {
	err := s.staticTG.Add()
	if err != nil {
		return err
	}

	go s.threadedScanAndPin()

	return nil
}

// threadedScanAndPin defines the scanning operation of Scanner.
func (s *Scanner) threadedScanAndPin() {
	defer s.staticTG.Done()

	// Main execution loop, goes on forever while the service is running.
	for {
		ctx := context.Background()
		// Get the time of the next scan.
		t, err := conf.NextScan(ctx, s.staticDB, s.staticLogger)
		// On error, we'll sleep for half an hour and we'll try again.
		if err != nil {
			s.staticLogger.Debug(errors.AddContext(err, "failed to fetch next scan time"))
			stopped := s.staticSleepForOrUntilStopped(conf.SleepBetweenChecksForScan)
			if stopped {
				return
			}
			continue
		}
		// Schedule a new scan if the time has passed. Round to seconds.
		if t.Before(lib.Now().Truncate(time.Second)) {
			stopped := s.staticScheduleNextScan()
			if stopped {
				return
			}
			continue
		}
		// If there is more than half an hour until the next scan, we'll sleep
		// for half an hour and we'll check again. It's possible for the
		// schedule to change in the meantime.
		if t.After(lib.Now().Add(conf.SleepBetweenChecksForScan)) {
			stopped := s.staticSleepForOrUntilStopped(conf.SleepBetweenChecksForScan)
			if stopped {
				return
			}
			continue
		}
		// Sleep until the scan time and then perform a scan.
		time.Sleep(t.Sub(lib.Now()))

		// Check if this server is eligible to pin skylinks.

		eligible, err := s.staticEligibleToPin(ctx)
		if err != nil {
			s.staticLogger.Warnf("Failed to determine if server is eligible to repin underpinned skylinks. Skipping repin. Error:: %v", err)
			continue
		}
		if !eligible {
			s.staticLogger.Debug("Server not eligible to repin underpinned skylinks. Skipping repin.")
			continue
		}

		// Perform a scan:

		// Rebuild the cache and watch for service shutdown while doing that.
		res := s.staticSkydClient.RebuildCache()
		select {
		case <-s.staticTG.StopChan():
			return
		case <-res.ErrAvail:
			if res.ExternErr != nil {
				s.staticLogger.Warn(errors.AddContext(res.ExternErr, "failed to rebuild skyd client cache"))
			}
		}

		s.staticLogger.Tracef("Start scanning")
		s.managedRefreshDryRun()
		s.managedRefreshMinPinners()
		s.managedPinUnderpinnedSkylinks()
		s.staticLogger.Tracef("End scanning")

		// Schedule the next scan, unless already scheduled:
		stopped := s.staticScheduleNextScan()
		if stopped {
			return
		}
	}
}

// staticScheduleNextScan attempts to set the time of the next scan until either we
// succeed, another server succeeds, or Scanner's TG is stopped. Returns true
// when Scanner's TG is stopped.
func (s *Scanner) staticScheduleNextScan() bool {
	// Keep trying to set the time until you succeed or until some other server
	// succeeds.
	for {
		// Fetch the next scan time. Another server might have already set it.
		t, err := conf.NextScan(context.Background(), s.staticDB, s.staticLogger)
		if err != nil {
			s.staticLogger.Debug(errors.AddContext(err, "failed to fetch next scan time"))
			stopped := s.staticSleepForOrUntilStopped(conf.SleepBetweenChecksForScan)
			if stopped {
				return true
			}
			continue
		}
		// Schedule the next scan time if the scheduled time is in the past.
		if t.Before(lib.Now()) {
			// Set the next scan to be after sleepBetweenScans.
			err = conf.SetNextScan(context.Background(), s.staticDB, lib.Now().Add(sleepBetweenScans))
			if err != nil {
				// Log the error and sleep for half an hour. Meanwhile, another
				// server will finish its scan and will set the next scan time.
				s.staticLogger.Debug(errors.AddContext(err, "failed to set next scan time"))
				stopped := s.staticSleepForOrUntilStopped(conf.SleepBetweenChecksForScan)
				if stopped {
					return true
				}
				continue
			}
		}
		// It's set.
		return false
	}
}

// managedPinUnderpinnedSkylinks loops over all underpinned skylinks and pins
// them.
func (s *Scanner) managedPinUnderpinnedSkylinks() {
	s.staticLogger.Trace("Entering managedPinUnderpinnedSkylinks")
	defer s.staticLogger.Trace("Exiting  managedPinUnderpinnedSkylinks")

	// Clear out the skipped skylinks from the previous run.
	s.mu.Lock()
	s.skipSkylinks = []string{}
	s.mu.Unlock()

	intermediateStatsTicker := time.NewTicker(printPinningStatisticsPeriod)
	defer intermediateStatsTicker.Stop()
	countPinned := 0
	t0 := lib.Now()

	// Print final statistics when finishing the method.
	defer func() {
		t1 := lib.Now()
		s.staticLogger.Infof("Finished at %s, runtime %s, pinned skylinks %d", t1.Format(conf.TimeFormat), t1.Sub(t0).String(), countPinned)
		s.mu.Lock()
		skipped := s.skipSkylinks
		s.mu.Unlock()
		s.staticLogger.Tracef("Skipped %d skylinks: %v", len(skipped), skipped)
	}()

	for {
		// Check for service shutdown before talking to the DB.
		select {
		case <-s.staticTG.StopChan():
			s.staticLogger.Trace("Stop channel closed")
			return
		default:
		}

		// Print intermediate statistics.
		select {
		case <-intermediateStatsTicker.C:
			t1 := lib.Now()
			s.staticLogger.Infof("Time %s, runtime %s, pinned skylinks %d", t1.Format(conf.TimeFormat), t1.Sub(t0).String(), countPinned)
		default:
		}

		skylink, sp, continueScanning, err := s.managedFindAndPinOneUnderpinnedSkylink()
		if err == nil {
			countPinned++
		} else {
			s.staticLogger.Trace(err)
		}
		if !continueScanning {
			return
		}
		// We only check the error if we want to continue scanning. The error is
		// already logged and the only indication it gives us is whether we
		// should wait for the file we pinned to become healthy or not. If there
		// is an error, then there is nothing to wait for.
		if err == nil {
			// Block until the pinned skylink becomes healthy or until a timeout.
			s.staticWaitUntilHealthy(skylink, sp)
			continue
		}
		// In case of error we still want to sleep for a moment in order to
		// avoid a tight(ish) loop of errors when we either fail to pin or
		// fail to mark as pinned. Note that this only happens when we want
		// to continue scanning, otherwise we would have exited right after
		// managedFindAndPinOneUnderpinnedSkylink.
		stopped := s.staticSleepForOrUntilStopped(SleepBetweenPins)
		if stopped {
			return
		}
	}
}

// managedFindAndPinOneUnderpinnedSkylink scans the database for one skylinks which is
// either locked by the current server or underpinned. If it finds such a
// skylink, it pins it to the local skyd. The method returns true until it finds
// no further skylinks to process or until it encounters an unrecoverable error,
// such as bad credentials, dead skyd, etc.
func (s *Scanner) managedFindAndPinOneUnderpinnedSkylink() (skylink skymodules.Skylink, sp skymodules.SiaPath, continueScanning bool, err error) {
	s.staticLogger.Trace("Entering managedFindAndPinOneUnderpinnedSkylink")
	defer s.staticLogger.Trace("Exiting  managedFindAndPinOneUnderpinnedSkylink")

	s.mu.Lock()
	dryRun := s.dryRun
	minPinners := s.minPinners
	skipSkylinks := s.skipSkylinks
	s.mu.Unlock()

	ctx := context.TODO()

	sl, err := s.staticDB.FindAndLockUnderpinned(ctx, s.staticServerName, skipSkylinks, minPinners)
	if database.IsNoSkylinksNeedPinning(err) {
		return skymodules.Skylink{}, skymodules.SiaPath{}, false, err
	}
	if err != nil {
		s.staticLogger.Warn(errors.AddContext(err, "failed to fetch underpinned skylink"))
		return skymodules.Skylink{}, skymodules.SiaPath{}, false, err
	}
	// This is a flag we are going to raise if we delete the skylink from the DB
	// while processing it.
	var deleted bool
	defer func() {
		if deleted {
			return
		}
		err = s.staticDB.UnlockSkylink(ctx, sl, s.staticServerName)
		if err != nil {
			s.staticLogger.Debug(errors.AddContext(err, "failed to unlock skylink after trying to pin it"))
		}
	}()

	// Check for a dry run.
	if dryRun {
		s.staticLogger.Infof("[DRY RUN] Successfully pinned '%s'", sl)
		return skymodules.Skylink{}, skymodules.SiaPath{}, false, errors.New("dry run")
	}

	sp, err = s.staticSkydClient.Pin(sl.String())
	if errors.Contains(err, skyd.ErrSkylinkAlreadyPinned) {
		s.staticLogger.Info(err)
		// The skylink is already pinned locally but it's not marked as such.
		s.managedSkipSkylink(sl)
		err = s.staticDB.AddServerForSkylinks(ctx, []string{sl.String()}, s.staticServerName, false)
		if err != nil {
			s.staticLogger.Debug(errors.AddContext(err, "failed to mark as pinned by this server"))
		}
		return skymodules.Skylink{}, skymodules.SiaPath{}, true, err
	}
	if errors.Contains(err, skyd.ErrSkylinkIsBlocked) {
		s.staticLogger.Info(err)
		s.managedSkipSkylink(sl)
		// The skylink is blocked by skyd. We'll remove it from the database, so
		// no other server will try to repin it.
		err = s.staticDB.DeleteSkylink(ctx, sl)
		deleted = err == nil
		return skymodules.Skylink{}, skymodules.SiaPath{}, true, err
	}
	if err != nil && (strings.Contains(err.Error(), "API authentication failed.") || strings.Contains(err.Error(), "connect: connection refused")) {
		err = errors.AddContext(err, fmt.Sprintf("unrecoverable error while pinning '%s'", sl))
		s.staticLogger.Error(err)
		return skymodules.Skylink{}, skymodules.SiaPath{}, false, err
	}
	if err != nil {
		s.staticLogger.Warn(errors.AddContext(err, fmt.Sprintf("failed to pin '%s'", sl)))
		s.managedSkipSkylink(sl)
		// Since this is not an unrecoverable error, we'll signal the caller to
		// continue trying to pin other skylinks.
		return skymodules.Skylink{}, skymodules.SiaPath{}, true, err
	}
	s.staticLogger.Infof("Successfully pinned '%s'", sl)
	err = s.staticDB.AddServerForSkylinks(ctx, []string{sl.String()}, s.staticServerName, false)
	if err != nil {
		s.staticLogger.Debug(errors.AddContext(err, "failed to mark as pinned by this server"))
	}
	return sl, sp, true, nil
}

// managedSkipSkylink adds a skylink to the list of skipped skylinks.
func (s *Scanner) managedSkipSkylink(sl skymodules.Skylink) {
	s.mu.Lock()
	s.skipSkylinks = append(s.skipSkylinks, sl.String())
	s.mu.Unlock()
}

// staticEstimateTimeToFull calculates how long we should sleep after pinning
// the given skylink in order to give the renter time to fully upload it before
// we pin another one. It returns a ballpark value.
//
// This method makes some assumptions for simplicity:
// * assumes lazy pinning, meaning that none of the fanout is uploaded
// * all skyfiles are assumed to be large files (base sector + fanout) and the
//	metadata is assumed to fill up the base sector (to err on the safe side)
func (s *Scanner) staticEstimateTimeToFull(skylink skymodules.Skylink) time.Duration {
	meta, err := s.staticSkydClient.Metadata(skylink.String())
	if err != nil {
		err = errors.AddContext(err, "failed to get metadata for skylink")
		s.staticLogger.Error(err)
		return SleepBetweenPins
	}
	chunkSize := 10 * modules.SectorSizeStandard
	numChunks := meta.Length / chunkSize
	if meta.Length%chunkSize > 0 {
		numChunks++
	}
	// We always have at least the base sector.
	if numChunks == 0 {
		numChunks = 1
	}
	// remainingUpload is the amount of data we expect to need to upload until
	// the skyfile reaches full redundancy.
	remainingUpload := numChunks*chunkSize*fanoutRedundancy + (baseSectorRedundancy-1)*modules.SectorSize
	secondsRemaining := remainingUpload / assumedUploadSpeedInBytes
	return time.Duration(secondsRemaining) * time.Second
}

// managedRefreshDryRun makes sure the local value of dry_run matches the one
// in the database.
func (s *Scanner) managedRefreshDryRun() {
	dr, err := conf.DryRun(context.TODO(), s.staticDB)
	if err != nil {
		s.staticLogger.Warn(errors.AddContext(err, "failed to fetch the DB value for dry_run"))
		return
	}
	s.staticLogger.Tracef("Current dry_run value: %t", dr)
	s.mu.Lock()
	s.dryRun = dr
	s.mu.Unlock()
}

// managedRefreshMinPinners makes sure the local value of min pinners matches the one
// in the database.
func (s *Scanner) managedRefreshMinPinners() {
	mp, err := conf.MinPinners(context.TODO(), s.staticDB)
	if err != nil {
		s.staticLogger.Warn(errors.AddContext(err, "failed to fetch the DB value for min_pinners"))
		return
	}
	s.mu.Lock()
	s.minPinners = mp
	s.mu.Unlock()
}

// staticEligibleToPin returns true when this server is either pinning less than
// AlwaysPinThreshold data or it's in the lower PinningRangeThresholdPercent
// section of servers when ordered by contract data in descending order.
// A server is always eligible if it's last in the list.
func (s *Scanner) staticEligibleToPin(ctx context.Context) (bool, error) {
	pinnedData, err := s.staticDB.ServerLoad(ctx, s.staticServerName)
	if errors.Contains(err, database.ErrServerLoadNotFound) {
		// We don't know what the server's load is. Get that data.
		load, err := s.staticSkydClient.ContractData()
		if err != nil {
			return false, errors.AddContext(err, "failed to fetch server's load")
		}
		err = s.staticDB.SetServerLoad(ctx, s.staticServerName, int64(load))
		if err != nil {
			return false, errors.AddContext(err, "failed to set server's load")
		}
		pinnedData, err = s.staticDB.ServerLoad(ctx, s.staticServerName)
	}
	if err != nil {
		return false, err
	}
	// Below the hard limit.
	if pinnedData < int64(AlwaysPinThreshold) {
		return true, nil
	}
	pos, total, err := s.staticDB.ServerLoadPosition(ctx, s.staticServerName)
	if err != nil {
		return false, err
	}
	// Last in the list.
	if pos == total {
		return true, nil
	}
	// In the bottom PinningRangeThresholdPercent.
	posPercent := 1.0 - float64(pos)/float64(total)
	return posPercent < PinningRangeThresholdPercent, nil
}

// staticWaitUntilHealthy blocks until the given skylinks becomes fully healthy
// or a timeout occurs.
func (s *Scanner) staticWaitUntilHealthy(skylink skymodules.Skylink, sp skymodules.SiaPath) {
	deadlineTimer := s.staticDeadline(skylink)
	defer deadlineTimer.Stop()
	ticker := time.NewTicker(SleepBetweenHealthChecks)
	defer ticker.Stop()

	// Wait for the pinned file to become fully healthy.
LOOP:
	for {
		health, err := s.staticSkydClient.FileHealth(sp)
		if err != nil {
			err = errors.AddContext(err, "failed to get sia file's health")
			s.staticLogger.Error(err)
			break
		}
		// We use NeedsRepair instead of comparing the health to zero because
		// skyd might stop repairing the file before it reaches perfect health.
		if !skymodules.NeedsRepair(health) {
			break
		}
		select {
		case <-ticker.C:
			s.staticLogger.Debugf("Waiting for '%s' to become fully healthy. Current health: %.2f", skylink, health)
		case <-deadlineTimer.C:
			s.staticLogger.Warnf("Skylink '%s' failed to reach full health within the time limit.", skylink)
			break LOOP
		case <-s.staticTG.StopChan():
			return
		}
	}
}

// staticSleepForOrUntilStopped is a helper function that blocks for the given
// duration  or until the Scanner's thread group is stopped. Returns true if the
// TG was  stopped.
func (s *Scanner) staticSleepForOrUntilStopped(dur time.Duration) bool {
	select {
	case <-time.After(dur):
		return false
	case <-s.staticTG.StopChan():
		s.staticLogger.Trace("Stopping scanner")
		return true
	}
}

// staticDeadline calculates how much we are willing to wait for a skylink to be fully
// healthy before giving up. It's twice the expected time, as returned by
// staticEstimateTimeToFull.
func (s *Scanner) staticDeadline(skylink skymodules.Skylink) *time.Timer {
	return time.NewTimer(2 * s.staticEstimateTimeToFull(skylink))
}
