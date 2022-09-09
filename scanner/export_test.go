package scanner

import (
	"context"
	"time"

	"gitlab.com/SkynetLabs/skyd/skymodules"
)

/**
The only purpose of this file is to make certain unexported methods available
for testing. The file is only used during testing and does not add its exported
methods to the final production binary.

For this approach to work, the test which uses the methods and consts exported
here needs to be located in the same directory. It can, however, have a
different package name. This allows us to avoid certain cyclical imports.
*/

// These constants are testing proxies for the unexported constants in the main
// package.
const (
	AssumedUploadSpeedInBytes = assumedUploadSpeedInBytes
	BaseSectorRedundancy      = baseSectorRedundancy
	FanoutRedundancy          = fanoutRedundancy
)

var (
	// SleepBetweenScans is a testing proxy for sleepBetweenScans
	SleepBetweenScans = sleepBetweenScans
)

// ManagedEligibleToPin is a helper method that exports managedEligibleToPin
// for testing purposes and only in testing context.
func (s *Scanner) ManagedEligibleToPin(ctx context.Context) (bool, error) {
	return s.managedEligibleToPin(ctx)
}

// ManagedFindAndPinOneUnderpinnedSkylink is a helper method that exports
// managedFindAndPinOneUnderpinnedSkylink for testing purposes and only in
// testing context.
func (s *Scanner) ManagedFindAndPinOneUnderpinnedSkylink() (skylink skymodules.Skylink, sp skymodules.SiaPath, continueScanning bool, err error) {
	return s.managedFindAndPinOneUnderpinnedSkylink()
}

// ManagedEligibleToPin is a helper method that exports managedEligibleToPin
// for testing purposes and only in testing context.
func (s *Scanner) StaticEstimateTimeToFull(sl skymodules.Skylink) time.Duration {
	return s.staticEstimateTimeToFull(sl)
}

// StaticSleepForOrUntilStopped is a helper method that exports
// staticSleepForOrUntilStopped for testing purposes and only in testing context.
func (s *Scanner) StaticSleepForOrUntilStopped(d time.Duration) bool {
	return s.staticSleepForOrUntilStopped(d)
}

// StaticWaitUntilHealthy is a helper method that exports staticWaitUntilHealthy
// for testing purposes and only in testing context.
func (s *Scanner) StaticWaitUntilHealthy(sl skymodules.Skylink, sp skymodules.SiaPath) {
	s.staticWaitUntilHealthy(sl, sp)
}
