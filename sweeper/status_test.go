package sweeper

import (
	"github.com/sirupsen/logrus"
	"github.com/skynetlabs/pinner/lib"
	"gitlab.com/NebulousLabs/errors"
	"io/ioutil"
	"testing"
	"time"
)

// TestStatus ensures the basic operation of the status type.
func TestStatus(t *testing.T) {
	logger := logrus.New()
	logger.Out = ioutil.Discard

	s := &status{staticLogger: logger}
	sentinelErr := errors.New("this should not get set")

	// isEmpty is a helper that returns true when the given status has its zero
	// value.
	isEmpty := func(st Status) bool {
		return !(st.InProgress || st.Error != nil || (st.EndTime != time.Time{}) || (st.StartTime != time.Time{}))
	}

	// Check the status, expect not in progress.
	st := s.Status()
	if !isEmpty(st) {
		t.Fatalf("Status not empty: %+v", st)
	}
	// Try to finalize before starting, expect nothing to happen.
	s.Finalize(sentinelErr)
	st = s.Status()
	if !isEmpty(st) {
		t.Fatalf("Status not empty: %+v", st)
	}
	// Start and verify.
	s.Start()
	st = s.Status()
	if !st.InProgress || st.StartTime.After(lib.Now()) {
		t.Fatalf("Unexpected status: %+v", st)
	}
	// Store the start time and verify that attempting to start again will not
	// change it.
	startTime := st.StartTime
	s.Start()
	st = s.Status()
	if st.StartTime != startTime {
		t.Fatalf("Expected start time '%s', got '%s'", startTime, st.StartTime)
	}
	// Finalize and verify.
	s.Finalize(sentinelErr)
	st = s.Status()
	if st.InProgress || !errors.Contains(st.Error, sentinelErr) || (st.EndTime == time.Time{}) {
		t.Fatalf("Unexpected status: %+v", st)
	}
	// Save end time and verify that finalising again has no effect.
	endTime := st.EndTime
	s.Finalize(nil)
	st = s.Status()
	if st.EndTime != endTime || !errors.Contains(st.Error, sentinelErr) {
		t.Fatalf("Unexpected status: %+v", st)
	}
	// Start the same status again.
	s.Start()
	st = s.Status()
	if !st.InProgress || st.Error != nil {
		t.Fatalf("Unexpected status: %+v", st)
	}
}
