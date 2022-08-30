package scanner

import (
	"github.com/skynetlabs/pinner/lib"
	"github.com/skynetlabs/pinner/logger"
	"sync"
	"time"
)

type (
	// Status represents the status of a sweep.
	// All times are UTC-based in order to simplify handling and comparison.
	Status struct {
		InProgress bool
		Error      error
		StartTime  time.Time
		EndTime    time.Time
		NumPinned  int
		NumFailed  int

		failed []string
	}
	// status is the internal status type that allows thread-safe updates.
	status struct {
		mu           sync.Mutex
		staticLogger logger.Logger
		status       Status
	}
)

// Start marks the start of a new process, unless one is already in progress.
// If there is a process in progress then Start returns without any action.
func (st *status) Start() {
	st.mu.Lock()
	// Double-check for parallel sweeps.
	if st.status.InProgress {
		st.mu.Unlock()
		st.staticLogger.Debug("Attempted to start a sweep while another one was already ongoing.")
		return
	}
	// Initialise the status to "a sweep is running".
	st.status.InProgress = true
	st.status.Error = nil
	st.status.StartTime = lib.Now()
	st.status.EndTime = time.Time{}
	st.mu.Unlock()
	st.staticLogger.Info("Started a sweep.")
}

// Status returns a copy of the current status.
func (st *status) Status() Status {
	st.mu.Lock()
	s := st.status
	st.mu.Unlock()
	return s
}

// MarkSuccess marks a successfully pinned skylink.
func (st *status) MarkSuccess() {
	st.mu.Lock()
	st.status.NumPinned++
	st.mu.Unlock()
}

// MarkFailure marks a successfully pinned skylink.
func (st *status) MarkFailure(s string) {
	st.mu.Lock()
	st.status.NumFailed++
	st.status.failed = append(st.status.failed, s)
	st.mu.Unlock()
}

// Failed returns a list of all failed
func (st *status) Failed() []string {
	st.mu.Lock()
	failed := st.status.failed
	st.mu.Unlock()
	return failed
}

// Finalize marks a run as completed with the given error.
func (st *status) Finalize(err error) {
	st.mu.Lock()
	if !st.status.InProgress {
		st.mu.Unlock()
		return
	}
	st.status.InProgress = false
	st.status.EndTime = lib.Now()
	st.status.NumPinned = 0
	st.status.NumFailed = 0
	st.status.failed = make([]string, 0)
	st.status.Error = err
	st.mu.Unlock()
	st.staticLogger.Info("Finalized a sweep.")
}
