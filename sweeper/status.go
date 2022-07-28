package sweeper

import (
	"github.com/skynetlabs/pinner/logger"
	"sync"
	"time"
)

type (
	// Status represents the status of a sweep.
	Status struct {
		InProgress bool
		Error      error
		StartTime  time.Time
		EndTime    time.Time
	}
	// status is the internal status type that allows thread-safe updates.
	status struct {
		mu           sync.Mutex
		staticLogger logger.ExtFieldLogger
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
	st.status.StartTime = time.Now().UTC()
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

// Finalize marks a run as completed with the given error.
func (st *status) Finalize(err error) {
	st.mu.Lock()
	if !st.status.InProgress {
		st.mu.Unlock()
		return
	}
	st.status.InProgress = false
	st.status.EndTime = time.Now().UTC()
	st.status.Error = err
	st.mu.Unlock()
	st.staticLogger.Info("Finalized a sweep.")
}
