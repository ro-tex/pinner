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
		InProgress bool      `json:"inProgress"`
		StartTime  time.Time `json:"startTime"`
		EndTime    time.Time `json:"endTime"`
		NumPinned  int       `json:"numPinned"`
		NumFailed  int       `json:"numFailed"`
		Failed     []string  `json:"failed,omitempty"`
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
	// Double-check for parallel scans.
	if st.status.InProgress {
		st.mu.Unlock()
		st.staticLogger.Debug("Attempted to start a scan while another one was already ongoing.")
		return
	}
	// Initialise the status to "a scan is running".
	st.status.InProgress = true
	st.status.StartTime = lib.Now()
	st.status.EndTime = time.Time{}
	st.status.NumPinned = 0
	st.status.NumFailed = 0
	st.status.Failed = make([]string, 0)
	st.mu.Unlock()
	st.staticLogger.Info("Started a scan.")
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
	st.status.Failed = append(st.status.Failed, s)
	st.mu.Unlock()
}

// Finish marks a run as completed.
func (st *status) Finish() {
	st.mu.Lock()
	if !st.status.InProgress {
		st.mu.Unlock()
		return
	}
	st.status.InProgress = false
	st.status.EndTime = lib.Now()
	st.mu.Unlock()
	st.staticLogger.Info("Finalized a scan.")
}
