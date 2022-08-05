package lib

import "time"

// Now returns the current time in UTC, truncated to milliseconds.
func Now() time.Time {
	return time.Now().UTC().Truncate(time.Millisecond)
}
