package logger

import (
	"os"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
)

// TestNewLogger ensures the log file is created where it should be.
func TestNewLogger(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	dir := t.TempDir()

	// Initialise the logger with an unwritable log file.
	unwritableDir := dir + "/unwritable"
	// Make the dir unwritable.
	err := os.Mkdir(unwritableDir, 0400)
	if err != nil {
		t.Fatal(err)
	}
	unwritableLog := unwritableDir + "/pinner.log"
	_, err = New(logrus.TraceLevel, unwritableLog)
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("Expected 'permission denied', got '%s'", err)
	}

	// Initialise the logger with a writable log file.
	writableLog := dir + "/pinner.log"
	_, err = New(logrus.TraceLevel, writableLog)
	if err != nil {
		t.Fatal(err)
	}
	// Ensure the log file is created.
	f, err := os.Open(writableLog)
	if err != nil {
		t.Fatal(err)
	}
	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}
}

// TestNormalizeLogFileName tests normalizeLogFileName
func TestNormalizeLogFileName(t *testing.T) {
	tests := []struct {
		name string
		in   string
		out  string
		err  error
	}{
		{name: "simple", in: "pinner.log", out: "/logs/pinner.log", err: nil},
		{name: "with dir with slash", in: "/mylogs/pinner.log", out: "/logs/mylogs/pinner.log", err: nil},
		{name: "with dir no slash", in: "mylogs/pinner.log", out: "/logs/mylogs/pinner.log", err: nil},
		{name: "with leading space", in: " pinner.log", out: "/logs/ pinner.log", err: nil},
		{name: "with traversal", in: "../pinner.log", out: "", err: errInvalidLogFileName},
	}

	for _, tt := range tests {
		out, err := normalizeLogFileName(tt.in)
		if err != tt.err {
			t.Errorf("Expected error '%v', got '%v'", tt.err, err)
		}
		if out != tt.out {
			t.Errorf("Expected '%v', got '%v'", tt.out, out)
		}
	}
}
