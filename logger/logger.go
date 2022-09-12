package logger

import (
	"gitlab.com/SkynetLabs/skyd/build"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	"gitlab.com/NebulousLabs/errors"
)

var (
	errInvalidLogFileName = errors.New("invalid log file name")
	// logFileDir defines the directory where all logs are stored.
	logFileDir = build.Select(build.Var{
		Standard: "/logs",
		Dev:      "./logs",
		Testing:  "./logs",
	}).(string)
)

type (
	// Logger defines the logger interface we need.
	//
	// It is identical to logrus.Ext1FieldLogger but we are not using that
	// because it's marked as "Do not use". Instead, we're defining our own in
	// order to be sure that potential Logrus changes won't break us.
	Logger interface {
		logrus.FieldLogger
		Tracef(format string, args ...interface{})
		Trace(args ...interface{})
		Traceln(args ...interface{})
	}

	// SkyLogger is a wrapper of *logrus.Logger which allows logging to a file
	// on  disk.
	SkyLogger struct {
		*logrus.Logger
		logFile *os.File
	}
)

// New creates a new SkyLogger that can optionally write to disk.
//
// If the given logfile argument is an empty string, the SkyLogger will not
// write to disk.
func New(level logrus.Level, logfile string) (logger *SkyLogger, err error) {
	logger = &SkyLogger{
		logrus.New(),
		nil,
	}
	logger.SetLevel(level)
	// Open and start writing to the log file, unless we have an empty string,
	// which signifies "don't log to disk".
	if logfile != "" {
		normalizedLogfile, err := normalizeLogFileName(logfile)
		if err != nil {
			return nil, err
		}
		err = os.MkdirAll(filepath.Dir(normalizedLogfile), 0755)
		if err != nil {
			return nil, errors.AddContext(err, "failed to create path to log file")
		}
		logger.logFile, err = os.OpenFile(normalizedLogfile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			return nil, errors.AddContext(err, "failed to open log file")
		}

		logger.SetOutput(io.MultiWriter(os.Stdout, logger.logFile))
	}
	return logger, nil
}

// Close gracefully closes all resources used by SkyLogger.
func (l *SkyLogger) Close() error {
	if l.logFile == nil {
		return nil
	}
	return l.logFile.Close()
}

// normalizeLogFileName ensures that we have a valid log file name.
func normalizeLogFileName(logfile string) (string, error) {
	if strings.HasPrefix(logfile, "..") {
		return "", errInvalidLogFileName
	}
	if !strings.HasPrefix(logfile, "/") {
		return logFileDir + "/" + logfile, nil
	}
	return logFileDir + logfile, nil
}
