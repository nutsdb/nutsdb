// Copyright 2026 The nutsdb Author. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nutsdb

import "github.com/nutsdb/nutsdb/internal/logger"

// Logger is the pluggable logging sink used by nutsdb internals.
type Logger = logger.Logger

// Level is a log severity.
type Level = logger.Level

const (
	LevelDebug = logger.LevelDebug
	LevelInfo  = logger.LevelInfo
	LevelWarn  = logger.LevelWarn
	LevelError = logger.LevelError
)

// SetLogger installs the process-wide logger used by nutsdb internals.
// Passing nil installs a no-op logger.
func SetLogger(l Logger) {
	logger.SetLogger(l)
}

// GetLogger returns the current process-wide logger.
func GetLogger() Logger {
	return logger.GetLogger()
}

// SetLevel sets the minimum log level for package-level logging helpers.
func SetLevel(min Level) {
	logger.SetLevel(min)
}

// GetLevel returns the current minimum log level.
func GetLevel() Level {
	return logger.GetLevel()
}

// Default returns a Logger backed by the standard library log.Default().
func Default() Logger {
	return logger.Default()
}

// Nop returns a Logger that discards all messages.
func Nop() Logger {
	return logger.Nop()
}

// PrintfAdapter wraps a Printf-style logger (e.g. *log.Logger) as a Logger.
func PrintfAdapter(l PrintfLogger) Logger {
	return logger.PrintfAdapter(l)
}

// PrintfLogger is anything that implements Printf (e.g. *log.Logger).
type PrintfLogger = logger.PrintfLogger

