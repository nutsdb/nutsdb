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

// Package logger provides a pluggable, leveled logger for nutsdb internals.
// Users configure it via the root nutsdb package (SetLogger / SetLevel);
// internal packages call Debugf / Infof / Warnf / Errorf.
package logger

import (
	"fmt"
	"log"
	"sync/atomic"
)

// Level is a log severity.
type Level int32

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	default:
		return fmt.Sprintf("LEVEL(%d)", int(l))
	}
}

// Logger is the pluggable logging sink.
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Debugf(string, ...any) {}
func (nopLogger) Infof(string, ...any)  {}
func (nopLogger) Warnf(string, ...any)  {}
func (nopLogger) Errorf(string, ...any) {}

// Nop returns a logger that discards all messages.
func Nop() Logger { return nopLogger{} }

type stdLogger struct {
	l *log.Logger
}

func (s *stdLogger) Debugf(format string, args ...any) {
	s.l.Printf(LevelDebug.String()+": "+format, args...)
}
func (s *stdLogger) Infof(format string, args ...any) {
	s.l.Printf(LevelInfo.String()+": "+format, args...)
}
func (s *stdLogger) Warnf(format string, args ...any) {
	s.l.Printf(LevelWarn.String()+": "+format, args...)
}
func (s *stdLogger) Errorf(format string, args ...any) {
	s.l.Printf(LevelError.String()+": "+format, args...)
}

// Default returns a Logger backed by log.Default().
func Default() Logger {
	return &stdLogger{l: log.Default()}
}

// PrintfLogger is anything that implements Printf (e.g. *log.Logger).
type PrintfLogger interface {
	Printf(format string, args ...any)
}

type printfAdapter struct {
	l PrintfLogger
}

func (a *printfAdapter) Debugf(format string, args ...any) {
	a.l.Printf(LevelDebug.String()+": "+format, args...)
}
func (a *printfAdapter) Infof(format string, args ...any) {
	a.l.Printf(LevelInfo.String()+": "+format, args...)
}
func (a *printfAdapter) Warnf(format string, args ...any) {
	a.l.Printf(LevelWarn.String()+": "+format, args...)
}
func (a *printfAdapter) Errorf(format string, args ...any) {
	a.l.Printf(LevelError.String()+": "+format, args...)
}

// PrintfAdapter wraps a Printf-style logger as a Logger.
func PrintfAdapter(l PrintfLogger) Logger {
	if l == nil {
		return Nop()
	}
	return &printfAdapter{l: l}
}

var (
	globalLogger atomic.Value // *loggerBox
	globalLevel  atomic.Int32
)

type loggerBox struct {
	l Logger
}

func init() {
	globalLogger.Store(&loggerBox{l: Default()})
	globalLevel.Store(int32(LevelInfo))
}

// SetLogger installs the process-wide logger. nil becomes Nop().
func SetLogger(l Logger) {
	if l == nil {
		l = Nop()
	}
	globalLogger.Store(&loggerBox{l: l})
}

// GetLogger returns the current process-wide logger.
func GetLogger() Logger {
	return globalLogger.Load().(*loggerBox).l
}

// SetLevel sets the minimum level that package-level helpers will emit.
func SetLevel(min Level) {
	globalLevel.Store(int32(min))
}

// GetLevel returns the current minimum log level.
func GetLevel() Level {
	return Level(globalLevel.Load())
}

// Debugf logs at LevelDebug if enabled by SetLevel.
func Debugf(format string, args ...any) {
	if LevelDebug < GetLevel() {
		return
	}
	GetLogger().Debugf(format, args...)
}

// Infof logs at LevelInfo if enabled by SetLevel.
func Infof(format string, args ...any) {
	if LevelInfo < GetLevel() {
		return
	}
	GetLogger().Infof(format, args...)
}

// Warnf logs at LevelWarn if enabled by SetLevel.
func Warnf(format string, args ...any) {
	if LevelWarn < GetLevel() {
		return
	}
	GetLogger().Warnf(format, args...)
}

// Errorf logs at LevelError if enabled by SetLevel.
func Errorf(format string, args ...any) {
	if LevelError < GetLevel() {
		return
	}
	GetLogger().Errorf(format, args...)
}
