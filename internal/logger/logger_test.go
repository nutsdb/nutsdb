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

package logger

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
)

type captureLogger struct {
	mu   sync.Mutex
	msgs []string
}

func (c *captureLogger) append(level Level, format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, level.String()+": "+fmt.Sprintf(format, args...))
}

func (c *captureLogger) Debugf(format string, args ...any) {
	c.append(LevelDebug, format, args...)
}
func (c *captureLogger) Infof(format string, args ...any) {
	c.append(LevelInfo, format, args...)
}
func (c *captureLogger) Warnf(format string, args ...any) {
	c.append(LevelWarn, format, args...)
}
func (c *captureLogger) Errorf(format string, args ...any) {
	c.append(LevelError, format, args...)
}

func (c *captureLogger) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.msgs)
}

func TestSetLoggerNilIsNop(t *testing.T) {
	prev := GetLogger()
	prevLevel := GetLevel()
	t.Cleanup(func() {
		SetLogger(prev)
		SetLevel(prevLevel)
	})

	SetLogger(nil)
	if GetLogger() == nil {
		t.Fatal("GetLogger() must not return nil after SetLogger(nil)")
	}
	// must not panic
	GetLogger().Infof("hello")
	Infof("hello")
}

func TestLevelFiltering(t *testing.T) {
	prev := GetLogger()
	prevLevel := GetLevel()
	t.Cleanup(func() {
		SetLogger(prev)
		SetLevel(prevLevel)
	})

	cap := &captureLogger{}
	SetLogger(cap)
	SetLevel(LevelWarn)

	Debugf("d")
	Infof("i")
	Warnf("w")
	Errorf("e")

	if cap.len() != 2 {
		t.Fatalf("want 2 messages (warn+error), got %d: %v", cap.len(), cap.msgs)
	}
	if !strings.Contains(cap.msgs[0], "WARN") || !strings.Contains(cap.msgs[0], "w") {
		t.Fatalf("first msg = %q", cap.msgs[0])
	}
	if !strings.Contains(cap.msgs[1], "ERROR") || !strings.Contains(cap.msgs[1], "e") {
		t.Fatalf("second msg = %q", cap.msgs[1])
	}
}

func TestPrintfAdapter(t *testing.T) {
	var buf bytes.Buffer
	std := log.New(&buf, "", 0)
	l := PrintfAdapter(std)
	l.Infof("hi %d", 1)
	got := buf.String()
	if !strings.Contains(got, "INFO") || !strings.Contains(got, "hi 1") {
		t.Fatalf("got %q", got)
	}
}

func TestConcurrentSetAndLog(t *testing.T) {
	prev := GetLogger()
	prevLevel := GetLevel()
	t.Cleanup(func() {
		SetLogger(prev)
		SetLevel(prevLevel)
	})

	SetLevel(LevelDebug)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if j%10 == 0 {
					SetLogger(&captureLogger{})
					SetLevel(LevelInfo)
				}
				Infof("n=%d j=%d", i, j)
				Debugf("dbg")
			}
		}(i)
	}
	wg.Wait()
}

func TestLevelString(t *testing.T) {
	cases := map[Level]string{
		LevelDebug: "DEBUG",
		LevelInfo:  "INFO",
		LevelWarn:  "WARN",
		LevelError: "ERROR",
		Level(99):  "LEVEL(99)",
	}
	for lv, want := range cases {
		if got := lv.String(); got != want {
			t.Fatalf("Level(%d).String()=%q want %q", lv, got, want)
		}
	}
}

func TestNop(t *testing.T) {
	Nop().Debugf("d")
	Nop().Infof("i")
	Nop().Warnf("w")
	Nop().Errorf("should discard")
}
