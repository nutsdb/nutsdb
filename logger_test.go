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

import (
	"bytes"
	"log"
	"testing"
)

func TestRootLoggerAPI(t *testing.T) {
	prev := GetLogger()
	prevLevel := GetLevel()
	t.Cleanup(func() {
		SetLogger(prev)
		SetLevel(prevLevel)
	})

	SetLogger(Default())
	if GetLogger() == nil {
		t.Fatal("Default logger is nil")
	}
	SetLevel(LevelWarn)
	if GetLevel() != LevelWarn {
		t.Fatalf("GetLevel=%v", GetLevel())
	}

	SetLogger(Nop())
	GetLogger().Infof("discarded")

	SetLogger(nil)
	if GetLogger() == nil {
		t.Fatal("nil SetLogger must install Nop")
	}

	var buf bytes.Buffer
	SetLogger(PrintfAdapter(log.New(&buf, "", 0)))
	GetLogger().Warnf("warn-%d", 7)
	if !bytes.Contains(buf.Bytes(), []byte("warn-7")) {
		t.Fatalf("adapter output=%q", buf.String())
	}
}
