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

package nutsdb_test

import (
	"fmt"

	"github.com/nutsdb/nutsdb"
)

type testLogger struct{}

func (l *testLogger) Printf(format string, args ...any) {
	fmt.Printf("testlogger:"+format+"\n", args...)
}

func ExampleILogger_Printf() {
	logger := &testLogger{}
	nutsdb.SetLogger(logger)

	nutsdb.GetLogger().Printf("test")
	nutsdb.GetLogger().Printf("test: %d", 1)
	nutsdb.GetLogger().Printf("test: %d, %s", 1, "test")
	// Output:
	// testlogger:test
	// testlogger:test: 1
	// testlogger:test: 1, test
}
