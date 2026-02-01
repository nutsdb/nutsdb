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

import "github.com/nutsdb/nutsdb/internal/utils"

type ILogger = utils.ILogger

// SetLogger Set the internal logger for nutsdb.
func SetLogger(logger ILogger) {
	utils.SetLogger(logger)
}

// GetLogger Get the internal logger for nutsdb.
func GetLogger() ILogger {
	return utils.GetLogger()
}
