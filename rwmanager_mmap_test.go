// Copyright 2019 The nutsdb Author. All rights reserved.
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
	"testing"
)

func TestRWManager_MMap_Release(t *testing.T) {
	filePath := "/tmp/foo_rw_MMap"
	fdm := newFileManager(MMap, 1024, 0.5)
	rwmanager, err := fdm.getMMapRWManager(filePath, 1024)
	if err != nil {
		t.Error("err TestRWManager_MMap_Release getMMapRWManager")
	}

	b := []byte("hello")
	off := int64(0)
	_, err = rwmanager.WriteAt(b, off)
	if err != nil {
		t.Error("err TestRWManager_MMap_Release WriteAt")
	}

	if !rwmanager.IsActive() {
		t.Error("err TestRWManager_MMap_Release FdInfo:using not correct")
	}

	err = rwmanager.Release()
	if err != nil {
		t.Error("err TestRWManager_MMap_Release Release")
	}

	if rwmanager.IsActive() {
		t.Error("err TestRWManager_MMap_Release Release Failed")
	}
}

func (rwmanager *MMapRWManager) IsActive() bool {
	return	rwmanager.fdm.cache[rwmanager.path].using != 0
}
