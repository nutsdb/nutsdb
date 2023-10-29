// Copyright 2023 The nutsdb Author. All rights reserved.
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

type garbageMeta struct {
	fileSize    int64
	garbageSize int64
}

type garbageManager struct {
	metas map[int64]*garbageMeta
}

func newGarbageManager() *garbageManager {
	return &garbageManager{metas: map[int64]*garbageMeta{}}
}

func (gm *garbageManager) updateGarbageMeta(fid int64, fileSizeDelta int64, garbageSizeDelta int64) {
	meta, ok := gm.metas[fid]

	if !ok {
		gm.metas[fid] = &garbageMeta{fileSize: 0, garbageSize: 0}
		meta = gm.metas[fid]
	}

	meta.fileSize += fileSizeDelta
	meta.garbageSize += garbageSizeDelta
}

func (gm *garbageManager) removeGarbageMeta(fid int64) {
	delete(gm.metas, fid)
}

func (gm *garbageManager) exceedThresholdFids(threshold float64) []int64 {
	fids := make([]int64, 0)

	for fid, meta := range gm.metas {
		if (float64(meta.garbageSize) / float64(meta.fileSize)) >= threshold {
			fids = append(fids, fid)
		}
	}

	return fids
}
