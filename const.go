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

// Data structure identifiers for buckets (aligned with nutsdb v1).
// Bucket 的数据结构类型标识（对齐 nutsdb v1）。
const (
	// DataStructureBTree is the KV (BTree) data structure.
	// DataStructureBTree 表示 KV（BTree）结构。
	DataStructureBTree uint16 = iota

	// DataStructureList is the List data structure (not implemented yet).
	// DataStructureList 表示 List 结构（本版未实现）。
	DataStructureList

	// DataStructureSet is the Set data structure (not implemented yet).
	// DataStructureSet 表示 Set 结构（本版未实现）。
	DataStructureSet

	// DataStructureSortedSet is the SortedSet data structure (not implemented yet).
	// DataStructureSortedSet 表示 SortedSet 结构（本版未实现）。
	DataStructureSortedSet
)
