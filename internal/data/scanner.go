// Copyright 2025 The nutsdb Author. All rights reserved.
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

package data

import "github.com/nutsdb/nutsdb/internal/core"

// ScanDirection defines the iteration direction.
type ScanDirection int

const (
	Forward ScanDirection = iota
	Reverse
)

// Scanner defines the interface for building scan operations.
// Implementations should support fluent API pattern (method chaining).
type Scanner interface {
	// Direction configuration
	Direction(d ScanDirection) Scanner
	Ascending() Scanner
	Descending() Scanner

	// Range configuration
	From(key []byte) Scanner
	To(key []byte) Scanner
	Prefix(prefix []byte) Scanner

	// Filter configuration
	Match(pattern string) Scanner
	Where(fn func(*core.Item[core.Record]) bool) Scanner
	IncludeExpired() Scanner
	WithDataStructure(ds uint16) Scanner

	// Pagination
	Skip(n int) Scanner
	Take(n int) Scanner

	// Terminal operations
	Collect() []*core.Record
	CollectItems() []*core.Item[core.Record]
	First() (*core.Item[core.Record], bool)
	Count() int
	ForEach(fn func(*core.Item[core.Record]) bool)
}

// iterResult represents the result of processing an item during iteration.
type iterResult int

const (
	iterContinue iterResult = iota // continue to next item
	iterStop                       // stop iteration
	iterMatch                      // item matches, process it
)
