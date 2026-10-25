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

// Options holds database open configuration (minimal set for U0).
// Options 为打开数据库的配置（U0 最小集）。
type Options struct {
	// Dir is the data directory (required).
	// Dir 为数据目录（必填）。
	Dir string
}

// Option mutates Options when opening a DB.
// Option 在 Open 时修改 Options。
type Option func(*Options)

// DefaultOptions is the baseline options; set Dir via WithDir.
// DefaultOptions 为默认选项；请用 WithDir 设置目录。
var DefaultOptions = Options{}

// WithDir sets the data directory.
// WithDir 设置数据目录。
func WithDir(dir string) Option {
	return func(o *Options) {
		o.Dir = dir
	}
}

func applyOptions(base Options, ops ...Option) Options {
	opts := base
	for _, op := range ops {
		if op != nil {
			op(&opts)
		}
	}
	return opts
}
