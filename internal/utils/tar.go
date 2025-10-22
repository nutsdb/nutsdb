// Copyright 2021 The nutsdb Author. All rights reserved.
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

package utils

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func TarGZCompress(dst io.Writer, src string) error {
	gz := gzip.NewWriter(dst)
	defer gz.Close()
	return TarCompress(gz, src)
}

// https://blog.ralch.com/articles/golang-working-with-tar-and-gzip
func TarCompress(dst io.Writer, src string) error {
	tarball := tar.NewWriter(dst)
	defer tarball.Close()

	info, err := os.Stat(src)
	if err != nil {
		return nil
	}

	var baseDir string
	if info.IsDir() {
		baseDir = filepath.Base(src)
	}

	return filepath.Walk(src,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			header, err := tar.FileInfoHeader(info, info.Name())
			if err != nil {
				return err
			}

			if baseDir != "" {
				header.Name = filepath.Join(baseDir, strings.TrimPrefix(path, src))
			}

			if err := tarball.WriteHeader(header); err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			file, err := os.Open(filepath.Clean(path))
			if err != nil {
				return err
			}

			defer file.Close()
			_, err = io.Copy(tarball, file)
			return err
		})
}
