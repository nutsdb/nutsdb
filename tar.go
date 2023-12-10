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

package nutsdb

import (
	"archive/tar"
	"compress/gzip"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
)

func tarGZCompress(dst io.Writer, src string) error {
	gzipWriter := gzip.NewWriter(dst)
	defer gzipWriter.Close()

	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}

		// Update the header name to use relative paths
		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		header.Name = relPath

		if err = tarWriter.WriteHeader(header); err != nil {
			return err
		}

		// If the file is a regular file, write its contents to the tarball
		if info.Mode().IsRegular() {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()

			if _, err = io.Copy(tarWriter, f); err != nil {
				return err
			}
		}

		return nil
	})
}

func tarDecompress(dst string, src io.Reader) error {
	gzipReader, err := gzip.NewReader(src)
	if err != nil {
		return err
	}
	defer gzipReader.Close()

	tarReader := tar.NewReader(gzipReader)

	for {
		header, err := tarReader.Next()

		if err != nil {
			if err == io.EOF {
				break // end of archive
			}
			return err
		}

		targetPath := filepath.Join(dst, filepath.Clean(header.Name))

		switch header.Typeflag {
		case tar.TypeDir:
			if oErr := os.MkdirAll(targetPath, os.FileMode(header.Mode)); oErr != nil {
				return oErr
			}
		case tar.TypeReg:
			f, oErr := os.Create(targetPath)
			if oErr != nil {
				return oErr
			}
			defer f.Close()

			if _, cErr := io.Copy(f, tarReader); cErr != nil {
				return cErr
			}
		default:
			return errors.Errorf("unsupported tar entry: %s", header.Name)
		}
	}

	return nil
}
