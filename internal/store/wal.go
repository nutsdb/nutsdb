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

package store

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"

	"github.com/nutsdb/nutsdb/internal/fileio"
)

const (
	walOpPut    byte = 1
	walOpDelete byte = 2
)

type walStore struct {
	st   fileio.Store
	sync fileio.SyncMode
}

func openWAL(dir string, syncMode fileio.SyncMode, base fileio.Options) (*walStore, error) {
	opts := base
	opts.Dir = dir
	opts.SyncMode = syncMode
	st, err := fileio.Open(opts)
	if err != nil {
		return nil, err
	}
	return &walStore{st: st, sync: syncMode}, nil
}

func encodeWALPayload(op byte, key []byte, ref ValueRef, seq uint64) []byte {
	refBytes := encodeValueRef(nil, ref)
	n := 1 + 8 + 4 + len(key) + len(refBytes)
	buf := make([]byte, n)
	buf[0] = op
	binary.LittleEndian.PutUint64(buf[1:9], seq)
	binary.LittleEndian.PutUint32(buf[9:13], uint32(len(key)))
	copy(buf[13:], key)
	copy(buf[13+len(key):], refBytes)
	return buf
}

func decodeWALPayload(payload []byte) (op byte, key []byte, ref ValueRef, seq uint64, err error) {
	if len(payload) < 13 {
		return 0, nil, ValueRef{}, 0, ErrCorruptEntry
	}
	op = payload[0]
	seq = binary.LittleEndian.Uint64(payload[1:9])
	keyLen := int(binary.LittleEndian.Uint32(payload[9:13]))
	if keyLen <= 0 || len(payload) < 13+keyLen {
		return 0, nil, ValueRef{}, 0, ErrCorruptEntry
	}
	key = append([]byte(nil), payload[13:13+keyLen]...)
	ref, _, err = decodeValueRef(payload[13+keyLen:])
	if err != nil {
		return 0, nil, ValueRef{}, 0, err
	}
	return op, key, ref, seq, nil
}

func (w *walStore) Append(op byte, key []byte, ref ValueRef, seq uint64) error {
	payload := encodeWALPayload(op, key, ref, seq)
	var err error
	if w.sync == fileio.SyncEveryWrite {
		_, err = w.st.AppendSync(payload, fileio.RecordMeta)
	} else {
		_, err = w.st.Append(payload, fileio.RecordMeta)
	}
	return err
}

func (w *walStore) Sync() error {
	return w.st.Sync()
}

func (w *walStore) Replay(fn func(op byte, key []byte, ref ValueRef, seq uint64) error) (maxSeq uint64, err error) {
	ids, err := w.st.ListFileIDs()
	if err != nil {
		return 0, err
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		err = w.st.Iterate(id, func(_ fileio.Location, typ fileio.RecordType, payload []byte) error {
			if typ != fileio.RecordMeta {
				return nil
			}
			op, key, ref, seq, derr := decodeWALPayload(payload)
			if derr != nil {
				return derr
			}
			if seq > maxSeq {
				maxSeq = seq
			}
			return fn(op, key, ref, seq)
		})
		if err != nil {
			return maxSeq, err
		}
	}
	return maxSeq, nil
}

func (w *walStore) Reset(dir string, syncMode fileio.SyncMode, base fileio.Options) error {
	_ = w.st.Close()
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	opts := base
	opts.Dir = dir
	opts.SyncMode = syncMode
	st, err := fileio.Open(opts)
	if err != nil {
		return err
	}
	w.st = st
	w.sync = syncMode
	return nil
}

func (w *walStore) Close() error {
	return w.st.Close()
}

func defaultWALDir(root string) string {
	return filepath.Join(root, "wal")
}
