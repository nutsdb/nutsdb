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

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(DefaultOptions, WithDir(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close(context.Background())
	})
	return db
}

func TestOpen_DirRequired(t *testing.T) {
	_, err := Open(DefaultOptions)
	require.ErrorIs(t, err, ErrDirRequired)
}

func TestDB_PutGetDeleteHas(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	err := db.Update(ctx, func(ctx context.Context, tx *Tx) error {
		require.NoError(t, tx.NewKVBucket("b1"))
		b := tx.GetBucket(DataStructureBTree, "b1")
		require.Equal(t, "b1", b.Name())
		require.NoError(t, b.Put([]byte("k"), []byte("v"), 0))
		v, err := b.Get([]byte("k"))
		require.NoError(t, err)
		require.Equal(t, []byte("v"), v)
		ok, err := b.Has([]byte("k"))
		require.NoError(t, err)
		require.True(t, ok)
		return nil
	})
	require.NoError(t, err)

	err = db.View(ctx, func(ctx context.Context, tx *Tx) error {
		b := tx.GetBucket(DataStructureBTree, "b1")
		v, err := b.Get([]byte("k"))
		require.NoError(t, err)
		require.Equal(t, []byte("v"), v)
		return nil
	})
	require.NoError(t, err)

	err = db.Update(ctx, func(ctx context.Context, tx *Tx) error {
		b := tx.GetBucket(DataStructureBTree, "b1")
		require.NoError(t, b.Delete([]byte("k")))
		_, err := b.Get([]byte("k"))
		require.ErrorIs(t, err, ErrKeyNotFound)
		ok, err := b.Has([]byte("k"))
		require.NoError(t, err)
		require.False(t, ok)
		return nil
	})
	require.NoError(t, err)

	err = db.View(ctx, func(ctx context.Context, tx *Tx) error {
		_, err := tx.GetBucket(DataStructureBTree, "b1").Get([]byte("k"))
		require.ErrorIs(t, err, ErrKeyNotFound)
		return nil
	})
	require.NoError(t, err)
}

func TestDB_BucketIsolation(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	require.NoError(t, db.Update(ctx, func(ctx context.Context, tx *Tx) error {
		require.NoError(t, tx.NewKVBucket("a"))
		require.NoError(t, tx.NewKVBucket("b"))
		require.NoError(t, tx.GetBucket(DataStructureBTree, "a").Put([]byte("k"), []byte("va"), 0))
		require.NoError(t, tx.GetBucket(DataStructureBTree, "b").Put([]byte("k"), []byte("vb"), 0))
		return nil
	}))

	require.NoError(t, db.View(ctx, func(ctx context.Context, tx *Tx) error {
		va, err := tx.GetBucket(DataStructureBTree, "a").Get([]byte("k"))
		require.NoError(t, err)
		vb, err := tx.GetBucket(DataStructureBTree, "b").Get([]byte("k"))
		require.NoError(t, err)
		require.Equal(t, []byte("va"), va)
		require.Equal(t, []byte("vb"), vb)
		return nil
	}))
}

func TestDB_BucketLifecycle(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	require.NoError(t, db.Update(ctx, func(ctx context.Context, tx *Tx) error {
		require.False(t, tx.ExistBucket(DataStructureBTree, "x"))
		require.NoError(t, tx.NewKVBucket("x"))
		require.True(t, tx.ExistBucket(DataStructureBTree, "x"))
		require.ErrorIs(t, tx.NewKVBucket("x"), ErrBucketAlreadyExist)
		require.NoError(t, tx.GetBucket(DataStructureBTree, "x").Put([]byte("k"), []byte("v"), 0))
		return nil
	}))

	require.NoError(t, db.Update(ctx, func(ctx context.Context, tx *Tx) error {
		require.True(t, tx.ExistBucket(DataStructureBTree, "x"))
		require.NoError(t, tx.DeleteBucket(DataStructureBTree, "x"))
		require.False(t, tx.ExistBucket(DataStructureBTree, "x"))
		_, err := tx.GetBucket(DataStructureBTree, "x").Get([]byte("k"))
		require.ErrorIs(t, err, ErrBucketNotFound)
		return nil
	}))

	require.NoError(t, db.View(ctx, func(ctx context.Context, tx *Tx) error {
		require.False(t, tx.ExistBucket(DataStructureBTree, "x"))
		return nil
	}))
}

func TestDB_IterateBuckets(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	require.NoError(t, db.Update(ctx, func(ctx context.Context, tx *Tx) error {
		require.NoError(t, tx.NewKVBucket("foo"))
		require.NoError(t, tx.NewKVBucket("bar"))
		require.NoError(t, tx.NewKVBucket("baz"))
		var got []string
		require.NoError(t, tx.IterateBuckets(DataStructureBTree, "ba*", func(name string) bool {
			got = append(got, name)
			return true
		}))
		require.Equal(t, []string{"bar", "baz"}, got)
		return nil
	}))
}

func TestDB_ViewReadOnly(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	require.NoError(t, db.Update(ctx, func(ctx context.Context, tx *Tx) error {
		return tx.NewKVBucket("b")
	}))

	err := db.View(ctx, func(ctx context.Context, tx *Tx) error {
		require.ErrorIs(t, tx.NewKVBucket("c"), ErrTxNotWritable)
		require.ErrorIs(t, tx.GetBucket(DataStructureBTree, "b").Put([]byte("k"), []byte("v"), 0), ErrTxNotWritable)
		return nil
	})
	require.NoError(t, err)
}

func TestDB_PutWithoutBucket(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	err := db.Update(ctx, func(ctx context.Context, tx *Tx) error {
		return tx.GetBucket(DataStructureBTree, "missing").Put([]byte("k"), []byte("v"), 0)
	})
	require.ErrorIs(t, err, ErrBucketNotFound)
}

func TestDB_EmptyKey(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	require.NoError(t, db.Update(ctx, func(ctx context.Context, tx *Tx) error {
		require.NoError(t, tx.NewKVBucket("b"))
		b := tx.GetBucket(DataStructureBTree, "b")
		require.ErrorIs(t, b.Put(nil, []byte("v"), 0), ErrKeyEmpty)
		_, err := b.Get(nil)
		require.ErrorIs(t, err, ErrKeyEmpty)
		return nil
	}))
}

func TestDB_UnsupportedDS(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	require.NoError(t, db.Update(ctx, func(ctx context.Context, tx *Tx) error {
		require.ErrorIs(t, tx.NewBucket(DataStructureList, "l"), ErrNotSupported)
		require.ErrorIs(t, tx.GetBucket(DataStructureList, "l").Put([]byte("k"), []byte("v"), 0), ErrNotSupported)
		return nil
	}))
}

func TestDB_UpdateRollback(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	require.NoError(t, db.Update(ctx, func(ctx context.Context, tx *Tx) error {
		return tx.NewKVBucket("b")
	}))

	boom := errors.New("boom")
	err := db.Update(ctx, func(ctx context.Context, tx *Tx) error {
		require.NoError(t, tx.GetBucket(DataStructureBTree, "b").Put([]byte("k"), []byte("v"), 0))
		return boom
	})
	require.ErrorIs(t, err, boom)

	require.NoError(t, db.View(ctx, func(ctx context.Context, tx *Tx) error {
		_, err := tx.GetBucket(DataStructureBTree, "b").Get([]byte("k"))
		require.ErrorIs(t, err, ErrKeyNotFound)
		return nil
	}))
}

func TestDB_ReopenPersists(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := Open(DefaultOptions, WithDir(dir))
	require.NoError(t, err)
	require.NoError(t, db.Update(ctx, func(ctx context.Context, tx *Tx) error {
		require.NoError(t, tx.NewKVBucket("b"))
		return tx.GetBucket(DataStructureBTree, "b").Put([]byte("k"), []byte("v"), 0)
	}))
	require.NoError(t, db.Close(ctx))

	db2, err := Open(DefaultOptions, WithDir(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close(context.Background()) })

	require.NoError(t, db2.View(ctx, func(ctx context.Context, tx *Tx) error {
		require.True(t, tx.ExistBucket(DataStructureBTree, "b"))
		v, err := tx.GetBucket(DataStructureBTree, "b").Get([]byte("k"))
		require.NoError(t, err)
		require.Equal(t, []byte("v"), v)
		return nil
	}))
}

func TestDB_CloseIdempotentError(t *testing.T) {
	db, err := Open(DefaultOptions, WithDir(t.TempDir()))
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, db.Close(ctx))
	require.ErrorIs(t, db.Close(ctx), ErrDBClosed)
	require.ErrorIs(t, db.Update(ctx, func(context.Context, *Tx) error { return nil }), ErrDBClosed)
}

func TestDB_ContextCancelBeforeUpdate(t *testing.T) {
	db := openTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := db.Update(ctx, func(ctx context.Context, tx *Tx) error {
		t.Fatal("fn should not run")
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestDB_PutWithTTL(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Control plane forwards TTL to the engine; expiry semantics are covered in internal/store.
	require.NoError(t, db.Update(ctx, func(ctx context.Context, tx *Tx) error {
		require.NoError(t, tx.NewKVBucket("b"))
		return tx.GetBucket(DataStructureBTree, "b").Put([]byte("k"), []byte("v"), 60)
	}))

	require.NoError(t, db.View(ctx, func(ctx context.Context, tx *Tx) error {
		v, err := tx.GetBucket(DataStructureBTree, "b").Get([]byte("k"))
		require.NoError(t, err)
		require.Equal(t, []byte("v"), v)
		return nil
	}))
}

func TestDB_DeleteBucketPurgesKeys(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := Open(DefaultOptions, WithDir(dir))
	require.NoError(t, err)
	require.NoError(t, db.Update(ctx, func(ctx context.Context, tx *Tx) error {
		require.NoError(t, tx.NewKVBucket("b"))
		return tx.GetBucket(DataStructureBTree, "b").Put([]byte("k"), []byte("v"), 0)
	}))
	require.NoError(t, db.Update(ctx, func(ctx context.Context, tx *Tx) error {
		return tx.DeleteBucket(DataStructureBTree, "b")
	}))
	require.NoError(t, db.Close(ctx))

	// Recreate bucket with same name; old key must not resurface.
	db2, err := Open(DefaultOptions, WithDir(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close(context.Background()) })
	require.NoError(t, db2.Update(ctx, func(ctx context.Context, tx *Tx) error {
		require.NoError(t, tx.NewKVBucket("b"))
		_, err := tx.GetBucket(DataStructureBTree, "b").Get([]byte("k"))
		require.ErrorIs(t, err, ErrKeyNotFound)
		return nil
	}))
}

func TestDB_ReadOwnWrites(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	require.NoError(t, db.Update(ctx, func(ctx context.Context, tx *Tx) error {
		require.NoError(t, tx.NewKVBucket("b"))
		b := tx.GetBucket(DataStructureBTree, "b")
		require.NoError(t, b.Put([]byte("k"), []byte("v1"), 0))
		v, err := b.Get([]byte("k"))
		require.NoError(t, err)
		require.Equal(t, []byte("v1"), v)
		require.NoError(t, b.Put([]byte("k"), []byte("v2"), 0))
		v, err = b.Get([]byte("k"))
		require.NoError(t, err)
		require.Equal(t, []byte("v2"), v)
		require.NoError(t, b.Delete([]byte("k")))
		_, err = b.Get([]byte("k"))
		require.ErrorIs(t, err, ErrKeyNotFound)
		return nil
	}))
}
