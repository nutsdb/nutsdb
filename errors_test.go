package nutsdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestIsKeyNotFound(t *testing.T) {
	ts := []struct {
		err error

		want bool
	}{
		{
			ErrKeyNotFound,
			true,
		},
		{
			errors.Wrap(ErrKeyNotFound, "foobar"),
			true,
		},
		{
			errors.New("foo bar"),
			false,
		},
	}

	for _, tc := range ts {
		got := IsKeyNotFound(tc.err)

		assert.Equal(t, tc.want, got)
	}
}

func TestIsKeyEmpty(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"case1", args{fmt.Errorf("foo error")}, false},
		{"case2", args{fmt.Errorf("foo error,%w", errors.New("sourceErr"))}, false},
		{"case3", args{fmt.Errorf("foo error,%w", ErrKeyEmpty)}, true},
		{"case4_errors.Wrap", args{errors.Wrap(ErrKeyEmpty, "foo Err")}, true},
		{"case5_errors.Wrapf", args{errors.Wrapf(ErrKeyEmpty, "foo Err %s", ErrKeyEmpty.Error())}, true},
		{"case6_errors.rawError", args{ErrKeyEmpty}, true},
		{"case6_errors.keynotfoundErr", args{ErrKeyNotFound}, false},
		// {"case5", args{errors.Wrap())}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsKeyEmpty(tt.args.err); got != tt.want {
				t.Errorf("IsKeyEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsBucketNotFound(t *testing.T) {
	ts := []struct {
		err  error
		want bool
	}{
		{
			ErrBucketNotFound,
			true,
		},
		{
			errors.Wrap(ErrBucketNotFound, "foobar"),
			true,
		},
		{
			errors.New("foobar"),
			false,
		},
	}

	for _, tc := range ts {
		got := IsBucketNotFound(tc.err)

		assert.Equal(t, tc.want, got)
	}
}

func TestIsBucketEmpty(t *testing.T) {
	ts := []struct {
		err  error
		want bool
	}{
		{
			ErrBucketEmpty,
			true,
		},
		{
			errors.Wrap(ErrBucketEmpty, "foobar"),
			true,
		},
		{
			errors.New("foobar"),
			false,
		},
	}

	for _, tc := range ts {
		got := IsBucketEmpty(tc.err)

		assert.Equal(t, tc.want, got)
	}
}

func TestIsDBClosed(t *testing.T) {
	InitOpt("", true)
	bucket := "test_closed"
	key := []byte("foo")
	val := []byte("bar")
	db, err = Open(opt)

	t.Run("db can be used before closed", func(t *testing.T) {
		txCreateBucket(t, db, DataStructureBTree, bucket, nil)

		err = db.Update(
			func(tx *Tx) error {
				return tx.Put(bucket, key, val, Persistent)
			})
		require.NoError(t, err)
	})
	assert.NoError(t, db.Close())

	t.Run("db can't be used after closed", func(t *testing.T) {
		err = db.Update(
			func(tx *Tx) error {
				return tx.Put(bucket, key, val, Persistent)
			})
		got := IsDBClosed(err)
		assert.Equal(t, true, got)
	})
}

func TestIsPrefixScan(t *testing.T) {
	bucket := "test_prefix_scan"
	t.Run("if prefix scanning not found the result return true", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			{
				txCreateBucket(t, db, DataStructureBTree, bucket, nil)
				tx, err := db.Begin(true)
				require.NoError(t, err)
				for i := 0; i <= 10; i++ {
					key := []byte("key_" + fmt.Sprintf("%07d", i))
					val := []byte("val" + fmt.Sprintf("%07d", i))
					err = tx.Put(bucket, key, val, Persistent)
					assert.NoError(t, err)
				}
				assert.NoError(t, tx.Commit())
			}
			{
				tx, err = db.Begin(false)
				require.NoError(t, err)
				prefix := []byte("key_")
				es, err := tx.PrefixScan(bucket, prefix, 0, 10)
				assert.NoError(t, tx.Commit())
				assert.NotEmpty(t, es)
				got := IsPrefixScan(err)
				assert.Equal(t, false, got)
			}
			{
				tx, err = db.Begin(false)
				require.NoError(t, err)
				prefix := []byte("foo_")
				es, err := tx.PrefixScan(bucket, prefix, 0, 10)
				assert.NoError(t, tx.Commit())
				assert.Empty(t, es)
				got := IsPrefixScan(err)
				assert.Equal(t, true, got)
			}
		})
	})
}

func TestIsPrefixSearchScan(t *testing.T) {
	regs := "(.+)"
	bucket := "test_prefix_search_scan"
	t.Run("if prefix and search scanning not found the result return true", func(t *testing.T) {
		withDefaultDB(t, func(t *testing.T, db *DB) {
			{
				txCreateBucket(t, db, DataStructureBTree, bucket, nil)

				tx, err := db.Begin(true)
				require.NoError(t, err)
				for i := 0; i <= 10; i++ {
					key := []byte("key_" + fmt.Sprintf("%07d", i))
					val := []byte("val" + fmt.Sprintf("%07d", i))
					err = tx.Put(bucket, key, val, Persistent)
					assert.NoError(t, err)
				}
				assert.NoError(t, tx.Commit())
			}
			{
				tx, err = db.Begin(false)
				require.NoError(t, err)
				prefix := []byte("key_")
				es, err := tx.PrefixSearchScan(bucket, prefix, regs, 0, 10)
				assert.NoError(t, tx.Commit())
				assert.NotEmpty(t, es)
				got := IsPrefixSearchScan(err)
				assert.Equal(t, false, got)
			}
			{
				tx, err = db.Begin(false)
				require.NoError(t, err)
				prefix := []byte("foo_")
				es, err := tx.PrefixSearchScan(bucket, prefix, regs, 0, 10)
				assert.NoError(t, tx.Commit())
				assert.Empty(t, es)
				got := IsPrefixSearchScan(err)
				assert.Equal(t, true, got)
			}
		})
	})
}

func TestGetMergeReadEntryError(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "test error",
			args: args{
				err: errors.New("test error"),
			},
			wantErr: fmt.Errorf("when merge operation build hintIndex readAt err: %s", errors.New("test error")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantErr, GetMergeReadEntryError(tt.args.err))
		})
	}
}
