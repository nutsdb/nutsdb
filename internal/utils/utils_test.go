package utils_test

import (
	"testing"

	"github.com/nutsdb/nutsdb"
	"github.com/nutsdb/nutsdb/internal/utils"
	"github.com/stretchr/testify/assert"
)

func TestGetDiskSizeFromSingleObject(t *testing.T) {
	type args struct {
		obj any
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "happy path for getting entry header size",
			args: args{
				obj: nutsdb.MetaData{},
			},
			want: 50,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, utils.GetDiskSizeFromSingleObject(tt.args.obj), "GetDiskSizeFromSingleObject(%v)", tt.args.obj)
		})
	}
}

func TestMarshalInts(t *testing.T) {
	assertions := assert.New(t)
	data, err := utils.MarshalInts([]int{})
	assertions.NoError(err, "TestMarshalInts")

	ints, err := utils.UnmarshalInts(data)
	assertions.NoError(err, "TestMarshalInts")
	assertions.Equal(0, len(ints), "TestMarshalInts")

	data, err = utils.MarshalInts([]int{1, 3})
	assertions.NoError(err, "TestMarshalInts")

	ints, err = utils.UnmarshalInts(data)
	assertions.NoError(err, "TestMarshalInts")
	assertions.Equal(2, len(ints), "TestMarshalInts")
	assertions.Equal(1, ints[0], "TestMarshalInts")
	assertions.Equal(3, ints[1], "TestMarshalInts")
}

func TestMatchForRange(t *testing.T) {
	assertions := assert.New(t)

	end, err := utils.MatchForRange("*", "hello", func(key string) bool {
		return true
	})
	assertions.NoError(err, "TestMatchForRange")
	assertions.False(end, "TestMatchForRange")

	_, err = utils.MatchForRange("[", "hello", func(key string) bool {
		return true
	})
	assertions.Error(err, "TestMatchForRange")

	end, err = utils.MatchForRange("*", "hello", func(key string) bool {
		return false
	})
	assertions.NoError(err, "TestMatchForRange")
	assertions.True(end, "TestMatchForRange")
}

func TestEncodeListKeyAndDecodeListKey(t *testing.T) {
	key := []byte("test_key")
	seq := uint64(114514)
	dataKey := utils.EncodeListKey(key, seq)
	newKey, newSeq := utils.DecodeListKey(dataKey)
	assert.Equal(t, key, newKey)
	assert.Equal(t, seq, newSeq)
}

func TestFnvHash32(t *testing.T) {
	t.Run("test hash32", func(t *testing.T) {
		bytes := []byte("test-fnv-hash32")
		hash_result, err := utils.GetFnv32(bytes)
		assert.NoError(t, err)
		assert.Equal(t, uint32(1486366400), hash_result)
	})
}
