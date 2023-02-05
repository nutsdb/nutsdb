package nutsdb

import (
	"fmt"
	"testing"

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
