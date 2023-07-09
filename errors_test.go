package nutsdb

import (
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

func TestIsDBClosed(t *testing.T) {
	ts := []struct {
		err  error
		want bool
	}{
		{
			ErrDBClosed,
			true,
		},
		{
			errors.Wrap(ErrDBClosed, "test"),
			true,
		},
		{
			errors.New("test"),
			false,
		},
	}

	for _, tc := range ts {
		got := IsDBClosed(tc.err)
		assert.Equal(t, tc.want, got)
	}
}
