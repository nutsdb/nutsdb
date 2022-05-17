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
