package nutsdb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetDiskSizeFromSingleObject(t *testing.T) {
	type args struct {
		obj interface{}
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "happy path for getting entry header size",
			args: args{
				obj: MetaData{},
			},
			want: 42,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, GetDiskSizeFromSingleObject(tt.args.obj), "GetDiskSizeFromSingleObject(%v)", tt.args.obj)
		})
	}
}
