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
			want: 50,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, getDiskSizeFromSingleObject(tt.args.obj), "getDiskSizeFromSingleObject(%v)", tt.args.obj)
		})
	}
}
