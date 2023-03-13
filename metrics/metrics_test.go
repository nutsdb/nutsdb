package metrics

import (
	"reflect"
	"testing"
)

func TestDeleteMetrics(t *testing.T) {
	Init()
	reset()
	type args struct {
		fd int
	}
	tests := []struct {
		name string
		args args
	}{
		{"test", args{3}},
		{"test", args{4}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			DeleteMetrics(tt.args.fd)
		})
	}
}

func TestGetMetrics(t *testing.T) {
	Init()
	reset()
	PutMetrics(1, &FileMetrics{1, 1, 1, 1})
	PutMetrics(2, &FileMetrics{2, 2, 2, 2})
	type args struct {
		fd int
	}
	tests := []struct {
		name   string
		args   args
		wantM  *FileMetrics
		wantOk bool
	}{
		{"", args{1}, &FileMetrics{1, 1, 1, 1}, true},
		{"", args{2}, &FileMetrics{2, 2, 2, 2}, true},
		{"", args{3}, &FileMetrics{0, 0, 0, 0}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotM, gotOk := GetMetrics(tt.args.fd)
			if !reflect.DeepEqual(gotM, tt.wantM) {
				t.Errorf("GetMetrics() gotM = %v, want %v", gotM, tt.wantM)
			}
			if gotOk != tt.wantOk {
				t.Errorf("GetMetrics() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestInitMergeMetrics(t *testing.T) {
	Init()
	reset()
	tests := []struct {
		name string
	}{
		{""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Init()
		})
	}
}

func TestPutMetrics(t *testing.T) {
	Init()
	reset()
	type args struct {
		fd int
		m  *FileMetrics
	}
	tests := []struct {
		name string
		args args
	}{
		{"", args{1, &FileMetrics{1, 1, 1, 1}}},
		{"", args{12, &FileMetrics{2, 2, 2, 2}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			PutMetrics(tt.args.fd, tt.args.m)
		})
	}
}

func TestGetFDsExceedThreshold(t *testing.T) {
	Init()
	reset()
	if gotFds := GetFDsExceedThreshold(.2); gotFds != nil {
		t.Errorf("GetFDsExceedThreshold() = %v, want %v", gotFds, nil)
	}
	PutMetrics(0, &FileMetrics{8, 2, 70, 30})
	PutMetrics(1, &FileMetrics{9, 1, 81, 19})
	PutMetrics(2, &FileMetrics{9, 1, 77, 23})
	type args struct {
		threshold float64
	}
	tests := []struct {
		name    string
		args    args
		wantFds []int32
	}{
		{"", args{.2}, []int32{0, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotFds := GetFDsExceedThreshold(tt.args.threshold); !reflect.DeepEqual(gotFds, tt.wantFds) {
				t.Errorf("GetFDsExceedThreshold() = %v, want %v", gotFds, tt.wantFds)
			}
		})
	}
}

func TestCountMetrics(t *testing.T) {
	Init()
	reset()
	PutMetrics(0, &FileMetrics{8, 2, 70, 30})
	PutMetrics(1, &FileMetrics{9, 1, 81, 19})
	PutMetrics(2, &FileMetrics{9, 1, 77, 23})
	DeleteMetrics(0)
	tests := []struct {
		name string
		want int
	}{
		{"", 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CountMetrics(); got != tt.want {
				t.Errorf("CountMetrics() = %v, want %v", got, tt.want)
			}
		})
	}
}
