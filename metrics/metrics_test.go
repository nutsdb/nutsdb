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
			DeleteFileMetrics(tt.args.fd)
		})
	}
}

func TestGetMetrics(t *testing.T) {
	Init()
	reset()
	PutFileMetrics(1, &FileMetrics{1, 1, 1, 1})
	PutFileMetrics(2, &FileMetrics{2, 2, 2, 2})
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
			gotM, gotOk := GetFileMetrics(tt.args.fd)
			if !reflect.DeepEqual(gotM, tt.wantM) {
				t.Errorf("GetFileMetrics() gotM = %v, want %v", gotM, tt.wantM)
			}
			if gotOk != tt.wantOk {
				t.Errorf("GetFileMetrics() gotOk = %v, want %v", gotOk, tt.wantOk)
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
			PutFileMetrics(tt.args.fd, tt.args.m)
		})
	}
}

func TestGetFDsExceedThreshold(t *testing.T) {
	Init()
	reset()
	if gotFds := GetFDsExceedThreshold(.2); gotFds != nil {
		t.Errorf("GetFDsExceedThreshold() = %v, want %v", gotFds, nil)
	}
	PutFileMetrics(0, &FileMetrics{8, 2, 70, 30})
	PutFileMetrics(1, &FileMetrics{9, 1, 81, 19})
	PutFileMetrics(2, &FileMetrics{9, 1, 77, 23})
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
	PutFileMetrics(0, &FileMetrics{8, 2, 70, 30})
	PutFileMetrics(1, &FileMetrics{9, 1, 81, 19})
	PutFileMetrics(2, &FileMetrics{9, 1, 77, 23})
	DeleteFileMetrics(0)
	tests := []struct {
		name string
		want int
	}{
		{"", 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CountFileMetrics(); got != tt.want {
				t.Errorf("CountFileMetrics() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUpdateFileMetrics(t *testing.T) {
	Init()
	reset()
	PutFileMetrics(0, &FileMetrics{8, 2, 70, 30})
	PutFileMetrics(1, &FileMetrics{9, 1, 81, 19})
	PutFileMetrics(2, &FileMetrics{9, 1, 77, 23})
	type args struct {
		fd     int
		change *FileMetrics
	}
	tests := []struct {
		name   string
		args   args
		wantFM *FileMetrics
	}{
		{"", args{1, &FileMetrics{1, 1, 1, 1}}, &FileMetrics{10, 2, 82, 20}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			UpdateFileMetrics(tt.args.fd, tt.args.change)
			if fm, _ := GetFileMetrics(tt.args.fd); !reflect.DeepEqual(fm, tt.wantFM) {
				t.Errorf("get  %v, want %v", fm, tt.wantFM)
			}
		})
	}
}
