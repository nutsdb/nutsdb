package metrics

import (
	"reflect"
	"sync/atomic"
	"testing"
)

func TestDeleteMetric(t *testing.T) {
	InitTest()
	type args struct {
		fd int32
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
			DeleteFileMetric(tt.args.fd)
		})
	}
}

func TestGetMetric(t *testing.T) {
	InitTest()
	_ = UpdateFileMetric(1, &FileMetric{1, 1, 1, 1})
	_ = UpdateFileMetric(2, &FileMetric{2, 2, 2, 2})
	type args struct {
		fd int32
	}
	tests := []struct {
		name   string
		args   args
		wantM  *FileMetric
		wantOk bool
	}{
		{"", args{1}, &FileMetric{1, 1, 1, 1}, true},
		{"", args{2}, &FileMetric{2, 2, 2, 2}, true},
		{"", args{3}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotM, gotOk := GetFileMetric(tt.args.fd)
			if !reflect.DeepEqual(gotM, tt.wantM) {
				t.Errorf("GetFileMetric() gotM = %v, want %v", gotM, tt.wantM)
			}
			if gotOk != tt.wantOk {
				t.Errorf("GetFileMetric() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestInit(t *testing.T) {
	InitTest()
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

func TestGetFDsExceedThreshold(t *testing.T) {
	InitTest()
	if gotFds := GetFDsExceedThreshold(.2); gotFds != nil {
		t.Errorf("GetFDsExceedThreshold() = %v, want %v", gotFds, nil)
	}
	_ = UpdateFileMetric(0, &FileMetric{8, 2, 70, 30})
	_ = UpdateFileMetric(1, &FileMetric{9, 1, 81, 19})
	_ = UpdateFileMetric(2, &FileMetric{9, 1, 77, 23})
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
	InitTest()
	_ = UpdateFileMetric(0, &FileMetric{8, 2, 70, 30})
	_ = UpdateFileMetric(1, &FileMetric{9, 1, 81, 19})
	_ = UpdateFileMetric(2, &FileMetric{9, 1, 77, 23})
	DeleteFileMetric(0)
	tests := []struct {
		name string
		want int
	}{
		{"", 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetFileMetricSize(); got != tt.want {
				t.Errorf("GetFileMetricSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUpdateFileMetric(t *testing.T) {
	InitTest()
	_ = UpdateFileMetric(0, &FileMetric{8, 2, 70, 30})
	_ = UpdateFileMetric(1, &FileMetric{9, 1, 81, 19})
	_ = UpdateFileMetric(2, &FileMetric{9, 1, 77, 23})
	_ = UpdateFileMetric(12, &FileMetric{9, 1, 77, 23})
	type args struct {
		fd     int32
		change *FileMetric
	}
	tests := []struct {
		name   string
		args   args
		wantFM *FileMetric
	}{
		{"", args{1, &FileMetric{1, 1, 1, 1}}, &FileMetric{10, 2, 82, 20}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = UpdateFileMetric(tt.args.fd, tt.args.change)
			if fm, _ := GetFileMetric(tt.args.fd); !reflect.DeepEqual(fm, tt.wantFM) {
				t.Errorf("get  %v, want %v", fm, tt.wantFM)
			}
		})
	}
}

func TestInitFileMetricForFd(t *testing.T) {
	type args struct {
		fd int32
	}
	tests := []struct {
		name string
		args args
	}{
		{"", args{4}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			InitFileMetricForFile(tt.args.fd)
		})
	}
}

func InitTest() {
	Init()
	reset()
	InitFileMetricForFile(0)
	InitFileMetricForFile(1)
	InitFileMetricForFile(2)
}

// this method is for test purpose
func reset() {
	fileMetrics = make(map[int32]*atomic.Value)
}
