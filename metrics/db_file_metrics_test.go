package metrics

import (
	"reflect"
	"testing"
)

func TestDeleteMetrics(t *testing.T) {
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
			DeleteFileMetrics(tt.args.fd)
		})
	}
}

func TestGetMetrics(t *testing.T) {
	InitTest()
	_ = UpdateFileMetrics(1, &FileMetric{1, 1, 1, 1})
	_ = UpdateFileMetrics(2, &FileMetric{2, 2, 2, 2})
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

func TestPutMetrics(t *testing.T) {
	InitTest()
	type args struct {
		fd int32
		m  *FileMetric
	}
	tests := []struct {
		name string
		args args
	}{
		{"", args{1, &FileMetric{1, 1, 1, 1}}},
		{"", args{12, &FileMetric{2, 2, 2, 2}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = UpdateFileMetrics(tt.args.fd, tt.args.m)
		})
	}
}

func TestGetFDsExceedThreshold(t *testing.T) {
	InitTest()
	if gotFds := GetFDsExceedThreshold(.2); gotFds != nil {
		t.Errorf("GetFDsExceedThreshold() = %v, want %v", gotFds, nil)
	}
	_ = UpdateFileMetrics(0, &FileMetric{8, 2, 70, 30})
	_ = UpdateFileMetrics(1, &FileMetric{9, 1, 81, 19})
	_ = UpdateFileMetrics(2, &FileMetric{9, 1, 77, 23})
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
	_ = UpdateFileMetrics(0, &FileMetric{8, 2, 70, 30})
	_ = UpdateFileMetrics(1, &FileMetric{9, 1, 81, 19})
	_ = UpdateFileMetrics(2, &FileMetric{9, 1, 77, 23})
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
	InitTest()
	_ = UpdateFileMetrics(0, &FileMetric{8, 2, 70, 30})
	_ = UpdateFileMetrics(1, &FileMetric{9, 1, 81, 19})
	_ = UpdateFileMetrics(2, &FileMetric{9, 1, 77, 23})
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
			_ = UpdateFileMetrics(tt.args.fd, tt.args.change)
			if fm, _ := GetFileMetrics(tt.args.fd); !reflect.DeepEqual(fm, tt.wantFM) {
				t.Errorf("get  %v, want %v", fm, tt.wantFM)
			}
		})
	}
}

func TestInitFileMetricsForFd(t *testing.T) {
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
			InitFileMetricsForFd(tt.args.fd)
		})
	}
}

func InitTest() {
	Init()
	reset()
	InitFileMetricsForFd(0)
	InitFileMetricsForFd(1)
	InitFileMetricsForFd(2)
}

func TestDeleteFileMetrics(t *testing.T) {
	type args struct {
		fd int32
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			DeleteFileMetrics(tt.args.fd)
		})
	}
}
