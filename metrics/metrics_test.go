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

func TestMetrics_Update(t *testing.T) {
	Init()
	reset()
	type fields struct {
		validEntries   int32
		invalidEntries int32
		validBytes     int64
		invalidBytes   int64
	}
	type args struct {
		validEntriesChange   int
		invalidEntriesChange int
		validBytesChange     int
		invalidBytesChange   int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *FileMetrics
	}{
		{"", fields{1, 1, 1, 1}, args{1, 1, 1, 1}, &FileMetrics{2, 2, 2, 2}},
		{"", fields{1, 1, 1, 1}, args{-1, -1, -1, -1}, &FileMetrics{0, 0, 0, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &FileMetrics{
				ValidEntries:   tt.fields.validEntries,
				InvalidEntries: tt.fields.invalidEntries,
				ValidBytes:     tt.fields.validBytes,
				InvalidBytes:   tt.fields.invalidBytes,
			}
			if err := m.Update(tt.args.validEntriesChange, tt.args.invalidEntriesChange, tt.args.validBytesChange, tt.args.invalidBytesChange); err != nil {
				return
			} else if !reflect.DeepEqual(m, tt.want) {
				t.Errorf("GetZeroMetrics() = %v, want %v", m, tt.want)
			}
		})
	}
}
func TestGetZeroMetrics(t *testing.T) {
	Init()
	reset()
	tests := []struct {
		name string
		want *FileMetrics
	}{
		{"", &FileMetrics{0, 0, 0, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetZeroMetrics(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetZeroMetrics() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetFDsExceedThreshold(t *testing.T) {
	Init()
	reset()
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

func TestFileMetrics_Update(t *testing.T) {
	Init()
	reset()
	type fields struct {
		ValidEntries   int32
		InvalidEntries int32
		ValidBytes     int64
		InvalidBytes   int64
	}
	type args struct {
		change []int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"", fields{1, 1, 1, 1}, args{[]int{1, 1, 1, 1, 1, 1}}, true},
		{"", fields{1, 1, 1, 1}, args{[]int{1, 1, 1, 1}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &FileMetrics{
				ValidEntries:   tt.fields.ValidEntries,
				InvalidEntries: tt.fields.InvalidEntries,
				ValidBytes:     tt.fields.ValidBytes,
				InvalidBytes:   tt.fields.InvalidBytes,
			}
			if err := m.Update(tt.args.change...); (err != nil) != tt.wantErr {
				t.Errorf("Update() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFileMetrics_UpdateValid(t *testing.T) {
	Init()
	reset()
	type fields struct {
		ValidEntries   int32
		InvalidEntries int32
		ValidBytes     int64
		InvalidBytes   int64
	}
	type args struct {
		change []int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"", fields{1, 1, 1, 1}, args{[]int{1, 1}}, false},
		{"", fields{1, 1, 1, 1}, args{[]int{1}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &FileMetrics{
				ValidEntries:   tt.fields.ValidEntries,
				InvalidEntries: tt.fields.InvalidEntries,
				ValidBytes:     tt.fields.ValidBytes,
				InvalidBytes:   tt.fields.InvalidBytes,
			}
			if err := m.UpdateValid(tt.args.change...); (err != nil) != tt.wantErr {
				t.Errorf("UpdateValid() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFileMetrics_UpdateInvalid(t *testing.T) {
	Init()
	reset()
	type fields struct {
		ValidEntries   int32
		InvalidEntries int32
		ValidBytes     int64
		InvalidBytes   int64
	}
	type args struct {
		change []int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"", fields{1, 1, 1, 1}, args{[]int{1, 1}}, false},
		{"", fields{1, 1, 1, 1}, args{[]int{1, 1, 1}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &FileMetrics{
				ValidEntries:   tt.fields.ValidEntries,
				InvalidEntries: tt.fields.InvalidEntries,
				ValidBytes:     tt.fields.ValidBytes,
				InvalidBytes:   tt.fields.InvalidBytes,
			}
			if err := m.UpdateInvalid(tt.args.change...); (err != nil) != tt.wantErr {
				t.Errorf("UpdateInvalid() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
