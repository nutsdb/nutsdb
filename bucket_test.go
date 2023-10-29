package nutsdb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBucketMeta_Decode(t *testing.T) {
	type fields struct {
		Crc  uint32
		Size uint32
	}
	type args struct {
		bytes []byte
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := &BucketMeta{
				Crc:  tt.fields.Crc,
				Size: tt.fields.Size,
			}
			meta.Decode(tt.args.bytes)
		})
	}
}

func TestBucket_Decode(t *testing.T) {
	type fields struct {
		meta *BucketMeta
		Id   uint64
		Name string
	}
	type args struct {
		bytes []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Bucket{
				Meta: tt.fields.meta,
				Id:   tt.fields.Id,
				Name: tt.fields.Name,
			}
			tt.wantErr(t, b.Decode(tt.args.bytes), fmt.Sprintf("Decode(%v)", tt.args.bytes))
		})
	}
}

func TestBucket_Encode(t *testing.T) {
	type fields struct {
		meta *BucketMeta
		Id   uint64
		Name string
	}
	tests := []struct {
		name   string
		fields fields
		want   []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Bucket{
				Meta: tt.fields.meta,
				Id:   tt.fields.Id,
				Name: tt.fields.Name,
			}
			assert.Equalf(t, tt.want, b.Encode(), "Encode()")
		})
	}
}

func TestBucket_GetEntrySize(t *testing.T) {
	type fields struct {
		meta *BucketMeta
		Id   uint64
		Name string
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Bucket{
				Meta: tt.fields.meta,
				Id:   tt.fields.Id,
				Name: tt.fields.Name,
			}
			assert.Equalf(t, tt.want, b.GetEntrySize(), "GetEntrySize()")
		})
	}
}

func TestBucket_GetPayloadSize(t *testing.T) {
	type fields struct {
		meta *BucketMeta
		Id   uint64
		Name string
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Bucket{
				Meta: tt.fields.meta,
				Id:   tt.fields.Id,
				Name: tt.fields.Name,
			}
			assert.Equalf(t, tt.want, b.GetPayloadSize(), "GetPayloadSize()")
		})
	}
}
