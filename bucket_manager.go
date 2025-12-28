package nutsdb

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/nutsdb/nutsdb/internal/core"
)

var ErrBucketNotExist = errors.New("bucket not found")

const BucketStoreFileName = "bucket.Meta"

type BucketManager struct {
	fd *os.File
	// BucketInfoMapper BucketID => Bucket itself
	BucketInfoMapper core.InfoMapperInBucket
	BucketIDMarker   core.IDMarkerInBucket
	// IDGenerator helps generates an ID for every single bucket
	Gen *IDGenerator
}

func NewBucketManager(dir string) (*BucketManager, error) {
	bm := &BucketManager{
		BucketInfoMapper: make(core.InfoMapperInBucket),
		BucketIDMarker:   make(core.IDMarkerInBucket),
	}
	bucketFilePath := filepath.Join(dir, BucketStoreFileName)
	_, err := os.Stat(bucketFilePath)
	mode := os.O_RDWR
	if err != nil {
		mode |= os.O_CREATE
	}
	fd, err := os.OpenFile(bucketFilePath, mode, os.ModePerm)
	if err != nil {
		return nil, err
	}
	bm.fd = fd
	bm.Gen = &IDGenerator{currentMaxId: 0}
	return bm, nil
}

type bucketSubmitRequest struct {
	ds     core.Ds
	name   core.BucketName
	bucket *core.Bucket
}

func (bm *BucketManager) SubmitPendingBucketChange(reqs []*bucketSubmitRequest) error {
	bytes := make([]byte, 0)
	for _, req := range reqs {
		bs := req.bucket.Encode()
		bytes = append(bytes, bs...)
		// update the marker info
		if _, exist := bm.BucketIDMarker[req.name]; !exist {
			bm.BucketIDMarker[req.name] = map[core.Ds]core.BucketId{}
		}
		// recover maxid otherwise new bucket start from 1 again
		bm.Gen.CompareAndSetMaxId(req.bucket.Id)
		switch req.bucket.Meta.Op {
		case core.BucketInsertOperation:
			bm.BucketInfoMapper[req.bucket.Id] = req.bucket
			bm.BucketIDMarker[req.name][req.bucket.Ds] = req.bucket.Id
		case core.BucketDeleteOperation:
			if len(bm.BucketIDMarker[req.name]) == 1 {
				delete(bm.BucketIDMarker, req.name)
			} else {
				delete(bm.BucketIDMarker[req.name], req.bucket.Ds)
			}
			delete(bm.BucketInfoMapper, req.bucket.Id)
		}
	}
	_, err := bm.fd.Write(bytes)
	return err
}

type IDGenerator struct {
	currentMaxId uint64
}

func (g *IDGenerator) GenId() uint64 {
	g.currentMaxId++
	return g.currentMaxId
}

func (g *IDGenerator) CompareAndSetMaxId(id uint64) {
	if id > g.currentMaxId {
		g.currentMaxId = id
	}
}

func (bm *BucketManager) ExistBucket(ds core.Ds, name core.BucketName) bool {
	bucket, err := bm.GetBucket(ds, name)
	if bucket != nil && err == nil {
		return true
	}
	return false
}

func (bm *BucketManager) GetBucket(ds core.Ds, name core.BucketName) (b *core.Bucket, err error) {
	ds2IdMapper := bm.BucketIDMarker[name]
	if ds2IdMapper == nil {
		return nil, ErrBucketNotExist
	}

	if id, exist := ds2IdMapper[ds]; exist {
		if bucket, ok := bm.BucketInfoMapper[id]; ok {
			return bucket, nil
		} else {
			return nil, ErrBucketNotExist
		}
	} else {
		return nil, ErrBucketNotExist
	}
}

func (bm *BucketManager) GetBucketById(id core.BucketId) (*core.Bucket, error) {
	if bucket, exist := bm.BucketInfoMapper[id]; exist {
		return bucket, nil
	} else {
		return nil, ErrBucketNotExist
	}
}

func (bm *BucketManager) GetBucketID(ds core.Ds, name core.BucketName) (core.BucketId, error) {
	if bucket, err := bm.GetBucket(ds, name); err != nil {
		return 0, err
	} else {
		return bucket.Id, nil
	}
}

func (bm *BucketManager) Close() error {
	return bm.fd.Close()
}
