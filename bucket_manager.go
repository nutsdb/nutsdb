package nutsdb

import (
	"errors"
	"os"
)

var ErrBucketNotExist = errors.New("bucket not exist")

const BucketStoreFileName = "bucket.Meta"

type Ds = uint16
type BucketId = uint64
type BucketName = string
type IDMarkerInBucket map[BucketName]map[Ds]BucketId
type InfoMapperInBucket map[BucketId]*Bucket

type BucketManager struct {
	fd *os.File
	// BucketInfoMapper BucketID => Bucket itself
	BucketInfoMapper InfoMapperInBucket

	BucketIDMarker IDMarkerInBucket

	// IDGenerator helps generates an ID for every single bucket
	Gen *IDGenerator
}

func NewBucketManager(dir string) (*BucketManager, error) {
	bm := &BucketManager{
		BucketInfoMapper: map[BucketId]*Bucket{},
		BucketIDMarker:   map[BucketName]map[Ds]BucketId{},
	}
	bucketFilePath := dir + "/" + BucketStoreFileName
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
	ds     Ds
	name   BucketName
	bucket *Bucket
}

func (bm *BucketManager) SubmitPendingBucketChange(reqs []*bucketSubmitRequest) error {
	bytes := make([]byte, 0)
	for _, req := range reqs {
		bs := req.bucket.Encode()
		bytes = append(bytes, bs...)
		// update the marker info
		if _, exist := bm.BucketIDMarker[req.name]; !exist {
			bm.BucketIDMarker[req.name] = map[Ds]BucketId{}
		}
		switch req.bucket.Meta.Op {
		case BucketInsertOperation:
			bm.BucketInfoMapper[req.bucket.Id] = req.bucket
			bm.BucketIDMarker[req.name][req.bucket.Ds] = req.bucket.Id
		case BucketDeleteOperation:
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

func (bm *BucketManager) ExistBucket(ds Ds, name BucketName) bool {
	bucket, err := bm.GetBucket(ds, name)
	if bucket != nil && err == nil {
		return true
	}
	return false
}

func (bm *BucketManager) GetBucket(ds Ds, name BucketName) (b *Bucket, err error) {
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

func (bm *BucketManager) GetBucketById(id BucketId) (*Bucket, error) {
	if bucket, exist := bm.BucketInfoMapper[id]; exist {
		return bucket, nil
	} else {
		return nil, ErrBucketNotExist
	}
}

func (bm *BucketManager) GetBucketID(ds Ds, name BucketName) (BucketId, error) {
	if bucket, err := bm.GetBucket(ds, name); err != nil {
		return 0, err
	} else {
		return bucket.Id, nil
	}
}
