package nutsdb

import (
	"errors"
	"os"
)

var ErrBucketNotExist = errors.New("bucket not exist")

const bucketStoreFileName = "bucket.Meta"

type ds = uint16
type bucketId = uint64
type bucketName = string
type idMarkerInBucket map[bucketName]map[ds]bucketId
type infoMapperInBucket map[bucketId]*bucket

type bucketManager struct {
	fd *os.File
	// BucketInfoMapper BucketID => Bucket itself
	BucketInfoMapper infoMapperInBucket

	BucketIDMarker idMarkerInBucket

	// IDGenerator helps generates an ID for every single bucket
	Gen *IDGenerator
}

func newBucketManager(dir string) (*bucketManager, error) {
	bm := &bucketManager{
		BucketInfoMapper: map[bucketId]*bucket{},
		BucketIDMarker:   map[bucketName]map[ds]bucketId{},
	}
	bucketFilePath := dir + "/" + bucketStoreFileName
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
	ds     ds
	name   bucketName
	bucket *bucket
}

func (bm *bucketManager) submitPendingBucketChange(reqs []*bucketSubmitRequest) error {
	bytes := make([]byte, 0)
	for _, req := range reqs {
		bs := req.bucket.encode()
		bytes = append(bytes, bs...)
		// update the marker info
		if _, exist := bm.BucketIDMarker[req.name]; !exist {
			bm.BucketIDMarker[req.name] = map[ds]bucketId{}
		}
		switch req.bucket.Meta.Op {
		case bucketInsertOperation:
			bm.BucketInfoMapper[req.bucket.Id] = req.bucket
			bm.BucketIDMarker[req.name][req.bucket.Ds] = req.bucket.Id
		case bucketDeleteOperation:
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

func (g *IDGenerator) genId() uint64 {
	g.currentMaxId++
	return g.currentMaxId
}

func (bm *bucketManager) existBucket(ds ds, name bucketName) bool {
	bucket, err := bm.getBucket(ds, name)
	if bucket != nil && err == nil {
		return true
	}
	return false
}

func (bm *bucketManager) getBucket(ds ds, name bucketName) (b *bucket, err error) {
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

func (bm *bucketManager) getBucketById(id bucketId) (*bucket, error) {
	if bucket, exist := bm.BucketInfoMapper[id]; exist {
		return bucket, nil
	} else {
		return nil, ErrBucketNotExist
	}
}

func (bm *bucketManager) getBucketID(ds ds, name bucketName) (bucketId, error) {
	if bucket, err := bm.getBucket(ds, name); err != nil {
		return 0, err
	} else {
		return bucket.Id, nil
	}
}
