package nutsdb

import (
	"sync"
	"sync/atomic"
)

type request struct {
	tx  *Tx
	Wg  sync.WaitGroup
	Err error
	ref int32
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

func (req *request) reset() {
	req.tx = nil
	req.Wg = sync.WaitGroup{}
	req.Err = nil

	atomic.StoreInt32(&req.ref, 0)
}

func (req *request) incrRef() {
	atomic.AddInt32(&req.ref, 1)
}

func (req *request) decrRef() {
	nRef := atomic.AddInt32(&req.ref, -1)
	if nRef > 0 {
		return
	}
	req.tx = nil
	requestPool.Put(req)
}

func (req *request) wait() error {
	req.Wg.Wait()
	err := req.Err
	req.decrRef() // decrRef after writing to DB.
	return err
}
