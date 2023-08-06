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

func (req *request) IncrRef() {
	atomic.AddInt32(&req.ref, 1)
}

func (req *request) DecrRef() {
	nRef := atomic.AddInt32(&req.ref, -1)
	if nRef > 0 {
		return
	}
	req.tx = nil
	requestPool.Put(req)
}

func (req *request) Wait() error {
	req.Wg.Wait()
	err := req.Err
	req.DecrRef() // DecrRef after writing to DB.
	return err
}
