package nutsdb

import (
	"sync"
	"sync/atomic"
)

type request struct {
	tx  *Tx
	Wg  sync.WaitGroup
	Err error
	ref atomic.Int32
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
	req.ref.Store(0)
}

func (req *request) IncrRef() {
	req.ref.Add(1)
}

func (req *request) DecrRef() {
	nRef := req.ref.Add(-1)
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

type requests []*request

func (reqs requests) DecrRef() {
	for _, req := range reqs {
		req.DecrRef()
	}
}

func (reqs requests) IncrRef() {
	for _, req := range reqs {
		req.IncrRef()
	}
}
