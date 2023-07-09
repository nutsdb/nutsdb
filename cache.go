package nutsdb

import (
	"bytes"
	"sync"
)

var c = &cache{
	p: sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}}

type cache struct {
	p sync.Pool
}

func (c *cache) getBuffer() bytes.Buffer {
	return c.p.Get().(bytes.Buffer)
}

func (c *cache) releaseBuffer(buffer bytes.Buffer) {
	buffer.Reset()
	c.p.Put(buffer)
}
