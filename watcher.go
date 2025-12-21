package nutsdb

import (
	"errors"
	"sync"
	"time"
)

type WatchingFunc func() error

type Watcher struct {
	readyCh      chan struct{}
	watchingFunc WatchingFunc
	isReady      bool

	muReady sync.Mutex
}

func (w *Watcher) WaitReady(timeout time.Duration) error {
	select {
	case <-w.readyCh:
		return nil
	case <-time.After(timeout):
		return errors.New("wait for watcher ready timeout")
	}
}

func (w *Watcher) Run() error {
	w.muReady.Lock()
	defer w.muReady.Unlock()
	if w.isReady {
		return nil
	}

	w.isReady = true
	close(w.readyCh)
	return w.watchingFunc()
}
