package nutsdb

import (
	"errors"
	"sync"
	"time"
)

type WatchingFunc func() error

type Watcher struct {
	readyCh      chan struct{} // the channel to signal that the watcher is ready
	watchingFunc WatchingFunc  // the function to watch the key and bucket
	isReady      bool          // indicates whether the watcher is ready

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
