package nutsdb

import (
	"context"
	"errors"
	"sync"
	"time"
)

type WatchingFunc func() error

type Watcher struct {
	readyCh      chan struct{}   // the channel to signal that the watcher is ready
	watchingFunc WatchingFunc    // the function to watch the key and bucket
	isReady      bool            // indicates whether the watcher is ready
	ctx          context.Context // the context for the watch

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

	if w.isReady {
		w.muReady.Unlock()
		return nil
	}

	w.isReady = true
	close(w.readyCh)
	w.muReady.Unlock()

	return w.watchingFunc()
}
