package nutsdb

import (
	"errors"
	"time"
)

type WatchingFunc func() error

type Watcher struct {
	ready        chan struct{}
	watchingFunc WatchingFunc
}

func (w Watcher) WaitReady(timeout time.Duration) error {
	select {
	case <-w.ready:
		return nil
	case <-time.After(timeout):
		return errors.New("wait ready timeout")
	}
}
