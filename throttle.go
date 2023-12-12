package nutsdb

import (
	"sync"
)

// throttle allows a limited number of workers to run at a time. It also
// provides a mechanism to check for errors encountered by workers and wait for
// them to finish.
type throttle struct {
	once      sync.Once
	wg        sync.WaitGroup
	ch        chan struct{}
	errCh     chan error
	finishErr error
}

// newThrottle creates a new throttle with a max number of workers.
func newThrottle(max int) *throttle {
	return &throttle{
		ch:    make(chan struct{}, max),
		errCh: make(chan error, max),
	}
}

// do should be called by workers before they start working. It blocks if there
// are already maximum number of workers working. If it detects an error from
// previously done workers, it would return it.
func (t *throttle) do() error {
	for {
		select {
		case t.ch <- struct{}{}:
			t.wg.Add(1)
			return nil
		case err := <-t.errCh:
			if err != nil {
				return err
			}
		}
	}
}

// done should be called by workers when they finish working. They can also
// pass the error status of work done.
func (t *throttle) done(err error) {
	if err != nil {
		t.errCh <- err
	}
	select {
	case <-t.ch:
	default:
		panic("throttle do done mismatch")
	}
	t.wg.Done()
}

// finish waits until all workers have finished working. It would return any error passed by done.
// If finish is called multiple time, it will wait for workers to finish only once(first time).
// From next calls, it will return same error as found on first call.
func (t *throttle) finish() error {
	t.once.Do(func() {
		t.wg.Wait()
		close(t.ch)
		close(t.errCh)
		for err := range t.errCh {
			if err != nil {
				t.finishErr = err
				return
			}
		}
	})

	return t.finishErr
}
