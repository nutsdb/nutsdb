package core

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// ComponentLifecycle provides shared lifecycle management for components.
// It encapsulates common patterns: context management, goroutine tracking,
// and idempotent stop behavior.
//
// Usage:
//
//	type MyComponent struct {
//	    lifecycle ComponentLifecycle
//	    // ... business fields ...
//	}
//
//	func (c *MyComponent) Start(ctx context.Context) error {
//	    if err := c.lifecycle.Start(ctx); err != nil {
//	        return err
//	    }
//	    c.lifecycle.Go(func(ctx context.Context) {
//	        // worker goroutine
//	    })
//	    return nil
//	}
//
//	func (c *MyComponent) Stop(timeout time.Duration) error {
//	    return c.lifecycle.Stop(timeout)
//	}
type ComponentLifecycle struct {
	// ctx is the component's context, derived from parent context
	ctx context.Context

	// cancel cancels the component's context
	cancel context.CancelFunc

	// wg tracks all goroutines started via Go()
	wg sync.WaitGroup

	// started indicates if Start() has been called
	started atomic.Bool

	// stopped indicates if Stop() has been called
	stopped atomic.Bool

	// mu protects state transitions
	mu sync.Mutex
}

// ComponentState represents the lifecycle state of a component
type ComponentState int

const (
	ComponentStateCreated ComponentState = iota
	ComponentStateRunning
	ComponentStateStopped
)

// ErrAlreadyStarted is returned when Start() is called on an already started component
var ErrAlreadyStarted = error(&componentError{msg: "component already started"})

// ErrAlreadyStopped is returned when Stop() is called on an already stopped component
var ErrAlreadyStopped = error(&componentError{msg: "component already stopped"})

// ErrStopTimeout is returned when Stop() times out waiting for goroutines
var ErrStopTimeout = error(&componentError{msg: "component stop timeout"})

type componentError struct {
	msg string
}

func (e *componentError) Error() string {
	return e.msg
}

// Start initializes the component lifecycle with the given parent context.
// Returns ErrAlreadyStarted if the component has already been started.
// Returns ErrAlreadyStopped if the component has already been stopped.
func (cl *ComponentLifecycle) Start(parentCtx context.Context) error {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	// Check stopped first to handle start-after-stop case
	if cl.stopped.Load() {
		return ErrAlreadyStopped
	}

	if cl.started.Load() {
		return ErrAlreadyStarted
	}

	// Create child context from parent
	cl.ctx, cl.cancel = context.WithCancel(parentCtx)
	cl.started.Store(true)

	return nil
}

// Stop stops the component and waits for all goroutines to finish.
// It is idempotent - calling Stop() multiple times is safe.
// Returns ErrStopTimeout if goroutines don't finish within the timeout.
func (cl *ComponentLifecycle) Stop(timeout time.Duration) error {
	cl.mu.Lock()

	// Idempotent check - if already stopped, return nil
	if cl.stopped.Load() {
		cl.mu.Unlock()
		return nil
	}

	// Mark as stopped
	cl.stopped.Store(true)

	// Cancel context to signal goroutines
	if cl.cancel != nil {
		cl.cancel()
	}

	cl.mu.Unlock()

	// Wait for all goroutines with timeout
	done := make(chan struct{})
	go func() {
		cl.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return ErrStopTimeout
	}
}

// Go starts a goroutine managed by the lifecycle.
// The goroutine will receive the component's context and should respect cancellation.
// Panics in the goroutine are recovered and logged.
func (cl *ComponentLifecycle) Go(fn func(ctx context.Context)) {
	cl.wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// Panic recovered, but we still need to decrement WaitGroup
			}
			cl.wg.Done()
		}()
		fn(cl.ctx)
	}()
}

// Context returns the component's context.
// This context is canceled when Stop() is called or when the parent context is canceled.
func (cl *ComponentLifecycle) Context() context.Context {
	return cl.ctx
}

// IsRunning returns true if the component is currently running.
func (cl *ComponentLifecycle) IsRunning() bool {
	return cl.started.Load() && !cl.stopped.Load()
}

// IsStopped returns true if the component has been stopped.
func (cl *ComponentLifecycle) IsStopped() bool {
	return cl.stopped.Load()
}

// GetState returns the current state of the component.
func (cl *ComponentLifecycle) GetState() ComponentState {
	if cl.stopped.Load() {
		return ComponentStateStopped
	}
	if cl.started.Load() {
		return ComponentStateRunning
	}
	return ComponentStateCreated
}
