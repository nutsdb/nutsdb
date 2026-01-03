package nutsdb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWatcher_Run(t *testing.T) {
	t.Run("should execute watchingFunc and set ready", func(t *testing.T) {
		executed := false
		watcher := &Watcher{
			readyCh: make(chan struct{}),
			watchingFunc: func() error {
				executed = true
				return nil
			},
			isReady: false,
		}

		err := watcher.Run()

		assert.NoError(t, err)
		assert.True(t, executed, "watchingFunc should have been executed")
		assert.True(t, watcher.isReady, "watcher should be marked as ready")
	})

	t.Run("should close readyCh when started", func(t *testing.T) {
		watcher := &Watcher{
			readyCh: make(chan struct{}),
			watchingFunc: func() error {
				return nil
			},
			isReady: false,
		}

		go func() {
			_ = watcher.Run()
		}()

		// Wait for readyCh to be closed
		select {
		case <-watcher.readyCh:
			// Success - channel was closed
		case <-time.After(1 * time.Second):
			t.Fatal("readyCh should have been closed")
		}
	})

	t.Run("should return error from watchingFunc", func(t *testing.T) {
		expectedErr := errors.New("watch error")
		watcher := &Watcher{
			readyCh: make(chan struct{}),
			watchingFunc: func() error {
				return expectedErr
			},
			isReady: false,
		}

		err := watcher.Run()

		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("should be idempotent - calling Run twice should not execute twice", func(t *testing.T) {
		callCount := 0
		var mu sync.Mutex

		watcher := &Watcher{
			readyCh: make(chan struct{}),
			watchingFunc: func() error {
				mu.Lock()
				callCount++
				mu.Unlock()
				time.Sleep(50 * time.Millisecond)
				return nil
			},
			isReady: false,
		}

		// First call
		go func() {
			_ = watcher.Run()
		}()
		time.Sleep(10 * time.Millisecond) // Let it start

		// Second call should return immediately without executing
		err := watcher.Run()

		assert.NoError(t, err)
		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		assert.Equal(t, 1, callCount, "watchingFunc should only be called once")
		mu.Unlock()
	})

	t.Run("should handle blocking watchingFunc", func(t *testing.T) {
		done := make(chan struct{})
		watcher := &Watcher{
			readyCh: make(chan struct{}),
			watchingFunc: func() error {
				<-done // Block until done is closed
				return nil
			},
			isReady: false,
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := watcher.Run()
			assert.NoError(t, err)
		}()

		// Wait for ready
		select {
		case <-watcher.readyCh:
			assert.True(t, watcher.isReady)
		case <-time.After(1 * time.Second):
			t.Fatal("watcher should be ready")
		}

		// Close done to unblock
		close(done)
		wg.Wait()
	})
}

func TestWatcher_WaitReady(t *testing.T) {
	t.Run("should return nil when watcher is ready", func(t *testing.T) {
		watcher := &Watcher{
			readyCh: make(chan struct{}),
			watchingFunc: func() error {
				return nil
			},
			isReady: false,
		}

		// Start watcher in background
		go func() {
			err := watcher.Run()
			assert.NoError(t, err)
		}()

		// WaitReady should return nil quickly
		err := watcher.WaitReady(5 * time.Second)
		assert.NoError(t, err)
	})

	t.Run("should return timeout error when timeout expires", func(t *testing.T) {
		watcher := &Watcher{
			readyCh: make(chan struct{}), // Never closed
			watchingFunc: func() error {
				return nil
			},
			isReady: false,
		}

		// Don't start the watcher, so readyCh never closes
		start := time.Now()
		err := watcher.WaitReady(100 * time.Millisecond)

		elapsed := time.Since(start)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "timeout")
		assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond)
	})

	t.Run("should return immediately if already ready", func(t *testing.T) {
		readyCh := make(chan struct{})
		close(readyCh) // Pre-close the channel

		watcher := &Watcher{
			readyCh: readyCh,
			watchingFunc: func() error {
				return nil
			},
			isReady: true,
		}

		start := time.Now()
		err := watcher.WaitReady(5 * time.Second)
		elapsed := time.Since(start)

		assert.NoError(t, err)
		assert.Less(t, elapsed, 100*time.Millisecond, "should return immediately")
	})

	t.Run("should work with concurrent Run call", func(t *testing.T) {
		watcher := &Watcher{
			readyCh: make(chan struct{}),
			watchingFunc: func() error {
				time.Sleep(100 * time.Millisecond)
				return nil
			},
			isReady: false,
		}

		// Start Run and WaitReady concurrently
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = watcher.Run()
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			err := watcher.WaitReady(5 * time.Second)
			require.NoError(t, err)
		}()

		wg.Wait()
	})
}

func TestWatcher_ThreadSafety(t *testing.T) {
	t.Run("concurrent Run calls should be safe", func(t *testing.T) {
		callCount := 0
		var mu sync.Mutex

		watcher := &Watcher{
			readyCh: make(chan struct{}),
			watchingFunc: func() error {
				mu.Lock()
				callCount++
				mu.Unlock()
				time.Sleep(50 * time.Millisecond)
				return nil
			},
			isReady: false,
		}

		var wg sync.WaitGroup
		// Start multiple goroutines calling Run
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := watcher.Run()
				assert.NoError(t, err)
			}()
		}

		wg.Wait()

		mu.Lock()
		assert.Equal(t, 1, callCount, "watchingFunc should only be called once despite concurrent calls")
		mu.Unlock()
	})

	t.Run("concurrent WaitReady and Run should work", func(t *testing.T) {
		watcher := &Watcher{
			readyCh: make(chan struct{}),
			watchingFunc: func() error {
				time.Sleep(50 * time.Millisecond)
				return nil
			},
			isReady: false,
		}

		var wg sync.WaitGroup

		// Multiple WaitReady calls
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := watcher.WaitReady(2 * time.Second)
				assert.NoError(t, err)
			}()
		}

		// One Run call
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond) // Slight delay to ensure WaitReady starts first
			err := watcher.Run()
			assert.NoError(t, err)
		}()

		wg.Wait()
	})
}

func TestWatcher_Integration(t *testing.T) {
	t.Run("typical usage pattern", func(t *testing.T) {
		messageReceived := make(chan string, 1)
		watcher := &Watcher{
			readyCh: make(chan struct{}),
			watchingFunc: func() error {
				// Simulate receiving a message
				time.Sleep(50 * time.Millisecond)
				messageReceived <- "test message"
				return nil
			},
			isReady: false,
		}

		// Start watcher
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := watcher.Run()
			assert.NoError(t, err)
		}()

		// Wait for ready
		err := watcher.WaitReady(1 * time.Second)
		require.NoError(t, err)

		// Wait for message
		select {
		case msg := <-messageReceived:
			assert.Equal(t, "test message", msg)
		case <-time.After(2 * time.Second):
			t.Fatal("should have received message")
		}

		wg.Wait()
	})

	t.Run("watcher with context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		started := make(chan struct{})
		watcher := &Watcher{
			readyCh: make(chan struct{}),
			watchingFunc: func() error {
				close(started)
				<-ctx.Done()
				return ctx.Err()
			},
			isReady: false,
		}

		var wg sync.WaitGroup
		var runErr error

		wg.Add(1)
		go func() {
			defer wg.Done()
			runErr = watcher.Run()
		}()

		// Wait for watcher to start
		<-started

		// Cancel context
		cancel()

		wg.Wait()

		assert.ErrorIs(t, runErr, context.Canceled)
	})
}
