package core

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestComponentLifecycle_Start(t *testing.T) {
	t.Run("normal start", func(t *testing.T) {
		var cl ComponentLifecycle
		ctx := context.Background()

		err := cl.Start(ctx)
		if err != nil {
			t.Fatalf("Start() failed: %v", err)
		}

		if !cl.IsRunning() {
			t.Error("Component should be running after Start()")
		}

		if cl.Context() == nil {
			t.Error("Context() should return non-nil context")
		}
	})

	t.Run("duplicate start returns error", func(t *testing.T) {
		var cl ComponentLifecycle
		ctx := context.Background()

		err := cl.Start(ctx)
		if err != nil {
			t.Fatalf("First Start() failed: %v", err)
		}

		err = cl.Start(ctx)
		if err != ErrAlreadyStarted {
			t.Errorf("Second Start() should return ErrAlreadyStarted, got: %v", err)
		}
	})

	t.Run("start after stop returns error", func(t *testing.T) {
		var cl ComponentLifecycle
		ctx := context.Background()

		_ = cl.Start(ctx)
		_ = cl.Stop(time.Second)

		err := cl.Start(ctx)
		if err != ErrAlreadyStopped {
			t.Errorf("Start() after Stop() should return ErrAlreadyStopped, got: %v", err)
		}
	})
}

func TestComponentLifecycle_Stop(t *testing.T) {
	t.Run("normal stop", func(t *testing.T) {
		var cl ComponentLifecycle
		ctx := context.Background()

		_ = cl.Start(ctx)

		err := cl.Stop(time.Second)
		if err != nil {
			t.Fatalf("Stop() failed: %v", err)
		}

		if !cl.IsStopped() {
			t.Error("Component should be stopped after Stop()")
		}

		if cl.IsRunning() {
			t.Error("Component should not be running after Stop()")
		}
	})

	t.Run("idempotent stop", func(t *testing.T) {
		var cl ComponentLifecycle
		ctx := context.Background()

		_ = cl.Start(ctx)

		err := cl.Stop(time.Second)
		if err != nil {
			t.Fatalf("First Stop() failed: %v", err)
		}

		err = cl.Stop(time.Second)
		if err != nil {
			t.Errorf("Second Stop() should be idempotent, got error: %v", err)
		}
	})

	t.Run("stop waits for goroutines", func(t *testing.T) {
		var cl ComponentLifecycle
		ctx := context.Background()

		_ = cl.Start(ctx)

		done := make(chan bool, 1)
		cl.Go(func(ctx context.Context) {
			<-ctx.Done()
			done <- true
		})

		// Give goroutine time to start
		time.Sleep(10 * time.Millisecond)

		err := cl.Stop(2 * time.Second)
		if err != nil {
			t.Fatalf("Stop() failed: %v", err)
		}

		select {
		case <-done:
			// Goroutine completed
		case <-time.After(100 * time.Millisecond):
			t.Error("Goroutine did not complete")
		}
	})

	t.Run("stop timeout", func(t *testing.T) {
		var cl ComponentLifecycle
		ctx := context.Background()

		_ = cl.Start(ctx)

		cl.Go(func(ctx context.Context) {
			time.Sleep(2 * time.Second)
		})

		err := cl.Stop(100 * time.Millisecond)
		if err != ErrStopTimeout {
			t.Errorf("Stop() should return ErrStopTimeout, got: %v", err)
		}
	})
}

func TestComponentLifecycle_Go(t *testing.T) {
	t.Run("goroutine receives context", func(t *testing.T) {
		var cl ComponentLifecycle
		ctx := context.Background()

		_ = cl.Start(ctx)

		received := make(chan context.Context, 1)
		cl.Go(func(ctx context.Context) {
			received <- ctx
		})

		select {
		case gotCtx := <-received:
			if gotCtx == nil {
				t.Error("Goroutine received nil context")
			}
		case <-time.After(time.Second):
			t.Error("Goroutine did not execute")
		}

		_ = cl.Stop(time.Second)
	})

	t.Run("panic recovery", func(t *testing.T) {
		var cl ComponentLifecycle
		ctx := context.Background()

		_ = cl.Start(ctx)

		cl.Go(func(ctx context.Context) {
			panic("test panic")
		})

		time.Sleep(100 * time.Millisecond)

		err := cl.Stop(time.Second)
		if err != nil {
			t.Errorf("Stop() should succeed even after panic, got: %v", err)
		}
	})

	t.Run("multiple goroutines", func(t *testing.T) {
		var cl ComponentLifecycle
		ctx := context.Background()

		_ = cl.Start(ctx)

		count := 10
		var wg sync.WaitGroup
		wg.Add(count)

		for i := 0; i < count; i++ {
			cl.Go(func(ctx context.Context) {
				time.Sleep(50 * time.Millisecond)
				wg.Done()
			})
		}

		wg.Wait()

		err := cl.Stop(time.Second)
		if err != nil {
			t.Errorf("Stop() failed: %v", err)
		}
	})
}

func TestComponentLifecycle_Context(t *testing.T) {
	t.Run("context cancellation on stop", func(t *testing.T) {
		var cl ComponentLifecycle
		ctx := context.Background()

		_ = cl.Start(ctx)

		componentCtx := cl.Context()

		_ = cl.Stop(time.Second)

		select {
		case <-componentCtx.Done():
			// Context was canceled
		case <-time.After(time.Second):
			t.Error("Context should be canceled after Stop()")
		}
	})

	t.Run("parent context cancellation propagates", func(t *testing.T) {
		var cl ComponentLifecycle
		ctx, cancel := context.WithCancel(context.Background())

		_ = cl.Start(ctx)

		componentCtx := cl.Context()

		cancel()

		select {
		case <-componentCtx.Done():
			// Context was canceled
		case <-time.After(time.Second):
			t.Error("Context should be canceled when parent is canceled")
		}

		_ = cl.Stop(time.Second)
	})
}

func TestComponentLifecycle_GetState(t *testing.T) {
	var cl ComponentLifecycle

	if cl.GetState() != ComponentStateCreated {
		t.Error("Initial state should be Created")
	}

	_ = cl.Start(context.Background())

	if cl.GetState() != ComponentStateRunning {
		t.Error("State should be Running after Start()")
	}

	_ = cl.Stop(time.Second)

	if cl.GetState() != ComponentStateStopped {
		t.Error("State should be Stopped after Stop()")
	}
}

func TestComponentLifecycle_ConcurrentStart(t *testing.T) {
	var cl ComponentLifecycle
	ctx := context.Background()

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	errors := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			err := cl.Start(ctx)
			errors <- err
		}()
	}

	wg.Wait()
	close(errors)

	successCount := 0
	errorCount := 0

	for err := range errors {
		if err == nil {
			successCount++
		} else if err == ErrAlreadyStarted {
			errorCount++
		} else {
			t.Errorf("Unexpected error: %v", err)
		}
	}

	if successCount != 1 {
		t.Errorf("Expected exactly 1 successful Start(), got %d", successCount)
	}

	if errorCount != goroutines-1 {
		t.Errorf("Expected %d ErrAlreadyStarted errors, got %d", goroutines-1, errorCount)
	}

	_ = cl.Stop(time.Second)
}

func TestComponentLifecycle_ConcurrentStop(t *testing.T) {
	var cl ComponentLifecycle
	ctx := context.Background()

	_ = cl.Start(ctx)

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	errors := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			err := cl.Stop(time.Second)
			errors <- err
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		if err != nil {
			t.Errorf("Stop() should be idempotent, got error: %v", err)
		}
	}
}
