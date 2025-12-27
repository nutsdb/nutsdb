package nutsdb

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type MockComponent struct {
	name        string
	startErr    error
	stopErr     error
	startDelay  time.Duration
	stopDelay   time.Duration
	startCalled atomic.Int32
	stopCalled  atomic.Int32
}

func NewMockComponent(name string) *MockComponent {
	return &MockComponent{
		name: name,
	}
}

func (mc *MockComponent) Name() string {
	return mc.name
}

func (mc *MockComponent) Start(ctx context.Context) error {
	mc.startCalled.Add(1)
	if mc.startDelay > 0 {
		time.Sleep(mc.startDelay)
	}
	return mc.startErr
}

func (mc *MockComponent) Stop(timeout time.Duration) error {
	mc.stopCalled.Add(1)
	if mc.stopDelay > 0 {
		time.Sleep(mc.stopDelay)
	}
	return mc.stopErr
}

func (mc *MockComponent) WithStartError(err error) *MockComponent {
	mc.startErr = err
	return mc
}

func (mc *MockComponent) WithStopError(err error) *MockComponent {
	mc.stopErr = err
	return mc
}

func (mc *MockComponent) WithStartDelay(delay time.Duration) *MockComponent {
	mc.startDelay = delay
	return mc
}

func (mc *MockComponent) WithStopDelay(delay time.Duration) *MockComponent {
	mc.stopDelay = delay
	return mc
}

func (mc *MockComponent) GetStartCallCount() int32 {
	return mc.startCalled.Load()
}

func (mc *MockComponent) GetStopCallCount() int32 {
	return mc.stopCalled.Load()
}

type blockingComponent struct {
	stopStarted chan struct{}
	stopRelease chan struct{}
}

func newBlockingComponent() *blockingComponent {
	return &blockingComponent{
		stopStarted: make(chan struct{}),
		stopRelease: make(chan struct{}),
	}
}

func (bc *blockingComponent) Name() string {
	return "blocking"
}

func (bc *blockingComponent) Start(ctx context.Context) error {
	return nil
}

func (bc *blockingComponent) Stop(timeout time.Duration) error {
	close(bc.stopStarted)
	<-bc.stopRelease
	return nil
}

func TestStatusManager_InitialState(t *testing.T) {
	sm := NewStatusManager(DefaultStatusManagerConfig())

	if sm.isClosingOrClosed() {
		t.Fatal("expected StatusManager to start open for work")
	}

	select {
	case <-sm.closedCh:
		t.Fatal("expected closedCh to remain open before Close()")
	default:
	}

	select {
	case <-sm.Context().Done():
		t.Fatal("expected Context to not be cancelled before Close()")
	default:
	}
}

func TestStatusManager_StartRollbackOnFailure(t *testing.T) {
	sm := NewStatusManager(DefaultStatusManagerConfig())

	comp1 := NewMockComponent("component1")
	comp2 := NewMockComponent("component2").WithStartError(ErrDBClosed)

	if err := sm.RegisterComponent("component1", comp1); err != nil {
		t.Fatalf("Failed to register component1: %v", err)
	}
	if err := sm.RegisterComponent("component2", comp2); err != nil {
		t.Fatalf("Failed to register component2: %v", err)
	}

	if err := sm.Start(); err == nil {
		t.Fatal("Expected Start to fail when a component returns an error")
	}

	if comp1.GetStartCallCount() != 1 {
		t.Fatalf("Expected component1 Start to be called once, got %d", comp1.GetStartCallCount())
	}

	if comp1.GetStopCallCount() != 1 {
		t.Fatalf("Expected component1 Stop to be called once on rollback, got %d", comp1.GetStopCallCount())
	}
}

func TestStatusManager_ContextCancellation(t *testing.T) {
	config := StatusManagerConfig{
		ShutdownTimeout: 5 * time.Second,
	}

	sm := NewStatusManager(config)

	comp := NewMockComponent("test_component")
	if err := sm.RegisterComponent("test_component", comp); err != nil {
		t.Fatalf("Failed to register component: %v", err)
	}

	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	ctx := sm.Context()
	if ctx == nil {
		t.Fatal("Expected non-nil context")
	}

	select {
	case <-ctx.Done():
		t.Error("Context should not be cancelled before Close()")
	default:
	}

	if err := sm.Close(); err != nil {
		t.Fatalf("Failed to close StatusManager: %v", err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(1 * time.Second):
		t.Error("Context should be cancelled after Close()")
	}
}

func TestStatusManager_CloseWaitsWhenAlreadyClosing(t *testing.T) {
	config := StatusManagerConfig{
		ShutdownTimeout: 2 * time.Second,
	}
	sm := NewStatusManager(config)

	comp := newBlockingComponent()
	if err := sm.RegisterComponent("blocking", comp); err != nil {
		t.Fatalf("Failed to register component: %v", err)
	}
	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- sm.Close()
	}()

	<-comp.stopStarted

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- sm.Close()
	}()

	select {
	case <-secondDone:
		t.Fatal("Expected second Close to wait for the first to finish")
	case <-time.After(50 * time.Millisecond):
	}

	close(comp.stopRelease)

	if err := <-firstDone; err != nil {
		t.Fatalf("First Close returned error: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("Second Close returned error: %v", err)
	}
}

func TestStatusManager_CloseTimeoutsWhenGoroutinesHang(t *testing.T) {
	config := StatusManagerConfig{
		ShutdownTimeout: 50 * time.Millisecond,
	}

	sm := NewStatusManager(config)
	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	sm.Add(1)

	start := time.Now()
	err := sm.Close()
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Expected Close to return a timeout error")
	}

	if elapsed < config.ShutdownTimeout {
		t.Fatalf("Expected Close to wait at least %v, got %v", config.ShutdownTimeout, elapsed)
	}

	if elapsed > config.ShutdownTimeout+200*time.Millisecond {
		t.Fatalf("Expected Close to return near timeout, got %v", elapsed)
	}

	sm.Done()
}
