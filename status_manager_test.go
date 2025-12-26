package nutsdb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// MockComponent is a lightweight component used in tests
type MockComponent struct {
	name        string
	startErr    error
	stopErr     error
	startDelay  time.Duration
	stopDelay   time.Duration
	startCalled atomic.Int32
	stopCalled  atomic.Int32
}

// NewMockComponent creates a configured mock for lifecycle assertions
func NewMockComponent(name string) *MockComponent {
	return &MockComponent{
		name: name,
	}
}

// Name returns the mock's registered component name
func (mc *MockComponent) Name() string {
	return mc.name
}

// Start records the invocation and optionally delays to simulate startup latency
func (mc *MockComponent) Start(ctx context.Context) error {
	mc.startCalled.Add(1)
	if mc.startDelay > 0 {
		time.Sleep(mc.startDelay)
	}
	return mc.startErr
}

// Stop simulates shutdown timing before returning the configured error
func (mc *MockComponent) Stop(timeout time.Duration) error {
	mc.stopCalled.Add(1)
	if mc.stopDelay > 0 {
		time.Sleep(mc.stopDelay)
	}
	return mc.stopErr
}

// WithStartError configures a startup error to test error handling
func (mc *MockComponent) WithStartError(err error) *MockComponent {
	mc.startErr = err
	return mc
}

// WithStopError configures a stop error for failure paths
func (mc *MockComponent) WithStopError(err error) *MockComponent {
	mc.stopErr = err
	return mc
}

// WithStartDelay introduces start latency to exercise concurrency
func (mc *MockComponent) WithStartDelay(delay time.Duration) *MockComponent {
	mc.startDelay = delay
	return mc
}

// WithStopDelay introduces stop latency to exercise shutdown timing
func (mc *MockComponent) WithStopDelay(delay time.Duration) *MockComponent {
	mc.stopDelay = delay
	return mc
}

// GetStartCallCount exposes how many times Start was invoked
func (mc *MockComponent) GetStartCallCount() int32 {
	return mc.startCalled.Load()
}

// GetStopCallCount exposes how many times Stop was invoked
func (mc *MockComponent) GetStopCallCount() int32 {
	return mc.stopCalled.Load()
}

// TestStatusManager_StateInitialization verifies the initial state of a new StatusManager
func TestStatusManager_StateInitialization(t *testing.T) {
	tests := []struct {
		name           string
		config         StatusManagerConfig
		expectedStatus Status
	}{
		{
			name:           "default config initialization",
			config:         DefaultStatusManagerConfig(),
			expectedStatus: StatusInitializing,
		},
		{
			name: "custom config initialization",
			config: StatusManagerConfig{
				ShutdownTimeout: 10 * time.Second,
			},
			expectedStatus: StatusInitializing,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := NewStatusManager(tt.config)
			defer sm.Close()

			// Assert the initial state matches expectations
			if status := sm.Status(); status != tt.expectedStatus {
				t.Errorf("Expected initial status %v, got %v", tt.expectedStatus, status)
			}

			// IsOperational should be false before startup
			if sm.IsOperational() {
				t.Error("Expected IsOperational to return false for Initializing state")
			}

			// The manager should have a non-nil context
			if sm.Context() == nil {
				t.Error("Expected Context to be non-nil")
			}

			// The context should not be cancelled yet
			select {
			case <-sm.Context().Done():
				t.Error("Expected Context to not be cancelled initially")
			default:
				// Context remains alive as expected
			}
		})
	}
}

// TestStatusManager_ConcurrentStateQueries ensures concurrent state reads stay consistent
func TestStatusManager_ConcurrentStateQueries(t *testing.T) {
	config := StatusManagerConfig{
		ShutdownTimeout: 5 * time.Second,
	}

	sm := NewStatusManager(config)
	defer sm.Close()

	// Register mock components to bring the manager up
	comp1 := NewMockComponent("component1")
	comp2 := NewMockComponent("component2")
	comp3 := NewMockComponent("component3")

	if err := sm.RegisterComponent("component1", comp1); err != nil {
		t.Fatalf("Failed to register component1: %v", err)
	}
	if err := sm.RegisterComponent("component2", comp2); err != nil {
		t.Fatalf("Failed to register component2: %v", err)
	}
	if err := sm.RegisterComponent("component3", comp3); err != nil {
		t.Fatalf("Failed to register component3: %v", err)
	}

	// Start the manager to reach open state
	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	// Confirm the status is Open after Start()
	if status := sm.Status(); status != StatusOpen {
		t.Errorf("Expected status Open after Start(), got %v", status)
	}

	// Perform concurrent status queries
	const numGoroutines = 100
	const queriesPerGoroutine = 100

	var wg sync.WaitGroup
	statusResults := make([]Status, numGoroutines*queriesPerGoroutine)
	operationalResults := make([]bool, numGoroutines*queriesPerGoroutine)

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < queriesPerGoroutine; j++ {
				idx := goroutineID*queriesPerGoroutine + j
				statusResults[idx] = sm.Status()
				operationalResults[idx] = sm.IsOperational()
			}
		}(i)
	}

	wg.Wait()

	// Validate that every query saw the same status/op state
	expectedStatus := StatusOpen
	expectedOperational := true

	for i, status := range statusResults {
		if status != expectedStatus {
			t.Errorf("Query %d: expected status %v, got %v", i, expectedStatus, status)
		}
	}

	for i, operational := range operationalResults {
		if operational != expectedOperational {
			t.Errorf("Query %d: expected operational %v, got %v", i, expectedOperational, operational)
		}
	}
}

// TestStatusManager_ConcurrentStateQueriesDuringTransition exercises state reads during transitions
func TestStatusManager_ConcurrentStateQueriesDuringTransition(t *testing.T) {
	config := StatusManagerConfig{
		ShutdownTimeout: 5 * time.Second,
	}

	sm := NewStatusManager(config)

	// Register a delayed component so startup takes longer
	slowComp := NewMockComponent("slow_component").WithStartDelay(200 * time.Millisecond)
	if err := sm.RegisterComponent("slow_component", slowComp); err != nil {
		t.Fatalf("Failed to register component: %v", err)
	}

	// Start StatusManager in the background so queries overlap with startup
	startDone := make(chan error, 1)
	go func() {
		startDone <- sm.Start()
	}()

	// Query state concurrently while startup is still running
	const numGoroutines = 50
	var wg sync.WaitGroup
	statusCounts := make(map[Status]*atomic.Int32)
	statusCounts[StatusInitializing] = &atomic.Int32{}
	statusCounts[StatusOpen] = &atomic.Int32{}

	stopQuerying := make(chan struct{})

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			// Keep querying until we are asked to stop
			for {
				select {
				case <-stopQuerying:
					return
				default:
					status := sm.Status()
					if counter, ok := statusCounts[status]; ok {
						counter.Add(1)
					}
					time.Sleep(1 * time.Millisecond)
				}
			}
		}()
	}

	// Wait for the simulated startup to finish
	if err := <-startDone; err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	// Keep querying briefly after startup to ensure we observed Open
	time.Sleep(50 * time.Millisecond)

	// Stop the background query goroutine
	close(stopQuerying)
	wg.Wait()

	// Verify the final status is Open
	if status := sm.Status(); status != StatusOpen {
		t.Errorf("Expected final status Open, got %v", status)
	}

	// Confirm that we observed the expected operational transition
	initCount := statusCounts[StatusInitializing].Load()
	openCount := statusCounts[StatusOpen].Load()

	if initCount == 0 {
		t.Error("Expected to observe Initializing state during startup")
	}
	if openCount == 0 {
		t.Error("Expected to observe Open state after startup")
	}

	t.Logf("Observed states: Initializing=%d, Open=%d", initCount, openCount)

	// Clean up resources
	sm.Close()
}

// TestStatusManager_StateQueryConsistency checks consistency of repeated status queries
func TestStatusManager_StateQueryConsistency(t *testing.T) {
	config := StatusManagerConfig{
		ShutdownTimeout: 5 * time.Second,
	}

	sm := NewStatusManager(config)
	defer sm.Close()

	// Register a component to keep the manager running
	comp := NewMockComponent("test_component")
	if err := sm.RegisterComponent("test_component", comp); err != nil {
		t.Fatalf("Failed to register component: %v", err)
	}

	// Start the manager
	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	// Run many queries simultaneously to verify consistency
	const numQueries = 1000
	results := make([]Status, numQueries)

	// Use a barrier so all goroutines start querying at the same time
	var startBarrier sync.WaitGroup
	startBarrier.Add(1)

	var wg sync.WaitGroup
	wg.Add(numQueries)

	for i := 0; i < numQueries; i++ {
		go func(idx int) {
			defer wg.Done()
			startBarrier.Wait() // Wait until all goroutines are ready
			results[idx] = sm.Status()
		}(i)
	}

	// Release all goroutines to begin queries
	startBarrier.Done()
	wg.Wait()

	// Verify each query saw the same status
	firstResult := results[0]
	for i, result := range results {
		if result != firstResult {
			t.Errorf("Query %d: expected status %v, got %v (inconsistent with first query)", i, firstResult, result)
		}
	}

	t.Logf("All %d concurrent queries returned consistent status: %v", numQueries, firstResult)
}

// TestStatusManager_IsOperationalConsistency ensures IsOperational behaves consistently
func TestStatusManager_IsOperationalConsistency(t *testing.T) {
	config := StatusManagerConfig{
		ShutdownTimeout: 5 * time.Second,
	}

	sm := NewStatusManager(config)
	defer sm.Close()

	// Register a mock component to exercise state transitions
	comp := NewMockComponent("test_component")
	if err := sm.RegisterComponent("test_component", comp); err != nil {
		t.Fatalf("Failed to register component: %v", err)
	}

	// Test IsOperational in various states
	testCases := []struct {
		name                string
		setupFunc           func() error
		expectedOperational bool
		expectedStatus      Status
	}{
		{
			name:                "Initializing state",
			setupFunc:           func() error { return nil },
			expectedOperational: false,
			expectedStatus:      StatusInitializing,
		},
		{
			name: "Open state",
			setupFunc: func() error {
				return sm.Start()
			},
			expectedOperational: true,
			expectedStatus:      StatusOpen,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.setupFunc(); err != nil {
				t.Fatalf("Setup failed: %v", err)
			}

			// Query IsOperational concurrently
			const numGoroutines = 100
			results := make([]bool, numGoroutines)
			var wg sync.WaitGroup

			wg.Add(numGoroutines)
			for i := 0; i < numGoroutines; i++ {
				go func(idx int) {
					defer wg.Done()
					results[idx] = sm.IsOperational()
				}(i)
			}

			wg.Wait()

			// Confirm every result matched the expected operational state
			for i, result := range results {
				if result != tc.expectedOperational {
					t.Errorf("Query %d: expected IsOperational=%v, got %v", i, tc.expectedOperational, result)
				}
			}

			// Verify the status matches the expectation
			if status := sm.Status(); status != tc.expectedStatus {
				t.Errorf("Expected status %v, got %v", tc.expectedStatus, status)
			}
		})
	}
}

// TestStatusManager_ContextCancellation ensures the context cancels as part of shutdown
func TestStatusManager_ContextCancellation(t *testing.T) {
	config := StatusManagerConfig{
		ShutdownTimeout: 5 * time.Second,
	}

	sm := NewStatusManager(config)

	// Register a component so the manager has work
	comp := NewMockComponent("test_component")
	if err := sm.RegisterComponent("test_component", comp); err != nil {
		t.Fatalf("Failed to register component: %v", err)
	}

	// Start the manager
	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start StatusManager: %v", err)
	}

	// Grab the manager's context for later cancellation checks
	ctx := sm.Context()
	if ctx == nil {
		t.Fatal("Expected non-nil context")
	}

	// Confirm the context is still active before closing
	select {
	case <-ctx.Done():
		t.Error("Context should not be cancelled before Close()")
	default:
		// Expected path: context still running
	}

	// Close the manager to trigger context cancellation
	if err := sm.Close(); err != nil {
		t.Fatalf("Failed to close StatusManager: %v", err)
	}

	// Verify the context was cancelled after Close()
	select {
	case <-ctx.Done():
		// Expected path: context was cancelled
	case <-time.After(1 * time.Second):
		t.Error("Context should be cancelled after Close()")
	}
}
