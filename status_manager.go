package nutsdb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Status represents the lifecycle state of the database
type Status int

const (
	StatusInitializing Status = iota
	StatusOpen
	StatusClosing
	StatusClosed
)

// StatusManager orchestrates the lifecycle and state of the database and its components
type StatusManager struct {
	// state machine
	state atomic.Value // Status

	// component registry and ordering guard
	components   map[string]*ComponentWrapper
	componentsMu sync.RWMutex // Ensures ordering while registering components

	// Maintains component names in registration order
	componentNames []string

	// Controls lifecycle cancellation
	ctx    context.Context
	cancel context.CancelFunc

	// Coordinates running goroutines
	wg sync.WaitGroup

	// Configuration for shutdown behavior
	config StatusManagerConfig

	// Error handling strategy
	errorHandler ComponentErrorHandler

	// Tracks shutdown progress
	shutdownProgress *ShutdownProgress
}

// StatusManagerConfig holds configuration choices for StatusManager
type StatusManagerConfig struct {
	ShutdownTimeout time.Duration // Timeout used when closing components
}

// DefaultStatusManagerConfig provides the default settings
func DefaultStatusManagerConfig() StatusManagerConfig {
	return StatusManagerConfig{
		ShutdownTimeout: 30 * time.Second,
	}
}

// NewStatusManager builds a new manager with the provided configuration
func NewStatusManager(config StatusManagerConfig) *StatusManager {
	ctx, cancel := context.WithCancel(context.Background())

	sm := &StatusManager{
		componentNames:   make([]string, 0),
		components:       make(map[string]*ComponentWrapper),
		ctx:              ctx,
		cancel:           cancel,
		config:           config,
		errorHandler:     NewDefaultErrorHandler(nil),
		shutdownProgress: nil,
	}

	// Start in Initializing so Start() can proceed
	sm.state.Store(StatusInitializing)

	return sm
}

// Status returns the current database status
func (sm *StatusManager) Status() Status {
	if v := sm.state.Load(); v != nil {
		return v.(Status)
	}
	return StatusClosed
}

// IsOperational reports whether the database is ready for traffic (only true while Open)
func (sm *StatusManager) IsOperational() bool {
	return sm.Status() == StatusOpen
}

// Context provides the cancellation context that components should monitor for shutdown
func (sm *StatusManager) Context() context.Context {
	return sm.ctx
}

// String returns the human-readable form of a Status
func (s Status) String() string {
	switch s {
	case StatusInitializing:
		return "Initializing"
	case StatusOpen:
		return "Open"
	case StatusClosing:
		return "Closing"
	case StatusClosed:
		return "Closed"
	default:
		return "Unknown"
	}
}

// RegisterComponent adds a component so StatusManager can manage it
func (sm *StatusManager) RegisterComponent(name string, component Component) error {
	sm.componentsMu.Lock()
	defer sm.componentsMu.Unlock()

	// Prevent duplicate component registration by name
	if _, exists := sm.components[name]; exists {
		return fmt.Errorf("component %s already registered", name)
	}

	// Wrap and store the component
	wrapper := NewComponentWrapper(component)
	sm.components[name] = wrapper
	sm.componentNames = append(sm.componentNames, name)

	return nil
}

// getComponentWrapper retrieves a registered wrapper (internal helper)
func (sm *StatusManager) getComponentWrapper(name string) (*ComponentWrapper, error) {
	sm.componentsMu.RLock()
	defer sm.componentsMu.RUnlock()

	wrapper, ok := sm.components[name]
	if !ok {
		return nil, fmt.Errorf("component %s not found", name)
	}

	return wrapper, nil
}

// getAllComponents returns the registered names in order (internal helper)
func (sm *StatusManager) getAllComponents() []string {
	sm.componentsMu.RLock()
	defer sm.componentsMu.RUnlock()

	// Copy to avoid concurrent mutation
	names := make([]string, len(sm.componentNames))
	copy(names, sm.componentNames)
	return names
}

// transitionTo performs a state transition after validating it follows allowed paths
func (sm *StatusManager) transitionTo(toState Status) error {
	fromState := sm.Status()

	// Confirm the transition is valid before updating state
	if !sm.isValidTransition(fromState, toState) {
		return fmt.Errorf("invalid state transition from %s to %s", fromState, toState)
	}

	// Store the new state after validation
	sm.state.Store(toState)
	return nil
}

// isValidTransition enforces allowed lifecycle transitions
// Rules:
// - Initializing -> Open (when all components start)
// - Open -> Closing (when Close is called)
// - Closing -> Closed (after every component stops)
// - Closed -> Closed (idempotent, can close multiple times)
func (sm *StatusManager) isValidTransition(from, to Status) bool {
	switch from {
	case StatusInitializing:
		return to == StatusOpen
	case StatusOpen:
		return to == StatusClosing
	case StatusClosing:
		return to == StatusClosed
	case StatusClosed:
		return to == StatusClosed // idempotent
	default:
		return false
	}
}

// Start initializes every registered component in order and rolls back on failure
func (sm *StatusManager) Start() error {
	currentStatus := sm.Status()
	if currentStatus != StatusInitializing {
		return fmt.Errorf("cannot start: current status is %s, expected Initializing", currentStatus)
	}

	// Fetch names in registration order for deterministic startup
	componentNames := sm.getAllComponents()

	// Track started components so we can roll back on failure
	startedComponents := make([]string, 0, len(componentNames))

	// Start components in registration order
	for _, name := range componentNames {
		wrapper, err := sm.getComponentWrapper(name)
		if err != nil {
			sm.rollbackStartup(startedComponents)
			return err
		}

		component := wrapper.GetComponent()

		wrapper.SetStatus(ComponentStatusStarting)

		// Start the component to transition it to running
		if err := component.Start(sm.ctx); err != nil {
			wrapper.SetStatus(ComponentStatusFailed)

			// Report the startup error via the handler
			wrappedErr := sm.errorHandler.HandleStartupError(name, err)

			// Roll back the components that already started
			sm.rollbackStartup(startedComponents)

			return wrappedErr
		}

		// Mark the wrapper as running
		wrapper.SetStatus(ComponentStatusRunning)
		startedComponents = append(startedComponents, name)
	}

	// All components started, transition to Open
	return sm.transitionTo(StatusOpen)
}

// rollbackStartup stops started components in reverse order to undo partial start-ups
func (sm *StatusManager) rollbackStartup(startedComponents []string) {
	// Stop components in reverse order of startup
	for i := len(startedComponents) - 1; i >= 0; i-- {
		name := startedComponents[i]
		wrapper, err := sm.getComponentWrapper(name)
		if err != nil {
			continue
		}

		component := wrapper.GetComponent()
		wrapper.SetStatus(ComponentStatusStopping)

		// Use a shorter timeout for the rollback stop
		if err := component.Stop(5 * time.Second); err != nil {
			wrapper.SetStatus(ComponentStatusFailed)
		} else {
			wrapper.SetStatus(ComponentStatusStopped)
		}
	}
}

// Close terminates all components in reverse registration order for graceful shutdown
func (sm *StatusManager) Close() error {
	// Use mutex to prevent concurrent Close() calls from racing
	sm.componentsMu.Lock()
	currentStatus := sm.Status()

	// Idempotent: if already Closed, return immediately
	if currentStatus == StatusClosed {
		sm.componentsMu.Unlock()
		return nil
	}

	// If already closing, wait for completion
	if currentStatus == StatusClosing {
		sm.componentsMu.Unlock()
		return sm.waitForClosed()
	}

	// Transition to Closing state while holding the lock
	if err := sm.transitionTo(StatusClosing); err != nil {
		sm.componentsMu.Unlock()
		return err
	}
	sm.componentsMu.Unlock()

	// Cancel the shared context to signal components to stop
	sm.cancel()

	// Collect component names and reverse them so shutdown happens in reverse registration order
	componentNames := sm.getAllComponents()

	// Reverse the list to compute shutdown order
	shutdownOrder := make([]string, len(componentNames))
	for i, name := range componentNames {
		shutdownOrder[len(componentNames)-1-i] = name
	}

	// Initialize shutdown progress tracking
	sm.shutdownProgress = NewShutdownProgress(len(shutdownOrder))
	sm.shutdownProgress.SetCurrentPhase("Stopping components")

	// Create a timed context to bound shutdown duration
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), sm.config.ShutdownTimeout)

	// Track shutdown completion via a channel
	doneCh := make(chan struct{})
	go func() {
		sm.shutdownComponents(shutdownCtx, shutdownOrder)
		close(doneCh)
	}()

	// Wait for shutdown completion or timeout
	select {
	case <-doneCh:
	case <-shutdownCtx.Done():
		sm.shutdownProgress.SetCurrentPhase("Timeout - forcing closure")
	}

	shutdownCancel()

	// Wait for any remaining goroutines to finish
	sm.wg.Wait()

	// Transition to Closed after cleanup
	sm.transitionTo(StatusClosed)

	return nil
}

// shutdownComponents shuts down components following the provided order
func (sm *StatusManager) shutdownComponents(ctx context.Context, order []string) {
	// Handle empty component list
	if len(order) == 0 {
		sm.shutdownProgress.SetCurrentPhase("Complete")
		return
	}

	deadline, hasDeadline := ctx.Deadline()

	for idx, name := range order {
		select {
		case <-ctx.Done():
			sm.shutdownProgress.SetCurrentPhase("Timeout - forcing closure")
			return
		default:
		}

		wrapper, err := sm.getComponentWrapper(name)
		if err != nil {
			sm.shutdownProgress.AddFailedComponent(name)
			sm.shutdownProgress.IncrementStopped()
			continue
		}

		component := wrapper.GetComponent()
		wrapper.SetStatus(ComponentStatusStopping)

		// Dedicate a slice of the remaining timeout to this component
		remainingTimeout := sm.config.ShutdownTimeout
		if hasDeadline {
			remainingTimeout = time.Until(deadline)
		}
		if remainingTimeout <= 0 {
			wrapper.SetStatus(ComponentStatusFailed)
			sm.errorHandler.HandleShutdownError(name, context.DeadlineExceeded)
			sm.shutdownProgress.AddFailedComponent(name)
			sm.shutdownProgress.IncrementStopped()
			continue
		}

		remainingComponents := len(order) - idx
		componentTimeout := remainingTimeout / time.Duration(remainingComponents)
		if componentTimeout <= 0 {
			componentTimeout = remainingTimeout
		}

		// Attempt to stop regardless of error so shutdown continues
		if err := component.Stop(componentTimeout); err != nil {
			wrapper.SetStatus(ComponentStatusFailed)
			sm.errorHandler.HandleShutdownError(name, err)
			sm.shutdownProgress.AddFailedComponent(name)
		} else {
			wrapper.SetStatus(ComponentStatusStopped)
		}

		sm.shutdownProgress.IncrementStopped()
	}

	sm.shutdownProgress.SetCurrentPhase("Complete")
}

// waitForClosed polls until the manager reaches Closed status
func (sm *StatusManager) waitForClosed() error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(sm.config.ShutdownTimeout)

	for {
		select {
		case <-ticker.C:
			if sm.Status() == StatusClosed {
				return nil
			}
		case <-timeout:
			return fmt.Errorf("timeout waiting for StatusManager to close")
		}
	}
}

// GetShutdownProgress returns the shutdown progress tracker (nil if shutdown hasn't started)
func (sm *StatusManager) GetShutdownProgress() *ShutdownProgress {
	return sm.shutdownProgress
}

// Add adjusts the internal WaitGroup so components can register goroutines
func (sm *StatusManager) Add(delta int) {
	sm.wg.Add(delta)
}

// Done decrements the WaitGroup once a component goroutine finishes
func (sm *StatusManager) Done() {
	sm.wg.Done()
}
