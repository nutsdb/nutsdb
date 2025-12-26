package nutsdb

import (
	"fmt"
)

// ComponentErrorHandler defines how components report startup/runtime/shutdown errors
// Note: Named differently to avoid conflicting with options.go's ErrorHandler
type ComponentErrorHandler interface {
	// HandleStartupError wraps startup errors and optionally modifies them
	HandleStartupError(component string, err error) error

	// HandleRuntimeError defers runtime recovery to callers
	HandleRuntimeError(component string, err error)

	// HandleShutdownError lets shutdown proceed even if a component reports errors
	HandleShutdownError(component string, err error)
}

// RecoveryStrategy enumerates how to react to component errors
type RecoveryStrategy int

const (
	RecoveryIgnore   RecoveryStrategy = iota // Ignore errors to keep running
	RecoveryShutdown                         // Trigger graceful shutdown
)

// String returns the textual representation of a recovery strategy
func (rs RecoveryStrategy) String() string {
	switch rs {
	case RecoveryIgnore:
		return "Ignore"
	case RecoveryShutdown:
		return "Shutdown"
	default:
		return "Unknown"
	}
}

// DefaultErrorHandler provides a fallback error handler used across components
type DefaultErrorHandler struct {
	onFatalError func(error) // Callback so fatal errors can be surfaced
}

// NewDefaultErrorHandler constructs the default handler with a fatal-error callback
func NewDefaultErrorHandler(onFatalError func(error)) *DefaultErrorHandler {
	return &DefaultErrorHandler{
		onFatalError: onFatalError,
	}
}

// HandleStartupError wraps startup errors and notifies the fatal callback
func (deh *DefaultErrorHandler) HandleStartupError(component string, err error) error {
	if err == nil {
		return nil
	}

	wrappedErr := fmt.Errorf("component %s failed to start: %w", component, err)

	// Startup failures are typically fatal, so signal the callback
	if deh.onFatalError != nil {
		deh.onFatalError(wrappedErr)
	}

	return wrappedErr
}

// HandleRuntimeError otherwise just records runtime errors for downstream recovery choices
func (deh *DefaultErrorHandler) HandleRuntimeError(component string, err error) {
	if err == nil {
		return
	}

	// Runtime errors may require recovery decisions elsewhere, so do nothing here
}

// HandleShutdownError ignores shutdown errors so other components can continue closing
func (deh *DefaultErrorHandler) HandleShutdownError(component string, err error) {
	if err == nil {
		return
	}

	// Shutdown errors should not block other components from closing
}
