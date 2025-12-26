package nutsdb

import (
	"context"
	"sync/atomic"
	"time"
)

// Component defines the standard lifecycle interface for managed components
type Component interface {
	// Name returns the component's registered name
	Name() string

	// Start launches the component using the provided context for cancellation
	Start(ctx context.Context) error

	// Stop shuts down the component within the given timeout without blocking others
	Stop(timeout time.Duration) error
}

// ComponentWrapper tracks a component and its current status
type ComponentWrapper struct {
	component Component
	status    atomic.Value // ComponentStatus
}

// NewComponentWrapper prepares a wrapper around a component
func NewComponentWrapper(component Component) *ComponentWrapper {
	cw := &ComponentWrapper{
		component: component,
	}
	cw.status.Store(ComponentStatusStopped)
	return cw
}

// GetComponent returns the wrapped component
func (cw *ComponentWrapper) GetComponent() Component {
	return cw.component
}

// GetStatus reports the current ComponentStatus
func (cw *ComponentWrapper) GetStatus() ComponentStatus {
	return cw.status.Load().(ComponentStatus)
}

// SetStatus updates the stored status value
func (cw *ComponentWrapper) SetStatus(status ComponentStatus) {
	cw.status.Store(status)
}
