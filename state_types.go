package nutsdb

import (
	"sync"
	"time"
)

// Note: Status type and constants are already defined in status_manager.go
// We reuse those definitions for the new state management system

// ComponentStatus describes the lifecycle state of a component
type ComponentStatus int

const (
	ComponentStatusStopped ComponentStatus = iota
	ComponentStatusStarting
	ComponentStatusRunning
	ComponentStatusStopping
	ComponentStatusFailed
)

// String returns the textual representation of the component status
func (cs ComponentStatus) String() string {
	switch cs {
	case ComponentStatusStopped:
		return "Stopped"
	case ComponentStatusStarting:
		return "Starting"
	case ComponentStatusRunning:
		return "Running"
	case ComponentStatusStopping:
		return "Stopping"
	case ComponentStatusFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// ShutdownProgress tracks shutdown progress so callers can observe it
type ShutdownProgress struct {
	TotalComponents   int
	StoppedComponents int
	FailedComponents  []string
	StartTime         time.Time
	CurrentPhase      string
	mu                sync.RWMutex
}

// NewShutdownProgress builds a tracker for the given component count
func NewShutdownProgress(totalComponents int) *ShutdownProgress {
	return &ShutdownProgress{
		TotalComponents:   totalComponents,
		StoppedComponents: 0,
		FailedComponents:  make([]string, 0),
		StartTime:         time.Now(),
		CurrentPhase:      "Initializing",
	}
}

// IncrementStopped records another stopped component
func (sp *ShutdownProgress) IncrementStopped() {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.StoppedComponents++
}

// AddFailedComponent notes components that failed during shutdown
func (sp *ShutdownProgress) AddFailedComponent(componentName string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.FailedComponents = append(sp.FailedComponents, componentName)
}

// SetCurrentPhase records the current shutdown phase
func (sp *ShutdownProgress) SetCurrentPhase(phase string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.CurrentPhase = phase
}

// IsComplete reports when shutdown has stopped all components
func (sp *ShutdownProgress) IsComplete() bool {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return sp.StoppedComponents >= sp.TotalComponents
}

// GetFailedComponents returns a snapshot of failed components
func (sp *ShutdownProgress) GetFailedComponents() []string {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	result := make([]string, len(sp.FailedComponents))
	copy(result, sp.FailedComponents)
	return result
}

// GetCurrentPhase returns the current shutdown phase label
func (sp *ShutdownProgress) GetCurrentPhase() string {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return sp.CurrentPhase
}
