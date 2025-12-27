package nutsdb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type StatusManager struct {
	components     map[string]Component
	componentsMu   sync.RWMutex
	componentNames []string
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	config         StatusManagerConfig
	startMu        sync.Mutex
	started        bool
	closing        atomic.Bool
	closed         atomic.Bool
	closedCh       chan struct{}
	closeErrMu     sync.Mutex
	closeErr       error
}

type StatusManagerConfig struct {
	ShutdownTimeout time.Duration
}

func DefaultStatusManagerConfig() StatusManagerConfig {
	return StatusManagerConfig{
		ShutdownTimeout: 30 * time.Second,
	}
}

func NewStatusManager(config StatusManagerConfig) *StatusManager {
	ctx, cancel := context.WithCancel(context.Background())

	sm := &StatusManager{
		componentNames: make([]string, 0),
		components:     make(map[string]Component),
		ctx:            ctx,
		cancel:         cancel,
		config:         config,
		closedCh:       make(chan struct{}),
	}

	return sm
}

func (sm *StatusManager) Context() context.Context {
	return sm.ctx
}

func (sm *StatusManager) RegisterComponent(name string, component Component) error {
	sm.componentsMu.Lock()
	defer sm.componentsMu.Unlock()

	if _, exists := sm.components[name]; exists {
		return fmt.Errorf("component %s already registered", name)
	}

	sm.components[name] = component
	sm.componentNames = append(sm.componentNames, name)

	return nil
}

func (sm *StatusManager) getComponent(name string) (Component, error) {
	sm.componentsMu.RLock()
	defer sm.componentsMu.RUnlock()

	component, ok := sm.components[name]
	if !ok {
		return nil, fmt.Errorf("component %s not found", name)
	}

	return component, nil
}

func (sm *StatusManager) getAllComponents() []string {
	sm.componentsMu.RLock()
	defer sm.componentsMu.RUnlock()

	names := make([]string, len(sm.componentNames))
	copy(names, sm.componentNames)
	return names
}

func (sm *StatusManager) Start() error {
	if sm.isClosingOrClosed() {
		return ErrDBClosed
	}

	sm.startMu.Lock()
	defer sm.startMu.Unlock()

	if sm.started {
		return fmt.Errorf("status manager already started")
	}

	componentNames := sm.getAllComponents()

	startedComponents := make([]string, 0, len(componentNames))

	for _, name := range componentNames {
		component, err := sm.getComponent(name)
		if err != nil {
			sm.rollbackStartup(startedComponents)
			return err
		}

		if err := component.Start(sm.ctx); err != nil {
			sm.rollbackStartup(startedComponents)

			return fmt.Errorf("component %s failed to start: %w", name, err)
		}

		startedComponents = append(startedComponents, name)
	}

	sm.started = true
	return nil
}

func (sm *StatusManager) rollbackStartup(startedComponents []string) {
	for i := len(startedComponents) - 1; i >= 0; i-- {
		name := startedComponents[i]
		component, err := sm.getComponent(name)
		if err != nil {
			continue
		}

		_ = component.Stop(5 * time.Second)
	}
}

func (sm *StatusManager) Close() error {
	if sm.closed.Load() {
		return sm.loadCloseErr()
	}

	if !sm.closing.CompareAndSwap(false, true) {
		<-sm.closedCh
		return sm.loadCloseErr()
	}

	err := sm.close()
	sm.setCloseErr(err)
	sm.closed.Store(true)
	close(sm.closedCh)

	return err
}

func (sm *StatusManager) close() error {
	sm.cancel()

	componentNames := sm.getAllComponents()

	shutdownOrder := make([]string, len(componentNames))
	for i, name := range componentNames {
		shutdownOrder[len(componentNames)-1-i] = name
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), sm.config.ShutdownTimeout)
	deadline, _ := shutdownCtx.Deadline()

	doneCh := make(chan struct{})
	go func() {
		sm.shutdownComponents(shutdownCtx, shutdownOrder)
		close(doneCh)
	}()

	var shutdownErr error
	select {
	case <-doneCh:
	case <-shutdownCtx.Done():
		shutdownErr = fmt.Errorf("timeout stopping components")
	}

	shutdownCancel()

	if err := sm.waitForGoroutines(deadline); err != nil {
		if shutdownErr != nil {
			shutdownErr = fmt.Errorf("%v; %w", shutdownErr, err)
		} else {
			shutdownErr = err
		}
	}

	return shutdownErr
}

func (sm *StatusManager) shutdownComponents(ctx context.Context, order []string) {
	if len(order) == 0 {
		return
	}

	deadline, hasDeadline := ctx.Deadline()

	for idx, name := range order {
		select {
		case <-ctx.Done():
			return
		default:
		}

		component, err := sm.getComponent(name)
		if err != nil {
			continue
		}

		remainingTimeout := sm.config.ShutdownTimeout
		if hasDeadline {
			remainingTimeout = time.Until(deadline)
		}
		if remainingTimeout <= 0 {
			continue
		}

		remainingComponents := len(order) - idx
		componentTimeout := remainingTimeout / time.Duration(remainingComponents)
		if componentTimeout <= 0 {
			componentTimeout = remainingTimeout
		}

		_ = component.Stop(componentTimeout)
	}
}

func (sm *StatusManager) waitForGoroutines(deadline time.Time) error {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return fmt.Errorf("timeout waiting for background goroutines to stop")
	}

	doneCh := make(chan struct{})
	go func() {
		sm.wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		return nil
	case <-time.After(remaining):
		return fmt.Errorf("timeout waiting for background goroutines to stop")
	}
}

func (sm *StatusManager) isClosingOrClosed() bool {
	return sm.closing.Load() || sm.closed.Load()
}

func (sm *StatusManager) isClosed() bool {
	return sm.closed.Load()
}

func (sm *StatusManager) loadCloseErr() error {
	sm.closeErrMu.Lock()
	defer sm.closeErrMu.Unlock()
	return sm.closeErr
}

func (sm *StatusManager) setCloseErr(err error) {
	sm.closeErrMu.Lock()
	defer sm.closeErrMu.Unlock()
	sm.closeErr = err
}

func (sm *StatusManager) Add(delta int) {
	sm.wg.Add(delta)
}

func (sm *StatusManager) Done() {
	sm.wg.Done()
}
