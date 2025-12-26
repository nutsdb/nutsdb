package nutsdb

import (
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/nutsdb/nutsdb/internal/ttl"
)

// TestProperty_ContextCancellationPropagation_RealComponents tests with real components
//
// Integration test: verifies context cancellation with actual component implementations
// working together in a real system configuration.
func TestProperty_ContextCancellationPropagation_RealComponents(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 50
	parameters.MaxSize = 5

	properties := gopter.NewProperties(parameters)

	properties.Property("real components respond to context cancellation", prop.ForAll(
		func(enableMerge bool, enableTTL bool, enableWatch bool) bool {
			// Create test environment
			db := &DB{
				snowflakeMgr: NewSnowflakeManager(1),
			}
			sm := NewStatusManager(DefaultStatusManagerConfig())

			// Start StatusManager
			if err := sm.Start(); err != nil {
				t.Logf("Failed to start StatusManager: %v", err)
				return false
			}

			// Track which components are registered
			registeredComponents := make([]string, 0)

			// Register TransactionManager (always)
			tm := newTxManager(db, sm)
			if err := sm.RegisterComponent("TransactionManager", tm); err != nil {
				t.Logf("Failed to register TransactionManager: %v", err)
				sm.Close()
				return false
			}
			if err := tm.Start(sm.Context()); err != nil {
				t.Logf("Failed to start TransactionManager: %v", err)
				sm.Close()
				return false
			}
			registeredComponents = append(registeredComponents, "TransactionManager")

			// Conditionally register mergeWorker
			var mw *mergeWorker
			if enableMerge {
				config := DefaultMergeConfig()
				config.EnableAutoMerge = false // Disable auto-merge for testing
				mw = newMergeWorker(db, sm, config)
				if err := sm.RegisterComponent("MergeWorker", mw); err != nil {
					t.Logf("Failed to register MergeWorker: %v", err)
					sm.Close()
					return false
				}
				if err := mw.Start(sm.Context()); err != nil {
					t.Logf("Failed to start MergeWorker: %v", err)
					sm.Close()
					return false
				}
				registeredComponents = append(registeredComponents, "MergeWorker")
			}

			// Conditionally register TTLService
			var ttlSvc *ttl.Service
			if enableTTL {
				mockClock := ttl.NewMockClock(1000000)
				config := ttl.DefaultConfig()
				config.ScanInterval = 100 * time.Millisecond
				config.BatchTimeout = 50 * time.Millisecond
				callback := func(events []*ttl.ExpirationEvent) {}
				scanFn := func() ([]*ttl.ExpirationEvent, error) { return nil, nil }
				ttlSvc = ttl.NewService(mockClock, config, callback, scanFn)
				if err := sm.RegisterComponent("TTLService", ttlSvc); err != nil {
					t.Logf("Failed to register TTLService: %v", err)
					sm.Close()
					return false
				}
				if err := ttlSvc.Start(sm.Context()); err != nil {
					t.Logf("Failed to start TTLService: %v", err)
					sm.Close()
					return false
				}
				registeredComponents = append(registeredComponents, "TTLService")
			}

			// Conditionally register WatchManager
			var wm *watchManager
			if enableWatch {
				wm = NewWatchManager()
				if err := sm.RegisterComponent("WatchManager", wm); err != nil {
					t.Logf("Failed to register WatchManager: %v", err)
					sm.Close()
					return false
				}
				if err := wm.Start(sm.Context()); err != nil {
					t.Logf("Failed to start WatchManager: %v", err)
					sm.Close()
					return false
				}
				registeredComponents = append(registeredComponents, "WatchManager")
			}

			// Give components a moment to fully start
			time.Sleep(50 * time.Millisecond)

			// Cancel context by closing StatusManager
			startTime := time.Now()
			closeErr := sm.Close()
			elapsed := time.Since(startTime)

			// Verification 1: Close should complete successfully
			if closeErr != nil {
				t.Logf("StatusManager.Close() returned error: %v", closeErr)
				return false
			}

			// Verification 2: All components should have stopped
			// TransactionManager
			if tm.lifecycle.IsRunning() {
				t.Logf("TransactionManager still running after Close()")
				return false
			}

			// MergeWorker
			if enableMerge && mw != nil {
				// Check if MergeWorker's context was cancelled
				select {
				case <-mw.lifecycle.Context().Done():
					// Context was cancelled, good
				default:
					t.Logf("MergeWorker context not cancelled after Close()")
					return false
				}
			}

			// TTLService
			// Note: TTLService doesn't expose running state, but the context cancellation
			// will trigger its Run method to exit

			// WatchManager
			if enableWatch && wm != nil {
				if !wm.isClosed() {
					t.Logf("WatchManager still running after Close()")
					return false
				}
			}

			// Verification 3: Close should complete within reasonable time
			maxExpectedTime := sm.config.ShutdownTimeout + 2*time.Second
			if elapsed > maxExpectedTime {
				t.Logf("Close took %v, exceeded max expected time %v", elapsed, maxExpectedTime)
				return false
			}

			// Verification 4: StatusManager should be in Closed state
			if sm.Status() != StatusClosed {
				t.Logf("Expected StatusManager to be Closed, got: %s", sm.Status())
				return false
			}

			// Verification 5: StatusManager's context should be cancelled
			select {
			case <-sm.Context().Done():
				// Context was cancelled, good
			default:
				t.Logf("StatusManager context not cancelled after Close()")
				return false
			}

			return true
		},
		gen.Bool(), // enableMerge
		gen.Bool(), // enableTTL
		gen.Bool(), // enableWatch
	))

	properties.TestingRun(t)
}
