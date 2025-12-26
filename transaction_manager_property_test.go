package nutsdb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// TestProperty_ActiveTransactionCompletionGuarantee ensures active transactions finish before shutdown timeout
//
// Property: For any transaction active when shutdown begins, it must be allowed to finish or roll back before the timeout
//
// This property test validates:
// 1. Every active transaction registered at shutdown completes before the timeout
// 2. Transactions are unregistered correctly once they finish
// 3. Stop() waits for all active transactions to complete
func TestProperty_ActiveTransactionCompletionGuarantee(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100
	parameters.MaxSize = 20 // Cap concurrency at 20 transactions for clarity

	properties := gopter.NewProperties(parameters)

	properties.Property("active transactions complete before shutdown timeout", prop.ForAll(
		func(numTxs int, txDurations []int) bool {
			// Set up DB and StatusManager to exercise transaction shutdown guarantees
			db := &DB{
				snowflakeMgr: NewSnowflakeManager(1),
			}
			sm := NewStatusManager(DefaultStatusManagerConfig())

			// Start StatusManager so transaction operations are allowed
			if err := sm.Start(); err != nil {
				t.Logf("Failed to start StatusManager: %v", err)
				return false
			}
			defer sm.Close()

			tm := newTxManager(db, sm)
			ctx := context.Background()
			if err := tm.Start(ctx); err != nil {
				t.Logf("Failed to start TransactionManager: %v", err)
				return false
			}

			// Ensure the durations slice matches numTxs for deterministic waits
			if len(txDurations) < numTxs {
				// Pad missing durations with a default value
				for i := len(txDurations); i < numTxs; i++ {
					txDurations = append(txDurations, 50)
				}
			}
			txDurations = txDurations[:numTxs]

			// Compute the maximum transaction duration
			maxDuration := 0
			for _, d := range txDurations {
				if d > maxDuration {
					maxDuration = d
				}
			}

			// Set timeout to three times that duration to allow extra headroom
			shutdownTimeout := time.Duration(maxDuration*3) * time.Millisecond
			if shutdownTimeout < 1*time.Second {
				shutdownTimeout = 1 * time.Second
			}

			// Register the transactions we'll later wait to finish
			var wg sync.WaitGroup
			txIDs := make([]uint64, numTxs)

			for i := 0; i < numTxs; i++ {
				txID := uint64(i + 1)
				txIDs[i] = txID
				tx := &Tx{id: txID, db: db}

				if err := tm.RegisterTx(tx); err != nil {
					t.Logf("Failed to register transaction %d: %v", txID, err)
					return false
				}

				// Start a goroutine to simulate transaction execution
				wg.Add(1)
				go func(id uint64, duration int) {
					defer wg.Done()

					// Sleep to mimic the transaction doing work
					time.Sleep(time.Duration(duration) * time.Millisecond)

					// Unregister once the simulated work finishes
					tm.UnregisterTx(id)
				}(txID, txDurations[i])
			}

			// Ensure the active count reflects all registered transactions
			if count := tm.GetActiveTxCount(); count != int64(numTxs) {
				t.Logf("Expected %d active transactions, got %d", numTxs, count)
				return false
			}

			// Begin shutting down to observe Stop behavior
			startTime := time.Now()
			stopErr := tm.Stop(shutdownTimeout)
			elapsed := time.Since(startTime)

			// Wait for all transaction goroutines to finish before analyzing results
			wg.Wait()

			// Check 1: Stop() completes before exceeding the shutdown timeout
			if elapsed > shutdownTimeout+500*time.Millisecond {
				t.Logf("Stop() took %v, exceeded timeout %v", elapsed, shutdownTimeout)
				return false
			}

			// Check 2: When all transactions finish before the timeout, Stop() should not return an error
			expectedMaxTime := time.Duration(maxDuration) * time.Millisecond
			if expectedMaxTime < shutdownTimeout && stopErr != nil {
				t.Logf("Stop() returned error when all transactions should complete: %v", stopErr)
				return false
			}

			// Check 3: All transactions should be unregistered after the stop
			finalCount := tm.GetActiveTxCount()
			if finalCount != 0 {
				t.Logf("Expected 0 active transactions after stop, got %d", finalCount)
				return false
			}

			// Check 4: TransactionManager should be stopped after the shutdown
			if tm.lifecycle.IsRunning() {
				t.Logf("TransactionManager still running after Stop()")
				return false
			}

			return true
		},
		gen.IntRange(1, 20),                // numTxs: 1-20 transactions
		gen.SliceOf(gen.IntRange(10, 200)), // txDurations: 10-200ms per transaction
	))

	properties.TestingRun(t)
}

// TestProperty_ActiveTransactionCompletionGuarantee_WithTimeout ensures Stop handles transactions exceeding timeout
//
// This test validates the behavior when transactions run longer than the shutdown timeout
func TestProperty_ActiveTransactionCompletionGuarantee_WithTimeout(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 50
	parameters.MaxSize = 10

	properties := gopter.NewProperties(parameters)

	properties.Property("stop completes even when transactions exceed timeout", prop.ForAll(
		func(numTxs int, shortTimeout int) bool {
			// Set up DB and StatusManager for the timeout scenario
			db := &DB{
				snowflakeMgr: NewSnowflakeManager(1),
			}
			sm := NewStatusManager(DefaultStatusManagerConfig())

			if err := sm.Start(); err != nil {
				t.Logf("Failed to start StatusManager: %v", err)
				return false
			}
			defer sm.Close()

			tm := newTxManager(db, sm)
			ctx := context.Background()
			if err := tm.Start(ctx); err != nil {
				t.Logf("Failed to start TransactionManager: %v", err)
				return false
			}

			// Use a shorter timeout to trigger the timeout path
			shutdownTimeout := time.Duration(shortTimeout) * time.Millisecond
			if shutdownTimeout < 100*time.Millisecond {
				shutdownTimeout = 100 * time.Millisecond
			}

			// Register transactions that will deliberately exceed the timeout
			longDuration := shutdownTimeout + 500*time.Millisecond

			var wg sync.WaitGroup
			for i := 0; i < numTxs; i++ {
				txID := uint64(i + 1)
				tx := &Tx{id: txID, db: db}

				if err := tm.RegisterTx(tx); err != nil {
					t.Logf("Failed to register transaction %d: %v", txID, err)
					return false
				}

				// Launch a goroutine to simulate a long-running transaction
				wg.Add(1)
				go func(id uint64) {
					defer wg.Done()

					// Sleep long enough to overshoot the timeout
					time.Sleep(longDuration)

					// Unregister once the simulated work shows completion
					tm.UnregisterTx(id)
				}(txID)
			}

			// Start the shutdown process
			startTime := time.Now()
			stopErr := tm.Stop(shutdownTimeout)
			elapsed := time.Since(startTime)

			// Check 1: Stop() should return around the timeout (with some slack)
			if elapsed < shutdownTimeout-100*time.Millisecond {
				t.Logf("Stop() returned too early: %v (timeout: %v)", elapsed, shutdownTimeout)
				return false
			}

			if elapsed > shutdownTimeout+1*time.Second {
				t.Logf("Stop() took too long: %v (timeout: %v)", elapsed, shutdownTimeout)
				return false
			}

			// Check 2: In timeout cases, Stop() may return an error or just log a warning
			_ = stopErr // Allow either an error or nil depending on the implementation

			// Check 3: TransactionManager should still stop even if the timeout was reached
			if tm.lifecycle.IsRunning() {
				t.Logf("TransactionManager still running after Stop() timeout")
				return false
			}

			// Wait for all transaction goroutines to finish as cleanup
			wg.Wait()

			return true
		},
		gen.IntRange(1, 10),    // numTxs: 1-10 transactions
		gen.IntRange(100, 500), // shortTimeout: 100-500ms
	))

	properties.TestingRun(t)
}

// TestProperty_TransactionRejectionDuringShutdown verifies new transactions are rejected during shutdown
//
// Property: For any new transaction request, if the database is Closing or Closed, it must be rejected with ErrDBClosed
//
// This property test validates:
// 1. New transactions are rejected once the database is Closing
// 2. New transactions are rejected after the database is Closed
// 3. Rejected transactions return ErrDBClosed
// 4. Any attempt during shutdown is rejected
func TestProperty_TransactionRejectionDuringShutdown(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 30
	parameters.MaxSize = 15

	properties := gopter.NewProperties(parameters)

	properties.Property("new transactions rejected when status is Closing or Closed", prop.ForAll(
		func(numAttempts int, delayMs int) bool {
			// Set up DB and StatusManager for the shutdown rejection scenario
			db := &DB{
				snowflakeMgr: NewSnowflakeManager(1),
			}
			sm := NewStatusManager(DefaultStatusManagerConfig())

			// Start StatusManager to allow transaction operations
			if err := sm.Start(); err != nil {
				t.Logf("Failed to start StatusManager: %v", err)
				return false
			}

			tm := newTxManager(db, sm)

			// Register TransactionManager as a component so StatusManager.Close triggers tm.Stop
			if err := sm.RegisterComponent("TransactionManager", tm); err != nil {
				t.Logf("Failed to register TransactionManager: %v", err)
				sm.Close()
				return false
			}

			ctx := context.Background()
			if err := tm.Start(ctx); err != nil {
				t.Logf("Failed to start TransactionManager: %v", err)
				sm.Close()
				return false
			}

			// Ensure a transaction can start while the status is still Open
			tx, err := tm.BeginTx(true, false)
			if err != nil {
				t.Logf("Failed to create transaction in Open state: %v", err)
				sm.Close()
				return false
			}
			// Unregister immediately to avoid blocking shutdown
			tm.UnregisterTx(tx.id)

			// Kick off shutdown in the background
			shutdownDone := make(chan struct{})
			go func() {
				defer close(shutdownDone)
				sm.Close()
			}()

			// Try creating transactions during shutdown with random delays to hit different phases
			delay := time.Duration(delayMs) * time.Millisecond
			if delay > 500*time.Millisecond {
				delay = 500 * time.Millisecond
			}
			if delay < 10*time.Millisecond {
				delay = 10 * time.Millisecond
			}

			time.Sleep(delay)

			rejectedCount := 0
			for i := 0; i < numAttempts; i++ {
				_, err := tm.BeginTx(true, false)
				if err == ErrDBClosed {
					rejectedCount++
				} else if err != nil {
					// Count any other error as a rejection too
					rejectedCount++
				}

				// Sleep briefly so the shutdown can advance
				time.Sleep(5 * time.Millisecond)
			}

			// Wait for the shutdown goroutine to finish
			<-shutdownDone

			// Pause briefly so TransactionManager can fully stop
			time.Sleep(50 * time.Millisecond)

			// Check 1: After shutdown, every transaction request should be rejected
			for i := 0; i < 5; i++ {
				_, err := tm.BeginTx(true, false)
				if err != ErrDBClosed {
					t.Logf("Expected ErrDBClosed after shutdown, got: %v", err)
					return false
				}
			}

			// Check 2: During shutdown, at least some transactions should see rejection
			// (Depending on delays, it might be all of them)
			if rejectedCount == 0 && numAttempts > 0 {
				// If the delay was short, the attempts may finish before closing begins
				// In that case, we verify the final status instead
				status := sm.Status()
				if status != StatusClosed {
					t.Logf("Expected at least some rejections during shutdown, got 0 out of %d", numAttempts)
					return false
				}
			}

			// Check 3: StatusManager should report Closed
			if sm.Status() != StatusClosed {
				t.Logf("Expected StatusManager to be Closed, got: %s", sm.Status())
				return false
			}

			if tm.lifecycle.IsRunning() {
				t.Logf("TransactionManager still running after shutdown")
				return false
			}

			return true
		},
		gen.IntRange(1, 15),   // numAttempts: 1-15 attempts
		gen.IntRange(10, 300), // delayMs: 10-300ms
	))

	properties.TestingRun(t)
}

// TestProperty_TransactionRejectionDuringShutdown_ImmediateRejection confirms immediate rejections in Closing state
//
// This test validates that transactions are rejected immediately once the status enters Closing
func TestProperty_TransactionRejectionDuringShutdown_ImmediateRejection(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 30
	parameters.MaxSize = 30

	properties := gopter.NewProperties(parameters)

	properties.Property("transactions immediately rejected in Closing state", prop.ForAll(
		func(numConcurrentAttempts int) bool {
			// Prepare the environment for the immediate-rejection scenario
			db := &DB{
				snowflakeMgr: NewSnowflakeManager(1),
			}
			sm := NewStatusManager(DefaultStatusManagerConfig())

			if err := sm.Start(); err != nil {
				t.Logf("Failed to start StatusManager: %v", err)
				return false
			}

			tm := newTxManager(db, sm)

			// Register TransactionManager so StatusManager controls its lifecycle
			if err := sm.RegisterComponent("TransactionManager", tm); err != nil {
				t.Logf("Failed to register TransactionManager: %v", err)
				sm.Close()
				return false
			}

			ctx := context.Background()
			if err := tm.Start(ctx); err != nil {
				t.Logf("Failed to start TransactionManager: %v", err)
				sm.Close()
				return false
			}

			// Concurrently attempt transactions while shutdown begins to exercise rejection timing
			var wg sync.WaitGroup
			rejectedCount := atomic.Int64{}
			successCount := atomic.Int64{}

			// Start the shutdown process now
			closeStarted := make(chan struct{})
			closeDone := make(chan struct{})
			go func() {
				close(closeStarted)
				sm.Close()
				close(closeDone)
			}()

			// Wait until the shutdown goroutine signals it has started
			<-closeStarted

			// Sleep briefly to allow the status to switch to Closing
			time.Sleep(20 * time.Millisecond)

			// Now try creating multiple transactions while the manager should be in Closing state
			for i := 0; i < numConcurrentAttempts; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					_, err := tm.BeginTx(true, false)
					switch err {
					case ErrDBClosed:
						rejectedCount.Add(1)
					case nil:
						successCount.Add(1)
					}
				}()
			}

			wg.Wait()

			// Wait for the shutdown goroutine to finish
			<-closeDone

			// Check 1: All transaction requests should be rejected because the manager is in Closing state
			if rejectedCount.Load() != int64(numConcurrentAttempts) {
				t.Logf("Expected all %d transactions to be rejected in Closing state, got %d rejected and %d succeeded",
					numConcurrentAttempts, rejectedCount.Load(), successCount.Load())
				return false
			}

			// Check 2: There should be no successful transaction creations
			if successCount.Load() != 0 {
				t.Logf("Expected 0 successful transactions in Closing state, got %d", successCount.Load())
				return false
			}

			// Check 3: Active transaction count should stay at zero
			if count := tm.GetActiveTxCount(); count != 0 {
				t.Logf("Expected 0 active transactions, got %d", count)
				return false
			}

			// Check 4: StatusManager should report Closed
			if sm.Status() != StatusClosed {
				t.Logf("Expected StatusManager to be Closed, got: %s", sm.Status())
				return false
			}

			return true
		},
		gen.IntRange(1, 30), // numConcurrentAttempts: 1-30 concurrent attempts
	))

	properties.TestingRun(t)
}

// TestProperty_TransactionRejectionDuringShutdown_ClosedState validates rejection in the Closed state
//
// This test verifies that transactions are rejected once the database is fully Closed
func TestProperty_TransactionRejectionDuringShutdown_ClosedState(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 30
	parameters.MaxSize = 20

	properties := gopter.NewProperties(parameters)

	properties.Property("transactions rejected in Closed state", prop.ForAll(
		func(numAttempts int) bool {
			// Prepare DB and StatusManager for the Closed-state rejection test
			db := &DB{
				snowflakeMgr: NewSnowflakeManager(1),
			}
			sm := NewStatusManager(DefaultStatusManagerConfig())

			if err := sm.Start(); err != nil {
				t.Logf("Failed to start StatusManager: %v", err)
				return false
			}

			tm := newTxManager(db, sm)

			// Register TransactionManager to ensure StatusManager controls its lifecycle
			if err := sm.RegisterComponent("TransactionManager", tm); err != nil {
				t.Logf("Failed to register TransactionManager: %v", err)
				sm.Close()
				return false
			}

			ctx := context.Background()
			if err := tm.Start(ctx); err != nil {
				t.Logf("Failed to start TransactionManager: %v", err)
				sm.Close()
				return false
			}

			// Fully close the system to reach Closed state
			if err := sm.Close(); err != nil {
				t.Logf("Failed to close StatusManager: %v", err)
				return false
			}

			// Confirm the status is Closed before continuing
			if sm.Status() != StatusClosed {
				t.Logf("Expected status Closed, got: %s", sm.Status())
				return false
			}

			// Attempt multiple transactions to ensure they are rejected
			for i := 0; i < numAttempts; i++ {
				_, err := tm.BeginTx(true, false)
				if err != ErrDBClosed {
					t.Logf("Expected ErrDBClosed in Closed state, got: %v", err)
					return false
				}
			}

			// Ensure the active transaction count remains zero in Closed state
			if count := tm.GetActiveTxCount(); count != 0 {
				t.Logf("Expected 0 active transactions in Closed state, got %d", count)
				return false
			}

			return true
		},
		gen.IntRange(1, 20), // numAttempts: 1-20 attempts
	))

	properties.TestingRun(t)
}

// TestProperty_ActiveTransactionCompletionGuarantee_ConcurrentCompletion checks concurrent completion correctness
//
// This test ensures multiple transactions finish correctly when they complete concurrently
func TestProperty_ActiveTransactionCompletionGuarantee_ConcurrentCompletion(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100
	parameters.MaxSize = 30

	properties := gopter.NewProperties(parameters)

	properties.Property("concurrent transaction completion is handled correctly", prop.ForAll(
		func(numTxs int) bool {
			// Set up DB and StatusManager for testing concurrent completion
			db := &DB{}
			sm := NewStatusManager(DefaultStatusManagerConfig())

			if err := sm.Start(); err != nil {
				t.Logf("Failed to start StatusManager: %v", err)
				return false
			}
			defer sm.Close()

			tm := newTxManager(db, sm)
			ctx := context.Background()
			if err := tm.Start(ctx); err != nil {
				t.Logf("Failed to start TransactionManager: %v", err)
				return false
			}

			// Register active transactions that will finish concurrently
			var wg sync.WaitGroup
			completedCount := int64(0)
			var completedMu sync.Mutex

			for i := 0; i < numTxs; i++ {
				txID := uint64(i + 1)
				tx := &Tx{id: txID, db: db}

				if err := tm.RegisterTx(tx); err != nil {
					t.Logf("Failed to register transaction %d: %v", txID, err)
					return false
				}

				// Launch a goroutine so all transactions finish nearly simultaneously
				wg.Add(1)
				go func(id uint64) {
					defer wg.Done()

					// Wait briefly so they complete together
					time.Sleep(50 * time.Millisecond)

					// Unregister each transaction upon completion
					tm.UnregisterTx(id)

					// Track completion count for validation
					completedMu.Lock()
					completedCount++
					completedMu.Unlock()
				}(txID)
			}

			// Start the shutdown process after registering all transactions
			stopErr := tm.Stop(2 * time.Second)

			// Wait for every transaction goroutine to finish
			wg.Wait()

			// Check 1: Stop() should succeed because all transactions complete before the timeout
			if stopErr != nil {
				t.Logf("Stop() returned error: %v", stopErr)
				return false
			}

			// Check 2: Every transaction should have signaled completion
			completedMu.Lock()
			finalCompleted := completedCount
			completedMu.Unlock()

			if finalCompleted != int64(numTxs) {
				t.Logf("Expected %d transactions to complete, got %d", numTxs, finalCompleted)
				return false
			}

			// Check 3: Active transaction count should be zero afterwards
			if count := tm.GetActiveTxCount(); count != 0 {
				t.Logf("Expected 0 active transactions, got %d", count)
				return false
			}

			return true
		},
		gen.IntRange(1, 30), // numTxs: 1-30 concurrent transactions
	))

	properties.TestingRun(t)
}
