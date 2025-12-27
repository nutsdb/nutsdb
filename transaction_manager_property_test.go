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

func TestProperty_ActiveTransactionCompletionGuarantee(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100
	parameters.MaxSize = 20 // Cap concurrency at 20 transactions for clarity

	properties := gopter.NewProperties(parameters)

	properties.Property("active transactions complete before shutdown timeout", prop.ForAll(
		func(numTxs int, txDurations []int) bool {
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

			if len(txDurations) < numTxs {
				for i := len(txDurations); i < numTxs; i++ {
					txDurations = append(txDurations, 50)
				}
			}
			txDurations = txDurations[:numTxs]

			maxDuration := 0
			for _, d := range txDurations {
				if d > maxDuration {
					maxDuration = d
				}
			}

			shutdownTimeout := time.Duration(maxDuration*3) * time.Millisecond
			if shutdownTimeout < 1*time.Second {
				shutdownTimeout = 1 * time.Second
			}

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

				wg.Add(1)
				go func(id uint64, duration int) {
					defer wg.Done()

					time.Sleep(time.Duration(duration) * time.Millisecond)

					tm.UnregisterTx(id)
				}(txID, txDurations[i])
			}

			if count := tm.GetActiveTxCount(); count != int64(numTxs) {
				t.Logf("Expected %d active transactions, got %d", numTxs, count)
				return false
			}

			startTime := time.Now()
			stopErr := tm.Stop(shutdownTimeout)
			elapsed := time.Since(startTime)

			wg.Wait()

			if elapsed > shutdownTimeout+500*time.Millisecond {
				t.Logf("Stop() took %v, exceeded timeout %v", elapsed, shutdownTimeout)
				return false
			}

			expectedMaxTime := time.Duration(maxDuration) * time.Millisecond
			if expectedMaxTime < shutdownTimeout && stopErr != nil {
				t.Logf("Stop() returned error when all transactions should complete: %v", stopErr)
				return false
			}

			finalCount := tm.GetActiveTxCount()
			if finalCount != 0 {
				t.Logf("Expected 0 active transactions after stop, got %d", finalCount)
				return false
			}

			if tm.lifecycle.IsRunning() {
				t.Logf("TransactionManager still running after Stop()")
				return false
			}

			return true
		},
		gen.IntRange(1, 20),
		gen.SliceOf(gen.IntRange(10, 200)),
	))

	properties.TestingRun(t)
}

func TestProperty_ActiveTransactionCompletionGuarantee_WithTimeout(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 50
	parameters.MaxSize = 10

	properties := gopter.NewProperties(parameters)

	properties.Property("stop completes even when transactions exceed timeout", prop.ForAll(
		func(numTxs int, shortTimeout int) bool {
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

			shutdownTimeout := time.Duration(shortTimeout) * time.Millisecond
			if shutdownTimeout < 100*time.Millisecond {
				shutdownTimeout = 100 * time.Millisecond
			}

			longDuration := shutdownTimeout + 500*time.Millisecond

			var wg sync.WaitGroup
			for i := 0; i < numTxs; i++ {
				txID := uint64(i + 1)
				tx := &Tx{id: txID, db: db}

				if err := tm.RegisterTx(tx); err != nil {
					t.Logf("Failed to register transaction %d: %v", txID, err)
					return false
				}

				wg.Add(1)
				go func(id uint64) {
					defer wg.Done()

					time.Sleep(longDuration)

					tm.UnregisterTx(id)
				}(txID)
			}

			startTime := time.Now()
			stopErr := tm.Stop(shutdownTimeout)
			elapsed := time.Since(startTime)

			if elapsed < shutdownTimeout-100*time.Millisecond {
				t.Logf("Stop() returned too early: %v (timeout: %v)", elapsed, shutdownTimeout)
				return false
			}

			if elapsed > shutdownTimeout+1*time.Second {
				t.Logf("Stop() took too long: %v (timeout: %v)", elapsed, shutdownTimeout)
				return false
			}

			_ = stopErr // Allow either an error or nil depending on the implementation

			if tm.lifecycle.IsRunning() {
				t.Logf("TransactionManager still running after Stop() timeout")
				return false
			}

			wg.Wait()

			return true
		},
		gen.IntRange(1, 10),
		gen.IntRange(100, 500),
	))

	properties.TestingRun(t)
}

func TestProperty_TransactionRejectionDuringShutdown(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 30
	parameters.MaxSize = 15

	properties := gopter.NewProperties(parameters)

	properties.Property("new transactions rejected when status is Closing or Closed", prop.ForAll(
		func(numAttempts int, delayMs int) bool {
			db := &DB{
				snowflakeMgr: NewSnowflakeManager(1),
			}
			sm := NewStatusManager(DefaultStatusManagerConfig())

			if err := sm.Start(); err != nil {
				t.Logf("Failed to start StatusManager: %v", err)
				return false
			}

			tm := newTxManager(db, sm)

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

			tx, err := tm.BeginTx(true, false)
			if err != nil {
				t.Logf("Failed to create transaction in Open state: %v", err)
				sm.Close()
				return false
			}
			tm.UnregisterTx(tx.id)

			shutdownDone := make(chan struct{})
			go func() {
				defer close(shutdownDone)
				sm.Close()
			}()

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
					rejectedCount++
				}

				time.Sleep(5 * time.Millisecond)
			}

			<-shutdownDone

			time.Sleep(50 * time.Millisecond)

			for i := 0; i < 5; i++ {
				_, err := tm.BeginTx(true, false)
				if err != ErrDBClosed {
					t.Logf("Expected ErrDBClosed after shutdown, got: %v", err)
					return false
				}
			}

			if rejectedCount == 0 && numAttempts > 0 {
				if !sm.isClosed() {
					t.Logf("Expected at least some rejections during shutdown, got 0 out of %d", numAttempts)
					return false
				}
			}

			if !sm.isClosed() {
				t.Logf("Expected StatusManager to be Closed")
				return false
			}

			if tm.lifecycle.IsRunning() {
				t.Logf("TransactionManager still running after shutdown")
				return false
			}

			return true
		},
		gen.IntRange(1, 15),
		gen.IntRange(10, 300),
	))

	properties.TestingRun(t)
}

func TestProperty_TransactionRejectionDuringShutdown_ImmediateRejection(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 30
	parameters.MaxSize = 30

	properties := gopter.NewProperties(parameters)

	properties.Property("transactions immediately rejected in Closing state", prop.ForAll(
		func(numConcurrentAttempts int) bool {
			db := &DB{
				snowflakeMgr: NewSnowflakeManager(1),
			}
			sm := NewStatusManager(DefaultStatusManagerConfig())

			if err := sm.Start(); err != nil {
				t.Logf("Failed to start StatusManager: %v", err)
				return false
			}

			tm := newTxManager(db, sm)

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

			var wg sync.WaitGroup
			rejectedCount := atomic.Int64{}
			successCount := atomic.Int64{}

			closeStarted := make(chan struct{})
			closeDone := make(chan struct{})
			go func() {
				close(closeStarted)
				sm.Close()
				close(closeDone)
			}()

			<-closeStarted

			time.Sleep(20 * time.Millisecond)

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

			<-closeDone

			if rejectedCount.Load() != int64(numConcurrentAttempts) {
				t.Logf("Expected all %d transactions to be rejected in Closing state, got %d rejected and %d succeeded",
					numConcurrentAttempts, rejectedCount.Load(), successCount.Load())
				return false
			}

			if successCount.Load() != 0 {
				t.Logf("Expected 0 successful transactions in Closing state, got %d", successCount.Load())
				return false
			}

			if count := tm.GetActiveTxCount(); count != 0 {
				t.Logf("Expected 0 active transactions, got %d", count)
				return false
			}

			if !sm.isClosed() {
				t.Logf("Expected StatusManager to be Closed")
				return false
			}

			return true
		},
		gen.IntRange(1, 30),
	))

	properties.TestingRun(t)
}

func TestProperty_TransactionRejectionDuringShutdown_ClosedState(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 30
	parameters.MaxSize = 20

	properties := gopter.NewProperties(parameters)

	properties.Property("transactions rejected in Closed state", prop.ForAll(
		func(numAttempts int) bool {
			db := &DB{
				snowflakeMgr: NewSnowflakeManager(1),
			}
			sm := NewStatusManager(DefaultStatusManagerConfig())

			if err := sm.Start(); err != nil {
				t.Logf("Failed to start StatusManager: %v", err)
				return false
			}

			tm := newTxManager(db, sm)

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

			if err := sm.Close(); err != nil {
				t.Logf("Failed to close StatusManager: %v", err)
				return false
			}

			if !sm.isClosed() {
				t.Logf("Expected status Closed")
				return false
			}

			for i := 0; i < numAttempts; i++ {
				_, err := tm.BeginTx(true, false)
				if err != ErrDBClosed {
					t.Logf("Expected ErrDBClosed in Closed state, got: %v", err)
					return false
				}
			}

			if count := tm.GetActiveTxCount(); count != 0 {
				t.Logf("Expected 0 active transactions in Closed state, got %d", count)
				return false
			}

			return true
		},
		gen.IntRange(1, 20),
	))

	properties.TestingRun(t)
}

func TestProperty_ActiveTransactionCompletionGuarantee_ConcurrentCompletion(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100
	parameters.MaxSize = 30

	properties := gopter.NewProperties(parameters)

	properties.Property("concurrent transaction completion is handled correctly", prop.ForAll(
		func(numTxs int) bool {
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

				wg.Add(1)
				go func(id uint64) {
					defer wg.Done()

					time.Sleep(50 * time.Millisecond)

					tm.UnregisterTx(id)

					completedMu.Lock()
					completedCount++
					completedMu.Unlock()
				}(txID)
			}

			stopErr := tm.Stop(2 * time.Second)

			wg.Wait()

			if stopErr != nil {
				t.Logf("Stop() returned error: %v", stopErr)
				return false
			}

			completedMu.Lock()
			finalCompleted := completedCount
			completedMu.Unlock()

			if finalCompleted != int64(numTxs) {
				t.Logf("Expected %d transactions to complete, got %d", numTxs, finalCompleted)
				return false
			}

			if count := tm.GetActiveTxCount(); count != 0 {
				t.Logf("Expected 0 active transactions, got %d", count)
				return false
			}

			return true
		},
		gen.IntRange(1, 30),
	))

	properties.TestingRun(t)
}
