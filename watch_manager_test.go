package nutsdb

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startDistributor creates and starts a watch manager with distributor running
func startDistributor() *watchManager {
	wm := NewWatchManager()
	go wm.startDistributor()
	time.Sleep(100 * time.Millisecond) // Let distributor start
	return wm
}

// wmSubscribe subscribes to a bucket/key and verifies subscription
func wmSubscribe(t *testing.T, wm *watchManager, bucket, key string) <-chan *Message {
	receiveChan, err := wm.subscribe(bucket, key)
	require.NoError(t, err)

	wmVerifySubscriberExists(t, wm, bucket, key)
	return receiveChan
}

// wmUnsubscribe unsubscribes from a bucket/key and verifies removal
func wmUnsubscribe(t *testing.T, wm *watchManager, bucket, key string) {
	err := wm.unsubscribe(bucket, key)
	require.NoError(t, err)

	wm.mu.Lock()
	if bucketMap, ok := wm.lookup[bucket]; ok {
		assert.NotContains(t, bucketMap, key, "key should be removed from bucket")
	}
	wm.mu.Unlock()
}

// wmSendMessage sends a message and asserts no error
func wmSendMessage(t *testing.T, wm *watchManager, bucket, key string, value []byte) {
	err := wm.sendMessage(&Message{BucketName: bucket, Key: key, Value: value})
	assert.NoError(t, err)
}

// wmSendMessages sends multiple messages to the same bucket/key
func wmSendMessages(t *testing.T, wm *watchManager, bucket, key string, count int) {
	for i := 0; i < count; i++ {
		value := []byte(fmt.Sprintf("value_%d", i))
		wmSendMessage(t, wm, bucket, key, value)
	}
}

// wmReceiveMessage receives a message with timeout and asserts expected values
func wmReceiveMessage(t *testing.T, receiveChan <-chan *Message, expectBucket, expectKey string, timeout time.Duration) *Message {
	select {
	case msg := <-receiveChan:
		assert.Equal(t, expectBucket, msg.BucketName)
		assert.Equal(t, expectKey, msg.Key)
		assert.NotNil(t, msg.Value)
		return msg
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for message on %s/%s", expectBucket, expectKey)
		return nil
	}
}

// wmReceiveMessages receives count messages and returns the count received
func wmReceiveMessages(t *testing.T, receiveChan <-chan *Message, expectBucket, expectKey string, expectCount int, timeout time.Duration) int {
	received := 0
	timeoutChan := time.After(timeout)

	for received < expectCount {
		select {
		case msg, ok := <-receiveChan:
			if !ok {
				return received
			}
			assert.Equal(t, expectBucket, msg.BucketName)
			assert.Equal(t, expectKey, msg.Key)
			assert.NotNil(t, msg.Value)
			received++
		case <-timeoutChan:
			t.Logf("timeout after receiving %d/%d messages", received, expectCount)
			return received
		}
	}
	return received
}

// wmDrainChannel drains all messages from a channel until it's closed or timeout
func wmDrainChannel(t *testing.T, receiveChan <-chan *Message, timeout time.Duration) int {
	drained := 0
	timeoutChan := time.After(timeout)

	for {
		select {
		case _, ok := <-receiveChan:
			if !ok {
				return drained
			}
			drained++
		case <-timeoutChan:
			t.Logf("drain timeout after %d messages", drained)
			return drained
		}
	}
}

// wmVerifySubscriberExists checks if a subscriber exists in the lookup table
func wmVerifySubscriberExists(t *testing.T, wm *watchManager, bucket, key string) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	assert.Contains(t, wm.lookup, bucket)
	assert.Contains(t, wm.lookup[bucket], key)
}

// wmVerifySubscriberRemoved checks if a subscriber is removed from the lookup table
func wmVerifySubscriberRemoved(t *testing.T, wm *watchManager, bucket, key string) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	if bucketMap, ok := wm.lookup[bucket]; ok {
		assert.NotContains(t, bucketMap, key)
	}
}

// wmVerifyBucketRemoved checks if a bucket is completely removed
func wmVerifyBucketRemoved(t *testing.T, wm *watchManager, bucket string) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	assert.NotContains(t, wm.lookup, bucket)
}

// wmStartSender starts a goroutine that sends messages until stopped
func wmStartSender(wm *watchManager, bucket, key string, sent *int, stopChan <-chan struct{}, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stopChan:
				return
			case <-wm.done():
				return
			case <-ticker.C:
				value := []byte(fmt.Sprintf("value_%d", sent))
				err := wm.sendMessage(&Message{BucketName: bucket, Key: key, Value: value})
				if err == nil {
					*sent++
				}
			}
		}
	}()
}

// wmStartReceiver starts a goroutine that receives messages from a channel
// Returns a pointer to track the received count
func wmStartReceiver(t *testing.T, receiveChan <-chan *Message, expectBucket, expectKey string, received *int, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range receiveChan {
			assert.Equal(t, expectBucket, msg.BucketName)
			assert.Equal(t, expectKey, msg.Key)
			assert.NotNil(t, msg.Value)
			*received++
		}
	}()
}

func TestWatchManager_SubscribeAndSendMessage(t *testing.T) {
	t.Run("subscribe and send message", func(t *testing.T) {
		wm := startDistributor()
		defer wm.close()

		bucket := "test"
		key := "test"
		value := []byte("updated value")

		receiveChan := wmSubscribe(t, wm, bucket, key)
		wmSendMessage(t, wm, bucket, key, value)

		msg := wmReceiveMessage(t, receiveChan, bucket, key, 1*time.Second)
		assert.Equal(t, value, msg.Value)
	})

	t.Run("subscribe and drop messages", func(t *testing.T) {
		wm := startDistributor()
		defer wm.close()

		bucket := "bucket_test"
		key := "key_test"

		// Subscribe but don't receive messages
		receiveChan := wmSubscribe(t, wm, bucket, key)

		// Send enough messages to:
		// 1. Fill receiveChan buffer (1000)
		// 2. Trigger 100+ drops to force-unsubscribe
		// Total: 1100+ messages
		messagesToSend := 1100
		wmSendMessages(t, wm, bucket, key, messagesToSend)

		time.Sleep(100 * time.Millisecond)
		drained := wmDrainChannel(t, receiveChan, 2*time.Second)

		assert.Equal(t, 1000, drained, "should have buffered exactly 1000 messages")

		// Verify subscriber was force-unsubscribed due to slow consumption
		wmVerifySubscriberRemoved(t, wm, bucket, key)
		wmVerifyBucketRemoved(t, wm, bucket)
	})

	t.Run("multiple subscribers", func(t *testing.T) {
		wm := startDistributor()
		defer wm.close()

		numSubscribers := 100
		messagesPerSubscriber := 10

		wg := sync.WaitGroup{}

		// Start 100 subscriber goroutines
		for i := 0; i < numSubscribers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				bucket := fmt.Sprintf("bucket_test_%d", i)
				key := fmt.Sprintf("key_test_%d", i)
				receivedCounts := 0

				receiveChan, err := wm.subscribe(bucket, key)
				assert.NoError(t, err)

				timeout := time.After(5 * time.Second)

				for {
					select {
					case message, ok := <-receiveChan:
						if !ok {
							break
						}

						assert.Equal(t, bucket, message.BucketName)
						assert.Equal(t, key, message.Key)
						assert.NotNil(t, message.Value)

						receivedCounts++

					case <-timeout:
						goto done
					}
				}
			done:
				assert.Equal(t, messagesPerSubscriber, receivedCounts)
			}(i)
		}

		// Give subscribers time to register
		time.Sleep(100 * time.Millisecond)

		// Send exactly 10 messages to each subscriber (deterministic)
		for i := 0; i < numSubscribers; i++ {
			for j := 0; j < messagesPerSubscriber; j++ {
				bucket := fmt.Sprintf("bucket_test_%d", i)
				key := fmt.Sprintf("key_test_%d", i)
				value := fmt.Sprintf("value_test_%d_%d", i, j)

				err := wm.sendMessage(&Message{
					BucketName: bucket,
					Key:        key,
					Value:      []byte(value),
				})
				assert.NoError(t, err)
			}
		}

		// Wait for all subscribers to finish (with timeout)
		wg.Wait()
	})

	t.Run("send message and close channel", func(t *testing.T) {
		wm := startDistributor()
		isClosed := false

		defer func() {
			if !isClosed {
				wm.close()
			}
		}()

		bucket := "bucket_test"
		key := "key_test"

		receiveChan := wmSubscribe(t, wm, bucket, key)

		// Track successful sends/receives with WaitGroup
		stopChan := make(chan struct{})
		var sentCount, receivedCount int
		senderWg := sync.WaitGroup{}   // For sender only
		receiverWg := sync.WaitGroup{} // For receiver only

		// Start both sender and receiver goroutines
		wmStartSender(wm, bucket, key, &sentCount, stopChan, &senderWg)
		wmStartReceiver(t, receiveChan, bucket, key, &receivedCount, &receiverWg)

		time.Sleep(200 * time.Millisecond)
		close(stopChan) // Stop sender first
		senderWg.Wait()

		isClosed = true
		wm.close()

		receiverWg.Wait()

		t.Logf("Sent: %d, Received: %d", sentCount, receivedCount)
		assert.Greater(t, sentCount, 0, "should have sent some messages")
		assert.Greater(t, receivedCount, 0, "should have received some messages")
	})
}

func TestWatchManager_SubscribeAndUnsubscribe(t *testing.T) {
	t.Run("subscribe and unsubscribe", func(t *testing.T) {
		wm := startDistributor()
		defer wm.close()

		bucket := "bucket_test"
		key := "key_test"
		expectedMessages := 1000

		receiveChan := wmSubscribe(t, wm, bucket, key)

		wg := sync.WaitGroup{}

		// Send messages in goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			wmSendMessages(t, wm, bucket, key, expectedMessages)
		}()

		// Receive messages in goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			received := wmReceiveMessages(t, receiveChan, bucket, key, expectedMessages, 5*time.Second)
			assert.Equal(t, expectedMessages, received)
		}()

		wg.Wait()

		// Unsubscribe after receiving all messages
		wmUnsubscribe(t, wm, bucket, key)
		wmVerifyBucketRemoved(t, wm, bucket)
	})

	t.Run("subscribe and unsubscribe with multiple keys", func(t *testing.T) {
		wm := startDistributor()
		defer wm.close()

		expectedSubscribers := 10
		expectedMessages := 100 // Reduced for faster test
		bucket := "bucket_test"

		// Subscribe all keys
		receiveChans := make([]<-chan *Message, expectedSubscribers)
		keys := make([]string, expectedSubscribers)
		for i := 0; i < expectedSubscribers; i++ {
			keys[i] = fmt.Sprintf("key_test_%d", i)
			receiveChans[i] = wmSubscribe(t, wm, bucket, keys[i])
		}

		wg := sync.WaitGroup{}

		// Start sender goroutines
		for i := 0; i < expectedSubscribers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				wmSendMessages(t, wm, bucket, keys[i], expectedMessages)
			}(i)
		}

		// Start receiver goroutines with timeout
		for i := 0; i < expectedSubscribers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				received := wmReceiveMessages(t, receiveChans[i], bucket, keys[i], expectedMessages, 10*time.Second)
				assert.Equal(t, expectedMessages, received, "subscriber %d should receive all messages", i)
			}(i)
		}

		wg.Wait()

		// Unsubscribe all keys
		for i := 0; i < expectedSubscribers; i++ {
			wmUnsubscribe(t, wm, bucket, keys[i])
			wmVerifySubscriberRemoved(t, wm, bucket, keys[i])
		}

		// Verify bucket is completely removed after all keys unsubscribe
		wmVerifyBucketRemoved(t, wm, bucket)
	})
}
