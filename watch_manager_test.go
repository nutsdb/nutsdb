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
func wmSubscribe(t *testing.T, wm *watchManager, bucket, key string) (<-chan *Message, BucketId) {
	receiveChan, id, err := wm.subscribe(bucket, key)
	require.NoError(t, err)

	wmVerifySubscriberExists(t, wm, bucket, key, id)
	return receiveChan, id
}

// wmUnsubscribe unsubscribes from a bucket/key and verifies removal
func wmUnsubscribe(t *testing.T, wm *watchManager, bucket, key string, id uint64) {
	err := wm.unsubscribe(bucket, key, id)
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
				t.Logf("receive channel closed after receiving %d/%d messages", received, expectCount)
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
func wmVerifySubscriberExists(t *testing.T, wm *watchManager, bucket, key string, id BucketId) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	assert.Contains(t, wm.lookup, bucket)
	assert.Contains(t, wm.lookup[bucket], key)
	assert.Contains(t, wm.lookup[bucket][key], id)
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

		receiveChan, _ := wmSubscribe(t, wm, bucket, key)
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
		receiveChan, _ := wmSubscribe(t, wm, bucket, key)

		// Send enough messages to:
		// 1. Fill receiveChan buffer (1024)
		// 2. Trigger 100+ drops to force-unsubscribe
		// Total: 1100+ messages
		messagesToSend := 1124
		wmSendMessages(t, wm, bucket, key, messagesToSend)

		time.Sleep(100 * time.Millisecond)
		drained := wmDrainChannel(t, receiveChan, 2*time.Second)

		assert.Equal(t, 1024, drained, "should have buffered exactly 1024 messages")

		// Verify subscriber was force-unsubscribed due to slow consumption
		wmVerifySubscriberRemoved(t, wm, bucket, key)
		wmVerifyBucketRemoved(t, wm, bucket)
	})

	t.Run("multiple subscribers for the same key", func(t *testing.T) {
		wm := startDistributor()
		defer wm.close()

		bucket := "bucket_test"
		key := "key_test"
		numSubscribers := 10
		messagesPerSubscriber := 10

		wg := sync.WaitGroup{}

		// start subscribers goroutines for the same key
		for i := 0; i < numSubscribers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				receiveChan, _ := wmSubscribe(t, wm, bucket, key)
				received := wmReceiveMessages(t, receiveChan, bucket, key, messagesPerSubscriber, 5*time.Second)
				assert.Equal(t, messagesPerSubscriber, received)
			}(i)
		}

		// wait for subscribers to register
		time.Sleep(100 * time.Millisecond)

		// start sender goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			wmSendMessages(t, wm, bucket, key, messagesPerSubscriber)
		}()

		wg.Wait()

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

				receiveChan, _, err := wm.subscribe(bucket, key)
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

		// wait for subscribers to register
		time.Sleep(100 * time.Millisecond)

		// send exactly 10 messages to each subscriber (deterministic)
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

		// wait for all subscribers to finish (with timeout)
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

		receiveChan, _ := wmSubscribe(t, wm, bucket, key)

		// track successful sends/receives with WaitGroup
		stopChan := make(chan struct{})
		var sentCount, receivedCount int
		senderWg := sync.WaitGroup{}   // for sender only
		receiverWg := sync.WaitGroup{} // for receiver only

		// start both sender and receiver goroutines
		wmStartSender(wm, bucket, key, &sentCount, stopChan, &senderWg)
		wmStartReceiver(t, receiveChan, bucket, key, &receivedCount, &receiverWg)

		time.Sleep(200 * time.Millisecond)
		close(stopChan) // stop sender first
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

		receiveChan, id := wmSubscribe(t, wm, bucket, key)

		wg := sync.WaitGroup{}

		// send messages in goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			wmSendMessages(t, wm, bucket, key, expectedMessages)
		}()

		// receive messages in goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			received := wmReceiveMessages(t, receiveChan, bucket, key, expectedMessages, 5*time.Second)
			assert.Equal(t, expectedMessages, received)
		}()

		wg.Wait()

		// unsubscribe after receiving all messages
		wmUnsubscribe(t, wm, bucket, key, id)
		wmVerifyBucketRemoved(t, wm, bucket)
	})

	t.Run("subscribe and unsubscribe with multiple keys", func(t *testing.T) {
		wm := startDistributor()
		defer wm.close()

		expectedSubscribers := 10
		expectedMessages := 100 // reduced for faster test
		bucket := "bucket_test"

		// subscribe all keys
		receiveChans := make([]<-chan *Message, expectedSubscribers)
		keys := make([]string, expectedSubscribers)
		ids := make([]BucketId, expectedSubscribers)
		for i := 0; i < expectedSubscribers; i++ {
			keys[i] = fmt.Sprintf("key_test_%d", i)
			receiveChans[i], ids[i] = wmSubscribe(t, wm, bucket, keys[i])
		}

		wg := sync.WaitGroup{}

		// start sender goroutines
		for i := 0; i < expectedSubscribers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				wmSendMessages(t, wm, bucket, keys[i], expectedMessages)
			}(i)
		}

		// start receiver goroutines with timeout
		for i := 0; i < expectedSubscribers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				received := wmReceiveMessages(t, receiveChans[i], bucket, keys[i], expectedMessages, 5*time.Second)
				assert.Equal(t, expectedMessages, received, "subscriber %d should receive all messages", i)
			}(i)
		}

		wg.Wait()

		// unsubscribe all keys
		for i := 0; i < expectedSubscribers; i++ {
			wmUnsubscribe(t, wm, bucket, keys[i], ids[i])
			wmVerifySubscriberRemoved(t, wm, bucket, keys[i])
		}

		// verify bucket is completely removed after all keys unsubscribe
		wmVerifyBucketRemoved(t, wm, bucket)
	})
}

func TestWatchManager_RunCollector(t *testing.T) {
	t.Run("send messages and receive them in batches", func(t *testing.T) {

		wm := startDistributor()
		defer wm.close()
		time.Sleep(50 * time.Millisecond)

		bucket := "test_bucket"
		key := "test_key"

		receiveChan, _ := wmSubscribe(t, wm, bucket, key)

		messageCount := 128
		for i := 0; i < messageCount; i++ {
			wmSendMessage(t, wm, bucket, key, []byte(fmt.Sprintf("value_%d", i)))
		}

		received := wmReceiveMessages(t, receiveChan, bucket, key, messageCount, 2*time.Second)
		assert.Equal(t, messageCount, received, "should receive all messages")
	})

	t.Run("max batch size triggers immediate send", func(t *testing.T) {
		wm := startDistributor()
		defer wm.close()
		time.Sleep(50 * time.Millisecond)

		bucket := "test_bucket"
		key := "test_key"
		receiveChan, _ := wmSubscribe(t, wm, bucket, key)

		for i := 0; i < maxBatchSize; i++ {
			wmSendMessage(t, wm, bucket, key, []byte(fmt.Sprintf("value_%d", i)))
		}

		received := wmReceiveMessages(t, receiveChan, bucket, key, maxBatchSize, 3*time.Second)
		assert.Equal(t, maxBatchSize, received, "should receive full batch immediately")

		wm.close()
	})

	//TODO: This test will be fail because the channel of a subscriber is only 1024. then the rest of the messages will be dropped.
	// we need to fix this with plan for handling the dropped messages in the future.
	// t.Run("send messages and receive them in multiple batches", func(t *testing.T) {
	// 	wm := startDistributor()
	// 	defer wm.close()
	// 	time.Sleep(50 * time.Millisecond)

	// 	bucket := "test_bucket"
	// 	key := "test_key"
	// 	receiveChan, _ := wmSubscribe(t, wm, bucket, key)

	// 	totalMessages := 10 * maxBatchSize // 10 * 1024
	// 	for i := 0; i < totalMessages; i++ {
	// 		wmSendMessage(t, wm, bucket, key, []byte(fmt.Sprintf("value_%d", i)))
	// 	}

	// 	received := wmReceiveMessages(t, receiveChan, bucket, key, totalMessages, 5*time.Second)
	// 	assert.Equal(t, totalMessages, received, "should receive all messages across multiple batches")

	// 	wm.close()
	// })
}
