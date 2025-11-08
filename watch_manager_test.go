package nutsdb

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWatchManager_SubscribeAndSendMessage(t *testing.T) {

	t.Run("subscribe and send message", func(t *testing.T) {
		wm := startDistributor()
		defer wm.close()

		bucket := "test"
		key := "test"
		value := "updated value"

		receiveChan, err := wm.subscribe(bucket, key)
		assert.NoError(t, err)
		err = wm.sendMessage(&message{bucket: bucket, key: key, value: []byte(value)})
		assert.NoError(t, err, "send message to watch manager")

		select {
		case message := <-receiveChan:
			assert.Equal(t, bucket, message.bucket)
			assert.Equal(t, key, string(message.key))
			assert.Equal(t, []byte(value), message.value)
		case <-time.After(1 * time.Second):
			t.Fatal("timeout waiting for message")
		}
	})

	t.Run("subscribe and drop messages", func(t *testing.T) {
		wm := startDistributor()
		defer wm.close()

		bucket := "bucket_test"
		key := "key_test"

		//No need to receive the message

		receiveChan, err := wm.subscribe(bucket, key)
		assert.NoError(t, err)

		// Verify subscriber exists
		wm.mu.Lock()
		assert.Contains(t, wm.lookup, bucket)
		assert.Contains(t, wm.lookup[bucket], key)
		wm.mu.Unlock()

		// Send enough messages to:
		// 1. Fill receiveChan buffer (1000)
		// 2. Trigger 100+ drops
		// Total: 1100+ messages
		messagesToSend := 1100

		//Send messages to the watch manager until all messages are dropped
		for i := 0; i < messagesToSend; i++ {
			value := fmt.Sprintf("value_test_%d", i)
			err := wm.sendMessage(&message{bucket: bucket, key: key, value: []byte(value)})
			if err != nil {
				fmt.Printf("[watch_manager_test] Send message err %d: %v\n", i, err)
			}
			assert.NoError(t, err)
		}

		// Give distributor time to process messages and close channel
		time.Sleep(5 * time.Second)

		// Drain all buffered messages
		drained := 0
		for {
			select {
			case _, ok := <-receiveChan:
				if !ok {
					// Channel is closed AND empty
					t.Logf("Channel closed after draining %d messages", drained)
					goto channelClosed
				}
				drained++
			case <-time.After(2 * time.Second):
				t.Fatalf("Channel not closed after timeout, drained %d messages", drained)
			}
		}

	channelClosed:
		// Now verify subscriber was removed
		assert.Equal(t, drained, 1000)
		assert.NotContains(t, wm.lookup, bucket)
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

						assert.Equal(t, bucket, message.bucket)
						assert.Equal(t, key, message.key)
						assert.NotNil(t, message.value)

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

				err := wm.sendMessage(&message{
					bucket: bucket,
					key:    key,
					value:  []byte(value),
				})
				assert.NoError(t, err)
			}
		}

		// Wait for all subscribers to finish (with timeout)
		wg.Wait()
	})
}

func startDistributor() *watchManager {
	wm := NewWatchManager()
	go wm.startDistributor()

	time.Sleep(2 * time.Second) // Let distributor start

	return wm
}
