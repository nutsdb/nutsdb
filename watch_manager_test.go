package nutsdb

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSubscribeAndSendMessage(t *testing.T) {

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

	t.Run("Multiple subscribers", func(t *testing.T) {
		wm := startDistributor()
		defer wm.close()

		wg := sync.WaitGroup{}
		expectedMsgCount := 10 // Each subscriber expects 10 messages

		// Start 100 subscriber goroutines
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				bucket := fmt.Sprintf("bucket_test_%d", i)
				key := fmt.Sprintf("key_test_%d", i)

				receiveChan, err := wm.subscribe(bucket, key)
				assert.NoError(t, err)

				receivedCount := 0
				timeout := time.After(10 * time.Second)

				for receivedCount < expectedMsgCount {
					select {
					case message, ok := <-receiveChan:
						if !ok {
							// Channel closed
							return
						}

						assert.Equal(t, bucket, message.bucket)
						assert.Equal(t, key, message.key)
						assert.NotNil(t, message.value)

						receivedCount++

					case <-timeout:
						t.Errorf("Subscriber %d only received %d/%d messages",
							i, receivedCount, expectedMsgCount)
						return
					}
				}
			}(i)
		}

		// Give subscribers time to register
		time.Sleep(100 * time.Millisecond)

		// Send 1000 messages (10 per subscriber on average)
		for i := 0; i < 1000; i++ {
			index := rand.Intn(100)
			bucket := fmt.Sprintf("bucket_test_%d", index)
			key := fmt.Sprintf("key_test_%d", index)
			value := fmt.Sprintf("value_test_%d_%d", index, i)

			err := wm.sendMessage(&message{
				bucket: bucket,
				key:    key,
				value:  []byte(value),
			})
			assert.NoError(t, err)
		}

		// Wait for all subscribers to receive their messages
		wg.Wait()
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
}

func startDistributor() *watchManager {
	wm := NewWatchManager()
	go wm.startDistributor()

	time.Sleep(2 * time.Second) // Let distributor start

	return wm
}
