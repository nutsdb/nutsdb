package nutsdb

import (
	"context"
	"fmt"
	"sync"
)

// Constants for configuration
const (
	watchChanBufferSize      = 1000
	receiveChanBufferSize    = 1000
	maxBatchSize             = 1000
	deadMessageThreshold     = 100
	distributeChanBufferSize = 100
)

type message struct {
	bucket string
	key    string
	value  []byte
}

type subscriber struct {
	bucket       string
	key          string
	receiveChan  chan *message
	deadMessages int
	watching     int
}

type watchManager struct {
	lookup         map[string]map[string]*subscriber
	watchChan      chan *message
	distributeChan chan []*message
	// cancellation for in-flight tasks
	workerCtx    context.Context
	workerCancel context.CancelFunc
	wg           sync.WaitGroup

	mu sync.Mutex
}

func NewWatchManager() *watchManager {
	ctx := context.Background()
	workerCtx, workerCancel := context.WithCancel(ctx)
	return &watchManager{
		lookup:         make(map[string]map[string]*subscriber),
		watchChan:      make(chan *message, watchChanBufferSize),
		distributeChan: make(chan []*message, distributeChanBufferSize),
		mu:             sync.Mutex{},
		workerCtx:      workerCtx,
		workerCancel:   workerCancel,
		wg:             sync.WaitGroup{},
	}
}

// send a message to the watch manager
func (wm *watchManager) sendMessage(message *message) error {
	if wm.workerCtx.Err() != nil {
		return ErrWatchManagerClosed
	}

	select {
	case wm.watchChan <- message:
		return nil
	case <-wm.workerCtx.Done():
		return ErrWatchManagerClosed
	default:
		return ErrWatchChanCannotSend
	}
}

// startDistributor starts both the collector and distributor goroutines
func (wm *watchManager) startDistributor() error {
	defer wm.cleanUpSubscribers()

	// Start the distributor goroutine (consumes from distributeChan)
	wm.wg.Add(1)
	go func() {
		defer wm.wg.Done()
		wm.runDistributor()
	}()

	// Start the collector goroutine (collects messages into batches)
	wm.wg.Add(1)
	go func() {
		defer wm.wg.Done()
		wm.runCollector()
	}()

	wm.wg.Wait()

	return nil
}

// runCollector collects messages from watchChan and batches them
func (wm *watchManager) runCollector() {
	batches := make([]*message, 0, maxBatchSize)

	defer func() {
		// Drain and send final batch before exiting
		if len(batches) > 0 {
			select {
			case wm.distributeChan <- batches:
			default:
				fmt.Printf("[watch_manager] Dropping final batch of %d messages\n", len(batches))
			}
		}

		close(wm.distributeChan)
	}()

	sendBatchToDistributor := func(batch []*message) {
		select {
		case wm.distributeChan <- batch:
		case <-wm.workerCtx.Done():
		default:
			fmt.Printf("[watch_manager] Distribution channel full, dropping batch of %d messages\n", len(batch))
		}
	}

	for {
		select {
		case msg, ok := <-wm.watchChan:
			if !ok {
				return
			}
			batches = append(batches, msg)
		case <-wm.workerCtx.Done():
			return
		}

	accumulate:
		for {
			if len(batches) >= maxBatchSize {
				sendBatchToDistributor(batches)
				batches = make([]*message, 0, maxBatchSize)
				break accumulate
			}

			select {
			case msg, ok := <-wm.watchChan:
				if !ok {
					return
				}
				batches = append(batches, msg)

			case <-wm.workerCtx.Done():
				return

			default:
				if len(batches) > 0 {
					sendBatchToDistributor(batches)
					batches = make([]*message, 0, maxBatchSize)
				}
				break accumulate
			}
		}
	}
}

// runDistributor distributes batches to subscribers
func (wm *watchManager) runDistributor() {
	for {
		select {
		case batch, ok := <-wm.distributeChan:
			if !ok {
				return
			}
			wm.distributeAllMessages(batch)

		case <-wm.workerCtx.Done():
			// drain the distribute channel
			for {
				select {
				case batch, ok := <-wm.distributeChan:
					if !ok {
						return
					}
					wm.distributeAllMessages(batch)
				default:
					return
				}
			}
		}
	}
}

// distribute the messages to the subscribers
func (wm *watchManager) distributeAllMessages(messages []*message) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if len(messages) == 0 {
		return nil
	}

	for _, message := range messages {
		bucket := (*message).bucket
		bucketMap, ok1 := wm.lookup[bucket]
		if !ok1 {
			continue
		}

		key := string((*message).key)
		subscriber, ok2 := bucketMap[key]
		if !ok2 {
			continue
		}

		//Avoid blocking the distributor, all messages blocked will be dropped
		select {
		case subscriber.receiveChan <- message:
			subscriber.deadMessages = 0
		default:
			subscriber.deadMessages++
			if subscriber.deadMessages >= deadMessageThreshold {
				fmt.Printf("Force-unsubscribing slow subscriber %s/%s\n",
					message.bucket, message.key)
				close(subscriber.receiveChan)
				delete(wm.lookup[message.bucket], message.key)
				if len(wm.lookup[message.bucket]) == 0 {
					delete(wm.lookup, message.bucket)
				}
			}
		}
	}

	return nil
}

// subscribe to the key and bucket
func (wm *watchManager) subscribe(bucket string, key string) (<-chan *message, error) {
	if wm.workerCtx.Err() != nil {
		return nil, ErrWatchManagerClosed
	}

	wm.mu.Lock()
	defer wm.mu.Unlock()
	if _, ok := wm.lookup[bucket]; !ok {
		wm.lookup[bucket] = make(map[string]*subscriber)
	}

	if subscriber, ok := wm.lookup[bucket][key]; ok {
		subscriber.watching++
		return subscriber.receiveChan, nil
	}

	receiveChan := make(chan *message, receiveChanBufferSize)
	subscriber := subscriber{
		bucket:      bucket,
		key:         key,
		receiveChan: receiveChan,
		watching:    1,
	}

	wm.lookup[bucket][key] = &subscriber

	return receiveChan, nil
}

// unsubcribe from the key and bucket
func (wm *watchManager) unsubscribe(bucket string, key string) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	subscriber, err := wm.findKeyAndReturnSubscriber(bucket, key)
	if err != nil {
		return err
	}
	subscriber.watching--
	if subscriber.watching == 0 {
		close(subscriber.receiveChan)
		delete(wm.lookup[bucket], key)
	}

	if len(wm.lookup[bucket]) == 0 {
		delete(wm.lookup, bucket)
	}

	return nil
}

func (wm *watchManager) cleanUpSubscribers() {
	for bucket, bucketMap := range wm.lookup {
		for _, subscriber := range bucketMap {
			close(subscriber.receiveChan)
			delete(bucketMap, subscriber.key)
		}

		delete(wm.lookup, bucket)
	}
}

func (wm *watchManager) close() error {
	if wm.workerCtx.Err() != nil {
		return ErrWatchManagerClosed
	}

	wm.workerCancel()

	close(wm.watchChan)
	return nil
}

func (wm *watchManager) findKeyAndReturnSubscriber(bucket string, key string) (*subscriber, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if _, ok := wm.lookup[bucket]; !ok {
		return nil, ErrBucketSubscriberNotFound
	}
	if _, ok := wm.lookup[bucket][key]; !ok {
		return nil, ErrKeySubscriberNotFound
	}

	return wm.lookup[bucket][key], nil
}
