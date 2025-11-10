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

type Message struct {
	BucketName BucketName
	Key        string
	Value      []byte
	Flag       DataFlag
	Timestamp  uint64
}

func NewMessage(bucketName BucketName, key string, value []byte, flag DataFlag, timestamp uint64) *Message {
	return &Message{
		BucketName: bucketName,
		Key:        key,
		Value:      value,
		Flag:       flag,
		Timestamp:  timestamp,
	}
}

type subscriber struct {
	bucketName   BucketName
	key          string
	receiveChan  chan *Message
	deadMessages int
	watching     int
	closed       bool
}

type watchManager struct {
	lookup         map[BucketName]map[string]*subscriber
	watchChan      chan *Message
	distributeChan chan []*Message
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
		lookup:         make(map[BucketName]map[string]*subscriber),
		watchChan:      make(chan *Message, watchChanBufferSize),
		distributeChan: make(chan []*Message, distributeChanBufferSize),
		workerCtx:      workerCtx,
		workerCancel:   workerCancel,
	}
}

// send a message to the watch manager
func (wm *watchManager) sendMessage(message *Message) error {
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

func (wm *watchManager) sendUpdatedEntries(entries []*Entry, getBucketName func(bucketId BucketId) (BucketName, error)) error {
	for _, entry := range entries {
		bucketName, err := getBucketName(entry.Meta.BucketId)
		if err != nil {
			continue
		}

		message := NewMessage(bucketName, string(entry.Key), entry.Value, entry.Meta.Flag, entry.Meta.Timestamp)
		if err := wm.sendMessage(message); err != nil {
			return err
		}
	}
	return nil

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
	batches := make([]*Message, 0, maxBatchSize)

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

	sendBatchToDistributor := func(batch []*Message) {
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
				batches = make([]*Message, 0, maxBatchSize)
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
					batches = make([]*Message, 0, maxBatchSize)
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
func (wm *watchManager) distributeAllMessages(messages []*Message) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if len(messages) == 0 {
		return nil
	}

	for _, message := range messages {
		bucketMap, ok1 := wm.lookup[message.BucketName]
		if !ok1 {
			continue
		}

		key := string(message.Key)
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
					message.BucketName, message.Key)

				if _, err := wm.findKeyAndReturnSubscriber(message.BucketName, message.Key); err == nil {
					delete(wm.lookup[message.BucketName], message.Key)
					if len(wm.lookup[message.BucketName]) == 0 {
						delete(wm.lookup, message.BucketName)
					}

					if !subscriber.closed {
						close(subscriber.receiveChan)
						subscriber.closed = true
					}
				}
			}
		}
	}

	return nil
}

// subscribe to the key and bucket
func (wm *watchManager) subscribe(bucketName BucketName, key string) (<-chan *Message, error) {
	if wm.workerCtx.Err() != nil {
		return nil, ErrWatchManagerClosed
	}

	wm.mu.Lock()
	defer wm.mu.Unlock()
	if _, ok := wm.lookup[bucketName]; !ok {
		wm.lookup[bucketName] = make(map[string]*subscriber)
	}

	if subscriber, ok := wm.lookup[bucketName][key]; ok {
		subscriber.watching++
		return subscriber.receiveChan, nil
	}

	receiveChan := make(chan *Message, receiveChanBufferSize)
	subscriber := subscriber{
		bucketName:  bucketName,
		key:         key,
		receiveChan: receiveChan,
		watching:    1,
	}

	wm.lookup[bucketName][key] = &subscriber

	return receiveChan, nil
}

// unsubscribe from the key and bucket
func (wm *watchManager) unsubscribe(bucketName BucketName, key string) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	subscriber, err := wm.findKeyAndReturnSubscriber(bucketName, key)
	if err != nil {
		return err
	}
	subscriber.watching--
	if subscriber.watching == 0 {
		if !subscriber.closed {
			close(subscriber.receiveChan)
			subscriber.closed = true
		}
		delete(wm.lookup[bucketName], key)
	}

	if len(wm.lookup[bucketName]) == 0 {
		delete(wm.lookup, bucketName)
	}

	return nil
}

func (wm *watchManager) cleanUpSubscribers() {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	for bucket, bucketMap := range wm.lookup {
		for _, subscriber := range bucketMap {
			if !subscriber.closed {
				close(subscriber.receiveChan)
				subscriber.closed = true
			}
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

func (wm *watchManager) findKeyAndReturnSubscriber(bucketName BucketName, key string) (*subscriber, error) {
	if _, ok := wm.lookup[bucketName]; !ok {
		return nil, ErrBucketSubscriberNotFound
	}
	if _, ok := wm.lookup[bucketName][key]; !ok {
		return nil, ErrKeySubscriberNotFound
	}

	return wm.lookup[bucketName][key], nil
}

func (wm *watchManager) done() <-chan struct{} {
	return wm.workerCtx.Done()
}

func (wm *watchManager) isClosed() bool {
	return wm.workerCtx.Err() != nil
}
