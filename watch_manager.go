package nutsdb

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// errors
var (
	ErrBucketSubscriberNotFound = errors.New("bucket subscriber not found")
	ErrKeySubscriberNotFound    = errors.New("key subscriber not found")
	ErrSubscriberNotFound       = errors.New("subscriber not found")
	ErrWatchChanCannotSend      = errors.New("watch channel cannot send")
	ErrKeyAlreadySubscribed     = errors.New("key already subscribed")
	ErrWatchManagerClosed       = errors.New("watch manager closed")
	ErrWatchingCallbackFailed   = errors.New("watching callback failed")
	ErrWatchingChannelClosed    = errors.New("watching channel closed")
	ErrChannelNotAvailable      = errors.New("channel not available")
)

// convert these variables to var for testing
var (
	watchChanBufferSize      = 1024
	receiveChanBufferSize    = 1024
	maxBatchSize             = 1024
	deadMessageThreshold     = 100
	distributeChanBufferSize = 128
	victimBucketBufferSize   = 128
)

const (
	DefaultCallbackTimeout = 1 * time.Second
)

// message priority
const (
	MessagePriorityHigh   = iota // the messages must be ensured to deliver to distributor
	MessagePriorityMedium = 1    // the messages may be dropped
)

type (
	bucketToSubscribers       map[BucketName]map[string]map[uint64]*subscriber
	victimBucketToSubscribers map[uint64]map[string]map[uint64]*subscriber
	victimBucketChan          chan victimBucketToSubscribers
	MessagePriority           int

	WatchOptions struct {
		CallbackTimeout time.Duration
	}

	MessageOptions struct {
		Priority MessagePriority
	}
)

type Message struct {
	BucketName BucketName
	Key        string
	Value      []byte
	Flag       DataFlag
	Timestamp  uint64
	priority   MessagePriority
}

func NewMessage(bucketName BucketName, key string, value []byte, flag DataFlag, timestamp uint64, options ...MessageOptions) *Message {
	var priority MessagePriority
	// default priority is medium
	priority = MessagePriorityMedium

	if len(options) > 0 {
		priority = options[0].Priority
	}

	return &Message{
		BucketName: bucketName,
		Key:        key,
		Value:      value,
		Flag:       flag,
		Timestamp:  timestamp,
		priority:   priority,
	}
}

func NewWatchOptions() *WatchOptions {
	return &WatchOptions{
		CallbackTimeout: DefaultCallbackTimeout,
	}
}

// WithCallbackTimeout sets the callback timeout
func (opts *WatchOptions) WithCallbackTimeout(timeout time.Duration) {
	opts.CallbackTimeout = timeout
}

type subscriber struct {
	id           uint64
	bucketName   BucketName
	key          string
	receiveChan  chan *Message
	deadMessages int
	active       atomic.Bool
}

type watchManager struct {
	lookup         bucketToSubscribers // bucketName -> key -> id -> subscriber
	watchChan      chan *Message       // the hub channel to receive the messages
	distributeChan chan []*Message     // the collector worker collects messages from watchChan and sends them to the distributor worker through distributeChan
	victimMaps     victimBucketToSubscribers
	victimChan     victimBucketChan
	workerCtx      context.Context // cancellation for in-flight tasks
	workerCancel   context.CancelFunc
	wg             sync.WaitGroup

	closed      atomic.Bool
	mu          sync.Mutex
	idGenerator *IDGenerator
}

func NewWatchManager() *watchManager {
	ctx := context.Background()
	workerCtx, workerCancel := context.WithCancel(ctx)
	return &watchManager{
		lookup:         make(bucketToSubscribers),
		watchChan:      make(chan *Message, watchChanBufferSize),
		distributeChan: make(chan []*Message, distributeChanBufferSize),
		closed:         atomic.Bool{},
		workerCtx:      workerCtx,
		workerCancel:   workerCancel,
		idGenerator:    &IDGenerator{currentMaxId: 0},
		victimMaps:     make(victimBucketToSubscribers),
		victimChan:     make(victimBucketChan, victimBucketBufferSize),
	}
}

// send a message to the watch manager
func (wm *watchManager) sendMessage(message *Message) error {
	if wm.closed.Load() {
		return ErrWatchManagerClosed
	}

	// the high priority messages must be ensured to push to the watch channel
	if message.priority == MessagePriorityHigh {
		select {
		case wm.watchChan <- message:
			log.Printf("[watch_manager] Sent high priority message %s/%s to watch channel\n", message.BucketName, message.Key)
		case <-wm.workerCtx.Done():
			return ErrWatchManagerClosed
		}
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

func (wm *watchManager) sendUpdatedEntries(entries []*Entry, deletedbuckets map[BucketName]bool, getBucketName func(bucketId BucketId) (BucketName, error)) error {
	if wm.closed.Load() {
		return ErrWatchManagerClosed
	}

	// send all updated entries to the watch manager
	if len(entries) > 0 {
		for _, entry := range entries {
			bucketName, err := getBucketName(entry.Meta.BucketId)
			if err != nil {
				continue
			}

			rawKey, err := entry.getRawKey()
			if err != nil {
				log.Printf("get raw key %+v error: %+v", entry.Key, err)
				continue
			}

			message := NewMessage(bucketName, string(rawKey), entry.Value, entry.Meta.Flag, entry.Meta.Timestamp)
			if err := wm.sendMessage(message); err != nil {
				return err
			}
		}
	}

	//
	for bucketName := range deletedbuckets {
		message := NewMessage(bucketName, "", nil, DataBucketDeleteFlag, uint64(time.Now().Unix()), MessageOptions{Priority: MessagePriorityHigh})
		if err := wm.sendMessage(message); err != nil {
			return err
		}
	}

	return nil
}

// startDistributor starts both the collector and distributor goroutines
func (wm *watchManager) startDistributor() {
	defer wm.cleanUpSubscribers()

	// start the victim collector goroutine
	// it collects the victim buckets from the victim channel
	// and handle delete bucket operation
	wm.wg.Add(1)
	go func() {
		defer wm.wg.Done()
		wm.runVictimCollector()
	}()

	// Start the distributor goroutine (consumes from distributeChan)
	wm.wg.Add(1)
	go func() {
		defer wm.wg.Done()
		wm.runDistributor()
	}()

	// start the collector goroutine (collects messages into batches)
	wm.wg.Add(1)
	go func() {
		defer wm.wg.Done()
		wm.runCollector()
	}()

	wm.wg.Wait()
}

// runCollector collects messages from watchChan and batches them
func (wm *watchManager) runCollector() {
	batches := make([]*Message, 0, maxBatchSize)

	defer func() {
		// drain and send final batch before exiting
		if len(batches) > 0 {
			select {
			case wm.distributeChan <- batches:
			default:
				log.Printf("[watch_manager] Dropping final batch of %d messages\n", len(batches))
			}
		}

		close(wm.distributeChan)
		close(wm.watchChan)
	}()

	sendBatchToDistributor := func(batch []*Message) {
		sendBatch := make([]*Message, len(batch))
		copy(sendBatch, batch)

		select {
		case wm.distributeChan <- sendBatch:
		case <-wm.workerCtx.Done():
		default:
			log.Printf("[watch_manager] Distribution channel full, dropping batch of %d messages\n", len(sendBatch))
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
				batches = batches[:0]
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
					batches = batches[:0]
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

// runVictimCollector collects the victim buckets from the victim channel
// and handle delete bucket operation
// The bucket is deleted only when its all ds bucket are deleted
// we will send the delete bucket message to the subscribers when the bucket is deleted
func (wm *watchManager) runVictimCollector() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case victimBucketToSubscribers, ok := <-wm.victimChan:
			if !ok {
				return
			}

			for identifierId, bucketMap := range victimBucketToSubscribers {
				for key, keyMap := range bucketMap {
					for _, subscriber := range keyMap {
						if subscriber.active.Load() {
							message := NewMessage(subscriber.bucketName, subscriber.key, nil, DataBucketDeleteFlag, uint64(time.Now().Unix()))
							select {
							case subscriber.receiveChan <- message:
							default:
								// drop the message
							}
							close(subscriber.receiveChan)
							subscriber.active.Store(false)
						}
						delete(keyMap, subscriber.id)
					}
					delete(bucketMap, key)
				}
				delete(victimBucketToSubscribers, identifierId)
			}
		case <-ticker.C:
			// avoid busy spinning
		case <-wm.workerCtx.Done():
			return
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

	dropMessage := func(message *Message, subscriber *subscriber) {
		log.Printf("[watch_manager] Force-unsubscribing slow subscriber with id %d for message %s/%s\n",
			subscriber.id, message.BucketName, message.Key)

		if _, err := wm.findSubscriber(message.BucketName, message.Key, subscriber.id); err == nil {
			delete(wm.lookup[message.BucketName][message.Key], subscriber.id)
			if len(wm.lookup[message.BucketName][message.Key]) == 0 {
				delete(wm.lookup[message.BucketName], message.Key)
			}
			if len(wm.lookup[message.BucketName]) == 0 {
				delete(wm.lookup, message.BucketName)
			}
			if subscriber.active.Load() {
				close(subscriber.receiveChan)
				subscriber.active.Store(false)
			}
		}
	}

	for _, message := range messages {
		bucketMap, ok := wm.lookup[message.BucketName]
		if !ok {
			continue
		}

		if message.Flag == DataBucketDeleteFlag {
			// delete the bucket from the lookup
			wm.deleteBucket(*message)
			continue
		}

		key := message.Key
		subscriberMap, ok := bucketMap[key]
		if !ok {
			continue
		}

		// avoid blocking the distributor, all messages blocked will be dropped
		for _, subscriber := range subscriberMap {
			if !subscriber.active.Load() {
				log.Printf("[watch_manager] Skipping inactive subscriber with id %d for message %s/%s\n", subscriber.id, message.BucketName, message.Key)
				continue
			}

			select {
			case subscriber.receiveChan <- message:
				subscriber.deadMessages = 0
			default:
				// when the messages are not pushed to dropChan, we consider it as dead
				subscriber.deadMessages++
				if subscriber.deadMessages >= deadMessageThreshold {
					dropMessage(message, subscriber)
				}
			}
		}
	}

	return nil
}

// subscribe to the key and bucket
// each subscriber has a own channel to receive messages
func (wm *watchManager) subscribe(bucketName BucketName, key string) (*subscriber, error) {
	if wm.isClosed() {
		return nil, ErrWatchManagerClosed
	}

	wm.mu.Lock()
	defer wm.mu.Unlock()
	if _, ok := wm.lookup[bucketName]; !ok {
		wm.lookup[bucketName] = make(map[string]map[uint64]*subscriber)
	}

	receiveChan := make(chan *Message, receiveChanBufferSize)

	if _, ok := wm.lookup[bucketName][key]; !ok {
		wm.lookup[bucketName][key] = make(map[uint64]*subscriber)
	}

	id := wm.idGenerator.GenId()
	registeredSubscriber := subscriber{
		id:          id,
		bucketName:  bucketName,
		key:         key,
		receiveChan: receiveChan,
		active:      atomic.Bool{},
	}
	registeredSubscriber.active.Store(true)

	wm.lookup[bucketName][key][id] = &registeredSubscriber

	return &registeredSubscriber, nil
}

// unsubscribe from the key and bucket
func (wm *watchManager) unsubscribe(bucketName BucketName, key string, id BucketId) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	subscriber, err := wm.findSubscriber(bucketName, key, id)
	if err != nil {
		return err
	}

	// Clean up the subscriber
	delete(wm.lookup[bucketName][key], id)
	if len(wm.lookup[bucketName][key]) == 0 {
		delete(wm.lookup[bucketName], key)
	}
	if len(wm.lookup[bucketName]) == 0 {
		delete(wm.lookup, bucketName)
	}

	// Close channel if still active
	if subscriber.active.Load() {
		close(subscriber.receiveChan)
		subscriber.active.Store(false)
	}

	return nil
}

func (wm *watchManager) cleanUpSubscribers() {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	for bucket, bucketMap := range wm.lookup {
		for key, keyMap := range bucketMap {
			for _, subscriber := range keyMap {
				if subscriber.active.Load() {
					close(subscriber.receiveChan)
					subscriber.active.Store(false)
				}
				delete(keyMap, subscriber.id)
			}
			delete(bucketMap, key)
		}
		delete(wm.lookup, bucket)
	}
}

func (wm *watchManager) close() error {
	if wm.workerCtx.Err() != nil {
		return ErrWatchManagerClosed
	}

	wm.workerCancel()

	wm.closed.Store(true)
	return nil
}

func (wm *watchManager) findSubscriber(bucketName BucketName, key string, id uint64) (*subscriber, error) {
	if _, ok := wm.lookup[bucketName]; !ok {
		return nil, ErrBucketSubscriberNotFound
	}
	if _, ok := wm.lookup[bucketName][key]; !ok {
		return nil, ErrKeySubscriberNotFound
	}

	if subscriber, ok := wm.lookup[bucketName][key][id]; ok {
		return subscriber, nil
	}
	return nil, ErrSubscriberNotFound
}

func (wm *watchManager) done() <-chan struct{} {
	return wm.workerCtx.Done()
}

func (wm *watchManager) isClosed() bool {
	return wm.closed.Load()
}

/*
* delete the buckets from the watch manager
* and notify the subscribers that the keys are deleted due to deleted buckets
* @param deletedbuckets: the buckets to be deleted
 */
func (wm *watchManager) deleteBucket(deletingMessageBucket Message) {
	bucketName := deletingMessageBucket.BucketName

	if _, ok := wm.lookup[bucketName]; !ok {
		return
	}

	identifierId := wm.idGenerator.GenId()
	victimBucketToSubscribers := make(victimBucketToSubscribers)
	victimBucketToSubscribers[identifierId] = wm.lookup[bucketName]
	delete(wm.lookup, bucketName)

	// Log before sending to avoid race condition with victimCollector
	log.Printf("[watch_manager] Moving bucket %s to victim channel (identifier: %d)\n", bucketName, identifierId)

	// wait for the victim channel to be available
	timeOut := time.After(10 * time.Second)
	for {
		select {
		case <-timeOut:
			log.Printf("[watch_manager] Timeout sending victim bucket %s to channel\n", bucketName)
			return
		case wm.victimChan <- victimBucketToSubscribers:
			// Successfully sent - victimCollector now owns the map, don't access it anymore
			return
		}
	}
}
