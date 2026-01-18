package nutsdb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
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
	ErrCloseWatchManagerTimeout = errors.New("close watch manager timeout")
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
	bucketToSubscribers       map[core.BucketName]map[string]map[uint64]*subscriber
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
	BucketName core.BucketName
	Key        string
	Value      []byte
	Flag       DataFlag
	Timestamp  uint64
	priority   MessagePriority
}

func NewMessage(bucketName core.BucketName, key string, value []byte, flag DataFlag, timestamp uint64, options ...MessageOptions) *Message {
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

// Currently, use stats to track the success and failure of the watch manager
// for testing and benchmark
type WatcherManagerStat struct {
	successCnt atomic.Int64
	failureCnt atomic.Int64
	totalCnt   atomic.Int64
}

func NewWatcherManagerStat() *WatcherManagerStat {
	return &WatcherManagerStat{
		successCnt: atomic.Int64{},
		failureCnt: atomic.Int64{},
		totalCnt:   atomic.Int64{},
	}
}

func (ws *WatcherManagerStat) AddSuccess() {
	ws.successCnt.Add(1)
	ws.totalCnt.Add(1)
}

func (ws *WatcherManagerStat) AddFailure() {
	ws.failureCnt.Add(1)
	ws.totalCnt.Add(1)
}

func (ws *WatcherManagerStat) Reset() {
	ws.successCnt.Store(0)
	ws.failureCnt.Store(0)
	ws.totalCnt.Store(0)
}

func (ws *WatcherManagerStat) GetSuccessCount() int64 { return ws.successCnt.Load() }
func (ws *WatcherManagerStat) GetFailureCount() int64 { return ws.failureCnt.Load() }
func (ws *WatcherManagerStat) GetTotalCount() int64   { return ws.totalCnt.Load() }

type subscriber struct {
	id           uint64
	bucketName   core.BucketName
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

	closed      bool
	started     bool // indicates whether Start() has been called
	idGenerator *IDGenerator

	muClosed  sync.RWMutex
	muStarted sync.RWMutex
	mu        sync.Mutex
	stats     *WatcherManagerStat
}

func NewWatchManager() *watchManager {
	ctx := context.Background()
	workerCtx, workerCancel := context.WithCancel(ctx)
	stats := NewWatcherManagerStat()

	return &watchManager{
		lookup:         make(bucketToSubscribers),
		watchChan:      make(chan *Message, watchChanBufferSize),
		distributeChan: make(chan []*Message, distributeChanBufferSize),
		closed:         false,
		started:        false,
		workerCtx:      workerCtx,
		workerCancel:   workerCancel,
		idGenerator:    &IDGenerator{currentMaxId: 0},
		victimMaps:     make(victimBucketToSubscribers),
		victimChan:     make(victimBucketChan, victimBucketBufferSize),
		stats:          stats,
	}
}

// Name returns the component name
func (wm *watchManager) Name() string {
	return "WatchManager"
}

// send a message to the watch manager
func (wm *watchManager) sendMessage(message *Message) error {
	isError := false

	defer func() {
		if isError {
			wm.stats.AddFailure()
		}
	}()

	if wm.isClosed() {
		isError = true
		return ErrWatchManagerClosed
	}

	// the high priority messages must be ensured to push to the watch channel
	if message.priority == MessagePriorityHigh {
		select {
		case wm.watchChan <- message:
			log.Printf("[watch_manager] Sent high priority message %s/%s to watch channel\n", message.BucketName, message.Key)
			return nil
		case <-wm.workerCtx.Done():
			isError = true
			return ErrWatchManagerClosed
		}
	}

	select {
	case wm.watchChan <- message:
		return nil
	case <-wm.workerCtx.Done():
		isError = true
		return ErrWatchManagerClosed
	default:
		isError = true
		return ErrWatchChanCannotSend
	}
}

func (wm *watchManager) sendUpdatedEntries(entries []*core.Entry, deletedbuckets map[core.BucketName]bool, getBucketName func(bucketId core.BucketId) (core.BucketName, error)) error {
	if wm.isClosed() {
		return ErrWatchManagerClosed
	}

	// send all updated entries to the watch manager
	if len(entries) > 0 {
		for _, entry := range entries {
			bucketName, err := getBucketName(entry.Meta.BucketId)
			if err != nil {
				wm.stats.AddFailure()
				continue
			}

			rawKey, err := entry.GetRawKey()
			if err != nil {
				wm.stats.AddFailure()
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

	// Used for counting of batch faild to collect
	incrementBatchFailure := func(failedBatch []*Message) {
		for range failedBatch {
			wm.stats.AddFailure()
		}
	}

	defer func() {
		// drain and send final batch before exiting
		if len(batches) > 0 {
			select {
			case wm.distributeChan <- batches:
			default:
				log.Printf("[watch_manager] Dropping final batch of %d messages\n", len(batches))
				incrementBatchFailure(batches)
			}
		}

		close(wm.distributeChan)
		close(wm.watchChan)
	}()

	// function send the batch to the distributor
	sendBatchToDistributor := func(batch []*Message) {
		sendBatch := make([]*Message, len(batch))
		copy(sendBatch, batch)

		select {
		case wm.distributeChan <- sendBatch:
		case <-wm.workerCtx.Done():
		default:
			log.Printf("[watch_manager] Distribution channel full, dropping batch of %d messages\n", len(sendBatch))
			incrementBatchFailure(sendBatch)
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
			_ = wm.distributeAllMessages(batch)

		case <-wm.workerCtx.Done():
			// drain the distribute channel
			for {
				select {
				case batch, ok := <-wm.distributeChan:
					if !ok {
						return
					}
					_ = wm.distributeAllMessages(batch)
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
			wm.stats.AddSuccess()
			continue
		}

		if message.Flag == DataBucketDeleteFlag {
			// delete the bucket from the lookup
			wm.deleteBucket(message.BucketName)
			wm.stats.AddSuccess()
			continue
		}

		key := message.Key
		subscriberMap, ok := bucketMap[key]
		if !ok {
			wm.stats.AddSuccess()
			continue
		}

		isSuccess := false

		// avoid blocking the distributor, all messages blocked will be dropped
		for _, subscriber := range subscriberMap {
			if !subscriber.active.Load() {
				log.Printf("[watch_manager] Skipping inactive subscriber with id %d for message %s/%s\n", subscriber.id, message.BucketName, message.Key)
				continue
			}

			select {
			case subscriber.receiveChan <- message:
				isSuccess = true
				subscriber.deadMessages = 0
			default:
				// when the messages are not pushed to receiveChan, we consider it as dead
				subscriber.deadMessages++
				if subscriber.deadMessages >= deadMessageThreshold {
					dropMessage(message, subscriber)
				}
			}
		}

		// currently, i just view the message as success if there is at least one subscriber that receives the message
		// or it fails, if all subscribers fail to receive the message
		if !isSuccess {
			wm.stats.AddFailure()
		} else {
			wm.stats.AddSuccess()
		}

	}

	return nil
}

// subscribe to the key and bucket
// each subscriber has a own channel to receive messages
func (wm *watchManager) subscribe(bucketName core.BucketName, key string) (*subscriber, error) {
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
func (wm *watchManager) unsubscribe(bucketName core.BucketName, key string, id core.BucketId) error {
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
	wm.muClosed.Lock()
	defer wm.muClosed.Unlock()

	if wm.closed {
		return ErrWatchManagerClosed
	}

	wm.workerCancel()
	wm.closed = true

	return nil
}

func (wm *watchManager) findSubscriber(bucketName core.BucketName, key string, id uint64) (*subscriber, error) {
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
	wm.muClosed.RLock()
	defer wm.muClosed.RUnlock()

	return wm.closed
}

/*
* delete the buckets from the watch manager
* and notify the subscribers that the keys are deleted due to deleted buckets
* @param deletedbuckets: the buckets to be deleted
 */
func (wm *watchManager) deleteBucket(bucketName core.BucketName) {
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

// Start starts the watch manager
// Implements Component interface
func (wm *watchManager) Start(ctx context.Context) error {
	if wm.isClosed() {
		return ErrWatchManagerClosed
	}

	wm.muStarted.RLock()
	if wm.started {
		wm.muStarted.RUnlock()
		return nil
	}

	wm.started = true
	wm.muStarted.RUnlock()

	// use a local ready channel to wait for goroutine startup
	ready := make(chan struct{})

	go func() {
		close(ready) // signal that goroutine has started
		wm.startDistributor()
	}()

	// wait for distributor goroutine to start before returning
	select {
	case <-ready:
		log.Printf("[watch_manager] Watch manager distributor started\n")
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout waiting for watch manager distributor to start")
	}
}

// Stop stops the watch manager
// Notifies all subscribers that the database is closing and closes all subscription channels
// Implements Component interface
func (wm *watchManager) Stop(timeout time.Duration) (err error) {
	closeChan := make(chan struct{})

	// close watch manager
	// this cancels context and signals all goroutines to stop
	go func() {
		err = wm.close()
		close(closeChan)
	}()

	select {
	case <-closeChan:
		return err
	case <-time.After(timeout):
		return ErrCloseWatchManagerTimeout
	}
}
