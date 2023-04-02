package timingwheel

import (
	"container/heap"
	"sync"
	"time"
)

type item struct {
	Value interface{}
	TTL   int64
	// to mask which one is the first item
	Index int
}

// priorityQueue implements the heap interface represent a min heap
// it is used by delayQueue
type priorityQueue []*item

func newPriorityQueue(capacity int) priorityQueue {
	return make(priorityQueue, 0, capacity)
}

func (pq priorityQueue) Len() int {
	return len(pq)
}

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].TTL < pq[j].TTL
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	c := cap(*pq)
	if n+1 > c {
		npq := make(priorityQueue, n, c*2)
		copy(npq, *pq)
		*pq = npq
	}
	*pq = (*pq)[0 : n+1]
	item := x.(*item)
	item.Index = n
	(*pq)[n] = item
}

func (pq *priorityQueue) Pop() interface{} {
	n := len(*pq)
	c := cap(*pq)
	if n < (c/2) && c > 25 {
		npq := make(priorityQueue, n, c/2)
		copy(npq, *pq)
		*pq = npq
	}
	item := (*pq)[n-1]
	item.Index = -1
	*pq = (*pq)[0 : n-1]
	return item
}

func (pq *priorityQueue) GetFirstItem() *item {
	if pq.Len() == 0 {
		return nil
	}

	item := (*pq)[0]

	heap.Remove(pq, 0)

	return item
}

type DelayQueue struct {
	ExpireChan chan interface{}

	mu sync.Mutex
	pq priorityQueue

	wakeupChan chan struct{}
}

// New_DelayQueue creates a delayQueue with the specified size.
func New_DelayQueue(size int) *DelayQueue {
	return &DelayQueue{
		ExpireChan: make(chan interface{}),
		pq:         newPriorityQueue(size),
		wakeupChan: make(chan struct{}),
	}
}

// Offer insert item into the queue
func (dq *DelayQueue) Offer(elem interface{}, expiration int64) {
	item := &item{Value: elem, TTL: expiration}

	dq.mu.Lock()
	heap.Push(&dq.pq, item)
	index := item.Index
	dq.mu.Unlock()

	if index == 0 {
		// A new item with the earliest expiration is added.
		select {
		case dq.wakeupChan <- struct{}{}:

		default:

		}
	}
}

// Poll Loop to listen until an element in the queue expires.
func (dq *DelayQueue) Poll(exitC chan struct{}, nowF func() int64) {
	for {
		now := nowF()

		dq.mu.Lock()
		item := dq.pq.GetFirstItem()

		// Avoid false wakeup by previous elements
		select {
		case <-dq.wakeupChan:
		default:
		}

		dq.mu.Unlock()

		if item == nil {
			// if the queue is empty
			select {
			case <-dq.wakeupChan:
				// Wait until a new item is added.
				continue
			case <-exitC:
				goto exit
			}
		} else if item.TTL-now != 0 {
			select {
			case <-dq.wakeupChan:
				// it represents that there is a new item which earlier than this one
				continue
			case <-time.After(time.Duration(item.TTL-now) * time.Millisecond):
				// The current "earliest" item expires.
				continue
			case <-exitC:
				goto exit
			}
		}

		select {
		case dq.ExpireChan <- item.Value:
			// send item's value to the expired channel
			// It is convenient for us to use the streaming consumption of go language
		case <-exitC:
			goto exit
		}
	}

exit:
}
