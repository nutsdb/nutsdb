package nutsdb

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	DefaultMaxFileNums = 256
)

const (
	TooManyFileOpenErrSuffix = "too many open files"
)

// fdManager hold a fd cache in memory, it lru based cache.
type fdManager struct {
	sync.Mutex
	cache              map[string]*FdInfo
	fdList             *doubleLinkedList
	size               int
	cleanThresholdNums int
	maxFdNums          int
}

// newFdm will return a fdManager object
func newFdm(maxFdNums int, cleanThreshold float64) (fdm *fdManager) {
	fdm = &fdManager{
		cache:     map[string]*FdInfo{},
		fdList:    initDoubleLinkedList(),
		size:      0,
		maxFdNums: DefaultMaxFileNums,
	}
	fdm.cleanThresholdNums = int(math.Floor(0.5 * float64(fdm.maxFdNums)))
	if maxFdNums > 0 {
		fdm.maxFdNums = maxFdNums
	}

	if cleanThreshold > 0.0 && cleanThreshold < 1.0 {
		fdm.cleanThresholdNums = int(math.Floor(cleanThreshold * float64(fdm.maxFdNums)))
	}
	return fdm
}

// FdInfo holds base fd info
type FdInfo struct {
	fd    *os.File
	path  string
	using uint
	next  *FdInfo
	prev  *FdInfo
}

// getFd go through this method to get fd.
func (fdm *fdManager) getFd(path string) (fd *os.File, err error) {
	fdm.Lock()
	defer fdm.Unlock()
	cleanPath := filepath.Clean(path)
	if fdInfo := fdm.cache[cleanPath]; fdInfo == nil {
		fd, err = os.OpenFile(cleanPath, os.O_CREATE|os.O_RDWR, 0o644)
		if err == nil {
			// if the numbers of fd in cache larger than the cleanThreshold in config, we will clean useless fd in cache
			if fdm.size >= fdm.cleanThresholdNums {
				err = fdm.cleanUselessFd()
			}
			// if the numbers of fd in cache larger than the max numbers of fd in config, we will not add this fd to cache
			if fdm.size >= fdm.maxFdNums {
				return fd, nil
			}
			// add this fd to cache
			fdm.addToCache(fd, cleanPath)
			return fd, nil
		} else {
			// determine if there are too many open files, we will first clean useless fd in cache and try open this file again
			if strings.HasSuffix(err.Error(), TooManyFileOpenErrSuffix) {
				cleanErr := fdm.cleanUselessFd()
				// if something wrong in cleanUselessFd, we will return "open too many files" err, because we want user not the main err is that
				if cleanErr != nil {
					return nil, err
				}
				// try open this file again，if it still returns err, we will show this error to user
				fd, err = os.OpenFile(cleanPath, os.O_CREATE|os.O_RDWR, 0o644)
				if err != nil {
					return nil, err
				}
				// add to cache if open this file successfully
				fdm.addToCache(fd, cleanPath)
			}
			return fd, err
		}
	} else {
		fdInfo.using++
		fdm.fdList.moveNodeToFront(fdInfo)
		return fdInfo.fd, nil
	}
}

// addToCache add fd to cache
func (fdm *fdManager) addToCache(fd *os.File, cleanPath string) {
	fdInfo := &FdInfo{
		fd:    fd,
		using: 1,
		path:  cleanPath,
	}
	fdm.fdList.addNode(fdInfo)
	fdm.size++
	fdm.cache[cleanPath] = fdInfo
}

// reduceUsing when RWManager object close, it will go through this method let fdm know it return the fd to cache
func (fdm *fdManager) reduceUsing(path string) {
	fdm.Lock()
	defer fdm.Unlock()
	cleanPath := filepath.Clean(path)
	node, isExist := fdm.cache[cleanPath]
	if !isExist {
		panic("unexpected the node is not in cache")
	}
	node.using--
}

// close means close the cache.
func (fdm *fdManager) close() error {
	fdm.Lock()
	defer fdm.Unlock()
	node := fdm.fdList.tail.prev
	for node != fdm.fdList.head {
		err := node.fd.Close()
		if err != nil {
			return err
		}
		delete(fdm.cache, node.path)
		fdm.size--
		node = node.prev
	}
	fdm.fdList.head.next = fdm.fdList.tail
	fdm.fdList.tail.prev = fdm.fdList.head
	return nil
}

type doubleLinkedList struct {
	head *FdInfo
	tail *FdInfo
	size int
}

func initDoubleLinkedList() *doubleLinkedList {
	list := &doubleLinkedList{
		head: &FdInfo{},
		tail: &FdInfo{},
		size: 0,
	}
	list.head.next = list.tail
	list.tail.prev = list.head
	return list
}

func (list *doubleLinkedList) addNode(node *FdInfo) {
	list.head.next.prev = node
	node.next = list.head.next
	list.head.next = node
	node.prev = list.head
	list.size++
}

func (list *doubleLinkedList) removeNode(node *FdInfo) {
	node.prev.next = node.next
	node.next.prev = node.prev
	node.prev = nil
	node.next = nil
}

func (list *doubleLinkedList) moveNodeToFront(node *FdInfo) {
	list.removeNode(node)
	list.addNode(node)
}

func (fdm *fdManager) cleanUselessFd() error {
	cleanNums := fdm.cleanThresholdNums
	node := fdm.fdList.tail.prev
	for node != nil && node != fdm.fdList.head && cleanNums > 0 {
		nextItem := node.prev
		if node.using == 0 {
			fdm.fdList.removeNode(node)
			err := node.fd.Close()
			if err != nil {
				return err
			}
			fdm.size--
			delete(fdm.cache, node.path)
			cleanNums--
		}
		node = nextItem
	}
	return nil
}

func (fdm *fdManager) closeByPath(path string) error {
	fdm.Lock()
	defer fdm.Unlock()
	fdInfo, ok := fdm.cache[path]
	if !ok {
		return nil
	}
	fdm.fdList.removeNode(fdInfo)
	return fdInfo.fd.Close()
}
