package nutsdb

import (
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFdManager_All(t *testing.T) {
	dir := "test-data"
	testBasePath := dir + "/data-"
	err := os.Mkdir(dir, os.ModePerm)
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	startFdNums := 1
	maxFdNums := 20
	createFileLimit := 11
	cleanThreshold := 0.5

	defer func() {
		if panicErr := recover(); panicErr != nil {
			t.Logf("panic is %s", panicErr)
		}
		err := os.RemoveAll(testBasePath)
		assert.Nil(t, err)
	}()

	var fdm *fdManager
	t.Run("test init fdm", func(t *testing.T) {
		fdm = newFdm(maxFdNums, cleanThreshold)
		assert.NotNil(t, fdm)
		assert.Equal(t, maxFdNums, fdm.maxFdNums)
		assert.Equal(t, int(math.Floor(cleanThreshold*float64(fdm.maxFdNums))), fdm.cleanThresholdNums)
	})

	t.Run("create fd to cache", func(t *testing.T) {
		for i := startFdNums; i < createFileLimit; i++ {
			path := testBasePath + fmt.Sprint(i)
			fd, err := fdm.getFd(path)
			assert.Nil(t, err)
			assert.NotNil(t, fd)
		}
		positiveFdsSeq := []int{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}

		assertChainFromTailAndHead(t, fdm, testBasePath, positiveFdsSeq)
	})

	t.Run("test get fd in cache", func(t *testing.T) {
		t.Run("test get head item in cache", func(t *testing.T) {
			fd, err := fdm.getFd(fdm.fdList.head.next.path)
			assert.Nil(t, err)
			assert.NotNil(t, fd)
			assert.Equal(t, fdm.fdList.head.next.fd, fd)
			positiveFdsSeq := []int{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
			assertChainFromTailAndHead(t, fdm, testBasePath, positiveFdsSeq)
		})

		t.Run("test get tail item in cache", func(t *testing.T) {
			fd, err := fdm.getFd(fdm.fdList.tail.prev.path)
			assert.Nil(t, err)
			assert.NotNil(t, fd)
			assert.Equal(t, fdm.fdList.head.next.fd, fd)
			positiveFdsSeq := []int{1, 10, 9, 8, 7, 6, 5, 4, 3, 2}
			assertChainFromTailAndHead(t, fdm, testBasePath, positiveFdsSeq)
		})

		t.Run("test get middle item in cache", func(t *testing.T) {
			path := testBasePath + fmt.Sprint(5)
			fd, err := fdm.getFd(path)
			assert.Nil(t, err)
			assert.NotNil(t, fd)
			assert.Equal(t, fdm.fdList.head.next.fd, fd)
			positiveFdsSeq := []int{5, 1, 10, 9, 8, 7, 6, 4, 3, 2}
			assertChainFromTailAndHead(t, fdm, testBasePath, positiveFdsSeq)
		})
	})

	t.Run("test reduce using", func(t *testing.T) {
		path := testBasePath + fmt.Sprint(5)
		_, err := fdm.getFd(path)
		assert.Nil(t, err)
		using := fdm.fdList.head.next.using
		_, err = fdm.getFd(path)
		assert.Nil(t, err)
		assert.Equal(t, using+1, fdm.fdList.head.next.using)
		fdm.reduceUsing(path)
		assert.Nil(t, err)
		assert.Equal(t, using, fdm.fdList.head.next.using)
	})

	t.Run("test clean fd in cache", func(t *testing.T) {
		preReducePath := []int{2, 3, 4, 6, 7, 8}
		for _, pathNum := range preReducePath {
			path := testBasePath + fmt.Sprint(pathNum)
			fdm.reduceUsing(path)
			assert.Nil(t, err)
		}
		path := testBasePath + fmt.Sprint(11)
		fd, err := fdm.getFd(path)
		assert.Nil(t, err)
		assert.NotNil(t, fd)
		positiveFdsSeq := []int{11, 5, 1, 10, 9}
		assertChainFromTailAndHead(t, fdm, testBasePath, positiveFdsSeq)
	})

	t.Run("test close fdm", func(t *testing.T) {
		err := fdm.close()
		if err != nil {
			t.Logf("err during close is:%s", err)
		}
		assert.Nil(t, err)
		assert.Equal(t, 0, len(fdm.cache))
		assert.Equal(t, 0, fdm.size)
	})
}

func TestDoubleLinkedList_All(t *testing.T) {
	list := initDoubleLinkedList()
	nodeMap := make(map[int]*fdInfo)
	t.Run("test add node", func(t *testing.T) {
		for i := 1; i <= 10; i++ {
			fd := &fdInfo{
				path: fmt.Sprint(i),
			}
			list.addNode(fd)
			nodeMap[i] = fd
		}
		assert.Equal(t, `[10 9 8 7 6 5 4 3 2 1 ]`, fmt.Sprintf("%+v", getAllNodePathFromHead(list)))
		assert.Equal(t, `[1 2 3 4 5 6 7 8 9 10 ]`, fmt.Sprintf("%+v", getAllNodePathFromTail(list)))
	})

	t.Run("test remove node", func(t *testing.T) {
		t.Run("test remove first node", func(t *testing.T) {
			list.removeNode(nodeMap[10])
			assert.Equal(t, "[9 8 7 6 5 4 3 2 1 ]", fmt.Sprintf("%+v", getAllNodePathFromHead(list)))
			assert.Equal(t, "[1 2 3 4 5 6 7 8 9 ]", fmt.Sprintf("%+v", getAllNodePathFromTail(list)))
		})
		t.Run("test remove last node", func(t *testing.T) {
			list.removeNode(nodeMap[1])
			assert.Equal(t, "[9 8 7 6 5 4 3 2 ]", fmt.Sprintf("%+v", getAllNodePathFromHead(list)))
			assert.Equal(t, "[2 3 4 5 6 7 8 9 ]", fmt.Sprintf("%+v", getAllNodePathFromTail(list)))
		})
		t.Run("test remove middle node", func(t *testing.T) {
			list.removeNode(nodeMap[5])
			assert.Equal(t, "[9 8 7 6 4 3 2 ]", fmt.Sprintf("%+v", getAllNodePathFromHead(list)))
			assert.Equal(t, "[2 3 4 6 7 8 9 ]", fmt.Sprintf("%+v", getAllNodePathFromTail(list)))
		})
	})

	t.Run("test move node to head", func(t *testing.T) {
		t.Run("test move first node", func(t *testing.T) {
			list.moveNodeToFront(nodeMap[9])
			assert.Equal(t, "[9 8 7 6 4 3 2 ]", fmt.Sprintf("%+v", getAllNodePathFromHead(list)))
			assert.Equal(t, "[2 3 4 6 7 8 9 ]", fmt.Sprintf("%+v", getAllNodePathFromTail(list)))
		})
		t.Run("test move last node", func(t *testing.T) {
			list.moveNodeToFront(nodeMap[2])
			assert.Equal(t, "[2 9 8 7 6 4 3 ]", fmt.Sprintf("%+v", getAllNodePathFromHead(list)))
			assert.Equal(t, "[3 4 6 7 8 9 2 ]", fmt.Sprintf("%+v", getAllNodePathFromTail(list)))
		})
		t.Run("test move middle node", func(t *testing.T) {
			list.moveNodeToFront(nodeMap[6])
			assert.Equal(t, "[6 2 9 8 7 4 3 ]", fmt.Sprintf("%+v", getAllNodePathFromHead(list)))
			assert.Equal(t, "[3 4 7 8 9 2 6 ]", fmt.Sprintf("%+v", getAllNodePathFromTail(list)))
		})
	})
}

func getAllNodePathFromHead(list *doubleLinkedList) (res []string) {
	node := list.head.next
	for node != nil {
		res = append(res, node.path)
		node = node.next
	}
	return res
}

func getAllNodePathFromTail(list *doubleLinkedList) (res []string) {
	node := list.tail.prev
	for node != nil {
		res = append(res, node.path)
		node = node.prev
	}
	return res
}

func assertChainFromTailAndHead(t *testing.T, fdm *fdManager, testBasePath string, positiveFdsSeq []int) {
	assertChainFromHead(t, fdm, testBasePath, positiveFdsSeq)
	assertChainFromTail(t, fdm, testBasePath, positiveFdsSeq)
}

func assertChainFromHead(t *testing.T, fdm *fdManager, testBasePath string, positiveFdsSeq []int) {
	node := fdm.fdList.head.next
	index := 0
	nums := 0
	for node != fdm.fdList.tail {
		expectedPath := testBasePath + fmt.Sprint(positiveFdsSeq[index])
		assert.NotNil(t, node.fd)
		assert.Equal(t, expectedPath, node.path)
		node = node.next
		index++
		nums++
	}
	assert.Equal(t, fdm.size, nums)
}

func assertChainFromTail(t *testing.T, fdm *fdManager, testBasePath string, positiveFdsSeq []int) {
	index := len(positiveFdsSeq) - 1
	node := fdm.fdList.tail.prev
	nums := 0
	for node != fdm.fdList.head {
		expectedPath := testBasePath + fmt.Sprint(positiveFdsSeq[index])
		assert.NotNil(t, node.fd)
		assert.Equal(t, expectedPath, node.path)
		node = node.prev
		index--
		nums++
	}
	assert.Equal(t, fdm.size, nums)
}

//func TestGetMaxNums(t *testing.T) {
//	maxNums := 30000
//	basePath := "test-path/"
//	err := os.RemoveAll(basePath)
//	assert.Nil(t, err)
//	err = os.Mkdir(basePath, os.ModePerm)
//	assert.Nil(t, err)
//	defer func() {
//		err := os.RemoveAll(basePath)
//		if err != nil {
//			t.Logf("err is %s", err)
//		}
//	}()
//	var fdList []*os.File
//	for i := 1; i <= maxNums; i++ {
//		path := basePath + fmt.Sprintf("%d", i)
//		fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
//		if err != nil {
//			if strings.HasSuffix(err.Error(), TooManyFileOpenErrSuffix) {
//				t.Logf("file num is %d, err is %s, and it had handle", i, err)
//			}
//			for _, fd := range fdList {
//				err := fd.Close()
//				if err != nil {
//					t.Logf("err is %s, and it had handle", err)
//				}
//			}
//			return
//		} else {
//			fdList = append(fdList, fd)
//		}
//	}
//}
