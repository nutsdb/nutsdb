package fileio

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/edsrzf/mmap-go"
)

func newTestMMapManager(t *testing.T) (*MMapRWManager, *os.File) {
	t.Helper()
	filePath := filepath.Join(t.TempDir(), "mmap")
	fdm := NewFdm(1024, 0.5)
	fd, err := fdm.GetFd(filePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(fd.Name()) })
	if err = Truncate(filePath, 8*MB, fd, false); err != nil {
		t.Fatal(err)
	}
	return GetMMapRWManager(fd, filePath, fdm, 8*MB), fd
}

func TestCacheNewMMap_KeepsFirstMapping(t *testing.T) {
	mm, fd := newTestMMapManager(t)

	first, err := mm.accessMMap(mm.ReadCache, 0, mmap.RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newMMapData(fd, 0, mmap.RDONLY)
	if err != nil {
		t.Fatal(err)
	}

	got := cacheNewMMap(mm.ReadCache, 0, second)
	if got != first {
		t.Fatal("lost race should return the mapping already in the cache")
	}
	if mm.ReadCache.Len() != 1 {
		t.Fatalf("ReadCache.Len()=%d, want 1", mm.ReadCache.Len())
	}
	if len(first.data) == 0 {
		t.Fatal("cached mapping was unmapped")
	}
	if err = second.Close(); err == nil {
		t.Fatal("duplicate mapping should already have been unmapped")
	}
}

func TestDiscardMMapData_RestoresFinalizerWhenUnmapFails(t *testing.T) {
	_, fd := newTestMMapManager(t)
	md, err := newMMapData(fd, 0, mmap.RDONLY)
	if err != nil {
		t.Fatal(err)
	}

	discardMMapData(md)
	if err = md.Close(); err == nil {
		t.Fatal("first discard should unmap the region")
	}

	discardMMapData(md)
	runtime.SetFinalizer(md, nil)
}

func TestAccessMMap_MapError(t *testing.T) {
	mm, fd := newTestMMapManager(t)
	if err := fd.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := mm.accessMMap(mm.ReadCache, 0, mmap.RDONLY); err == nil {
		t.Fatal("mapping a closed file should fail")
	}
}
