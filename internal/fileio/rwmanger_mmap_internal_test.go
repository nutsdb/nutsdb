package fileio

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/edsrzf/mmap-go"
	"github.com/stretchr/testify/suite"
)

type mmapInternalTestSuite struct {
	suite.Suite
}

func (s *mmapInternalTestSuite) newManager() (*MMapRWManager, *os.File) {
	t := s.T()
	filePath := filepath.Join(t.TempDir(), "mmap")
	fdm := NewFdm(1024, 0.5)
	fd, err := fdm.GetFd(filePath)
	s.Require().NoError(err)
	t.Cleanup(func() { _ = os.Remove(fd.Name()) })
	s.Require().NoError(Truncate(filePath, 8*MB, fd, false))
	return GetMMapRWManager(fd, filePath, fdm, 8*MB), fd
}

func (s *mmapInternalTestSuite) TestCacheNewMMap_KeepsFirstMapping() {
	mm, fd := s.newManager()

	first, err := mm.accessMMap(mm.ReadCache, 0, mmap.RDONLY)
	s.Require().NoError(err)
	second, err := newMMapData(fd, 0, mmap.RDONLY)
	s.Require().NoError(err)

	got := cacheNewMMap(mm.ReadCache, 0, second)
	s.Require().Equal(first, got, "lost race should return the mapping already in the cache")
	s.Require().Equal(1, mm.ReadCache.Len())
	s.Require().NotEmpty(first.data, "cached mapping was unmapped")
	s.Require().Error(second.Close(), "duplicate mapping should already have been unmapped")
}

func (s *mmapInternalTestSuite) TestDiscardMMapData_RestoresFinalizerWhenUnmapFails() {
	_, fd := s.newManager()
	md, err := newMMapData(fd, 0, mmap.RDONLY)
	s.Require().NoError(err)

	discardMMapData(md)
	s.Require().Error(md.Close(), "first discard should unmap the region")

	discardMMapData(md)
	runtime.SetFinalizer(md, nil)
}

func (s *mmapInternalTestSuite) TestAccessMMap_MapError() {
	mm, fd := s.newManager()
	s.Require().NoError(fd.Close())
	_, err := mm.accessMMap(mm.ReadCache, 0, mmap.RDONLY)
	s.Require().Error(err, "mapping a closed file should fail")
}

func TestMMapInternal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}
	suite.Run(t, new(mmapInternalTestSuite))
}
