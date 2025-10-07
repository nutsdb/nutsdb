package nutsdb

import "errors"

var errHintCollectorClosed = errors.New("hint collector closed")

const DefaultHintCollectorFlushEvery = 1024

type hintWriter interface {
	Write(*HintEntry) error
	Sync() error
	Close() error
}

type HintCollector struct {
	writer     hintWriter
	buf        []HintEntry
	fileID     int64
	flushEvery int
	closed     bool
}

func NewHintCollector(fileID int64, writer hintWriter, flushEvery int) *HintCollector {
	if flushEvery <= 0 {
		flushEvery = DefaultHintCollectorFlushEvery
	}
	return &HintCollector{
		writer:     writer,
		buf:        make([]HintEntry, 0, flushEvery),
		fileID:     fileID,
		flushEvery: flushEvery,
	}
}

func (hc *HintCollector) Add(entry *HintEntry) error {
	if hc.closed {
		return errHintCollectorClosed
	}
	if entry == nil {
		return ErrHintFileEntryInvalid
	}
	clone := *entry
	clone.FileID = hc.fileID
	if len(entry.Key) > 0 {
		clone.Key = append([]byte(nil), entry.Key...)
	}
	hc.buf = append(hc.buf, clone)
	if len(hc.buf) >= hc.flushEvery {
		return hc.flush(true)
	}
	return nil
}

func (hc *HintCollector) Flush() error {
	if hc.closed {
		return errHintCollectorClosed
	}
	return hc.flush(true)
}

func (hc *HintCollector) Sync() error {
	if hc.closed {
		return errHintCollectorClosed
	}
	if err := hc.flush(false); err != nil {
		return err
	}
	return hc.writer.Sync()
}

func (hc *HintCollector) Close() error {
	if hc.closed {
		return nil
	}
	if err := hc.flush(true); err != nil {
		return err
	}
	hc.closed = true
	return hc.writer.Close()
}

func (hc *HintCollector) flush(sync bool) error {
	if len(hc.buf) == 0 {
		if sync {
			return hc.writer.Sync()
		}
		return nil
	}
	for i := range hc.buf {
		entry := hc.buf[i]
		if err := hc.writer.Write(&entry); err != nil {
			return err
		}
	}
	hc.buf = hc.buf[:0]
	if sync {
		return hc.writer.Sync()
	}
	return nil
}
