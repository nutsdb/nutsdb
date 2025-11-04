package nutsdb

import "errors"

var (
	ErrBucketSubcriberNotFound = errors.New("bucket subcriber not found")
	ErrKeySubcriberNotFound    = errors.New("key subcriber not found")
	ErrWatchChanCannotSend     = errors.New("watch channel cannot send")
	ErrKeyAlreadySubcribed     = errors.New("key already subcribed")
	ErrWatchManagerClosed      = errors.New("watch manager closed")
	ErrWatchingCallbackFailed  = errors.New("watching callback failed")
)
