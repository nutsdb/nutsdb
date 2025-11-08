package nutsdb

import "errors"

var (
	ErrBucketSubscriberNotFound = errors.New("bucket subscriber not found")
	ErrKeySubscriberNotFound    = errors.New("key subscriber not found")
	ErrWatchChanCannotSend      = errors.New("watch channel cannot send")
	ErrKeyAlreadySubscribed     = errors.New("key already subscribed")
	ErrWatchManagerClosed       = errors.New("watch manager closed")
	ErrWatchingCallbackFailed   = errors.New("watching callback failed")
	ErrWatchingChannelClosed    = errors.New("watching channel closed")
)
