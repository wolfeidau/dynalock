package dynalock

import (
	"encoding/base64"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// WriteOption assign various settings to the write options
type WriteOption func(opts *WriteOptions)

// WriteOptions contains optional request parameters
type WriteOptions struct {
	value    *dynamodb.AttributeValue
	ttl      time.Duration
	previous *KVPair // Optional, previous value used to assert if the record has been modified before an atomic update
}

// NewWriteOptions create write options, assign defaults then accept overrides
func NewWriteOptions(opts ...WriteOption) *WriteOptions {

	// assign a place holder value to detect whether to assign the default TTL
	writeOpts := &WriteOptions{
		ttl: noTTLSet,
	}

	for _, opt := range opts {
		opt(writeOpts)
	}

	if writeOpts.ttl == noTTLSet {
		writeOpts.ttl = DefaultLockTTL
	}

	return writeOpts
}

// WriteWithTTL time to live (TTL) to the key which is written
func WriteWithTTL(ttl time.Duration) WriteOption {
	return func(opts *WriteOptions) {
		opts.ttl = ttl
	}
}

// WriteWithNoExpires time to live (TTL) is set not set so it never expires
func WriteWithNoExpires() WriteOption {
	return func(opts *WriteOptions) {
		opts.ttl = 0
	}
}

// WriteWithBytes byte slice to the key which is written
func WriteWithBytes(val []byte) WriteOption {
	return func(opts *WriteOptions) {
		opts.value = encodePayload(val)
	}
}

// WriteWithAttributeValue dynamodb attribute value which is written
func WriteWithAttributeValue(av *dynamodb.AttributeValue) WriteOption {
	return func(opts *WriteOptions) {
		opts.value = av
	}
}

// WriteWithPreviousKV previous KV which will be checked prior to update
func WriteWithPreviousKV(previous *KVPair) WriteOption {
	return func(opts *WriteOptions) {
		opts.previous = previous
		if opts.previous != nil {
			opts.ttl = time.Until(time.Unix(opts.previous.Expires, 0)) // update the TTL to the remaining time
		}
	}
}

// ReadOption assign various settings to the read options
type ReadOption func(opts *ReadOptions)

// ReadOptions contains optional request parameters
type ReadOptions struct {
	consistent bool
}

// NewReadOptions create read options, assign defaults then accept overrides
// enable the read consistent flag by default
func NewReadOptions(opts ...ReadOption) *ReadOptions {

	readOpts := &ReadOptions{
		consistent: true,
	}

	for _, opt := range opts {
		opt(readOpts)
	}

	return readOpts
}

// ReadConsistentDisable disable consistent reads
func ReadConsistentDisable() ReadOption {
	return func(opts *ReadOptions) {
		opts.consistent = false
	}
}

// LockOption assign various settings to the lock options
type LockOption func(opts *LockOptions)

// LockOptions contains optional request parameters
type LockOptions struct {
	value                *dynamodb.AttributeValue
	ttl                  time.Duration
	renewLock            chan struct{}
	tryLockPollingEnable bool
	renewEnable          bool
}

// NewLockOptions create lock options, assign defaults then accept overrides
func NewLockOptions(opts ...LockOption) *LockOptions {

	lockOpts := &LockOptions{
		ttl:                  DefaultLockTTL,
		renewEnable:          true, // defaults to true to retain existing default behaviour
		tryLockPollingEnable: true, // defaults to true to retain existing default behaviour
	}

	for _, opt := range opts {
		opt(lockOpts)
	}

	return lockOpts
}

// LockWithBytes byte slice to the key which is written when the lock is  acquired
func LockWithBytes(val []byte) LockOption {
	return func(opts *LockOptions) {
		opts.value = encodePayload(val)
	}
}

// LockWithTTL time to live (TTL) to the key which is written when the lock is acquired
func LockWithTTL(ttl time.Duration) LockOption {
	return func(opts *LockOptions) {
		opts.ttl = ttl
	}
}

// LockWithRenewLock renewal channel to the lock
func LockWithRenewLock(renewLockChan chan struct{}) LockOption {
	return func(opts *LockOptions) {
		opts.renewLock = renewLockChan
	}
}

// LockWithNoRenew renewal of the lock in the background will not be enabled
func LockWithNoRenew() LockOption {
	return func(opts *LockOptions) {
		opts.renewEnable = false
	}
}

// LockWithNoTryPolling lock will not poll to acquire a lock and block the caller, if the lock fails it will return straight away
func LockWithNoTryPolling() LockOption {
	return func(opts *LockOptions) {
		opts.tryLockPollingEnable = false
	}
}

func encodePayload(payload []byte) *dynamodb.AttributeValue {
	encodedValue := base64.StdEncoding.EncodeToString(payload)
	return &dynamodb.AttributeValue{S: aws.String(encodedValue)}
}
