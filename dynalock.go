package dynalock

import (
	"encoding/base64"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	// DefaultLockTTL default duration for locks
	DefaultLockTTL = 20 * time.Second

	listDefaultTimeout = 5 * time.Second
)

var (
	// ErrKeyNotFound record not found in the table
	ErrKeyNotFound = errors.New("key not found in table")

	// ErrKeyExists record already exists in table
	ErrKeyExists = errors.New("key already exists in table")

	// ErrKeyModified record has been modified, this probably means someone beat you to the change/lock
	ErrKeyModified = errors.New("key has been modified")

	// ErrLockAcquireCancelled lock acquire was cancelled
	ErrLockAcquireCancelled = errors.New("lock acquire was cancelled")
)

// Store represents the backend K/V storage
type Store interface {
	// Put a value at the specified key
	Put(key string, options ...WriteOption) error

	// Get a value given its key
	Get(key string, options ...ReadOption) (*KVPair, error)

	// List the content of a given prefix
	List(prefix string, options ...ReadOption) ([]*KVPair, error)

	// Delete the value at the specified key
	Delete(key string) error

	// Verify if a Key exists in the store
	Exists(key string, options ...ReadOption) (bool, error)

	// NewLock creates a lock for a given key.
	// The returned Locker is not held and must be acquired
	// with `.Lock`. The Value is optional.
	NewLock(key string, options ...LockOption) (Locker, error)

	// Atomic CAS operation on a single value.
	// Pass previous = nil to create a new key.
	AtomicPut(key string, options ...WriteOption) (bool, *KVPair, error)

	// Atomic delete of a single value
	AtomicDelete(key string, previous *KVPair) (bool, error)
}

// Locker provides locking mechanism on top of the store.
// Similar to `sync.Lock` except it may return errors.
type Locker interface {
	Lock(stopChan chan struct{}) (<-chan struct{}, error)
	Unlock() error
}

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

	writeOpts := &WriteOptions{
		ttl: DefaultLockTTL,
	}

	for _, opt := range opts {
		opt(writeOpts)
	}

	return writeOpts
}

// WriteWithTTL time to live (TTL) to the key which is written
func WriteWithTTL(ttl time.Duration) WriteOption {
	return func(opts *WriteOptions) {
		opts.ttl = ttl
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

// ReadWithConsistent enable consistent reads
func ReadConsistentDisable() ReadOption {
	return func(opts *ReadOptions) {
		opts.consistent = false
	}
}

// LockOption assign various settings to the lock options
type LockOption func(opts *LockOptions)

// LockOptions contains optional request parameters
type LockOptions struct {
	value     *dynamodb.AttributeValue
	ttl       time.Duration
	renewLock chan struct{}
}

// NewLockOptions create lock options, assign defaults then accept overrides
func NewLockOptions(opts ...LockOption) *LockOptions {

	lockOpts := &LockOptions{
		ttl: DefaultLockTTL,
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

func encodePayload(payload []byte) *dynamodb.AttributeValue {
	encodedValue := base64.StdEncoding.EncodeToString(payload)
	return &dynamodb.AttributeValue{S: aws.String(encodedValue)}
}
