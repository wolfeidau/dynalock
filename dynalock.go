package dynalock

import (
	"errors"
	"time"
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

	defaultLockTTL     = 20 * time.Second
	listDefaultTimeout = 5 * time.Second
)

// Store represents the backend K/V storage
type Store interface {
	// Put a value at the specified key
	Put(key string, value []byte, options *WriteOptions) error

	// Get a value given its key
	Get(key string, options *ReadOptions) (*KVPair, error)

	// List the content of a given prefix
	List(prefix string, options *ReadOptions) ([]*KVPair, error)

	// Delete the value at the specified key
	Delete(key string) error

	// Verify if a Key exists in the store
	Exists(key string, options *ReadOptions) (bool, error)

	// NewLock creates a lock for a given key.
	// The returned Locker is not held and must be acquired
	// with `.Lock`. The Value is optional.
	NewLock(key string, options *LockOptions) (Locker, error)

	// Atomic CAS operation on a single value.
	// Pass previous = nil to create a new key.
	AtomicPut(key string, value []byte, previous *KVPair, options *WriteOptions) (bool, *KVPair, error)

	// Atomic delete of a single value
	AtomicDelete(key string, previous *KVPair) (bool, error)
}

// Locker provides locking mechanism on top of the store.
// Similar to `sync.Lock` except it may return errors.
type Locker interface {
	Lock(stopChan chan struct{}) (<-chan struct{}, error)
	Unlock() error
}

// WriteOptions contains optional request parameters
type WriteOptions struct {
	TTL time.Duration
}

// ReadOptions contains optional request parameters
type ReadOptions struct {
	Consistent bool
}

// LockOptions contains optional request parameters
type LockOptions struct {
	Value     []byte        // Optional, value to associate with the lock
	TTL       time.Duration // Optional, expiration ttl associated with the lock
	RenewLock chan struct{} // Optional, chan used to control and stop the session ttl renewal for the lock
}
