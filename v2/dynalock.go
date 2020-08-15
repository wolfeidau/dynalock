package dynalock

import (
	"context"
	"errors"
	"time"
)

const (
	// DefaultLockTTL default duration for locks
	DefaultLockTTL = 20 * time.Second

	noTTLSet = -1
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
	Put(ctx context.Context, key string, options ...WriteOption) error

	// Get a value given its key
	Get(ctx context.Context, key string, options ...ReadOption) (*KVPair, error)

	// List the content of a given prefix
	List(ctx context.Context, prefix string, options ...ReadOption) ([]*KVPair, error)

	// Delete the value at the specified key
	Delete(ctx context.Context, key string) error

	// Verify if a Key exists in the store
	Exists(ctx context.Context, key string, options ...ReadOption) (bool, error)

	// NewLock creates a lock for a given key.
	// The returned Locker is not held and must be acquired
	// with `.Lock`. The Value is optional.
	NewLock(ctx context.Context, key string, options ...LockOption) (Locker, error)

	// Atomic CAS operation on a single value.
	// Pass previous = nil to create a new key.
	// Pass previous = kv to update an existing value.
	AtomicPut(ctx context.Context, key string, options ...WriteOption) (bool, *KVPair, error)

	// Atomic delete of a single value
	AtomicDelete(ctx context.Context, key string, previous *KVPair) (bool, error)
}

// Locker provides locking mechanism on top of the store.
// Similar to `sync.Lock` except it may return errors.
type Locker interface {
	// Lock attempt to lock the store record, this will BLOCK and retry at a rate of once every 3 seconds
	Lock(ctx context.Context, stopChan chan struct{}) (<-chan struct{}, error)

	// Unlock this will unlock and perfom a DELETE to remove the store record
	Unlock(ctx context.Context) error
}
