# dynalock

This is a small lock library written Go, which uses [AWS DynamoDB](https://aws.amazon.com/dynamodb/) as the data store.

# Usage

The main interfaces are as follows.

```go
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
```

# References

This borrows a lot of ideas, tests and a subset of the API from https://github.com/abronan/valkeyrie.

# License

This code is released under the Apache 2.0 license.