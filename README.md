# dynalock

This is a small K/V library written Go, which uses [AWS DynamoDB](https://aws.amazon.com/dynamodb/) as the data store.

It supports create, read, update and delete (CRUD) for key/value pairs, and provides locks based on the `sync.Lock` API.

# Usage

The main interfaces are as follows, for something more complete see the [competing consumers example](examples/competing-consumers/main.go).

```go
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
	// Pass previous = kv to update an existing value.
	AtomicPut(key string, options ...WriteOption) (bool, *KVPair, error)

	// Atomic delete of a single value
	AtomicDelete(key string, previous *KVPair) (bool, error)
}

// Locker provides locking mechanism on top of the store.
// Similar to `sync.Lock` except it may return errors.
type Locker interface {
	// Lock attempt to lock the store record, this will BLOCK and retry at a rate of once every 3 seconds
	Lock(stopChan chan struct{}) (<-chan struct{}, error)

	// Unlock this will unlock and perfom a DELETE to remove the store record
	Unlock() error
}
```

# References

This borrows a lot of ideas, tests and a subset of the API from https://github.com/abronan/valkeyrie.

Updates to the original API are based on a great blog post by @davecheney https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis

# License

This code is released under the Apache 2.0 license.