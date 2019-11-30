# dynalock 

This is a small K/V library written Go, which uses [AWS DynamoDB](https://aws.amazon.com/dynamodb/) as the data store.

It supports create, read, update and delete (CRUD) for key/value pairs, and provides locks based on the `sync.Lock` API.

<a href="https://github.com/wolfeidau/dynalock/actions?query=workflow%3ATests"><img alt="GitHub Actions status" src="https://github.com/wolfeidau/dynalock/workflows/Tests/badge.svg?branch=master&event=push"></a>

# What is the problem?

The main problems I am trying to solve in with this package are:

1. Enable users of the API to store and coordinate work across resources, using multiple lambdas, and containers running in a range of services.
2. Do this locking and coordination without needing to spin up a cluster using etcd, or consul
3. Provide a solid and simple locking / storage API which can be used no matter how small your project is.
4. Try and make this API simple, while also reduce the operations for this service using AWS services.

# What sorts of things can this help with?

Some examples of uses for a library like this are:

1. When using scheduled lambda functions this library will enable you to lock resources before performing actions with it, this could be a payment api or a ECS cluster, either way it is important to ensure only ONE service is performing that task at one time.
2. When you start using step functions, how can you ensure only one workflow is active and performing some task, like provisioning, without having to worry about parallel executions.

So the key here is storing state, and coordinating changes across workers, or resources.

# Why DynamoDB?

DynamoDB is used for locking in a range of Amazon provided APIs and libraries, so I am not the first to do this. see [references](#references). This service also satisfy the requirement to be easy to start with as it is just a service.

# Cost?

I am currently working on some testing around this, but with a bit of tuning you can keep the read/write load very low. But this is specifically designed as a starting point, while ensuring there is a clear abstraction between the underlying services and your code. 

To manage this I would recommend you set alarms for read / write metrics, start with on demand but you will probably want to switch to specific read/write limits for production.

I will be posting some graphs, and analysis of my work as I go to help flesh this out better.

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

Prior work in this space:

* https://github.com/awslabs/dynamodb-lock-client
* https://github.com/intercom/lease

This borrows a lot of ideas, tests and a subset of the API from https://github.com/abronan/valkeyrie.

Updates to the original API are based on a great blog post by @davecheney https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis

# License

This code is released under the Apache 2.0 license.