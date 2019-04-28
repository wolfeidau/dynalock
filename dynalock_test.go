package dynalock

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/dhui/dktest"
	"github.com/stretchr/testify/require"
)

const (
	defaultRegion = "us-east-1"
)

var (
	opts = dktest.Options{PortRequired: true, ReadyFunc: isReady}
)

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {

	dbSvc := dynamodb.New(mustSession(c.FirstPort()))

	_, err := dbSvc.ListTablesWithContext(ctx, &dynamodb.ListTablesInput{})

	return err == nil
}

func Test(t *testing.T) {

	dktest.Run(t, "amazon/dynamodb-local:latest", opts,
		func(t *testing.T, c dktest.ContainerInfo) {

			assert := require.New(t)

			dbSvc := dynamodb.New(mustSession(c.FirstPort()))

			err := ensureVersionTable(dbSvc, "testing-locks")
			assert.NoError(err)

			dl := &Dynalock{dynamoSvc: dbSvc, tableName: "testing-locks", partition: "agent"}

			testPutGetDeleteExists(t, dl)
			testLockUnlock(t, dl)
			testList(t, dl)
			testAtomicPut(t, dl)
			testAtomicDelete(t, dl)
			testLockTTL(t, dl, dl)
		})
}

func mustSession(hostIP string, hostPort string, err error) *session.Session {

	if err != nil {
		panic(err)
	}

	ddbURL := fmt.Sprintf("http://%s:%s", hostIP, hostPort)

	creds := credentials.NewStaticCredentials("123", "test", "test")
	return session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(defaultRegion),
		Endpoint:    aws.String(ddbURL),
		Credentials: creds,
	}))
}

func ensureVersionTable(dbSvc dynamodbiface.DynamoDBAPI, tableName string) error {

	_, err := dbSvc.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []*dynamodb.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: aws.String(dynamodb.KeyTypeHash)},
			{AttributeName: aws.String("name"), KeyType: aws.String(dynamodb.KeyTypeRange)},
		},
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)},
			{AttributeName: aws.String("name"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		SSESpecification: &dynamodb.SSESpecification{
			Enabled: aws.Bool(true),
			SSEType: aws.String(dynamodb.SSETypeAes256),
		},
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeResourceInUseException:
				return nil
			}
		}
		return err
	}

	err = dbSvc.WaitUntilTableExists(&dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return err
	}

	_, err = dbSvc.UpdateTimeToLive(&dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(tableName),
		TimeToLiveSpecification: &dynamodb.TimeToLiveSpecification{
			AttributeName: aws.String("expires"),
			Enabled:       aws.Bool(true),
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func testPutGetDeleteExists(t *testing.T, kv Store) {
	assert := require.New(t)

	// Get a not exist key should return ErrKeyNotFound
	_, err := kv.Get("testPutGetDelete_not_exist_key")
	assert.Equal(ErrKeyNotFound, err)

	value := []byte("bar")
	for _, key := range []string{
		"testPutGetDeleteExists",
		"testPutGetDeleteExists/",
		"testPutGetDeleteExists/testbar/",
		"testPutGetDeleteExists/testbar/testfoobar",
	} {

		// Put the key
		err = kv.Put(key, WriteWithBytes(value), WriteWithTTL(2*time.Second))
		assert.NoError(err)

		// Get should return the value and an incremented index
		pair, err := kv.Get(key)
		assert.NoError(err)
		assert.NotNil(pair)
		assert.Equal(value, pair.BytesValue())
		assert.NotEqual(0, pair.Expires)

		assert.NotEqual(0, pair.Version)

		// Exists should return true
		exists, err := kv.Exists(key)
		assert.NoError(err)
		assert.True(exists)

		// Delete the key
		err = kv.Delete(key)
		assert.NoError(err)

		// Get should fail
		pair, err = kv.Get(key)
		assert.Error(err)
		assert.Nil(pair)
		assert.Nil(pair)

		// Exists should return false
		exists, err = kv.Exists(key)
		assert.NoError(err)
		assert.False(exists)
	}

	key := "something/withoutExpires"

	// Put the key
	err = kv.Put(key, WriteWithBytes(value), WriteWithNoExpires())
	assert.NoError(err)

	// Get should return the value and an incremented index
	pair, err := kv.Get(key)
	assert.NoError(err)
	assert.NotNil(pair)
	assert.Equal(value, pair.BytesValue())
	assert.Equal(int64(0), pair.Expires)
}

func testLockUnlock(t *testing.T, kv Store) {

	assert := require.New(t)

	key := "testLockUnlock"
	value := []byte("bar")

	// We should be able to create a new lock on key
	lock, err := kv.NewLock(key, LockWithTTL(2*time.Second), LockWithBytes(value))
	assert.NoError(err)
	assert.NotNil(lock)

	// Lock should successfully succeed or block
	lockChan, err := lock.Lock(nil)
	assert.NoError(err)
	assert.NotNil(lockChan)

	// Get should work
	pair, err := kv.Get(key)
	assert.NoError(err)
	assert.Equal(value, pair.BytesValue())
	assert.NotEqual(0, pair.Version)

	// Unlock should succeed
	err = lock.Unlock()
	assert.NoError(err)

	// Lock should succeed again
	lockChan, err = lock.Lock(nil)
	assert.NoError(err)
	assert.NotNil(lockChan)

	// Get should work
	pair, err = kv.Get(key)
	assert.NoError(err)
	assert.Equal(value, pair.BytesValue())
	assert.NotEqual(0, pair.Version)

	err = lock.Unlock()
	assert.NoError(err)
}

func testList(t *testing.T, kv Store) {

	assert := require.New(t)

	childKey := "testList/child"
	subfolderKey := "testList/subfolder"

	// Put the first child key
	err := kv.Put(childKey, WriteWithBytes([]byte("first")))
	assert.NoError(err)

	// Put the second child key which is also a directory
	err = kv.Put(subfolderKey, WriteWithBytes([]byte("second")))
	assert.NoError(err)

	// Put child keys under secondKey
	for i := 1; i <= 3; i++ {
		key := "testList/subfolder/key" + strconv.Itoa(i)
		err := kv.Put(key, WriteWithBytes([]byte("value")))
		assert.NoError(err)
	}

	// List should work and return five child entries
	pairs, err := kv.List("testList/subfolder/key")
	assert.NoError(err)
	assert.NotNil(pairs)
	assert.Equal(3, len(pairs))

}

func testAtomicPut(t *testing.T, kv Store) {

	assert := require.New(t)

	key := "testAtomicPut"
	value := []byte("world")

	// Put the key
	err := kv.Put(key, WriteWithBytes(value))
	assert.NoError(err)

	// Get should return the value and an incremented index
	pair, err := kv.Get(key)
	assert.NoError(err)
	assert.NotNil(pair)
	assert.Equal(value, pair.BytesValue())
	assert.NotEqual(0, pair.Version)

	// This CAS should fail: previous exists.
	success, _, err := kv.AtomicPut(key, WriteWithBytes([]byte("WORLD")))
	assert.Error(err)
	assert.False(success)

	// This CAS should succeed
	success, _, err = kv.AtomicPut(key, WriteWithPreviousKV(pair), WriteWithBytes([]byte("WORLD")))
	assert.NoError(err)
	assert.True(success)

	// This CAS should fail, key has wrong index.
	pair.Version = 6744
	success, _, err = kv.AtomicPut(key, WriteWithPreviousKV(pair), WriteWithBytes([]byte("WORLDWORLD")))
	assert.Equal(err, ErrKeyModified)
	assert.False(success)
}

func testAtomicDelete(t *testing.T, kv Store) {

	assert := require.New(t)

	key := "testAtomicDelete"
	value := []byte("world")

	// Put the key
	err := kv.Put(key, WriteWithBytes(value))
	assert.NoError(err)

	// Get should return the value and an incremented index
	pair, err := kv.Get(key)
	assert.NoError(err)
	assert.NotNil(pair)
	assert.Equal(value, pair.BytesValue())
	assert.NotEqual(0, pair.Version)

	tempIndex := pair.Version

	// AtomicDelete should fail
	pair.Version = 6744
	success, err := kv.AtomicDelete(key, pair)
	assert.Error(err)
	assert.False(success)

	// AtomicDelete should succeed
	pair.Version = tempIndex
	success, err = kv.AtomicDelete(key, pair)
	assert.NoError(err)
	assert.True(success)

	// Delete a non-existent key; should fail
	success, err = kv.AtomicDelete(key, pair)
	assert.Equal(ErrKeyNotFound, err)
	assert.False(success)
}

func testLockTTL(t *testing.T, kv Store, otherConn Store) {

	assert := require.New(t)

	key := "testLockTTL"
	value := []byte("bar")

	renewCh := make(chan struct{})

	// We should be able to create a new lock on key
	lock, err := otherConn.NewLock(key, LockWithBytes(value), LockWithTTL(2*time.Second), LockWithRenewLock(renewCh))
	assert.NoError(err)
	assert.NotNil(lock)

	// Lock should successfully succeed
	lockChan, err := lock.Lock(nil)
	assert.NoError(err)
	assert.NotNil(lockChan)

	// Get should work
	pair, err := otherConn.Get(key)
	assert.NoError(err)
	assert.NotNil(pair)
	assert.Equal(value, pair.BytesValue())
	assert.NotEqual(0, pair.Version)

	time.Sleep(3 * time.Second)

	done := make(chan struct{})
	stop := make(chan struct{})

	value = []byte("foobar")

	// Create a new lock with another connection
	lock, err = kv.NewLock(
		key,
		LockWithBytes(value),
		LockWithTTL(3*time.Second),
	)
	assert.NoError(err)
	assert.NotNil(lock)

	// Lock should block, the session on the lock
	// is still active and renewed periodically
	go func(<-chan struct{}) {
		_, _ = lock.Lock(stop)
		done <- struct{}{}
	}(done)

	select {
	case <-done:
		t.Fatal("Lock succeeded on a key that is supposed to be locked by another client")
	case <-time.After(4 * time.Second):
		// Stop requesting the lock as we are blocked as expected
		stop <- struct{}{}
		break
	}

	// Force stop the session renewal for the lock
	close(renewCh)

	// Let the session on the lock expire
	time.Sleep(3 * time.Second)
	locked := make(chan struct{})
	errCh := make(chan error)

	// Lock should now succeed for the other client
	go func(<-chan struct{}, <-chan error) {
		lockChan, err = lock.Lock(nil)
		if err != nil {
			errCh <- err
			return
		}
		locked <- struct{}{}
	}(locked, errCh)

	select {
	case err = <-errCh:
		t.Fatalf("Unable to take the lock: %v", err)
	case <-locked:
		break
	case <-time.After(4 * time.Second):
		t.Fatal("Unable to take the lock, timed out")
	}

	// Get should work with the new value
	pair, err = kv.Get(key)
	assert.NoError(err)
	assert.NotNil(pair)
	assert.Equal(value, pair.BytesValue())
	assert.NotEqual(0, pair.Version)

	err = lock.Unlock()
	assert.NoError(err)
}
