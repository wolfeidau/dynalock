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

			dl := &Dynalock{dynamoSvc: dbSvc, tableName: "testing-locks"}

			testPutGetDeleteExists(t, dl)
			testLockUnlock(t, dl)
			testList(t, dl)

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
		KeySchema: []*dynamodb.KeySchemaElement{{
			AttributeName: aws.String("id"), KeyType: aws.String(dynamodb.KeyTypeHash),
		}},
		AttributeDefinitions: []*dynamodb.AttributeDefinition{{
			AttributeName: aws.String("id"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
		}},
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
	_, err := kv.Get("testPutGetDelete_not_exist_key", &ReadOptions{})
	assert.Equal(ErrKeyNotFound, err)

	value := []byte("bar")
	for _, key := range []string{
		"testPutGetDeleteExists",
		"testPutGetDeleteExists/",
		"testPutGetDeleteExists/testbar/",
		"testPutGetDeleteExists/testbar/testfoobar",
	} {

		// Put the key
		err = kv.Put(key, value, &WriteOptions{TTL: 2 * time.Second})
		assert.NoError(err)

		// Get should return the value and an incremented index
		pair, err := kv.Get(key, nil)
		assert.NoError(err)
		assert.NotNil(pair)
		assert.Equal(pair.Value, value)
		assert.NotEqual(pair.Version, 0)

		// Exists should return true
		exists, err := kv.Exists(key, nil)
		assert.NoError(err)
		assert.True(exists)

		// Delete the key
		err = kv.Delete(key)
		assert.NoError(err)

		// Get should fail
		pair, err = kv.Get(key, nil)
		assert.Error(err)
		assert.Nil(pair)
		assert.Nil(pair)

		// Exists should return false
		exists, err = kv.Exists(key, nil)
		assert.NoError(err)
		assert.False(exists)
	}
}

func testLockUnlock(t *testing.T, kv Store) {

	assert := require.New(t)

	key := "testLockUnlock"
	value := []byte("bar")

	// We should be able to create a new lock on key
	lock, err := kv.NewLock(key, &LockOptions{Value: value, TTL: 2 * time.Second})
	assert.NoError(err)
	assert.NotNil(lock)

	// Lock should successfully succeed or block
	lockChan, err := lock.Lock(nil)
	assert.NoError(err)
	assert.NotNil(lockChan)

	// Get should work
	pair, err := kv.Get(key, nil)
	assert.NoError(err)
	assert.Equal(pair.Value, value)
	assert.NotEqual(pair.Version, 0)

	// Unlock should succeed
	err = lock.Unlock()
	assert.NoError(err)

	// Lock should succeed again
	lockChan, err = lock.Lock(nil)
	assert.NoError(err)
	assert.NotNil(lockChan)

	// Get should work
	pair, err = kv.Get(key, nil)
	assert.NoError(err)
	assert.Equal(pair.Value, value)
	assert.NotEqual(pair.Version, 0)

	err = lock.Unlock()
	assert.NoError(err)
}

func testList(t *testing.T, kv Store) {

	assert := require.New(t)

	childKey := "testList/child"
	subfolderKey := "testList/subfolder"

	// Put the first child key
	err := kv.Put(childKey, []byte("first"), nil)
	assert.NoError(err)

	// Put the second child key which is also a directory
	err = kv.Put(subfolderKey, []byte("second"), nil)
	assert.NoError(err)

	// Put child keys under secondKey
	for i := 1; i <= 3; i++ {
		key := "testList/subfolder/key" + strconv.Itoa(i)
		err := kv.Put(key, []byte("value"), nil)
		assert.NoError(err)
	}

	// List should work and return five child entries
	pairs, err := kv.List("testList/subfolder/key", nil)
	assert.NoError(err)
	assert.NotNil(pairs)
	assert.Equal(3, len(pairs))

}
