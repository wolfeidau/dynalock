package dynalock

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Dynalock lock store which is backed by AWS DynamoDB
type Dynalock struct {
	dynamoSvc dynamodbiface.DynamoDBAPI
	tableName string
	partition string
}

// KVPair represents {Key, Value, Version} tuple, internally
// this uses a *dynamodb.AttributeValue which can be used to
// store strings, slices or structs
type KVPair struct {
	Partition string `dynamodbav:"id"`
	Key       string `dynamodbav:"name"`
	Version   int64  `dynamodbav:"version"`
	Expires   int64  `dynamodbav:"expires"`
	// handled separately to enable an number of stored values
	value *dynamodb.AttributeValue
}

// BytesValue use the attribute to return a slice of bytes, a nil will be returned if it is empty or nil
func (kv *KVPair) BytesValue() []byte {
	buf := []byte{}

	err := dynamodbattribute.Unmarshal(kv.value, &buf)
	if err != nil {
		return nil
	}

	return buf
}

// AttributeValue return the current dynamodb attribute value, may be nil
func (kv *KVPair) AttributeValue() *dynamodb.AttributeValue {
	return kv.value
}

// New construct a DynamoDB backed locking store
func New(dynamoSvc dynamodbiface.DynamoDBAPI, tableName, partition string) Store {
	return &Dynalock{
		dynamoSvc: dynamoSvc,
		tableName: tableName,
		partition: partition,
	}
}

// Put a value at the specified key
func (ddb *Dynalock) Put(key string, options ...WriteOption) error {

	params := ddb.buildUpdateItemInput(key, NewWriteOptions(options...))

	_, err := ddb.dynamoSvc.UpdateItem(params)
	if err != nil {
		return err
	}

	return nil
}

// Exists if a Key exists in the store
func (ddb *Dynalock) Exists(key string, options ...ReadOption) (bool, error) {

	readOptions := NewReadOptions(options...)

	res, err := ddb.dynamoSvc.GetItem(&dynamodb.GetItemInput{
		TableName:      aws.String(ddb.tableName),
		Key:            buildKeys(ddb.partition, key),
		ConsistentRead: aws.Bool(readOptions.consistent),
	})

	if err != nil {
		return false, err
	}

	if res.Item == nil {
		return false, nil
	}

	// is the item expired?
	if isItemExpired(res.Item) {
		return false, nil
	}

	return true, nil
}

// Get a value given its key
func (ddb *Dynalock) Get(key string, options ...ReadOption) (*KVPair, error) {

	readOptions := NewReadOptions(options...)

	res, err := ddb.getKey(key, readOptions)
	if err != nil {
		return nil, err
	}
	if res.Item == nil {
		return nil, ErrKeyNotFound
	}

	// is the item expired?
	if isItemExpired(res.Item) {
		return nil, ErrKeyNotFound
	}

	item, err := decodeItem(res.Item)
	if err != nil {
		return nil, err
	}

	return item, nil
}

// Delete the value at the specified key
func (ddb *Dynalock) Delete(key string) error {
	_, err := ddb.dynamoSvc.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String(ddb.tableName),
		Key:       buildKeys(ddb.partition, key),
	})
	if err != nil {
		return err
	}

	return nil
}

// List the content of a given prefix
func (ddb *Dynalock) List(prefix string, options ...ReadOption) ([]*KVPair, error) {

	readOptions := NewReadOptions(options...)

	si := &dynamodb.QueryInput{
		TableName:              aws.String(ddb.tableName),
		KeyConditionExpression: aws.String("#id = :partition AND begins_with(#name, :namePrefix)"),
		ExpressionAttributeNames: map[string]*string{
			"#id":   aws.String("id"),
			"#name": aws.String("name"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":partition":  {S: aws.String(ddb.partition)},
			":namePrefix": {S: aws.String(prefix)},
		},
		ConsistentRead: aws.Bool(readOptions.consistent),
	}

	ctcx, cancel := context.WithTimeout(context.Background(), listDefaultTimeout)

	items := []map[string]*dynamodb.AttributeValue{}

	err := ddb.dynamoSvc.QueryPagesWithContext(ctcx, si,
		func(page *dynamodb.QueryOutput, lastPage bool) bool {
			items = append(items, page.Items...)

			if lastPage {
				cancel()
				return false
			}

			return true
		})
	if err != nil {
		return nil, err
	}

	if len(items) == 0 {
		return nil, ErrKeyNotFound
	}

	results := []*KVPair{}

	for _, item := range items {
		val, err := decodeItem(item)
		if err != nil {
			return nil, err
		}

		// skip records which are expired
		if isItemExpired(item) {
			continue
		}

		results = append(results, val)
	}

	return results, nil
}

// AtomicPut Atomic CAS operation on a single value.
func (ddb *Dynalock) AtomicPut(key string, options ...WriteOption) (bool, *KVPair, error) {

	writeOptions := NewWriteOptions(options...)

	params := ddb.buildUpdateItemInput(key, writeOptions)

	err := updateWithConditions(params, writeOptions.previous)
	if err != nil {
		return false, nil, err
	}

	res, err := ddb.dynamoSvc.UpdateItem(params)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				if writeOptions.previous == nil {
					return false, nil, ErrKeyExists
				}
				return false, nil, ErrKeyModified
			}
		}
		return false, nil, err
	}

	item, err := decodeItem(res.Attributes)
	if err != nil {
		return false, nil, err
	}

	return true, item, nil
}

// AtomicDelete delete of a single value
//
// This supports two different operations:
// * if previous is supplied assert it exists with the version supplied
// * if previous is nil then assert that the key doesn't exist
//
// FIXME: should the second case just return false, nil?
func (ddb *Dynalock) AtomicDelete(key string, previous *KVPair) (bool, error) {

	getRes, err := ddb.getKey(key, NewReadOptions())
	if err != nil {
		return false, err
	}

	if previous == nil && getRes.Item != nil && !isItemExpired(getRes.Item) {
		return false, ErrKeyExists
	}

	expAttr := map[string]*dynamodb.AttributeValue{
		":lastRevision": {N: aws.String(strconv.FormatInt(previous.Version, 10))},
	}

	req := &dynamodb.DeleteItemInput{
		TableName:                 aws.String(ddb.tableName),
		Key:                       buildKeys(ddb.partition, key),
		ConditionExpression:       aws.String("version = :lastRevision"),
		ExpressionAttributeValues: expAttr,
	}
	_, err = ddb.dynamoSvc.DeleteItem(req)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return false, ErrKeyNotFound
			}
		}
		return false, err
	}

	return true, nil
}

func (ddb *Dynalock) getKey(key string, options *ReadOptions) (*dynamodb.GetItemOutput, error) {
	return ddb.dynamoSvc.GetItem(&dynamodb.GetItemInput{
		TableName:      aws.String(ddb.tableName),
		ConsistentRead: aws.Bool(options.consistent),
		Key: map[string]*dynamodb.AttributeValue{
			"id":   {S: aws.String(ddb.partition)},
			"name": {S: aws.String(key)},
		},
	})
}

func (ddb *Dynalock) buildUpdateItemInput(key string, options *WriteOptions) *dynamodb.UpdateItemInput {

	expressions := map[string]*dynamodb.AttributeValue{
		":inc": {N: aws.String("1")},
	}

	updateExpression := "ADD version :inc"

	setArgs := []string{}

	// if a value assigned
	if options.value != nil {
		expressions[":payload"] = options.value

		setArgs = append(setArgs, "payload = :payload")
	}

	// if a TTL assigned
	if options.ttl > 0 {
		ttlVal := time.Now().Add(options.ttl).Unix()
		expressions[":ttl"] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(ttlVal, 10))}

		setArgs = append(setArgs, "expires = :ttl")
	}

	// if we have anything to set append them to the update expression
	if len(setArgs) > 0 {
		updateExpression = updateExpression + " SET " + strings.Join(setArgs, ",")
	}

	return &dynamodb.UpdateItemInput{
		TableName:                 aws.String(ddb.tableName),
		Key:                       buildKeys(ddb.partition, key),
		ExpressionAttributeValues: expressions,
		UpdateExpression:          aws.String(updateExpression),
		ReturnValues:              aws.String(dynamodb.ReturnValueAllNew),
	}

}

// NewLock has to implemented at the library level since its not supported by DynamoDB
func (ddb *Dynalock) NewLock(key string, options ...LockOption) (Locker, error) {
	var (
		value   *dynamodb.AttributeValue
		ttl     = DefaultLockTTL
		renewCh = make(chan struct{})
	)

	lockOptions := NewLockOptions(options...)

	if lockOptions.ttl != 0 {
		ttl = lockOptions.ttl
	}
	if lockOptions.value != nil {
		value = lockOptions.value
	}
	if lockOptions.renewLock != nil {
		renewCh = lockOptions.renewLock
	}

	return &dynamodbLock{
		ddb:      ddb,
		last:     nil,
		key:      key,
		value:    value,
		ttl:      ttl,
		renewCh:  renewCh,
		unlockCh: make(chan struct{}),
	}, nil
}

func buildKeys(partition, key string) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		"id":   {S: aws.String(partition)},
		"name": {S: aws.String(key)},
	}
}

func updateWithConditions(item *dynamodb.UpdateItemInput, previous *KVPair) error {

	if previous != nil {
		//
		// if there is a previous provided then we override the create check
		//

		item.ExpressionAttributeValues[":lastRevision"] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(previous.Version, 10))}
		item.ExpressionAttributeValues[":timeNow"] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(time.Now().Unix(), 10))}

		// the previous kv is in the DB and is at the expected revision, also if it has a TTL set it is NOT expired.
		item.ConditionExpression = aws.String("version = :lastRevision AND (attribute_not_exists(expires) OR (attribute_exists(expires) AND expires > :timeNow))")

		return nil
	}

	//
	// assign the create check to ensure record doesn't exist which isn't expired
	//

	item.ExpressionAttributeNames = map[string]*string{"#name": aws.String("name")}
	item.ExpressionAttributeValues[":timeNow"] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(time.Now().Unix(), 10))}

	// if the record exists and is NOT expired
	item.ConditionExpression = aws.String("(attribute_not_exists(id) AND attribute_not_exists(#name)) OR (attribute_exists(expires) AND expires < :timeNow)")

	return nil
}

func decodeItem(item map[string]*dynamodb.AttributeValue) (*KVPair, error) {
	kv := &KVPair{}

	err := dynamodbattribute.UnmarshalMap(item, kv)
	if err != nil {
		return nil, err
	}

	if val, ok := item["payload"]; ok {
		kv.value = val
	}

	return kv, nil
}

func isItemExpired(item map[string]*dynamodb.AttributeValue) bool {
	var ttl int64

	if v, ok := item["expires"]; ok {
		ttl, _ = strconv.ParseInt(aws.StringValue(v.N), 10, 64)
		return time.Unix(ttl, 0).Before(time.Now())
	}

	return false
}

// MarshalStruct this helper method marshals a struct into an *dynamodb.AttributeValue which contains a map
// in the format required to provide to WriteWithAttributeValue.
func MarshalStruct(in interface{}) (*dynamodb.AttributeValue, error) {
	item, err := dynamodbattribute.MarshalMap(in)
	if err != nil {
		return nil, err
	}

	return &dynamodb.AttributeValue{M: item}, nil
}

// UnmarshalStruct this helper method un-marshals a struct from an *dynamodb.AttributeValue returned by KVPair.AttributeValue.
func UnmarshalStruct(val *dynamodb.AttributeValue, out interface{}) error {
	return dynamodbattribute.UnmarshalMap(val.M, out)
}
