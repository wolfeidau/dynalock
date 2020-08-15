package dynalock

import (
	"context"
	"encoding/base64"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
)

// Dynalock lock store which is backed by AWS DynamoDB
type Dynalock struct {
	dynamoSvc dynamodbiface.ClientAPI
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

	if kv.value.S == nil {
		return nil
	}

	buf, err := base64.StdEncoding.DecodeString(aws.StringValue(kv.value.S))
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
func New(dynamoSvc dynamodbiface.ClientAPI, tableName, partition string) Store {
	return &Dynalock{
		dynamoSvc: dynamoSvc,
		tableName: tableName,
		partition: partition,
	}
}

// Put a value at the specified key
func (ddb *Dynalock) Put(ctx context.Context, key string, options ...WriteOption) error {

	params := ddb.buildUpdateItemInput(key, NewWriteOptions(options...))

	req := ddb.dynamoSvc.UpdateItemRequest(params)
	_, err := req.Send(ctx)
	if err != nil {
		return err
	}

	return nil
}

// Exists if a Key exists in the store
func (ddb *Dynalock) Exists(ctx context.Context, key string, options ...ReadOption) (bool, error) {

	readOptions := NewReadOptions(options...)

	req := ddb.dynamoSvc.GetItemRequest(&dynamodb.GetItemInput{
		TableName:      aws.String(ddb.tableName),
		Key:            buildKeys(ddb.partition, key),
		ConsistentRead: aws.Bool(readOptions.consistent),
	})
	res, err := req.Send(ctx)
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
func (ddb *Dynalock) Get(ctx context.Context, key string, options ...ReadOption) (*KVPair, error) {

	readOptions := NewReadOptions(options...)

	res, err := ddb.getKey(ctx, key, readOptions)
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
func (ddb *Dynalock) Delete(ctx context.Context, key string) error {
	req := ddb.dynamoSvc.DeleteItemRequest(&dynamodb.DeleteItemInput{
		TableName: aws.String(ddb.tableName),
		Key:       buildKeys(ddb.partition, key),
	})
	_, err := req.Send(ctx)
	if err != nil {
		return err
	}

	return nil
}

// List the content of a given prefix
func (ddb *Dynalock) List(ctx context.Context, prefix string, options ...ReadOption) ([]*KVPair, error) {

	readOptions := NewReadOptions(options...)

	input := &dynamodb.QueryInput{
		TableName:              aws.String(ddb.tableName),
		KeyConditionExpression: aws.String("#id = :partition AND begins_with(#name, :namePrefix)"),
		ExpressionAttributeNames: map[string]string{
			"#id":   "id",
			"#name": "name",
		},
		ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
			":partition":  {S: aws.String(ddb.partition)},
			":namePrefix": {S: aws.String(prefix)},
		},
		ConsistentRead: aws.Bool(readOptions.consistent),
	}

	items := []map[string]dynamodb.AttributeValue{}

	req := ddb.dynamoSvc.QueryRequest(input)
	p := dynamodb.NewQueryPaginator(req)

	for p.Next(ctx) {
		items = append(items, p.CurrentPage().Items...)
	}

	if err := p.Err(); err != nil {
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
func (ddb *Dynalock) AtomicPut(ctx context.Context, key string, options ...WriteOption) (bool, *KVPair, error) {

	writeOptions := NewWriteOptions(options...)

	// if writeOptions.previous != nil {
	// 	if writeOptions.ttl == DefaultLockTTL {
	// 		writeOptions.ttl = writeOptions.previous.Expires
	// 	}
	// }

	params := ddb.buildUpdateItemInput(key, writeOptions)

	err := updateWithConditions(params, writeOptions.previous)
	if err != nil {
		return false, nil, err
	}

	req := ddb.dynamoSvc.UpdateItemRequest(params)
	res, err := req.Send(ctx)
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
func (ddb *Dynalock) AtomicDelete(ctx context.Context, key string, previous *KVPair) (bool, error) {

	getRes, err := ddb.getKey(ctx, key, NewReadOptions())
	if err != nil {
		return false, err
	}

	if previous == nil && getRes.Item != nil && !isItemExpired(getRes.Item) {
		return false, ErrKeyExists
	}

	expAttr := map[string]dynamodb.AttributeValue{
		":lastRevision": {N: aws.String(strconv.FormatInt(previous.Version, 10))},
	}

	params := &dynamodb.DeleteItemInput{
		TableName:                 aws.String(ddb.tableName),
		Key:                       buildKeys(ddb.partition, key),
		ConditionExpression:       aws.String("version = :lastRevision"),
		ExpressionAttributeValues: expAttr,
	}
	req := ddb.dynamoSvc.DeleteItemRequest(params)
	_, err = req.Send(ctx)
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

func (ddb *Dynalock) getKey(ctx context.Context, key string, options *ReadOptions) (*dynamodb.GetItemResponse, error) {
	req := ddb.dynamoSvc.GetItemRequest(&dynamodb.GetItemInput{
		TableName:      aws.String(ddb.tableName),
		ConsistentRead: aws.Bool(options.consistent),
		Key: map[string]dynamodb.AttributeValue{
			"id":   {S: aws.String(ddb.partition)},
			"name": {S: aws.String(key)},
		},
	})

	return req.Send(ctx)
}

func (ddb *Dynalock) buildUpdateItemInput(key string, options *WriteOptions) *dynamodb.UpdateItemInput {

	expressions := map[string]dynamodb.AttributeValue{
		":inc": {N: aws.String("1")},
	}

	updateExpression := "ADD version :inc"

	var setArgs []string

	// if a value assigned
	if options.value != nil {
		expressions[":payload"] = *options.value
		setArgs = append(setArgs, "payload = :payload")
	}

	// if a TTL assigned
	if options.ttl > 0 {
		ttlVal := time.Now().Add(options.ttl).Unix()
		expressions[":ttl"] = dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(ttlVal, 10))}

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
		ReturnValues:              dynamodb.ReturnValueAllNew,
	}

}

// NewLock has to implemented at the library level since its not supported by DynamoDB
func (ddb *Dynalock) NewLock(ctx context.Context, key string, options ...LockOption) (Locker, error) {
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

	return &DynamodbLock{
		ddb:      ddb,
		last:     nil,
		key:      key,
		value:    value,
		ttl:      ttl,
		renewCh:  renewCh,
		unlockCh: make(chan struct{}),
	}, nil
}

func buildKeys(partition, key string) map[string]dynamodb.AttributeValue {
	return map[string]dynamodb.AttributeValue{
		"id":   {S: aws.String(partition)},
		"name": {S: aws.String(key)},
	}
}

func updateWithConditions(item *dynamodb.UpdateItemInput, previous *KVPair) error {

	if previous != nil {
		//
		// if there is a previous provided then we override the create check
		//

		item.ExpressionAttributeValues[":lastRevision"] = dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(previous.Version, 10))}
		item.ExpressionAttributeValues[":timeNow"] = dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(time.Now().Unix(), 10))}

		// the previous kv is in the DB and is at the expected revision, also if it has a TTL set it is NOT expired.
		item.ConditionExpression = aws.String("version = :lastRevision AND (attribute_not_exists(expires) OR (attribute_exists(expires) AND expires > :timeNow))")

		return nil
	}

	//
	// assign the create check to ensure record doesn't exist which isn't expired
	//

	item.ExpressionAttributeNames = map[string]string{"#name": "name"}
	item.ExpressionAttributeValues[":timeNow"] = dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(time.Now().Unix(), 10))}

	// if the record exists and is NOT expired
	item.ConditionExpression = aws.String("(attribute_not_exists(id) AND attribute_not_exists(#name)) OR (attribute_exists(expires) AND expires < :timeNow)")

	return nil
}

func decodeItem(item map[string]dynamodb.AttributeValue) (*KVPair, error) {
	kv := &KVPair{}

	err := dynamodbattribute.UnmarshalMap(item, kv)
	if err != nil {
		return nil, err
	}

	if val, ok := item["payload"]; ok {
		kv.value = &val
	}

	return kv, nil
}

func isItemExpired(item map[string]dynamodb.AttributeValue) bool {
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
