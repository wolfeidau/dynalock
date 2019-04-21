package dynalock

import (
	"context"
	"encoding/base64"
	"strconv"
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

// KVPair represents {Key, Value, Lastindex} tuple
type KVPair struct {
	Partition string `dynamodbav:"id"`
	Key       string `dynamodbav:"name"`
	Value     []byte `dynamodbav:"payload"`
	Version   int64  `dynamodbav:"version"`
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
func (ddb *Dynalock) Put(key string, payload []byte, options *WriteOptions) error {

	if options == nil {
		options = &WriteOptions{
			TTL: defaultLockTTL,
		}
	}

	params := ddb.buildUpdateItemInput(key, payload, options)

	_, err := ddb.dynamoSvc.UpdateItem(params)
	if err != nil {
		return err
	}

	return nil
}

// Exists if a Key exists in the store
func (ddb *Dynalock) Exists(key string, options *ReadOptions) (bool, error) {

	if options == nil {
		options = &ReadOptions{
			Consistent: true, // default to enabling read consistency
		}
	}

	res, err := ddb.dynamoSvc.GetItem(&dynamodb.GetItemInput{
		TableName:      aws.String(ddb.tableName),
		Key:            buildKeys(ddb.partition, key),
		ConsistentRead: aws.Bool(options.Consistent),
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
func (ddb *Dynalock) Get(key string, options *ReadOptions) (*KVPair, error) {

	if options == nil {
		options = &ReadOptions{
			Consistent: true, // default to enabling read consistency
		}
	}

	res, err := ddb.getKey(key, options)
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
func (ddb *Dynalock) List(prefix string, options *ReadOptions) ([]*KVPair, error) {

	if options == nil {
		options = &ReadOptions{
			Consistent: true, // default to enabling read consistency
		}
	}

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
		ConsistentRead: aws.Bool(options.Consistent),
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
func (ddb *Dynalock) AtomicPut(key string, payload []byte, previous *KVPair, options *WriteOptions) (bool, *KVPair, error) {

	if options == nil {
		options = &WriteOptions{
			TTL: defaultLockTTL,
		}
	}

	getRes, err := ddb.getKey(key, &ReadOptions{
		Consistent: true, // enable the read consistent flag
	})
	if err != nil {
		return false, nil, err
	}

	// AtomicPut is equivalent to Put if previous is nil and the Key
	// exist in the DB or is not expired.
	if previous == nil && getRes.Item != nil && !isItemExpired(getRes.Item) {
		return false, nil, ErrKeyExists
	}

	params := ddb.buildUpdateItemInput(key, payload, options)

	err = updateWithConditions(params, previous)
	if err != nil {
		return false, nil, err
	}

	res, err := ddb.dynamoSvc.UpdateItem(params)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
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
func (ddb *Dynalock) AtomicDelete(key string, previous *KVPair) (bool, error) {

	getRes, err := ddb.getKey(key, &ReadOptions{
		Consistent: true, // enable the read consistent flag
	})
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
		ConsistentRead: aws.Bool(options.Consistent),
		Key: map[string]*dynamodb.AttributeValue{
			"id":   {S: aws.String(ddb.partition)},
			"name": {S: aws.String(key)},
		},
	})
}

func (ddb *Dynalock) buildUpdateItemInput(key string, payload []byte, options *WriteOptions) *dynamodb.UpdateItemInput {

	ttlVal := time.Now().Add(options.TTL).Unix()

	expressions := map[string]*dynamodb.AttributeValue{
		":inc":     {N: aws.String("1")},
		":payload": encodePayload(payload),
		":ttl":     {N: aws.String(strconv.FormatInt(ttlVal, 10))},
	}

	updateExpression := aws.String("ADD version :inc SET payload = :payload, expires = :ttl")

	return &dynamodb.UpdateItemInput{
		TableName:                 aws.String(ddb.tableName),
		Key:                       buildKeys(ddb.partition, key),
		ExpressionAttributeValues: expressions,
		UpdateExpression:          updateExpression,
		ReturnValues:              aws.String(dynamodb.ReturnValueAllNew),
	}

}

// NewLock has to implemented at the library level since its not supported by DynamoDB
func (ddb *Dynalock) NewLock(key string, options *LockOptions) (Locker, error) {
	var (
		value   []byte
		ttl     = defaultLockTTL
		renewCh = make(chan struct{})
	)

	if options == nil {
		options = &LockOptions{
			TTL: defaultLockTTL,
		}
	}

	if options.TTL != 0 {
		ttl = options.TTL
	}
	if len(options.Value) != 0 {
		value = options.Value
	}
	if options.RenewLock != nil {
		renewCh = options.RenewLock
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

		item.ExpressionAttributeValues[":lastRevision"] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(previous.Version, 10))}
		item.ExpressionAttributeValues[":timeNow"] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(time.Now().Unix(), 10))}

		// the previous kv is in the DB and is at the expected revision, also if it has a TTL set it is NOT expired.
		item.ConditionExpression = aws.String("version = :lastRevision AND (attribute_not_exists(expires) OR (attribute_exists(expires) AND expires > :timeNow))")
	}

	return nil
}

func encodePayload(payload []byte) *dynamodb.AttributeValue {
	encodedValue := base64.StdEncoding.EncodeToString(payload)
	return &dynamodb.AttributeValue{S: aws.String(encodedValue)}
}

func decodeItem(item map[string]*dynamodb.AttributeValue) (*KVPair, error) {
	kv := &KVPair{}

	err := dynamodbattribute.UnmarshalMap(item, kv)
	if err != nil {
		return nil, err
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
