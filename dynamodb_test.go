package dynalock

import (
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/require"
)

type Person struct {
	Name string
}

func TestMarshalStruct(t *testing.T) {

	assert := require.New(t)

	want := &dynamodb.AttributeValue{
		M: map[string]*dynamodb.AttributeValue{
			"Name": {S: aws.String("Mark Wolfe")},
		},
	}

	got, err := MarshalStruct(&Person{Name: "Mark Wolfe"})
	assert.NoError(err)
	assert.Equal(want, got)
}

func TestUnmarshalStruct(t *testing.T) {

	assert := require.New(t)

	want := &Person{Name: "Mark Wolfe"}

	in := &dynamodb.AttributeValue{
		M: map[string]*dynamodb.AttributeValue{
			"Name": {S: aws.String("Mark Wolfe")},
		},
	}

	val := new(Person)

	err := UnmarshalStruct(in, val)
	assert.NoError(err)
	assert.Equal(want, val)
}

func Test_buildUpdateItemInput(t *testing.T) {
	assert := require.New(t)

	ddl := &Dynalock{partition: "test", tableName: "testTable"}

	update := ddl.buildUpdateItemInput("abc123", &WriteOptions{ttl: 300 * time.Second})

	expectedUpdate := dynamodb.UpdateItemInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":inc": {
				N: aws.String("1"),
			},
			":ttl": {
				N: aws.String("1575725615"),
			},
		},
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String("test"),
			},
			"name": {
				S: aws.String("abc123"),
			},
		},
		ReturnValues:     aws.String("ALL_NEW"),
		TableName:        aws.String("testTable"),
		UpdateExpression: aws.String("ADD version :inc SET expires = :ttl"),
	}

	assert.Equal(expectedUpdate.Key, update.Key)

	ttlTime := convertTTL(update.ExpressionAttributeValues[":ttl"].N)

	// ensure the TTL has been applied
	assert.True(time.Now().Add(200 * time.Second).Before(ttlTime))

}

func convertTTL(val *string) time.Time {
	ttl := aws.StringValue(val)
	tval, _ := strconv.ParseInt(ttl, 10, 64)
	return time.Unix(tval, 0)
}
