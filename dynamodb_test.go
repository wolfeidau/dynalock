package dynalock

import (
	"testing"

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
