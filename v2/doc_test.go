package dynalock_test

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/wolfeidau/dynalock/v2"
)

func ExampleDynalock_NewLock() {

	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		panic("unable to load SDK config, " + err.Error())
	}

	dbSvc := dynamodb.New(cfg)

	dl := dynalock.New(dbSvc, "testing-locks", "agent")

	lock, _ := dl.NewLock(
		context.Background(),
		"agents/123",
		dynalock.LockWithTTL(2*time.Second),
		dynalock.LockWithBytes([]byte(`{"agent": "testing"}`)),
	)
	lock.Lock(context.Background(), nil)
	defer lock.Unlock(context.Background())
}

func ExampleDynalock_Put() {

	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		panic("unable to load SDK config, " + err.Error())
	}

	dbSvc := dynamodb.New(cfg)

	dl := dynalock.New(dbSvc, "testing-locks", "agent")

	message := struct{ Message string }{Message: "hello"}

	attrVal, _ := dynalock.MarshalStruct(&message)

	dl.Put(
		context.Background(),
		"agents/123",
		dynalock.WriteWithAttributeValue(attrVal),
	)

}

func ExampleDynalock_Get() {

	type message struct{ Message string }

	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		panic("unable to load SDK config, " + err.Error())
	}

	dbSvc := dynamodb.New(cfg)
	dl := dynalock.New(dbSvc, "testing-locks", "agent")

	kv, _ := dl.Get(context.Background(), "agents/123")

	msg := &message{}

	av := kv.AttributeValue()

	dynalock.UnmarshalStruct(av, msg)

	fmt.Println("Message:", msg.Message)
}
