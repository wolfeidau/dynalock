package dynalock_test

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/wolfeidau/dynalock"
)

func ExampleDynalock_NewLock() {

	sess := session.Must(session.NewSession())

	dbSvc := dynamodb.New(sess)

	dl := dynalock.New(dbSvc, "testing-locks", "agent")

	lock, _ := dl.NewLock(
		"agents/123",
		dynalock.LockWithTTL(2*time.Second),
		dynalock.LockWithBytes([]byte(`{"agent": "testing"}`)),
	)
	lock.Lock(nil)
	defer lock.Unlock()
}

func ExampleDynalock_Put() {

	sess := session.Must(session.NewSession())

	dbSvc := dynamodb.New(sess)

	dl := dynalock.New(dbSvc, "testing-locks", "agent")

	message := struct{ Message string }{Message: "hello"}

	attrVal, _ := dynalock.MarshalStruct(&message)

	dl.Put(
		"agents/123",
		dynalock.WriteWithAttributeValue(attrVal),
	)

}

func ExampleDynalock_Get() {

	type message struct{ Message string }

	sess := session.Must(session.NewSession())

	dbSvc := dynamodb.New(sess)

	dl := dynalock.New(dbSvc, "testing-locks", "agent")

	kv, _ := dl.Get("agents/123")

	msg := &message{}

	dynalock.UnmarshalStruct(kv.AttributeValue(), msg)

	fmt.Println("Message:", msg.Message)
}
