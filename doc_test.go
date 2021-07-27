package dynalock_test

import (
	"context"
	"errors"
	"fmt"
	"log"
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

func ExampleDynalock_LockWithContext() {

	sess := session.Must(session.NewSession())

	dbSvc := dynamodb.New(sess)

	dl := dynalock.New(dbSvc, "testing-locks", "agent")

	lock, _ := dl.NewLock(
		"agents/123",
		dynalock.LockWithTTL(2*time.Second),
		dynalock.LockWithBytes([]byte(`{"agent": "testing"}`)),
	)

	// set up a context which will timeout after 5 seconds waiting for a lock
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()

	// this will block for up to 5 seconds
	_, err := lock.LockWithContext(ctx)
	if err != nil {
		if errors.Is(err, dynalock.ErrLockAcquireCancelled) {
			// handle the timeout of the context here
			log.Println("waiting for lock expired after timeout")

			return // we didn't get a lock so skip the unlock defer
		}

		// normal error here
		log.Fatalf("locking error: %v", err)

		return // we didn't get a lock so skip the unlock defer
	}
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
