package dynalock_test

import (
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

	dl.Put(
		"agents/123",
		dynalock.WriteWithBytes([]byte(`{"agent": "testing"}`)),
	)

}
