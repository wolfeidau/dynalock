// We have a table containing some entries, for each one we need to
// lock, do some work, then unlock the entry.
//
// To illustrate a typical distributed solution we make a couple of
// competing consumers which get a list of entries and try and lock
// them to perform this work. If one of the works fails the other
// will take up the slack.
package main

import (
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/kelseyhightower/envconfig"
	"github.com/wolfeidau/dynalock"
)

var (
	defaultLockValue = dynalock.LockWithBytes([]byte(`test`))
	defaultLockTtl   = dynalock.LockWithTTL(30 * time.Second)
)

// Config load configuration from env
type Config struct {
	RegisterTableName string `envconfig:"REGISTER_TABLE_NAME"`
}

func main() {

	// ensure the random is seeded and avoid not so random random
	rand.Seed(time.Now().UnixNano())

	// shows the log number and data for debugging
	log.SetFlags(log.Lshortfile | log.Ldate | log.Ltime)

	// load some basic configuration so i can feed in ENV variables
	var c Config

	err := envconfig.Process("", &c)
	if err != nil {
		log.Fatalf("failed to load env config: %+v", err)
	}

	// aws session with no configuration as it is driven via ENV variables
	sess, err := session.NewSession()
	if err != nil {
		log.Fatalf("failed to create session: %+v", err)
	}

	agentStore := dynalock.New(dynamodb.New(sess), c.RegisterTableName, "Agent")
	lockStore := dynalock.New(dynamodb.New(sess), c.RegisterTableName, "Lock")

	agentNames := []string{"w1", "w2", "w3", "w4"}

	workers := make([]*worker, len(agentNames))

	for n, name := range agentNames {

		agentName := "orchestra/" + name

		log.Printf("creating agent: %s", agentName)

		// create an agent entry, note if one exists this will simply update it
		err = agentStore.Put(agentName, dynalock.WriteWithAttributeValue(&dynamodb.AttributeValue{S: aws.String("test")}))
		if err != nil {
			log.Fatal(err.Error())
		}

		// create a matching worker to do some do some work like polling a service,
		// but in a way which must be syncronised across possible instances
		// of this service
		workers[n] = newWorker(name, agentStore, lockStore)

		go workers[n].startup()

	}

	log.Print("waiting")

	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, os.Interrupt)

	<-sigch

	log.Print("done")
}

type worker struct {
	name       string
	agentStore dynalock.Store
	lockStore  dynalock.Store
	totalWork  int
}

func newWorker(name string, agentStore dynalock.Store, lockStore dynalock.Store) *worker {
	return &worker{name: name, agentStore: agentStore, lockStore: lockStore}
}

func (w *worker) startup() {

	kv, err := w.agentStore.Get("orchestra/" + w.name)
	if err != nil {
		log.Fatalf("failed to get agent: %+v", err)
	}

	log.Printf("[%s] %v", w.name, kv)

	for {
		w.doWork(kv)
	}

}

// seperated out each iteration to take advantage of defer and ensure it is easier to test
func (w *worker) doWork(kv *dynalock.KVPair) {
	lock, err := w.lockStore.NewLock("orchestra/"+w.name, defaultLockTtl)
	if err != nil {
		log.Fatalf("failed to create a new lock on agent: %+v", err)
	}

	log.Printf("[%s] wait for lock", w.name)

	stopChan := make(chan struct{})

	_, err = lock.Lock(stopChan)
	if err != nil {
		if err == dynalock.ErrLockAcquireCancelled {
			log.Println("ErrLockAcquireCancelled")
			return // we are done for this loop
		}
		log.Fatalf("failed to lock agent: %+v", err)
	}

	defer unlockFunc(lock, w.name)

	log.Printf("[%s] record locked", w.name)

	work := 5 + rand.Intn(25) // basic random work interval between 5 and 30

	time.Sleep(duration(work))

	w.totalWork += work

	log.Printf("[%s] update agent: %s", w.name, kv.Key)

	err = w.agentStore.Put(kv.Key, dynalock.WriteWithAttributeValue(&dynamodb.AttributeValue{S: aws.String("test")}), dynalock.WriteWithTTL(5*time.Minute))
	if err != nil {
		log.Fatalf("failed to update agent: %+v", err)
	}

	log.Printf("[%s] work done total %d", w.name, w.totalWork)
}

// this function just helps with linters which hate me when I don't handle error return values, in this case it just logs it
func unlockFunc(lock dynalock.Locker, name string) {

	err := lock.Unlock()
	if err != nil {
		log.Printf("failed to unlock agent: %+v", err)
	}
	log.Printf("[%s] record unlocked", name)

}

func duration(n int) time.Duration {
	return time.Duration(n) * time.Second
}
