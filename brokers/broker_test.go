package brokers

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"
)

type TestMessage struct {
	Foo string
}

func TestMain(t *testing.T) {
	broker := NewBroker[TestMessage]()

	subscriber, err := broker.CreateSubscriber("asdf")
	if err != nil {
		log.Fatal(err)
	}

	broker.Subscribe(subscriber, "topic1")

	go func() {
		for {
			msg := fmt.Sprintf("%f", rand.Float64())
			go broker.Publish("topic1", TestMessage{Foo: msg})
			time.Sleep(time.Duration(rand.Intn(10000)) * time.Microsecond)
		}
	}()

	go func() {
		for range time.Tick(2 * time.Millisecond) {
			log.Printf("Subscriber: %s, Topics: %v\n", subscriber.ID, subscriber.GetTopics())

			select {
			case msg := <-subscriber.Messages:
				log.Printf("Subscriber: %s, Received: %v\n", subscriber.ID, msg)
			default:
				log.Printf("Subscriber: %s, No message\n", subscriber.ID)
			}
		}
	}()
	select {}
}
