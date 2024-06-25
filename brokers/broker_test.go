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

	s1 := broker.AddSubscriber()
	broker.Subscribe(s1, "topic1")

	go func() {
		for {
			msg := fmt.Sprintf("%f", rand.Float64())
			go broker.Publish("topic1", TestMessage{Foo: msg})
			time.Sleep(time.Duration(rand.Intn(10000)) * time.Microsecond)
		}
	}()

	go func() {
		for range time.Tick(2 * time.Millisecond) {
			log.Printf("Subscriber: %s, Topics: %v\n", s1.id, s1.GetTopics())

			select {
			case msg := <-s1.messages:
				log.Printf("Subscriber: %s, Received: %v\n", s1.id, msg)
			default:
				log.Printf("Subscriber: %s, No message\n", s1.id)
			}
		}
	}()
	select {}
}
