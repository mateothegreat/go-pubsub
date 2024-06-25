package brokers

import (
	"fmt"
	"sync"

	"github.com/mateothegreat/go-pubsub/subscribers"
	"github.com/mateothegreat/go-pubsub/types"
)

// Subscribers is a map[string] of subscribers where the key is the subscriber ID.
type Subscribers[T any] map[string]*subscribers.Subscriber[T]

// Broker is a struct that represents a broker that can be used to publish messages to subscribers.
type Broker[T any] struct {
	subscribers Subscribers[T]            // list of subscribers
	topics      map[string]Subscribers[T] // topics and subscribers
	mut         sync.RWMutex              // mutex lock
}

// NewBroker returns a new broker object.
func NewBroker[T any]() *Broker[T] {
	return &Broker[T]{
		subscribers: Subscribers[T]{},
		topics:      map[string]Subscribers[T]{},
	}
}

func (b *Broker[T]) AddSubscriber() (*subscribers.Subscriber[T], error) {
	// Add subscriber to the broker.
	b.mut.Lock()
	defer b.mut.Unlock()
	s, err := subscribers.CreateNewSubscriber[T]()
	if err != nil {
		return nil, err
	}

	b.subscribers[s.ID] = s
	return s, nil
}

func (b *Broker[T]) RemoveSubscriber(s *subscribers.Subscriber[T]) {
	// remove subscriber to the broker.
	// unsubscribe to all topics which s is subscribed to.
	for topic := range s.Topics {
		b.Unsubscribe(s, topic)
	}
	b.mut.Lock()
	// remove subscriber from list of subscribers.
	delete(b.subscribers, s.ID)
	b.mut.Unlock()
	s.Destruct()
}

func (b *Broker[T]) GetSubscribers(topic string) int {
	// get total subscribers subscribed to given topic.
	b.mut.RLock()
	defer b.mut.RUnlock()
	return len(b.topics[topic])
}

func (b *Broker[T]) Subscribe(s *subscribers.Subscriber[T], topic string) {
	// subscribe to given topic
	b.mut.Lock()
	defer b.mut.Unlock()

	if b.topics[topic] == nil {
		b.topics[topic] = Subscribers[T]{}
	}
	s.AddTopic(topic)
	b.topics[topic][s.ID] = s
	fmt.Printf("%s Subscribed for topic: %s\n", s.ID, topic)
}

func (b *Broker[T]) Unsubscribe(s *subscribers.Subscriber[T], topic string) {
	// unsubscribe to given topic
	b.mut.RLock()
	defer b.mut.RUnlock()

	delete(b.topics[topic], s.ID)
	s.RemoveTopic(topic)
	fmt.Printf("%s Unsubscribed for topic: %s\n", s.ID, topic)
}

// Publish message to given topic
func (b *Broker[T]) Publish(topic string, msg T) {
	b.mut.RLock()
	bTopics := b.topics[topic]
	b.mut.RUnlock()

	message := &types.Message[T]{Topic: topic, Body: msg}

	for _, s := range bTopics {
		if !s.Active {
			return
		}
		go (func(s *subscribers.Subscriber[T]) {
			s.Signal(message)
		})(s)
	}
}
