package queues

import (
	"fmt"
	"sync"
)

type Subscribers[T any] map[string]*Subscriber[T]

type Broker[T any] struct {
	subscribers Subscribers[T]            // map of subscribers id:Subscriber
	topics      map[string]Subscribers[T] // map of topics topic:Subscribers
	mut         sync.RWMutex              // mutex lock
}

func NewBroker[T any]() *Broker[T] {
	// returns new broker object
	return &Broker[T]{
		subscribers: Subscribers[T]{},
		topics:      map[string]Subscribers[T]{},
	}
}

func (b *Broker[T]) AddSubscriber() (*Subscriber[T], error) {
	// Add subscriber to the broker.
	b.mut.Lock()
	defer b.mut.Unlock()
	s, err := CreateNewSubscriber[T]()
	if err != nil {
		return nil, err
	}

	b.subscribers[s.ID] = s
	return s, nil
}

func (b *Broker[T]) RemoveSubscriber(s *Subscriber[T]) {
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

func (b *Broker[T]) Subscribe(s *Subscriber[T], topic string) {
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

func (b *Broker[T]) Unsubscribe(s *Subscriber[T], topic string) {
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

	message := &Message[T]{topic: topic, body: msg}

	for _, s := range bTopics {
		if !s.Active {
			return
		}
		go (func(s *Subscriber[T]) {
			s.Signal(message)
		})(s)
	}
}
