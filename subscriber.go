package queues

import (
	"crypto/rand"
	"fmt"
	"log"
	"sync"
)

type Subscriber[T any] struct {
	ID       string           // id of subscriber
	Messages chan *Message[T] // messages channel
	Topics   map[string]bool  // topics it is subscribed to.
	Active   bool             // if given subscriber is active
	Mutex    sync.RWMutex     // lock
}

func CreateNewSubscriber[T any]() (*Subscriber[T], error) {
	// returns a new subscriber.
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal(err)
	}
	id := fmt.Sprintf("%X-%X", b[0:4], b[4:8])
	return &Subscriber[T]{
		ID:       id,
		Messages: make(chan *Message[T]),
		Topics:   map[string]bool{},
		Active:   true,
	}, nil
}

func (s *Subscriber[T]) AddTopic(topic string) {
	// add topic to the subscriber
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	s.Topics[topic] = true
}

func (s *Subscriber[T]) RemoveTopic(topic string) {
	// remove topic to the subscriber
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	delete(s.Topics, topic)
}

func (s *Subscriber[T]) GetTopics() []string {
	// Get all topic of the subscriber
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	topics := []string{}
	for topic, _ := range s.Topics {
		topics = append(topics, topic)
	}
	return topics
}

func (s *Subscriber[T]) Destruct() {
	// destructor for subscriber.
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	s.Active = false
	close(s.Messages)
}

func (s *Subscriber[T]) Signal(msg *Message[T]) {
	// Gets the message from the channel
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	if s.Active {
		s.Messages <- msg
	}
}

// func (s *Subscriber[T]) Listen() {
// 	// Listens to the message channel, prints once received.
// 	for {
// 		if msg, ok := <-s.messages; ok {
// 			// fmt.Printf("Subscriber %s, received: %s from topic: %s\n", s.id, msg.body, msg.GetTopic())
// 		}
// 	}
// }
