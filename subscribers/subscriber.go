package subscribers

import (
	"crypto/rand"
	"log"
	"sync"

	"github.com/mateothegreat/go-pubsub/types"
)

type Subscriber[T any] struct {
	ID       string                 // id of subscriber
	Messages chan *types.Message[T] // messages channel
	Topics   map[string]bool        // topics it is subscribed to.
	Active   bool                   // if given subscriber is active
	Mutex    sync.RWMutex           // lock
}

// CreateNewSubscriber creates a new subscriber.
func CreateNewSubscriber[T any](ID string) (*Subscriber[T], error) {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal(err)
	}

	return &Subscriber[T]{
		ID:       ID,
		Messages: make(chan *types.Message[T]),
		Topics:   map[string]bool{},
		Active:   true,
	}, nil
}

// AddTopic adds a topic to the subscriber.
func (s *Subscriber[T]) AddTopic(topic string) {
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
	for topic := range s.Topics {
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

func (s *Subscriber[T]) Signal(msg *types.Message[T]) {
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
