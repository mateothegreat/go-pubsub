package queues

type Message[T any] struct {
	topic string
	body  T
}

func (m *Message[any]) GetTopic() string {
	// returns the topic of the message
	return m.topic
}
