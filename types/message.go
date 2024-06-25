package types

// Message is a struct that represents a message that can be sent to a queue.
// Because queues can contain multiple topics, the message has a topic field.
// It supports a generic type for the body of the message.
type Message[T any] struct {
	Topic string
	Body  T
}
