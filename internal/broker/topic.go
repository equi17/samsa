package broker

import "sync"

type Topic struct {
	Name string

	Messages []Message

	subscribers map[int]chan Message

	nextSubscriberID int

	mu sync.RWMutex
}

func NewTopic(name string) *Topic {
	return &Topic{
		Name:     name,
		Messages: make([]Message, 0),
		subscribers: make(map[int]chan Message),
	}
}

func (t *Topic) Publish(msg Message) {
	t.mu.Lock()

	t.Messages = append(t.Messages, msg)

	subscribers := make(map[int]chan Message)

	for id, ch := range t.subscribers {
		subscribers[id] = ch
	}

	t.mu.Unlock()

	for _, sub := range subscribers {
		select {
		case sub <- msg:
		default:
		}
	}
}

func (t *Topic) Consume(offset int) []Message {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if offset >= len(t.Messages) {
		return []Message{}
	}

	return t.Messages[offset:]
}

func (t *Topic) Subscribe() (int, chan Message) {
	t.mu.Lock()
	defer t.mu.Unlock()

	id := t.nextSubscriberID
	t.nextSubscriberID++

	ch := make(chan Message, 10)

	t.subscribers[id] = ch

	return id, ch
}

func (t *Topic) Unsubscribe(id int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	ch, exists := t.subscribers[id]
	if !exists {
		return
	}

	close(ch)

	delete(t.subscribers, id)
}