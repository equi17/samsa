package broker

import "sync"

type PublishCommand struct {
	Topic string
	Value string
}

type Broker struct {
	topics map[string]*Topic
	mu     sync.RWMutex

	nextID int64

	publishCh chan PublishCommand
}

func NewBroker() *Broker {
	b := &Broker{
		topics:   make(map[string]*Topic),
		publishCh: make(chan PublishCommand, 100),
	}

	go b.start()

	return b
}

func (b *Broker) start() {
	for cmd := range b.publishCh {
		b.handlePublish(cmd)
	}
}

func (b *Broker) handlePublish(cmd PublishCommand) {
	topic := b.getOrCreateTopic(cmd.Topic)

	b.nextID++

	msg := Message{
		ID:    b.nextID,
		Value: cmd.Value,
	}

	topic.Publish(msg)
}

func (b *Broker) getOrCreateTopic(name string) *Topic {
	b.mu.Lock()
	defer b.mu.Unlock()

	topic, exists := b.topics[name]
	if exists {
		return topic
	}

	topic = NewTopic(name)
	b.topics[name] = topic

	return topic
}

func (b *Broker) Publish(topicName string, value string) {
	cmd := PublishCommand{
		Topic: topicName,
		Value: value,
	}

	b.publishCh <- cmd
}

func (b *Broker) Consume(topicName string, offset int) []Message {
	b.mu.RLock()
	topic, exists := b.topics[topicName]
	b.mu.RUnlock()

	if !exists {
		return []Message{}
	}

	return topic.Consume(offset)
}

func (b *Broker) Subscribe(topicName string) (int, chan Message) {
	topic := b.getOrCreateTopic(topicName)

	return topic.Subscribe()
}

func (b *Broker) Unsubscribe(topicName string, subscriberID int) {
	b.mu.RLock()
	topic, exists := b.topics[topicName]
	b.mu.RUnlock()

	if !exists {
		return
	}

	topic.Unsubscribe(subscriberID)
}