package broker

import (
	"log"
	"sync"
)

type Message struct {
	Topic string
	Body  string
}

type Broker struct {
	mu          sync.RWMutex
	topics      map[string]map[*Subscriber]bool
	subscribers map[string]*Subscriber
	publishChan chan Message
}

type Subscriber struct {
	ID       string
	Messages chan Message
	broker   *Broker
}

func NewBroker() *Broker {
	b := &Broker{
		topics:      make(map[string]map[*Subscriber]bool),
		subscribers: make(map[string]*Subscriber),
		publishChan: make(chan Message, 100),
	}
	go b.run()
	return b
}

func (b *Broker) run() {
	for msg := range b.publishChan {
		subs := b.getSubscribers(msg.Topic)
		for _, sub := range subs {
			select {
			case sub.Messages <- msg:
			default:
				log.Printf("Subscriber %s is slow, dropping message", sub.ID)
			}
		}
	}
}

func (b *Broker) getSubscribers(topic string) []*Subscriber {
	b.mu.RLock()
	defer b.mu.RUnlock()
	subs := make([]*Subscriber, 0, len(b.topics[topic]))
	for sub := range b.topics[topic] {
		subs = append(subs, sub)
	}
	return subs
}

func (b *Broker) NewSubscriber(id string) *Subscriber {
	b.mu.Lock()
	defer b.mu.Unlock()

	sub := &Subscriber{
		ID:       id,
		Messages: make(chan Message, 100),
		broker:   b,
	}

	b.subscribers[id] = sub
	return sub
}

func (b *Broker) Publish(topic string, msg string) {
	b.publishChan <- Message{
		Topic: topic,
		Body:  msg,
	}
}

func (s *Subscriber) Subscribe(topic string) {
	s.broker.mu.Lock()
	defer s.broker.mu.Unlock()

	if s.broker.topics[topic] == nil {
		s.broker.topics[topic] = make(map[*Subscriber]bool)
	}

	s.broker.topics[topic][s] = true
}

func (s *Subscriber) Unsubscribe(topic string) {
	s.broker.mu.Lock()
	defer s.broker.mu.Unlock()

	delete(s.broker.topics[topic], s)
}

func (s *Subscriber) Close() {
	s.broker.mu.Lock()
	defer s.broker.mu.Unlock()

	delete(s.broker.subscribers, s.ID)

	for topic := range s.broker.topics {
		delete(s.broker.topics[topic], s)
	}
}
