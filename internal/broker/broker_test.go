package broker_test

import (
	"testing"
	"time"

	"github.com/KsanneK/packetq/internal/broker"
)

func TestSubscriber_SubscribeAndPublish(t *testing.T) {
	b := broker.NewBroker()
	sub := b.NewSubscriber("sub-1")

	sub.Subscribe("news")

	msg := "hello world"
	b.Publish("news", msg)

	select {
	case received := <-sub.Messages:
		if received != msg {
			t.Fatalf("expected message %q, got %q", msg, received)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for message")
	}
}

func TestSubscriber_PublishOnlyToSubscribedTopic(t *testing.T) {
	b := broker.NewBroker()
	sub := b.NewSubscriber("sub-1")

	sub.Subscribe("sports")

	b.Publish("weather", "sunny")

	select {
	case msg := <-sub.Messages:
		t.Fatalf("received unexpected message: %q", msg)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSubscriber_Unsubscribe(t *testing.T) {
	b := broker.NewBroker()
	sub := b.NewSubscriber("sub-1")

	sub.Subscribe("news")
	sub.Unsubscribe("news")

	b.Publish("news", "some update")

	select {
	case msg := <-sub.Messages:
		t.Fatalf("received message after unsubscribe: %q", msg)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSubscriber_Close(t *testing.T) {
	b := broker.NewBroker()
	sub := b.NewSubscriber("sub-1")

	sub.Subscribe("news")
	sub.Subscribe("sports")

	sub.Close()

	b.Publish("news", "breaking news")
	b.Publish("sports", "match results")

	select {
	case msg := <-sub.Messages:
		t.Fatalf("received message after Close: %q", msg)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestBroker_MultipleSubscribers(t *testing.T) {
	b := broker.NewBroker()

	sub1 := b.NewSubscriber("sub-1")
	sub2 := b.NewSubscriber("sub-2")

	sub1.Subscribe("tech")
	sub2.Subscribe("tech")

	b.Publish("tech", "go 1.27 released")

	for _, sub := range []*broker.Subscriber{sub1, sub2} {
		select {
		case msg := <-sub.Messages:
			if msg != "go 1.27 released" {
				t.Fatalf("expected 'go 1.27 released', got %q", msg)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("timeout waiting for message for %s", sub.ID)
		}
	}
}
