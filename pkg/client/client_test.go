package client_test

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/KsanneK/packetq/internal/server"
	"github.com/KsanneK/packetq/pkg/client"
)

func startTestServer(t *testing.T) (*server.Server, func()) {
	t.Helper()
	srv := server.NewServer("127.0.0.1:0")
	if err := srv.Listen(); err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve()
	}()

	cleanup := func() {
		_ = srv.Close()
		select {
		case err := <-errCh:
			if err != nil && !errors.Is(err, net.ErrClosed) {
				t.Logf("server shutdown error: %v", err)
			}
		case <-time.After(500 * time.Millisecond):
		}
	}
	return srv, cleanup
}

func TestClient_ConnectAndClose(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	c, err := client.Connect(srv.Addr())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("failed to close client: %v", err)
	}
}

func TestClient_Ping(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	c, err := client.Connect(srv.Addr())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer func() { _ = c.Close() }()

	if err := c.Ping(); err != nil {
		t.Fatalf("failed to ping server: %v", err)
	}
}

func TestClient_Publish(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	c, err := client.Connect(srv.Addr())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer func() { _ = c.Close() }()

	if err := c.Publish("tech", "Go 1.27 released"); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}
}

func TestClient_SubscribeAndPublish(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	sub, err := client.Connect(srv.Addr())
	if err != nil {
		t.Fatalf("failed to connect subscriber: %v", err)
	}
	defer func() { _ = sub.Close() }()

	msgs, err := sub.Subscribe("news")
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	pub, err := client.Connect(srv.Addr())
	if err != nil {
		t.Fatalf("failed to connect publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	expectedBody := "Breaking news from packetq"
	if err := pub.Publish("news", expectedBody); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	select {
	case msg, ok := <-msgs:
		if !ok {
			t.Fatal("message channel closed unexpectedly")
		}
		if msg.Topic != "news" {
			t.Errorf("expected topic 'news', got %q", msg.Topic)
		}
		if msg.Body != expectedBody {
			t.Errorf("expected body %q, got %q", expectedBody, msg.Body)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestClient_MultipleSubscribers(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	sub1, err := client.Connect(srv.Addr())
	if err != nil {
		t.Fatalf("failed to connect sub1: %v", err)
	}
	defer func() { _ = sub1.Close() }()

	msgs1, err := sub1.Subscribe("sports")
	if err != nil {
		t.Fatalf("failed to subscribe sub1: %v", err)
	}

	sub2, err := client.Connect(srv.Addr())
	if err != nil {
		t.Fatalf("failed to connect sub2: %v", err)
	}
	defer func() { _ = sub2.Close() }()

	msgs2, err := sub2.Subscribe("sports")
	if err != nil {
		t.Fatalf("failed to subscribe sub2: %v", err)
	}

	pub, err := client.Connect(srv.Addr())
	if err != nil {
		t.Fatalf("failed to connect pub: %v", err)
	}
	defer func() { _ = pub.Close() }()

	expected := "Match ended 2-1"
	if err := pub.Publish("sports", expected); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	for i, ch := range []<-chan client.Message{msgs1, msgs2} {
		select {
		case msg, ok := <-ch:
			if !ok {
				t.Fatalf("sub%d channel closed unexpectedly", i+1)
			}
			if msg.Topic != "sports" || msg.Body != expected {
				t.Fatalf("sub%d: unexpected message %+v", i+1, msg)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("sub%d: timed out waiting for message", i+1)
		}
	}
}

func TestClient_CloseClosesMessagesChannel(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	sub, err := client.Connect(srv.Addr())
	if err != nil {
		t.Fatalf("failed to connect subscriber: %v", err)
	}

	msgs, err := sub.Subscribe("updates")
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	if err := sub.Close(); err != nil {
		t.Fatalf("failed to close subscriber: %v", err)
	}

	select {
	case _, ok := <-msgs:
		if ok {
			t.Fatal("expected channel to be closed, but received a message")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for message channel to close")
	}
}
