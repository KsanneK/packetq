package server_test

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/KsanneK/packetq/internal/server"
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

func dialClient(t *testing.T, addr string) (net.Conn, *bufio.Scanner) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	scanner := bufio.NewScanner(conn)
	return conn, scanner
}

func TestServer_Ping(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	conn, scanner := dialClient(t, srv.Addr())
	defer func() { _ = conn.Close() }()

	if _, err := fmt.Fprintf(conn, "PING\n"); err != nil {
		t.Fatalf("failed to write PING: %v", err)
	}

	if !scanner.Scan() {
		t.Fatalf("expected response from server, got error: %v", scanner.Err())
	}

	if got := scanner.Text(); got != "PONG" {
		t.Fatalf("expected PONG, got %q", got)
	}
}

func TestServer_PubSub(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	// Subscriber
	subConn, subScanner := dialClient(t, srv.Addr())
	defer func() { _ = subConn.Close() }()

	if _, err := fmt.Fprintf(subConn, "SUB news\n"); err != nil {
		t.Fatalf("failed to send SUB: %v", err)
	}
	if !subScanner.Scan() {
		t.Fatalf("expected OK response for SUB, got error: %v", subScanner.Err())
	}
	if got := subScanner.Text(); got != "OK" {
		t.Fatalf("expected OK, got %q", got)
	}

	// Publisher
	pubConn, pubScanner := dialClient(t, srv.Addr())
	defer func() { _ = pubConn.Close() }()

	if _, err := fmt.Fprintf(pubConn, "PUB news Hello from packetq!\n"); err != nil {
		t.Fatalf("failed to send PUB: %v", err)
	}
	if !pubScanner.Scan() {
		t.Fatalf("expected OK response for PUB, got error: %v", pubScanner.Err())
	}
	if got := pubScanner.Text(); got != "OK" {
		t.Fatalf("expected OK, got %q", got)
	}

	// Verification of message receipt by subscriber
	if !subScanner.Scan() {
		t.Fatalf("expected MSG from server, got error: %v", subScanner.Err())
	}
	expectedMsg := "MSG news Hello from packetq!"
	if got := subScanner.Text(); got != expectedMsg {
		t.Fatalf("expected %q, got %q", expectedMsg, got)
	}
}

func TestServer_MultipleSubscribers(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	// Two subscriber connections to the same topic
	sub1Conn, sub1Scanner := dialClient(t, srv.Addr())
	defer func() { _ = sub1Conn.Close() }()
	if _, err := fmt.Fprintf(sub1Conn, "SUB tech\n"); err != nil {
		t.Fatalf("failed to send SUB: %v", err)
	}
	sub1Scanner.Scan() // read "OK"

	sub2Conn, sub2Scanner := dialClient(t, srv.Addr())
	defer func() { _ = sub2Conn.Close() }()
	if _, err := fmt.Fprintf(sub2Conn, "SUB tech\n"); err != nil {
		t.Fatalf("failed to send SUB: %v", err)
	}
	sub2Scanner.Scan() // read "OK"

	// Message publishing
	pubConn, pubScanner := dialClient(t, srv.Addr())
	defer func() { _ = pubConn.Close() }()
	if _, err := fmt.Fprintf(pubConn, "PUB tech Go 1.27 is awesome\n"); err != nil {
		t.Fatalf("failed to send PUB: %v", err)
	}
	pubScanner.Scan() // read "OK"

	expected := "MSG tech Go 1.27 is awesome"

	// Verification for both recipients
	for i, scanner := range []*bufio.Scanner{sub1Scanner, sub2Scanner} {
		if !scanner.Scan() {
			t.Fatalf("subscriber %d: expected message, got err: %v", i+1, scanner.Err())
		}
		if got := scanner.Text(); got != expected {
			t.Fatalf("subscriber %d: expected %q, got %q", i+1, expected, got)
		}
	}
}

func TestServer_Quit(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	conn, scanner := dialClient(t, srv.Addr())
	defer func() { _ = conn.Close() }()

	if _, err := fmt.Fprintf(conn, "QUIT\n"); err != nil {
		t.Fatalf("failed to write QUIT: %v", err)
	}

	// After the QUIT command, the server should close the connection (EOF)
	if scanner.Scan() {
		t.Fatalf("expected connection to close, but received: %q", scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
}
