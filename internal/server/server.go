package server

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/KsanneK/packetq/internal/broker"
	"github.com/KsanneK/packetq/internal/protocol"
)

type Server struct {
	addr     string
	broker   *broker.Broker
	listener net.Listener
}

func NewServer(addr string) *Server {
	return &Server{
		addr:   addr,
		broker: broker.NewBroker(),
	}
}

func (s *Server) Listen() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}
	s.listener = listener
	return nil
}

func (s *Server) Serve() error {
	defer func() {
		if s.listener != nil {
			_ = s.listener.Close()
		}
	}()

	fmt.Println("Server started on", s.Addr())

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) Start() error {
	if err := s.Listen(); err != nil {
		return err
	}
	return s.Serve()
}

func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

func (s *Server) Close() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("Error closing connection: %v", err)
		}
	}()
	done := make(chan struct{})
	defer close(done)

	log.Printf("New connection: %s", conn.RemoteAddr())

	sub := s.broker.NewSubscriber(conn.RemoteAddr().String())
	defer sub.Close()
	go s.listenForMessages(sub, conn, done)

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		if !s.handleMessage(sub, conn, line) {
			break
		}
	}

	if err := scanner.Err(); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Printf("Error reading from connection: %v", err)
	}
}

func (s *Server) handleMessage(sub *broker.Subscriber, conn net.Conn, line string) bool {
	cmd, err := protocol.ParseCommand(line)
	if err != nil {
		fmt.Println("Error parsing command:", err)
		return true
	}
	switch cmd.Type {
	case protocol.CmdPing:
		if _, err := conn.Write([]byte("PONG\n")); err != nil {
			log.Printf("Error writing to connection %s: %v", conn.RemoteAddr(), err)
		}
	case protocol.CmdQuit:
		log.Printf("Closing connection: %s", conn.RemoteAddr())
		return false
	case protocol.CmdSubscribe:
		topic := cmd.Payload[0]
		sub.Subscribe(topic)
		if _, err := conn.Write([]byte("OK\n")); err != nil {
			log.Printf("Error writing to connection %s: %v", conn.RemoteAddr(), err)
		} else {
			log.Printf("Subscriber %s subscribed to topic %s", sub.ID, topic)
		}
	case protocol.CmdPublish:
		topic := cmd.Payload[0]
		message := cmd.Payload[1:]
		s.broker.Publish(topic, strings.Join(message, " "))
		if _, err := conn.Write([]byte("OK\n")); err != nil {
			log.Printf("Error writing to connection %s: %v", conn.RemoteAddr(), err)
		} else {
			log.Printf("Subscriber %s published message %s to topic %s", sub.ID, message, topic)
		}
	}
	return true
}

func (s *Server) listenForMessages(sub *broker.Subscriber, conn net.Conn, done chan struct{}) {
	for {
		select {
		case message := <-sub.Messages:
			if _, err := fmt.Fprintf(conn, "MSG %s %s\n", message.Topic, message.Body); err != nil {
				log.Printf("Error writing to connection %s: %v", conn.RemoteAddr(), err)
			}
		case <-done:
			return
		}
	}
}
