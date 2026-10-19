package server

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"

	"github.com/KsanneK/packetq/internal/broker"
	"github.com/KsanneK/packetq/internal/protocol"
	"github.com/KsanneK/packetq/internal/version"
)

var ErrQuit = errors.New("client requested quit")

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
		return fmt.Errorf("server start failed: %w", err)
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

	slog.Info("server started", "addr", s.Addr(), "version", version.Version)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			slog.Warn("accept connection failed", "err", err)
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
			slog.Warn("connection close failed", "err", err)
		}
	}()
	done := make(chan struct{})
	defer close(done)

	slog.Info("new connection", "addr", conn.RemoteAddr())

	sub := s.broker.NewSubscriber(conn.RemoteAddr().String())
	defer sub.Close()
	go s.listenForMessages(sub, conn, done)

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		err := s.handleMessage(sub, conn, line)
		if err != nil {
			if errors.Is(err, ErrQuit) {
				break
			}
			slog.Warn("handle message failed", "err", err)
		}
	}

	if err := scanner.Err(); err != nil && !errors.Is(err, net.ErrClosed) {
		slog.Warn("read connection failed", "err", err)
	}
}

func (s *Server) handleMessage(sub *broker.Subscriber, conn net.Conn, line string) error {
	cmd, err := protocol.ParseCommand(line)
	if err != nil {
		return fmt.Errorf("error parsing command: %w", err)
	}
	switch cmd.Type {
	case protocol.CmdPing:
		slog.Debug("ping", "addr", conn.RemoteAddr())
		if _, err := conn.Write([]byte("PONG\n")); err != nil {
			return fmt.Errorf("error writing to connection %s: %v", conn.RemoteAddr(), err)
		}
	case protocol.CmdQuit:
		slog.Debug("quit", "addr", conn.RemoteAddr())
		return ErrQuit
	case protocol.CmdSubscribe:
		topic := cmd.Payload[0]
		sub.Subscribe(topic)
		if _, err := conn.Write([]byte("OK\n")); err != nil {
			return fmt.Errorf("error writing to connection %s: %v", conn.RemoteAddr(), err)
		}
		slog.Debug("subscribed to topic", "addr", conn.RemoteAddr(), "topic", topic)
	case protocol.CmdPublish:
		topic := cmd.Payload[0]
		message := cmd.Payload[1:]
		s.broker.Publish(topic, strings.Join(message, " "))
		if _, err := conn.Write([]byte("OK\n")); err != nil {
			return fmt.Errorf("error writing to connection %s: %v", conn.RemoteAddr(), err)
		}
		slog.Debug("message published", "addr", conn.RemoteAddr(), "topic", topic, "message", strings.Join(message, " "))
	}
	return nil
}

func (s *Server) listenForMessages(sub *broker.Subscriber, conn net.Conn, done chan struct{}) {
	for {
		select {
		case message := <-sub.Messages:
			if _, err := fmt.Fprintf(conn, "MSG %s %s\n", message.Topic, message.Body); err != nil {
				slog.Warn("connection write failed", "addr", conn.RemoteAddr(), "err", err)
			}
		case <-done:
			return
		}
	}
}
