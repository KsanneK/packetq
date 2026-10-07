package server

import (
	"bufio"
	"fmt"
	"log"
	"net"

	"github.com/KsanneK/packetq/internal/protocol"
)

type Server struct {
	addr string
}

func NewServer(addr string) *Server {
	return &Server{
		addr: addr,
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			log.Printf("Error closing listener: %v", err)
		}
	}()

	fmt.Println("Server started on", s.addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()
	log.Printf("New connection: %s", conn.RemoteAddr())

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		cmd, err := protocol.ParseCommand(line)
		if err != nil {
			fmt.Println("Error parsing command:", err)
			continue
		}
		switch cmd.Type {
		case protocol.CmdPing:
			if _, err := conn.Write([]byte("PONG\n")); err != nil {
				log.Printf("Error writing to connection %s: %v", conn.RemoteAddr(), err)
			}
		case protocol.CmdQuit:
			log.Printf("Closing connection: %s", conn.RemoteAddr())
			return
		case protocol.CmdSubscribe:
			fmt.Println("SUB", cmd.Payload)
		case protocol.CmdPublish:
			fmt.Println("PUB", cmd.Payload)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error reading from connection: %v", err)
	}
}
