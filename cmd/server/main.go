package main

import (
	"fmt"
	"log"
	"net"
)

func main() {
	listener, err := net.Listen("tcp", ":9099")
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			log.Printf("Error closing listener: %v", err)
		}
	}()

	fmt.Println("Server started on :9099")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()

	log.Printf("New connection: %s", conn.RemoteAddr())
}
