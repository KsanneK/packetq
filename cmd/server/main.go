package main

import (
	"flag"
	"log"
	"net"
	"strconv"

	"github.com/KsanneK/packetq/internal/server"
)

func main() {
	host := flag.String("host", "localhost", "host address to listen on")
	port := flag.Int("port", 9099, "port to listen on")
	flag.Parse()

	addr := net.JoinHostPort(*host, strconv.Itoa(*port))
	srv := server.NewServer(addr)
	if err := srv.Start(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
