package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"

	"github.com/KsanneK/packetq/internal/server"
	"github.com/KsanneK/packetq/internal/version"
)

func main() {
	host := flag.String("host", "localhost", "host address to listen on")
	port := flag.Int("port", 9099, "port to listen on")
	debug := flag.Bool("debug", false, "enable debug logging")
	showVersion := flag.Bool("version", false, "print version information and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version.Info())
		os.Exit(0)
	}

	var level slog.Level
	if *debug {
		level = slog.LevelDebug
	} else {
		level = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	addr := net.JoinHostPort(*host, strconv.Itoa(*port))
	srv := server.NewServer(addr)
	if err := srv.Start(); err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}
