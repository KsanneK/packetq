# packetq 📦

> A lightweight, concurrent TCP message broker written from scratch in Go.

*Status: Core in-memory broker, TCP server, and Go client library implemented and fully tested.*

---

## What is this?
`packetq` is an experimental message broker inspired by tools like RabbitMQ and Redis Pub/Sub, built to explore low-level network programming, Go concurrency primitives, and custom storage mechanics.

## Architecture & Features

- **Event Loop & FIFO Ordering**: Core broker uses an internal channel-driven event loop (`publishChan`) ensuring strict message ordering without blocking producers.
- **Thread-safe Pub/Sub**: Granular locking with `sync.RWMutex` to minimize contention; subscribers only receive messages for their subscribed topics.
- **Slow Consumer Protection**: Non-blocking delivery (`select`/`default`) drops or warns when subscriber buffers fill up, preventing slow clients from freezing the broker.
- **Concurrent TCP Server**: Handles multiple network clients concurrently with clean socket teardown (`done chan struct{}`) and graceful listener shutdown (`Listen`/`Serve`/`Close`).
- **Idiomatic Go Client (`pkg/client`)**: Features background multiplexing to cleanly separate synchronous command responses (`OK`, `PONG`) from asynchronous incoming message streams (`<-chan Message`).

## Protocol Specification

`packetq` uses a simple text-based protocol over TCP:

| Command / Frame | Direction | Description | Response / Behavior |
| :--- | :--- | :--- | :--- |
| `PING` | Client &rarr; Server | Healthcheck / heartbeat | `PONG` |
| `PUB <topic> <message>` | Client &rarr; Server | Publishes a message to a topic | `OK` |
| `SUB <topic>` | Client &rarr; Server | Subscribes client to a topic | `OK` |
| `MSG <topic> <message>` | Server &rarr; Client | Asynchronous message push | Streamed to subscriber |
| `QUIT` | Client &rarr; Server | Gracefully disconnects | Closes TCP connection |

## Quick Start

### 1. Run the Server
```bash
go run ./cmd/server/main.go --host localhost --port 9099
```

### 2. Connect via Netcat
```bash
# Terminal 1 (Subscriber)
nc localhost 9099
SUB news
# Response: OK

# Terminal 2 (Publisher)
nc localhost 9099
PUB news Hello World!
# Response: OK

# Terminal 1 receives:
# MSG news Hello World!
```

### 3. CLI Tool (`packetq`)

You can build and use the dedicated CLI tool to publish and subscribe to messages:

#### Building:
```bash
# Build the binary in the current directory
go build -o packetq ./cmd/cli

# Optionally install to $GOPATH/bin (available globally)
go install ./cmd/cli
```

#### Publishing (`pub`):
```bash
./packetq pub <topic> <message>

# Example using default address (localhost:9099):
./packetq pub news "Go 1.27 released!"

# Specify custom broker address:
./packetq pub -addr localhost:9099 alerts "High CPU usage"
```

#### Subscribing (`sub`):
The CLI supports subscribing to single or multiple topics simultaneously using the Fan-In pattern:
```bash
./packetq sub <topic1> [topic2...]

# Subscribe to a single topic:
./packetq sub news

# Subscribe to multiple topics at once:
./packetq sub news alerts metrics
```

### 4. Use the Go Client Library (`pkg/client`)
```go
package main

import (
	"fmt"
	"log"

	"github.com/KsanneK/packetq/pkg/client"
)

func main() {
	// Connect to broker
	c, err := client.Connect("localhost:9099")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Subscribe to a topic (returns a read-only channel)
	messages, err := c.Subscribe("news")
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for msg := range messages {
			fmt.Printf("[%s] %s\n", msg.Topic, msg.Body)
		}
	}()

	// Publish message
	if err := c.Publish("news", "Go 1.27 released!"); err != nil {
		log.Fatal(err)
	}
}
```

## Running Tests

Run the full test suite with race detector enabled:
```bash
go test -race -v -count=1 ./...
```

## Current Roadmap
- [x] Core TCP protocol & packet parsing
- [x] Thread-safe in-memory broker, topics & pub/sub routing
- [x] Official Go client library (`pkg/client`) with async stream multiplexing
- [x] Interactive terminal CLI (`cmd/cli`)
- [ ] Client authentication & access control (`AUTH <user> <pass>` / token)
- [ ] Persistent disk storage (WAL / append-only log)
- [ ] Consumer groups & message acknowledgments (ACK/NACK)