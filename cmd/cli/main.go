package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/KsanneK/packetq/pkg/client"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
		}
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 1 {
		printUsage()
		return errors.New("command required")
	}

	switch args[0] {
	case "pub":
		return runPub(args[1:])
	case "sub":
		return runSub(args[1:])
	default:
		printUsage()
		return fmt.Errorf("unknown command: %s", args[0])
	}
}

func runPub(args []string) error {
	cmd := flag.NewFlagSet("pub", flag.ContinueOnError)
	addr := cmd.String("addr", "localhost:9099", "broker address")
	if err := cmd.Parse(args); err != nil {
		return err
	}

	cmdArgs := cmd.Args()
	if len(cmdArgs) < 2 {
		printUsage()
		return errors.New("pub requires <topic> and <message>")
	}
	topic := cmdArgs[0]
	message := strings.Join(cmdArgs[1:], " ")

	fmt.Printf("publishing to %s on %s: %s\n", topic, *addr, message)
	c, err := client.Connect(*addr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = c.Close() }()

	if err := c.Publish(topic, message); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	return nil
}

func runSub(args []string) error {
	cmd := flag.NewFlagSet("sub", flag.ContinueOnError)
	addr := cmd.String("addr", "localhost:9099", "broker address")
	if err := cmd.Parse(args); err != nil {
		return err
	}

	topics := cmd.Args()
	if len(topics) < 1 {
		printUsage()
		return errors.New("sub requires <topic>")
	}

	fmt.Printf("subscribing to %v on %s\n", topics, *addr)
	c, err := client.Connect(*addr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = c.Close() }()

	merged := make(chan client.Message, 100)
	var wg sync.WaitGroup

	for _, topic := range topics {
		msgs, err := c.Subscribe(topic)
		if err != nil {
			return fmt.Errorf("subscribe: %w", err)
		}

		wg.Add(1)
		go func(ch <-chan client.Message) {
			defer wg.Done()
			for msg := range ch {
				merged <- msg
			}
		}(msgs)
	}

	go func() {
		wg.Wait()
		close(merged)
	}()

	for msg := range merged {
		fmt.Printf("[%s] %s\n", msg.Topic, msg.Body)
	}

	return nil
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  packetq pub [-addr <addr>] <topic> <message>")
	fmt.Println("  packetq sub [-addr <addr>] <topic>")
}
