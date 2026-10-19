package client

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

var (
	ErrNotConnected = errors.New("client is not connected")
)

type CommandType string

const (
	CmdPing      CommandType = "PING"
	CmdPublish   CommandType = "PUB"
	CmdSubscribe CommandType = "SUB"
	CmdMessage   CommandType = "MSG"
	CmdQuit      CommandType = "QUIT"

	RespPong CommandType = "PONG"
	RespOK   CommandType = "OK"
	RespMsg  CommandType = "MSG"
)

type Message struct {
	Topic string
	Body  string
}

type Client struct {
	conn    net.Conn
	scanner *bufio.Scanner
	subs    map[string]chan Message
	cmdResp chan CommandType
	mu      sync.Mutex
}

func Connect(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &Client{
		conn:    conn,
		scanner: bufio.NewScanner(conn),
		subs:    make(map[string]chan Message),
		cmdResp: make(chan CommandType, 10),
		mu:      sync.Mutex{},
	}
	go c.listenLoop()
	return c, nil
}

func (c *Client) Ping() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.conn.Write([]byte("PING\n")); err != nil {
		log.Println("Error pinging server:", err)
		return err
	}
	resp := <-c.cmdResp
	if resp != RespPong {
		return fmt.Errorf("unexpected response: %s", resp)
	}
	return nil
}

func (c *Client) Publish(topic, message string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.conn.Write([]byte("PUB " + topic + " " + message + "\n")); err != nil {
		log.Println("Error publishing message:", err)
		return err
	}
	resp := <-c.cmdResp
	if resp != RespOK {
		return fmt.Errorf("unexpected response: %s", resp)
	}
	return nil
}

func (c *Client) Subscribe(topic string) (<-chan Message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, err := c.conn.Write([]byte("SUB " + topic + "\n")); err != nil {
		log.Println("Error subscribing to topic:", err)
		return nil, err
	}
	resp := <-c.cmdResp
	if resp != RespOK {
		return nil, fmt.Errorf("unexpected response: %s", resp)
	}
	msgChan := make(chan Message, 100)
	c.subs[topic] = msgChan
	return msgChan, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, ch := range c.subs {
		close(ch)
	}
	return c.conn.Close()
}

func (c *Client) listenLoop() {
	for c.scanner.Scan() {
		line := c.scanner.Text()
		if err := c.handleMessage(line); err != nil {
			fmt.Println("Error parsing message:", err)
			continue
		}
	}
}

func (c *Client) handleMessage(line string) error {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return errors.New("empty response")
	}

	parts := strings.Fields(trimmed)
	cmdName := strings.ToUpper(parts[0])

	switch CommandType(cmdName) {
	case RespOK:
		c.cmdResp <- RespOK
	case RespPong:
		c.cmdResp <- RespPong
	case RespMsg:
		if len(parts) < 3 {
			return fmt.Errorf("expected MSG <TOPIC> <MESSAGE>, got %s", trimmed)
		}
		topic := parts[1]
		message := strings.Join(parts[2:], " ")

		msgChan, ok := c.subs[topic]
		if !ok {
			return fmt.Errorf("no subscription found for topic: %s", topic)
		}
		msgChan <- Message{
			Topic: topic,
			Body:  message,
		}
	default:
		return fmt.Errorf("unexpected response: %s", trimmed)
	}
	return nil
}
