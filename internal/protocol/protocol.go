package protocol

import (
	"errors"
	"fmt"
	"strings"
)

type CommandType string

const (
	CmdPing      CommandType = "PING"
	CmdPublish   CommandType = "PUB"
	CmdSubscribe CommandType = "SUB"
	CmdQuit      CommandType = "QUIT"
)

type Command struct {
	Type    CommandType
	Payload []string
}

func ParseCommand(line string) (*Command, error) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return nil, errors.New("empty command")
	}

	parts := strings.Fields(trimmed)
	cmdName := strings.ToUpper(parts[0])

	switch CommandType(cmdName) {
	case CmdPing:
		return &Command{Type: CmdPing}, nil
	case CmdQuit:
		return &Command{Type: CmdQuit}, nil
	case CmdSubscribe:
		if len(parts) != 2 {
			return nil, errors.New("SUB requires topic")
		}
		return &Command{Type: CmdSubscribe, Payload: parts[1:]}, nil
	case CmdPublish:
		if len(parts) < 3 {
			return nil, errors.New("PUB requires topic and message")
		}
		return &Command{Type: CmdPublish, Payload: parts[1:]}, nil
	default:
		return nil, fmt.Errorf("unknown command: %s", cmdName)
	}
}
