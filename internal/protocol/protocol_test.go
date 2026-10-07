package protocol

import (
	"errors"
	"slices"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedCmd   *Command
		expectedError error
	}{
		{
			name:          "PING",
			input:         "PING",
			expectedCmd:   &Command{Type: CmdPing},
			expectedError: nil,
		},
		{
			name:          "QUIT",
			input:         "QUIT",
			expectedCmd:   &Command{Type: CmdQuit},
			expectedError: nil,
		},
		{
			name:          "SUB",
			input:         "SUB topic",
			expectedCmd:   &Command{Type: CmdSubscribe, Payload: []string{"topic"}},
			expectedError: nil,
		},
		{
			name:          "PUB",
			input:         "PUB topic message",
			expectedCmd:   &Command{Type: CmdPublish, Payload: []string{"topic", "message"}},
			expectedError: nil,
		},
		{
			name:          "unknown",
			input:         "UNKNOWN",
			expectedCmd:   nil,
			expectedError: errors.New("unknown command: UNKNOWN"),
		},
		{
			name:          "empty",
			input:         "",
			expectedCmd:   nil,
			expectedError: errors.New("empty command"),
		},
		{
			name:          "whitespace",
			input:         "   ",
			expectedCmd:   nil,
			expectedError: errors.New("empty command"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := ParseCommand(tt.input)
			if tt.expectedError != nil {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if err.Error() != tt.expectedError.Error() {
					t.Errorf("expected error %v, got %v", tt.expectedError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cmd.Type != tt.expectedCmd.Type {
				t.Errorf("expected command type %s, got %s", tt.expectedCmd.Type, cmd.Type)
			}
			if !slices.Equal(cmd.Payload, tt.expectedCmd.Payload) {
				t.Errorf("expected payload %v, got %v", tt.expectedCmd.Payload, cmd.Payload)
			}
		})
	}
}
