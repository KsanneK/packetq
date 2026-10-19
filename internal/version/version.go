package version

import "fmt"

var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

func Info() string {
	return fmt.Sprintf("packetq %s (commit: %s, built at: %s)", Version, Commit, Date)
}
