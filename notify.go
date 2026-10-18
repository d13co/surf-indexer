package main

import (
	"log/slog"
	"net"
	"os"
)

// sdNotify sends state (READY=1, STOPPING=1) to systemd for a Type=notify unit. It does nothing
// outside one. Go maps a leading @ (an abstract socket) itself.
func sdNotify(state string) {
	path := os.Getenv("NOTIFY_SOCKET")
	if path == "" {
		return
	}
	c, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: path, Net: "unixgram"})
	if err == nil {
		_, err = c.Write([]byte(state))
		c.Close()
	}
	if err != nil {
		slog.Warn("sd_notify", "state", state, "err", err)
	}
}
