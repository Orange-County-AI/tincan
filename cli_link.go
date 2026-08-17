package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

func cmdPeers(args []string) error {
	fs := flag.NewFlagSet("peers", flag.ContinueOnError)
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: tincan peers [--json]")
	}
	res, err := daemonCall(map[string]any{"op": "peers"})
	if err != nil {
		return err
	}
	return printResult(res, *jsonOut, func(res map[string]any) string {
		peers, _ := res["peers"].([]any)
		if len(peers) == 0 {
			return "No peers configured or connected."
		}
		lines := make([]string, 0, len(peers))
		for _, raw := range peers {
			p, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			host, _ := p["host"].(string)
			dialable, _ := p["dialable"].(bool)
			link, _ := p["link"].(string)
			direction, _ := p["direction"].(string)
			queued := intFromJSON(p["queued"])
			line := fmt.Sprintf("%s  dialable=%t  link=%s  direction=%s  queued=%d", host, dialable, link, direction, queued)
			if detail, _ := p["last_error"].(string); detail != "" {
				line += "  " + detail
			}
			lines = append(lines, line)
		}
		return joinLines(lines)
	})
}

func intFromJSON(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	case int:
		return n
	default:
		return 0
	}
}

func joinLines(lines []string) string {
	if len(lines) == 0 {
		return "No peers configured or connected."
	}
	out := lines[0]
	for _, line := range lines[1:] {
		out += "\n" + line
	}
	return out
}

// cmdLink is the ssh-side acceptor. It deliberately knows nothing about
// addresses or messages: it starts/joins the local daemon then turns stdio
// into a byte-for-byte link stream.
func cmdLink(args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("usage: tincan link")
	}
	conn, err := dialLinkSocket()
	if err != nil && socketNeedsDaemon(err) {
		if err := startLinkDaemon(); err != nil {
			return fmt.Errorf("tincan link: %w", err)
		}
		conn, err = waitForLinkSocket(5 * time.Second)
	}
	if err != nil {
		return fmt.Errorf("tincan link: %w", err)
	}
	defer conn.Close()
	if err := writeLinkRequest(conn); err != nil {
		return fmt.Errorf("tincan link: %w", err)
	}

	copyDone := make(chan error, 1)
	go func() { _, err := io.Copy(os.Stdout, conn); copyDone <- err }()
	_, inErr := io.Copy(conn, os.Stdin)
	// EOF from either half is a clean end-of-link. Closing the socket wakes the
	// other copy even if ssh has not yet closed its stdout pipe.
	_ = conn.Close()
	outErr := <-copyDone
	if inErr != nil && !isClosedConnError(inErr) {
		return fmt.Errorf("tincan link: %w", inErr)
	}
	if outErr != nil && !isClosedConnError(outErr) {
		return fmt.Errorf("tincan link: %w", outErr)
	}
	return nil
}

func dialLinkSocket() (net.Conn, error) {
	return net.DialTimeout("unix", socketPath(), 500*time.Millisecond)
}

func socketNeedsDaemon(err error) bool {
	if err == nil {
		return false
	}
	if info, statErr := os.Lstat(socketPath()); statErr == nil && info.Mode()&os.ModeSocket == 0 {
		return false
	}
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ENOTSOCK)
}

func waitForLinkSocket(timeout time.Duration) (net.Conn, error) {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		conn, err := dialLinkSocket()
		if err == nil {
			return conn, nil
		}
		last = err
		time.Sleep(50 * time.Millisecond)
	}
	if last == nil {
		last = fmt.Errorf("timed out waiting for daemon socket")
	}
	return nil, last
}

func startLinkDaemon() error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dataDir(), 0700); err != nil {
		return err
	}
	log, err := os.OpenFile(filepath.Join(dataDir(), "daemon.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	cmd := exec.Command(self, "daemon")
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		return err
	}
	// The child inherited the fd. Do not Wait: this command is a short-lived
	// bridge and the daemon owns its own singleton lifecycle.
	_ = log.Close()
	return nil
}

func writeLinkRequest(w io.Writer) error {
	data, err := json.Marshal(map[string]string{"op": "link"})
	if err != nil {
		return err
	}
	_, err = w.Write(append(data, '\n'))
	return err
}

func isClosedConnError(err error) bool {
	return errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET)
}
