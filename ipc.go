package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"
)

// RosterAgent is the stable, transport-safe view of an agent.
type RosterAgent struct {
	Addr string `json:"addr"`
	// ReachableAs restates Addr with the names live links know this host by, and
	// LocalOnly marks an Addr no live link can route. A peer must be given a
	// ReachableAs form; handing out a LocalOnly Addr produces a dead address.
	ReachableAs []string `json:"reachable_as,omitempty"`
	LocalOnly   bool     `json:"local_only,omitempty"`
	Name        string   `json:"name"`
	PaneID      string   `json:"pane_id"`
	Kind        string   `json:"kind"`
	Status      string   `json:"status"`
	CWD         string   `json:"cwd,omitempty"`
	Title       string   `json:"title,omitempty"`
	Host        string   `json:"host"`
	Self        bool     `json:"self,omitempty"`
}

func listenIPC(path string) (net.Listener, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, err
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("refusing to replace non-socket IPC path %q", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = ln.Close()
		return nil, err
	}
	return ln, nil
}

// readLine reads only through the newline, without a buffered reader that could
// consume the first byte of a link stream following its IPC upgrade request.
func readLine(r io.Reader, limit int) ([]byte, error) {
	buf := make([]byte, 0, min(limit, 4096))
	one := []byte{0}
	for {
		n, err := r.Read(one)
		if n > 0 {
			if one[0] == '\n' {
				return buf, nil
			}
			if len(buf) == limit {
				return nil, fmt.Errorf("IPC request exceeds %d bytes", limit)
			}
			buf = append(buf, one[0])
		}
		if err != nil {
			return nil, err
		}
	}
}

func writeJSONLine(w io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	for len(data) > 0 {
		n, err := w.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func serveIPC(ctx context.Context, ln net.Listener, d *daemon) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				continue
			}
			return err
		}
		go d.serveIPCConn(ctx, conn)
	}
}

func (d *daemon) serveIPCConn(ctx context.Context, conn net.Conn) {
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	line, err := readLine(conn, 1<<20)
	if err != nil {
		_ = conn.Close()
		return
	}
	var req map[string]any
	if err := json.Unmarshal(line, &req); err != nil {
		_ = writeJSONLine(conn, failure(codedErrorf("bad_request", "invalid JSON request")))
		_ = conn.Close()
		return
	}
	if op, _ := req["op"].(string); op == "link" {
		_ = conn.SetDeadline(time.Time{})
		if d.links != nil {
			d.links.ServeInbound(ctx, conn)
			return
		}
		_ = conn.Close()
		return
	}
	defer conn.Close()
	res := d.handle(ctx, req)
	_ = writeJSONLine(conn, res)
}

func daemonCall(req map[string]any) (map[string]any, error) {
	conn, err := net.DialTimeout("unix", socketPath(), 30*time.Second)
	if err != nil {
		// Name the path that failed. A stale TINCAN_SOCKET pointing at a deleted
		// directory reports identically to a stopped daemon otherwise, which sends
		// the reader off to restart a service that was never down.
		path := socketPath()
		if override := os.Getenv("TINCAN_SOCKET"); override != "" {
			return nil, fmt.Errorf("no tincan daemon at %s (TINCAN_SOCKET is set): %v", path, err)
		}
		return nil, fmt.Errorf("tincan daemon is not running on this host (start it: tincan daemon; socket %s)", path)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	if err := writeJSONLine(conn, req); err != nil {
		return nil, err
	}
	line, err := readLine(conn, 1<<20)
	if err != nil {
		return nil, err
	}
	var res map[string]any
	if err := json.Unmarshal(line, &res); err != nil {
		return nil, err
	}
	return res, nil
}

func printResult(res map[string]any, jsonOut bool, human func(map[string]any) string) error {
	ok, _ := res["ok"].(bool)
	if !ok {
		message, _ := res["error"].(string)
		if message == "" {
			message = "request failed"
		}
		return errors.New(message)
	}
	if jsonOut {
		return writeJSONLine(os.Stdout, res)
	}
	if text := human(res); text != "" {
		fmt.Println(text)
	}
	return nil
}
