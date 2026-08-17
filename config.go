package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Peer struct {
	Host string   `json:"host"`
	SSH  string   `json:"ssh,omitempty"`
	Bin  string   `json:"bin,omitempty"`
	Dial []string `json:"dial,omitempty"`
}

func (p Peer) Dialable() bool { return p.SSH != "" || len(p.Dial) > 0 }

func (p Peer) RemoteBin() string {
	if p.Bin != "" {
		return p.Bin
	}
	return "~/.local/bin/tincan"
}

type Config struct {
	Host        string `json:"host"`
	HerdrSocket string `json:"herdr_socket,omitempty"`
	DeliverWhen string `json:"deliver_when,omitempty"`
	TTLSeconds  int    `json:"ttl_s,omitempty"`
	Peers       []Peer `json:"peers,omitempty"`
}

func defaultHost() string {
	if host := strings.ToLower(os.Getenv("TINCAN_HOST")); host != "" {
		return host
	}
	host, err := os.Hostname()
	if err == nil {
		host = strings.ToLower(strings.Split(host, ".")[0])
	}
	if host == "" {
		return "localhost"
	}
	return host
}

func configPath() (string, error) {
	if path := os.Getenv("TINCAN_CONFIG"); path != "" {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("locate config home: %w", err)
	}
	return filepath.Join(home, ".config", "tincan", "config.json"), nil
}

func loadConfig() (*Config, error) {
	path, err := configPath()
	if err != nil {
		return nil, err
	}

	cfg := &Config{}
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("read tincan config: %w", err)
		}
	} else if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse tincan config: %w", err)
	}

	if cfg.Host == "" {
		cfg.Host = defaultHost()
	}
	if !hostRe.MatchString(cfg.Host) {
		return nil, codedErrorf("bad_address", "invalid config host %q", cfg.Host)
	}
	if cfg.DeliverWhen == "" {
		cfg.DeliverWhen = "now"
	}
	if cfg.DeliverWhen != "now" && cfg.DeliverWhen != "settled" {
		return nil, fmt.Errorf("invalid deliver_when %q: must be now or settled", cfg.DeliverWhen)
	}
	if cfg.TTLSeconds == 0 {
		cfg.TTLSeconds = 86400
	}
	if cfg.TTLSeconds < 0 {
		return nil, fmt.Errorf("invalid ttl_s %d", cfg.TTLSeconds)
	}

	seen := make(map[string]struct{}, len(cfg.Peers))
	for i := range cfg.Peers {
		peer := &cfg.Peers[i]
		if !hostRe.MatchString(peer.Host) {
			return nil, codedErrorf("bad_address", "invalid peer host %q", peer.Host)
		}
		if peer.Host == cfg.Host {
			return nil, fmt.Errorf("peer %q is this host", peer.Host)
		}
		if _, exists := seen[peer.Host]; exists {
			return nil, fmt.Errorf("duplicate peer %q", peer.Host)
		}
		seen[peer.Host] = struct{}{}
	}
	return cfg, nil
}

func (c *Config) FindPeer(host string) (Peer, bool) {
	for _, peer := range c.Peers {
		if peer.Host == host {
			return peer, true
		}
	}
	return Peer{}, false
}

func (c *Config) TTL() time.Duration { return time.Duration(c.TTLSeconds) * time.Second }

func dataDir() string {
	if root := os.Getenv("TINCAN_DATA_DIR"); root != "" {
		return root
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", ".tincan")
	}
	return filepath.Join(home, ".local", "share", "tincan")
}

func socketPath() string {
	if path := os.Getenv("TINCAN_SOCKET"); path != "" {
		return path
	}
	return filepath.Join(dataDir(), "tincan.sock")
}

func herdrSocketPath(c *Config) (string, error) {
	if c != nil && c.HerdrSocket != "" {
		return c.HerdrSocket, nil
	}
	if path := os.Getenv("TINCAN_HERDR_SOCKET"); path != "" {
		return path, nil
	}
	if path := os.Getenv("HERDR_SOCKET_PATH"); path != "" {
		return path, nil
	}
	base := os.Getenv("XDG_CONFIG_HOME")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("locate herdr config home: %w", err)
		}
		base = filepath.Join(home, ".config")
	}
	return filepath.Join(base, "herdr", "herdr.sock"), nil
}

func pollInterval() time.Duration {
	seconds, err := strconv.Atoi(os.Getenv("TINCAN_POLL_SECONDS"))
	if err != nil || seconds <= 0 {
		return 2 * time.Second
	}
	return time.Duration(seconds) * time.Second
}
