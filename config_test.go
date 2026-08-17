package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTestConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfigDefaultsAndPeers(t *testing.T) {
	t.Setenv("TINCAN_CONFIG", writeTestConfig(t, `{"host":"titan","peers":[{"host":"ticket500","ssh":"ticket500"},{"host":"other","dial":["tincan","link"]}]}`))
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DeliverWhen != "now" || cfg.TTLSeconds != 86400 {
		t.Fatalf("defaults = deliver_when %q ttl %d", cfg.DeliverWhen, cfg.TTLSeconds)
	}
	if !cfg.Peers[0].Dialable() || !cfg.Peers[1].Dialable() {
		t.Fatal("configured ssh and dial peers must be dialable")
	}
	if got := cfg.Peers[0].RemoteBin(); got != "~/.local/bin/tincan" {
		t.Fatalf("default remote bin = %q", got)
	}
	if peer, ok := cfg.FindPeer("ticket500"); !ok || peer.SSH != "ticket500" {
		t.Fatalf("FindPeer = %#v, %v", peer, ok)
	}
	if cfg.TTL() != 24*time.Hour {
		t.Fatalf("TTL = %s", cfg.TTL())
	}
}

func TestLoadConfigMissingUsesDefaults(t *testing.T) {
	t.Setenv("TINCAN_CONFIG", filepath.Join(t.TempDir(), "missing.json"))
	t.Setenv("TINCAN_HOST", "mesh-host")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Host != "mesh-host" || cfg.DeliverWhen != "now" || cfg.TTLSeconds != 86400 {
		t.Fatalf("unexpected defaults: %#v", cfg)
	}
}

func TestLoadConfigRejectsBadDeliverWhen(t *testing.T) {
	t.Setenv("TINCAN_CONFIG", writeTestConfig(t, `{"host":"titan","deliver_when":"later"}`))
	if _, err := loadConfig(); err == nil {
		t.Fatal("loadConfig accepted invalid deliver_when")
	}
}

func TestHerdrSocketResolutionOrder(t *testing.T) {
	t.Run("config", func(t *testing.T) {
		t.Setenv("TINCAN_HERDR_SOCKET", "/env/tincan.sock")
		t.Setenv("HERDR_SOCKET_PATH", "/env/herdr.sock")
		got, err := herdrSocketPath(&Config{HerdrSocket: "/config/herdr.sock"})
		if err != nil || got != "/config/herdr.sock" {
			t.Fatalf("got %q, %v", got, err)
		}
	})
	t.Run("tincan env", func(t *testing.T) {
		t.Setenv("TINCAN_HERDR_SOCKET", "/env/tincan.sock")
		t.Setenv("HERDR_SOCKET_PATH", "/env/herdr.sock")
		got, err := herdrSocketPath(&Config{})
		if err != nil || got != "/env/tincan.sock" {
			t.Fatalf("got %q, %v", got, err)
		}
	})
	t.Run("herdr env", func(t *testing.T) {
		t.Setenv("TINCAN_HERDR_SOCKET", "")
		t.Setenv("HERDR_SOCKET_PATH", "/env/herdr.sock")
		got, err := herdrSocketPath(&Config{})
		if err != nil || got != "/env/herdr.sock" {
			t.Fatalf("got %q, %v", got, err)
		}
	})
	t.Run("xdg", func(t *testing.T) {
		t.Setenv("TINCAN_HERDR_SOCKET", "")
		t.Setenv("HERDR_SOCKET_PATH", "")
		t.Setenv("XDG_CONFIG_HOME", "/config")
		got, err := herdrSocketPath(&Config{})
		if err != nil || got != "/config/herdr/herdr.sock" {
			t.Fatalf("got %q, %v", got, err)
		}
	})
}
