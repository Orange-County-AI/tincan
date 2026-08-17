package main

import "testing"

func TestParseAddr(t *testing.T) {
	tests := []struct {
		name, input, agent, host, code string
	}{
		{"local name", "jessica", "jessica", "", ""},
		{"remote name", "jessica@titan", "jessica", "titan", ""},
		{"pane", "w61:p4@ticket500", "w61:p4", "ticket500", ""},
		{"reserved", "tincan", "", "", "reserved_name"},
		{"bad agent", "Jessica", "", "", "bad_address"},
		{"bad host", "jessica@Titan", "", "", "bad_address"},
		{"two separators", "jessica@a@b", "", "", "bad_address"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			agent, host, err := parseAddr(test.input)
			if codeOf(err) != test.code {
				t.Fatalf("parseAddr(%q) error code = %q, want %q (err %v)", test.input, codeOf(err), test.code, err)
			}
			if agent != test.agent || host != test.host {
				t.Fatalf("parseAddr(%q) = (%q, %q), want (%q, %q)", test.input, agent, host, test.agent, test.host)
			}
		})
	}
}

func TestQueueKeyPrefixesPreventCollision(t *testing.T) {
	if got, want := queueKey("w61_p4"), "n-w61_p4"; got != want {
		t.Fatalf("name queue key = %q, want %q", got, want)
	}
	if got, want := queueKey("w61:p4"), "p-w61_p4"; got != want {
		t.Fatalf("pane queue key = %q, want %q", got, want)
	}
}

func TestJoinAddr(t *testing.T) {
	if got := joinAddr("jessica", "titan"); got != "jessica@titan" {
		t.Fatalf("remote join = %q", got)
	}
	if got := joinAddr("jessica", ""); got != "jessica" {
		t.Fatalf("local join = %q", got)
	}
}
