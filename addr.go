package main

import (
	"regexp"
	"strings"
)

var (
	agentNameRe = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
	paneIDRe    = regexp.MustCompile(`^w[0-9A-Za-z]+:p[0-9]+$`)
	hostRe      = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{0,63}$`)
)

const reservedName = "tincan"

// parseAddr accepts a local agent address or agent@host. A local address has
// an empty returned host.
func parseAddr(s string) (agent, host string, err error) {
	if strings.Count(s, "@") > 1 {
		return "", "", codedErrorf("bad_address", "invalid address %q", s)
	}
	agent, host, hasHost := s, "", false
	if before, after, found := strings.Cut(s, "@"); found {
		agent, host, hasHost = before, after, true
	}
	if !agentNameRe.MatchString(agent) && !paneIDRe.MatchString(agent) {
		return "", "", codedErrorf("bad_address", "invalid agent address %q", s)
	}
	if agent == reservedName {
		return "", "", codedErrorf("reserved_name", "%q is reserved", reservedName)
	}
	if hasHost && !hostRe.MatchString(host) {
		return "", "", codedErrorf("bad_address", "invalid host in address %q", s)
	}
	return agent, host, nil
}

func isPaneID(s string) bool { return paneIDRe.MatchString(s) }

func queueKey(agent string) string {
	if isPaneID(agent) {
		return "p-" + strings.ReplaceAll(agent, ":", "_")
	}
	return "n-" + agent
}

func joinAddr(agent, host string) string {
	if host == "" {
		return agent
	}
	return agent + "@" + host
}
