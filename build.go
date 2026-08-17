package main

import (
	"runtime/debug"
	"strings"
)

// A semver constant alone cannot answer "what is running". This fleet has three
// courier binaries all self-reporting 0.1.0 that are demonstrably different
// builds — one of them wrote a newer schema than its own version claims — and a
// tincan whose MCP server reported 0.2.0 while the binary was 0.5.0. Until now
// md5 was the only reliable handle on a deployed copy.
//
// So the reported version carries the commit it was built from. This reads the
// stamp the Go toolchain already embeds from the VCS tree, which means a plain
// `go build` gets it right with no ldflags to remember and no build task to keep
// in sync.
func buildRevision() (revision string, modified bool) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", false
	}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			modified = setting.Value == "true"
		}
	}
	return revision, modified
}

// buildVersion is what every surface reports: the CLI, the MCP handshake, and
// any probe that asks a running process what it is.
func buildVersion() string {
	revision, modified := buildRevision()
	if revision == "" {
		// Built outside a VCS tree (a vendored or `go run` build). Say so rather
		// than implying a provenance this binary does not have.
		return version + " (unknown revision)"
	}
	if len(revision) > 12 {
		revision = revision[:12]
	}
	var reported strings.Builder
	reported.WriteString(version)
	reported.WriteString(" (")
	reported.WriteString(revision)
	if modified {
		reported.WriteString("-dirty")
	}
	reported.WriteByte(')')
	return reported.String()
}
