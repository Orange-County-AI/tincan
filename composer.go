package main

import "strings"

// ComposerState describes only positively recognizable harness composers.
// Unknown is deliberately fail-open: a missing or unfamiliar composer must not
// starve a durable message queue.
type ComposerState int

const (
	ComposerUnknown ComposerState = iota
	ComposerEmpty
	ComposerDraft
)

func (state ComposerState) String() string {
	switch state {
	case ComposerEmpty:
		return "empty"
	case ComposerDraft:
		return "draft"
	default:
		return "unknown"
	}
}

// DetectComposer reads a harness composer from its rendered pane screen. Herdr
// exposes the screen, not the input buffer, so this holds delivery only when a
// supported harness's non-empty composer is positively recognized.
func DetectComposer(agentKind, screen string) ComposerState {
	if strings.TrimSpace(screen) == "" {
		return ComposerUnknown
	}
	switch strings.ToLower(strings.TrimSpace(agentKind)) {
	case "omp", "pi":
		return detectOmpComposer(screen)
	case "claude":
		return detectClaudeComposer(screen)
	default:
		return ComposerUnknown
	}
}

func detectOmpComposer(screen string) ComposerState {
	lines := strings.Split(screen, "\n")
	for index := len(lines) - 1; index >= 0; index-- {
		interior, ok := ompComposerFooter(lines[index])
		if !ok {
			continue
		}
		if strings.TrimSpace(interior) != "" {
			return ComposerDraft
		}
		// A trailing newline leaves the footer empty; inspect the wrapped body.
		for above := index - 1; above >= 0; above-- {
			body, ok := ompComposerBody(lines[above])
			if !ok {
				break
			}
			if strings.TrimSpace(body) != "" {
				return ComposerDraft
			}
		}
		return ComposerEmpty
	}
	return ComposerUnknown
}

func ompComposerFooter(line string) (string, bool) {
	trimmed := strings.TrimRight(line, " \t\r")
	interior, ok := strings.CutPrefix(trimmed, "╰─")
	if !ok {
		return "", false
	}
	interior, ok = strings.CutSuffix(interior, "─╯")
	if !ok || !strings.HasPrefix(interior, " ") {
		return "", false
	}
	return interior, true
}

func ompComposerBody(line string) (string, bool) {
	trimmed := strings.TrimRight(line, " \t\r")
	interior, ok := strings.CutPrefix(trimmed, "│")
	if !ok {
		return "", false
	}
	return strings.CutSuffix(interior, "│")
}

func detectClaudeComposer(screen string) ComposerState {
	lines := strings.Split(screen, "\n")
	for index := len(lines) - 1; index >= 1; index-- {
		text, ok := strings.CutPrefix(strings.TrimSpace(lines[index]), "❯")
		if !ok || !claudeComposerRule(lines[index-1]) {
			continue
		}
		if strings.TrimSpace(text) != "" {
			return ComposerDraft
		}
		for below := index + 1; below < len(lines); below++ {
			row := strings.TrimSpace(lines[below])
			if claudeComposerRule(row) {
				break
			}
			if row != "" {
				return ComposerDraft
			}
		}
		return ComposerEmpty
	}
	return ComposerUnknown
}

func claudeComposerRule(line string) bool {
	trimmed := strings.TrimSpace(line)
	if len([]rune(trimmed)) < 8 {
		return false
	}
	for _, glyph := range trimmed {
		if glyph != '─' {
			return false
		}
	}
	return true
}
