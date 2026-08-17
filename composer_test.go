package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readScreenFixture(t *testing.T, name string) string {
	t.Helper()
	screen, err := os.ReadFile(filepath.Join("testdata", "screens", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return string(screen)
}

func TestDetectComposerOnLiveHarnessScreens(t *testing.T) {
	for _, test := range []struct {
		fixture string
		kind    string
		want    ComposerState
	}{
		{"omp-empty.txt", "omp", ComposerEmpty},
		{"omp-empty-working.txt", "omp", ComposerEmpty},
		{"omp-draft.txt", "omp", ComposerDraft},
		{"omp-draft-wrapped.txt", "omp", ComposerDraft},
		{"omp-draft-palette.txt", "omp", ComposerDraft},
		{"omp-draft-working.txt", "omp", ComposerDraft},
		{"claude-empty.txt", "claude", ComposerEmpty},
		{"claude-empty-working.txt", "claude", ComposerEmpty},
		{"claude-draft.txt", "claude", ComposerDraft},
		{"claude-draft-working.txt", "claude", ComposerDraft},
		{"shell.txt", "claude", ComposerUnknown},
		{"shell.txt", "omp", ComposerUnknown},
		{"omp-draft.txt", "codex", ComposerUnknown},
		{"claude-draft.txt", "", ComposerUnknown},
		{"claude-draft.txt", "omp", ComposerUnknown},
		{"omp-draft.txt", "claude", ComposerUnknown},
	} {
		t.Run(test.fixture+"/"+test.kind, func(t *testing.T) {
			if got := DetectComposer(test.kind, readScreenFixture(t, test.fixture)); got != test.want {
				t.Fatalf("DetectComposer(%q, %s) = %s, want %s", test.kind, test.fixture, got, test.want)
			}
		})
	}
}

func TestDetectComposerDiscriminatesComposerFrames(t *testing.T) {
	for _, test := range []struct {
		name   string
		kind   string
		screen string
		want   ComposerState
	}{
		{
			name: "omp plain box border is not a composer",
			kind: "omp",
			screen: strings.Join([]string{
				"╭────────────────┬──────────────╮",
				"│ Claude Opus 5  │ no LSP       │",
				"╰────────────────┴──────────────╯",
			}, "\n"),
			want: ComposerUnknown,
		},
		{
			name: "omp wrapped draft with blank footer holds",
			kind: "omp",
			screen: strings.Join([]string{
				"╭──   Opus 5 ───╮",
				"│  first line   │",
				"╰─              ─╯",
			}, "\n"),
			want: ComposerDraft,
		},
		{
			name: "claude transcript marker is not a composer",
			kind: "claude",
			screen: strings.Join([]string{
				"❯ an earlier prompt I already submitted",
				"",
				"  reply text",
			}, "\n"),
			want: ComposerUnknown,
		},
		{
			name: "claude continuation row holds",
			kind: "claude",
			screen: strings.Join([]string{
				"──────────────────────",
				"❯",
				"  wrapped second row",
				"──────────────────────",
			}, "\n"),
			want: ComposerDraft,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := DetectComposer(test.kind, test.screen); got != test.want {
				t.Fatalf("DetectComposer = %s, want %s", got, test.want)
			}
		})
	}
}
