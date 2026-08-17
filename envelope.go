package main

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	envelopeSchema = "tincan/1"
	maxInlineRunes = 4000
	maxBodyBytes   = 65536

	replyNote     = "[reply if needed: tincan send %s \"…\" --reply-to %s]"
	truncatedNote = "[clipped; read: tincan read %s; reply if needed: tincan send %s \"…\" --reply-to %s]"
)

var attributeReplacer = strings.NewReplacer(
	"&", "&amp;",
	"<", "&lt;",
	">", "&gt;",
	"\"", "&quot;",
)

// attr escapes a value used in the envelope's XML-ish attributes.
func attr(s string) string { return attributeReplacer.Replace(s) }

// clipRunes clips by Unicode code points, preserving valid UTF-8 boundaries.
func clipRunes(s string, limit int) string {
	if limit < 0 || utf8.RuneCountInString(s) <= limit {
		return s
	}
	if limit == 0 {
		return ""
	}
	count := 0
	for byteIndex := range s {
		if count == limit {
			return s[:byteIndex]
		}
		count++
	}
	return s
}

// neutralizeBody prevents content from closing its surrounding delivery frame.
// Scanning resumes after each replacement so a body packed with closing tags
// stays linear rather than rescanning from the start.
func neutralizeBody(s string) string {
	const needle = "</tincan"
	const escaped = "&lt;/tincan"
	var out strings.Builder
	for offset := 0; ; {
		idx := indexFold(s[offset:], needle)
		if idx < 0 {
			if out.Len() == 0 {
				return s
			}
			out.WriteString(s[offset:])
			return out.String()
		}
		out.WriteString(s[offset : offset+idx])
		out.WriteString(escaped)
		offset += idx + len(needle)
	}
}

func indexFold(s, needle string) int {
	for i := range len(s) - len(needle) + 1 {
		if strings.EqualFold(s[i:i+len(needle)], needle) {
			return i
		}
	}
	return -1
}

func renderEnvelope(m *Msg) string {
	body := neutralizeBody(m.Body)
	truncated := utf8.RuneCountInString(body) > maxInlineRunes
	if truncated {
		body = clipRunes(body, maxInlineRunes)
	}

	var opening strings.Builder
	opening.WriteString(`<tincan from="`)
	opening.WriteString(attr(m.From))
	opening.WriteString(`" id="`)
	opening.WriteString(attr(m.ID))
	opening.WriteString(`" ts="`)
	opening.WriteString(attr(m.TS.UTC().Format(time.RFC3339)))
	opening.WriteString(`"`)
	if m.ReplyTo != "" {
		opening.WriteString(` reply_to="`)
		opening.WriteString(attr(m.ReplyTo))
		opening.WriteString(`"`)
	}
	if truncated {
		opening.WriteString(` truncated="1"`)
	}
	opening.WriteString(` schema="`)
	opening.WriteString(envelopeSchema)
	opening.WriteString(`">`)

	note := fmt.Sprintf(replyNote, m.From, m.ID)
	if truncated {
		note = fmt.Sprintf(truncatedNote, m.ID, m.From, m.ID)
	}
	return opening.String() + "\n" + body + "\n" + note + "\n</tincan>"
}
