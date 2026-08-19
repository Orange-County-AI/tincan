package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const herdrSocketIOTimeout = 30 * time.Second

type herdrAgent struct {
	Name           string `json:"name"`
	Kind           string `json:"agent"`
	Status         string `json:"agent_status"`
	PaneID         string `json:"pane_id"`
	CWD            string `json:"cwd"`
	Title          string `json:"terminal_title_stripped"`
	LaunchPending  bool   `json:"launch_pending"`
	StateChangeSeq uint64 `json:"state_change_seq"`
}

type herdrDriver interface {
	Ping(ctx context.Context) (version string, protocol int, err error)
	ListAgents(ctx context.Context) ([]herdrAgent, error)
	GetAgent(ctx context.Context, target string) (*herdrAgent, error)
	PaneScreen(ctx context.Context, paneID string) (string, error)
	SendKeys(ctx context.Context, paneID string, keys []string) error
	Prompt(ctx context.Context, target, text string) error
	Rename(ctx context.Context, target, name string) (*herdrAgent, error)
	Notify(ctx context.Context, title, body string) error
}

type herdrWireRequest struct {
	ID     string `json:"id"`
	Method string `json:"method"`
	Params any    `json:"params"`
}

type herdrWireResponse struct {
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *herdrAPIError  `json:"error"`
}

type herdrAPIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *herdrAPIError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.Code != "" {
		return e.Code
	}
	return "unknown herdr API error"
}

type herdrPingResult struct {
	Type     string `json:"type"`
	Version  string `json:"version"`
	Protocol int    `json:"protocol"`
}

type herdrSocket struct {
	path              string
	logf              func(string)
	acceptedProtocols map[int]struct{}

	stallPollAttempts int
	stallPollInterval time.Duration

	mu             sync.Mutex
	nextID         uint64
	loggedProtocol bool
}

var _ herdrDriver = (*herdrSocket)(nil)

func newHerdrSocket(path string, logf func(string)) *herdrSocket {
	if logf == nil {
		logf = func(message string) { log.Print(message) }
	}
	accepted := map[int]struct{}{19: {}, 20: {}}
	for _, part := range strings.Split(os.Getenv("TINCAN_HERDR_PROTOCOL_ALLOW"), ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if protocol, err := strconv.Atoi(part); err == nil && protocol >= 0 {
			accepted[protocol] = struct{}{}
		}
	}
	return &herdrSocket{
		path:              path,
		logf:              logf,
		acceptedProtocols: accepted,
		stallPollAttempts: 15,
		stallPollInterval: time.Second,
	}
}

func (d *herdrSocket) Ping(ctx context.Context) (string, int, error) {
	ctx, cancel := context.WithTimeout(ctx, herdrSocketIOTimeout)
	defer cancel()

	d.mu.Lock()
	defer d.mu.Unlock()
	return d.pingLocked(ctx)
}

func (d *herdrSocket) ListAgents(ctx context.Context) ([]herdrAgent, error) {
	raw, err := d.call(ctx, "agent.list", struct{}{})
	if err != nil {
		return nil, err
	}
	var result struct {
		Type   string       `json:"type"`
		Agents []herdrAgent `json:"agents"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("decode herdr agent.list response: %w", err)
	}
	if result.Type != "agent_list" || result.Agents == nil {
		return nil, fmt.Errorf("unexpected herdr agent.list response")
	}
	return result.Agents, nil
}

func (d *herdrSocket) GetAgent(ctx context.Context, target string) (*herdrAgent, error) {
	raw, err := d.call(ctx, "agent.get", map[string]any{"target": target})
	if err != nil {
		if codeOf(err) == "agent_not_found" {
			return nil, nil
		}
		return nil, err
	}
	return decodeHerdrAgent(raw, "agent.get")
}

// PaneScreen returns the human-visible pane, including a harness composer.
// lines must be omitted: herdr treats lines=0 as an empty screen.
func (d *herdrSocket) PaneScreen(ctx context.Context, paneID string) (string, error) {
	raw, err := d.call(ctx, "pane.read", map[string]any{
		"pane_id":    paneID,
		"source":     "visible",
		"format":     "text",
		"strip_ansi": true,
	})
	if err != nil {
		return "", fmt.Errorf("herdr pane read %s: %w", paneID, err)
	}
	var result struct {
		Type string `json:"type"`
		Read *struct {
			Text string `json:"text"`
		} `json:"read"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", fmt.Errorf("decode herdr pane read %s: %w", paneID, err)
	}
	if result.Type != "pane_read" || result.Read == nil {
		return "", fmt.Errorf("unexpected herdr pane read %s response", paneID)
	}
	return result.Read.Text, nil
}

func (d *herdrSocket) SendKeys(ctx context.Context, paneID string, keys []string) error {
	_, err := d.call(ctx, "pane.send_keys", map[string]any{"pane_id": paneID, "keys": keys})
	return err
}

func (d *herdrSocket) Prompt(ctx context.Context, target, text string) error {
	raw, err := d.call(ctx, "agent.prompt", map[string]any{"target": target, "text": text})
	if err != nil {
		if codeOf(err) == "agent_prompt_stalled" && d.flushPastedPrompt(ctx, target) {
			d.logf(fmt.Sprintf("flushed a stalled prompt to %s with one Enter", target))
			return nil
		}
		return err
	}
	_, err = decodeHerdrAgentOfType(raw, "agent.prompt", "agent_prompted")
	return err
}

// flushPastedPrompt recovers a prompt herdr reported stalled. A large
// bracketed paste can collapse into an OMP attachment chip, and the collapse
// absorbs herdr's submit key, so the envelope sits unsent in the composer.
// One Enter submits it, and is harmless if the composer turned out to be
// empty, which is why it is the whole recovery. Accepting the key proves
// nothing though: only a moved state_change_seq proves the agent took the
// prompt, so every failed precondition reports false and leaves the original
// stall as the reported cause.
func (d *herdrSocket) flushPastedPrompt(ctx context.Context, target string) bool {
	agent, err := d.GetAgent(ctx, target)
	if err != nil || agent == nil || agent.PaneID == "" {
		return false
	}
	before := agent.StateChangeSeq
	if err := d.SendKeys(ctx, agent.PaneID, []string{"Enter"}); err != nil {
		return false
	}
	for range d.stallPollAttempts {
		timer := time.NewTimer(d.stallPollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return false
		case <-timer.C:
		}
		now, err := d.GetAgent(ctx, target)
		if err != nil || now == nil {
			return false
		}
		if now.StateChangeSeq != before {
			return true
		}
	}
	return false
}

func (d *herdrSocket) Rename(ctx context.Context, target, name string) (*herdrAgent, error) {
	raw, err := d.call(ctx, "agent.rename", map[string]any{"target": target, "name": name})
	if err != nil {
		return nil, err
	}
	return decodeHerdrAgent(raw, "agent.rename")
}

func (d *herdrSocket) Notify(ctx context.Context, title, body string) error {
	_, err := d.call(ctx, "notification.show", map[string]any{
		"title": title,
		"body":  body,
		"sound": "none",
	})
	return err
}

func (d *herdrSocket) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, herdrSocketIOTimeout)
	defer cancel()

	d.mu.Lock()
	defer d.mu.Unlock()
	if _, _, err := d.pingLocked(ctx); err != nil {
		return nil, err
	}

	response, err := d.exchangeLocked(ctx, method, params)
	if err != nil {
		return nil, err
	}
	if response.Error != nil {
		return nil, codedErrorf(response.Error.Code, "herdr %s: %s", method, response.Error.Message)
	}
	return response.Result, nil
}

func (d *herdrSocket) pingLocked(ctx context.Context) (string, int, error) {
	response, err := d.exchangeLocked(ctx, "ping", struct{}{})
	if err != nil {
		return "", 0, err
	}
	if response.Error != nil {
		return "", 0, codedErrorf(response.Error.Code, "herdr ping: %s", response.Error.Message)
	}

	var pong herdrPingResult
	if err := json.Unmarshal(response.Result, &pong); err != nil {
		return "", 0, fmt.Errorf("decode herdr ping response: %w", err)
	}
	if pong.Type != "pong" {
		return "", 0, fmt.Errorf("unexpected herdr ping response type %q", pong.Type)
	}
	if _, ok := d.acceptedProtocols[pong.Protocol]; !ok {
		return "", 0, fmt.Errorf("herdr protocol %d is not accepted (accepted: %s)", pong.Protocol, d.acceptedProtocolString())
	}
	if !d.loggedProtocol {
		d.logf(fmt.Sprintf("herdr protocol %d accepted", pong.Protocol))
		d.loggedProtocol = true
	}
	return pong.Version, pong.Protocol, nil
}

func (d *herdrSocket) exchangeLocked(ctx context.Context, method string, params any) (herdrWireResponse, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", d.path)
	if err != nil {
		return herdrWireResponse{}, err
	}
	defer conn.Close()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(herdrSocketIOTimeout)
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return herdrWireResponse{}, err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer func() {
		if stop() {
			_ = conn.SetDeadline(time.Time{})
		}
	}()

	d.nextID++
	id := fmt.Sprintf("req_%d", d.nextID)
	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(herdrWireRequest{ID: id, Method: method, Params: params}); err != nil {
		return herdrWireResponse{}, err
	}

	var response herdrWireResponse
	decoder := json.NewDecoder(conn)
	if err := decoder.Decode(&response); err != nil {
		return herdrWireResponse{}, err
	}
	if err := ctx.Err(); err != nil {
		return herdrWireResponse{}, err
	}
	if response.ID != id {
		return herdrWireResponse{}, fmt.Errorf("herdr response id %q does not match request id %q", response.ID, id)
	}
	if response.Error == nil && len(response.Result) == 0 {
		return herdrWireResponse{}, fmt.Errorf("herdr response contains neither result nor error")
	}
	return response, nil
}

func (d *herdrSocket) acceptedProtocolString() string {
	protocols := make([]int, 0, len(d.acceptedProtocols))
	for protocol := range d.acceptedProtocols {
		protocols = append(protocols, protocol)
	}
	sort.Ints(protocols)
	parts := make([]string, len(protocols))
	for i, protocol := range protocols {
		parts[i] = strconv.Itoa(protocol)
	}
	return strings.Join(parts, ", ")
}

func decodeHerdrAgent(raw json.RawMessage, method string) (*herdrAgent, error) {
	return decodeHerdrAgentOfType(raw, method, "agent_info")
}

func decodeHerdrAgentOfType(raw json.RawMessage, method, resultType string) (*herdrAgent, error) {
	var result struct {
		Type  string      `json:"type"`
		Agent *herdrAgent `json:"agent"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("decode herdr %s response: %w", method, err)
	}
	if result.Type != resultType || result.Agent == nil {
		return nil, fmt.Errorf("unexpected herdr %s response", method)
	}
	return result.Agent, nil
}
