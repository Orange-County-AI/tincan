package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type herdrTestReply struct {
	result any
	err    *herdrAPIError
	drop   bool
}

type herdrTestServer struct {
	listener net.Listener
	done     chan struct{}
	wg       sync.WaitGroup
}

func startHerdrTestServer(t *testing.T, protocol int, handler func(herdrWireRequest) herdrTestReply) *herdrTestServer {
	t.Helper()
	path := filepath.Join(t.TempDir(), "herdr.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	server := &herdrTestServer{listener: listener, done: make(chan struct{})}
	server.wg.Add(1)
	go func() {
		defer server.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-server.done:
					return
				default:
					t.Errorf("fake herdr accept: %v", err)
					return
				}
			}
			server.wg.Add(1)
			go server.serve(t, conn, protocol, handler)
		}
	}()
	t.Cleanup(func() {
		close(server.done)
		_ = listener.Close()
		server.wg.Wait()
	})
	return server
}

func (s *herdrTestServer) serve(t *testing.T, conn net.Conn, protocol int, handler func(herdrWireRequest) herdrTestReply) {
	defer s.wg.Done()
	defer conn.Close()

	var request herdrWireRequest
	if err := json.NewDecoder(conn).Decode(&request); err != nil {
		if !errors.Is(err, net.ErrClosed) && !strings.Contains(err.Error(), "closed") && !strings.Contains(err.Error(), "EOF") {
			t.Errorf("fake herdr decode: %v", err)
		}
		return
	}
	encoder := json.NewEncoder(conn)
	if request.Method == "ping" {
		_ = encoder.Encode(herdrWireResponse{
			ID: request.ID,
			Result: herdrTestJSON(t, map[string]any{
				"type":     "pong",
				"version":  "0.8.0",
				"protocol": protocol,
			}),
		})
		return
	}

	reply := handler(request)
	if reply.drop {
		return
	}
	response := herdrWireResponse{ID: request.ID, Error: reply.err}
	if reply.err == nil {
		response.Result = herdrTestJSON(t, reply.result)
	}
	_ = encoder.Encode(response)
}

func herdrTestJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func newHerdrTestDriver(t *testing.T, protocol int, handler func(herdrWireRequest) herdrTestReply) *herdrSocket {
	t.Helper()
	server := startHerdrTestServer(t, protocol, handler)
	return newHerdrSocket(server.listener.Addr().String(), func(string) {})
}

func TestHerdrPing(t *testing.T) {
	tests := []struct {
		name     string
		protocol int
		allow    string
		wantErr  string
	}{
		{name: "decodes accepted default protocol", protocol: 19},
		{name: "rejects unaccepted protocol", protocol: 21, wantErr: "21"},
		{name: "allows configured protocol", protocol: 21, allow: "21"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TINCAN_HERDR_PROTOCOL_ALLOW", tt.allow)
			driver := newHerdrTestDriver(t, tt.protocol, func(herdrWireRequest) herdrTestReply {
				t.Errorf("ping should not call operation handler")
				return herdrTestReply{}
			})
			version, protocol, err := driver.Ping(context.Background())
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) || !strings.Contains(err.Error(), "19, 20") {
					t.Fatalf("Ping() error = %v, want accepted-version rejection", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if version != "0.8.0" || protocol != tt.protocol {
				t.Fatalf("Ping() = (%q, %d), want (%q, %d)", version, protocol, "0.8.0", tt.protocol)
			}
		})
	}
}

func TestHerdrListAgents(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "decodes null and named records"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver := newHerdrTestDriver(t, 19, func(request herdrWireRequest) herdrTestReply {
				if request.Method != "agent.list" {
					t.Errorf("method = %q, want agent.list", request.Method)
				}
				return herdrTestReply{result: map[string]any{
					"type": "agent_list",
					"agents": []any{
						map[string]any{
							"name":                    nil,
							"agent":                   "omp",
							"agent_status":            "idle",
							"cwd":                     "/home/stephan/projects/titan-k8s",
							"pane_id":                 "w5B:p1",
							"state_change_seq":        1688,
							"terminal_title_stripped": "π > Build and deploy revision d55a0a0",
						},
						map[string]any{
							"name":             "jessica",
							"agent":            "claude",
							"agent_status":     "working",
							"pane_id":          "w9:p2",
							"state_change_seq": 99,
							"launch_pending":   true,
						},
					},
				}}
			})
			agents, err := driver.ListAgents(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(agents) != 2 {
				t.Fatalf("len(agents) = %d, want 2", len(agents))
			}
			if agents[0].Name != "" || agents[0].Kind != "omp" || agents[0].PaneID != "w5B:p1" || agents[0].StateChangeSeq != 1688 || agents[0].LaunchPending {
				t.Fatalf("unnamed agent = %#v", agents[0])
			}
			if agents[1].Name != "jessica" || !agents[1].LaunchPending || agents[1].StateChangeSeq != 99 {
				t.Fatalf("named agent = %#v", agents[1])
			}
		})
	}
}

func TestHerdrGetAgentErrors(t *testing.T) {
	tests := []struct {
		name     string
		apiCode  string
		wantNil  bool
		wantCode string
	}{
		{name: "not found becomes nil", apiCode: "agent_not_found", wantNil: true},
		{name: "herdr error preserves code", apiCode: "agent_name_taken", wantCode: "agent_name_taken"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver := newHerdrTestDriver(t, 19, func(request herdrWireRequest) herdrTestReply {
				if request.Method != "agent.get" {
					t.Errorf("method = %q, want agent.get", request.Method)
				}
				return herdrTestReply{err: &herdrAPIError{Code: tt.apiCode, Message: "fixture failure"}}
			})
			agent, err := driver.GetAgent(context.Background(), "missing")
			if tt.wantNil {
				if err != nil || agent != nil {
					t.Fatalf("GetAgent() = (%#v, %v), want (nil, nil)", agent, err)
				}
				return
			}
			if err == nil || codeOf(err) != tt.wantCode {
				t.Fatalf("GetAgent() error = %v, code = %q, want %q", err, codeOf(err), tt.wantCode)
			}
		})
	}
}

func TestHerdrPrompt(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "sends no wait parameter"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver := newHerdrTestDriver(t, 19, func(request herdrWireRequest) herdrTestReply {
				if request.Method != "agent.prompt" {
					t.Errorf("method = %q, want agent.prompt", request.Method)
				}
				params, ok := request.Params.(map[string]any)
				if !ok {
					t.Errorf("params type = %T, want map", request.Params)
					params = map[string]any{}
				}
				if _, found := params["wait"]; found {
					t.Errorf("agent.prompt params unexpectedly include wait: %#v", params)
				}
				if params["target"] != "agent-a" || params["text"] != "hello" {
					t.Errorf("agent.prompt params = %#v", params)
				}
				return herdrTestReply{result: map[string]any{
					"type": "agent_prompted",
					"agent": map[string]any{
						"agent":        "omp",
						"pane_id":      "w1:p1",
						"agent_status": "idle",
					},
				}}
			})
			if err := driver.Prompt(context.Background(), "agent-a", "hello"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHerdrTransportErrorsAreUncoded(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "server closes after request"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver := newHerdrTestDriver(t, 19, func(request herdrWireRequest) herdrTestReply {
				if request.Method != "agent.prompt" {
					t.Errorf("method = %q, want agent.prompt", request.Method)
				}
				return herdrTestReply{drop: true}
			})
			err := driver.Prompt(context.Background(), "agent-a", "hello")
			if err == nil {
				t.Fatal("Prompt() error = nil, want transport failure")
			}
			if codeOf(err) != "" {
				t.Fatalf("Prompt() code = %q, want empty for transport error (%v)", codeOf(err), err)
			}
		})
	}
}
