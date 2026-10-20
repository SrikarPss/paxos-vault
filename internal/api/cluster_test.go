package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"quorumledger/internal/consensus"
	"quorumledger/internal/ledger"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Exercise the production JSON transport and HTTP handlers together, including
// peer authentication, without requiring OS permission to listen on TCP ports.
func TestJSONPeerTransportFailover(t *testing.T) {
	cfg := consensus.Config{
		ClusterID: "http-cluster", Token: "test-peer-token-123456789",
		Nodes:       map[string]string{"n1": "http://n1:8100", "n2": "http://n2:8100", "n3": "http://n3:8100"},
		HeartbeatMS: 20, RPCTimeoutMS: 100, ElectionMinMS: 400,
	}
	nodes := make(map[string]*consensus.Node)
	handlers := make(map[string]http.Handler)
	var mu sync.RWMutex
	isolated := make(map[string]bool)
	for _, id := range cfg.IDs() {
		from := id
		transport := NewTransport(cfg)
		transport.Client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if err := r.Context().Err(); err != nil {
				return nil, err
			}
			peer := r.URL.Hostname()
			mu.RLock()
			blocked := isolated[from] || isolated[peer]
			handler := handlers[peer]
			mu.RUnlock()
			if blocked || handler == nil {
				return nil, fmt.Errorf("peer link unavailable")
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, r)
			return response.Result(), nil
		})}
		n, err := consensus.New(cfg, id, t.TempDir(), transport)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = n.Close() })
		nodes[id] = n
		handlers[id] = NewServer(n, cfg).Handler()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	check := func(id, method, path, body string, want int) *httptest.ResponseRecorder {
		t.Helper()
		response := httptest.NewRecorder()
		handlers[id].ServeHTTP(response, httptest.NewRequest(method, path, strings.NewReader(body)).WithContext(ctx))
		if response.Code != want {
			t.Fatalf("%s %s %s: HTTP %d: %s", id, method, path, response.Code, response.Body.String())
		}
		return response
	}
	if err := nodes["n1"].Campaign(ctx); err != nil {
		t.Fatal(err)
	}
	check("n1", "POST", "/v1/transactions", `{"id":"open-a","kind":"open","account":"alice"}`, 200)
	check("n1", "POST", "/v1/transactions", `{"id":"open-b","kind":"open","account":"bob"}`, 200)
	check("n1", "POST", "/v1/transactions", `{"id":"fund","kind":"transfer","transfers":[{"from":"treasury","to":"alice","amount_cents":1000}]}`, 200)
	mu.Lock()
	isolated["n1"] = true
	mu.Unlock()
	check("n1", "POST", "/v1/transactions", `{"id":"uncertain","kind":"transfer","transfers":[{"from":"alice","to":"bob","amount_cents":1}]}`, 503)
	check("n1", "GET", "/v1/state", "", 409)
	if err := nodes["n2"].Campaign(ctx); err != nil {
		t.Fatal(err)
	}
	check("n2", "POST", "/v1/transactions", `{"id":"payment","kind":"transfer","transfers":[{"from":"alice","to":"bob","amount_cents":200}]}`, 200)
	response := check("n2", "GET", "/v1/state", "", 200)
	var state ledger.Snapshot
	if err := json.Unmarshal(response.Body.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	if state.Accounts["alice"] != 800 || state.Accounts["bob"] != 200 || state.Total != ledger.Supply {
		t.Fatal(state)
	}
	mu.Lock()
	isolated["n1"] = false
	mu.Unlock()
	if err := nodes["n2"].Pulse(ctx); err != nil {
		t.Fatal(err)
	}
	for id, n := range nodes {
		if got := n.Local(); !reflect.DeepEqual(got, state) {
			t.Fatalf("%s diverged: %+v vs %+v", id, got, state)
		}
	}
}
