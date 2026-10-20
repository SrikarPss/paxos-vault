package api

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"quorumledger/internal/consensus"
)

func TestHTTPContracts(t *testing.T) {
	cfg := consensus.Config{ClusterID: "http-test", Token: "a-long-test-token-123", Nodes: map[string]string{"n1": "http://127.0.0.1:9999"}, HeartbeatMS: 20, RPCTimeoutMS: 20, ElectionMinMS: 200}
	n, err := consensus.New(cfg, "n1", t.TempDir(), NewTransport(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	handler := NewServer(n, cfg).Handler()
	check := func(method, path, body string, want int) string {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code != want {
			t.Fatalf("%s %s: got %d want %d: %s", method, path, response.Code, want, response.Body.String())
		}
		return response.Body.String()
	}
	check("GET", "/v1/state", "", 409)
	check("POST", "/internal/prepare", `{}`, 401)
	check("POST", "/v1/transactions", `{"kind":"noop"}`, 400)
	check("POST", "/v1/transactions", `{"id":"x","kind":"open","account":"alice","unknown":true}`, 400)
	check("POST", "/v1/transactions", `{"id":"x","kind":"open","account":"alice"} {}`, 400)
	if err := n.Campaign(context.Background()); err != nil {
		t.Fatal(err)
	}
	check("POST", "/v1/transactions", `{"id":"x","kind":"open","account":"alice"}`, 200)
	check("POST", "/v1/transactions", `{"id":"x","kind":"open","account":"bob"}`, 422)
	check("GET", "/v1/state", "", 200)
	check("GET", "/v1/audit?limit=1001", "", 400)
	if text := check("GET", "/metrics", "", 200); !strings.Contains(text, "ledger_total_cents{node=\"n1\"} 1000000000000") {
		t.Fatal(text)
	}
}
