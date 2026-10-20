package api

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"quorumledger/internal/consensus"
	"quorumledger/internal/ledger"
)

type Transport struct {
	Config consensus.Config
	Client *http.Client
}

func NewTransport(c consensus.Config) *Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConnsPerHost = 16
	t.MaxConnsPerHost = 32
	return &Transport{Config: c, Client: &http.Client{Transport: t}}
}
func (t *Transport) Call(ctx context.Context, peer, method string, req consensus.Request) (consensus.Reply, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return consensus.Reply{}, err
	}
	r, err := http.NewRequestWithContext(ctx, "POST", t.Config.Nodes[peer]+"/internal/"+method, bytes.NewReader(b))
	if err != nil {
		return consensus.Reply{}, err
	}
	r.Header.Set("Authorization", "Bearer "+t.Config.Token)
	r.Header.Set("Content-Type", "application/json")
	resp, err := t.Client.Do(r)
	if err != nil {
		return consensus.Reply{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return consensus.Reply{}, fmt.Errorf("peer %s: HTTP %d", peer, resp.StatusCode)
	}
	var reply consensus.Reply
	err = json.NewDecoder(io.LimitReader(resp.Body, 32<<20)).Decode(&reply)
	return reply, err
}

type Server struct {
	Node        *consensus.Node
	Config      consensus.Config
	started     time.Time
	requests    atomic.Uint64
	submissions atomic.Uint64
	rejected    atomic.Uint64
	latencyNS   atomic.Uint64
}

func NewServer(n *consensus.Node, cfg consensus.Config) *Server {
	return &Server{Node: n, Config: cfg, started: time.Now()}
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func decode(w http.ResponseWriter, r *http.Request, target any, max int64) error {
	r.Body = http.MaxBytesReader(w, r.Body, max)
	defer r.Body.Close()
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("body must contain one JSON object")
	}
	return nil
}

func (s *Server) failure(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, consensus.ErrNotLeader) {
		status = http.StatusConflict
	}
	if errors.Is(err, consensus.ErrNoQuorum) {
		status = http.StatusServiceUnavailable
	}
	n := s.Node.Status()
	writeJSON(w, status, map[string]any{"error": err.Error(), "leader_id": n.LeaderID, "leader_url": s.Config.Nodes[n.LeaderID], "retryable": consensus.IsUnavailable(err), "outcome_unknown": errors.Is(err, consensus.ErrNoQuorum)})
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		n := s.Node.Status()
		code := http.StatusOK
		if n.Failure != "" {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, map[string]any{"healthy": n.Failure == "", "id": n.ID})
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, s.Node.Status()) })
	mux.HandleFunc("GET /v1/local-state", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Ledger-Consistency", "local")
		writeJSON(w, http.StatusOK, s.Node.Local())
	})
	mux.HandleFunc("POST /v1/transactions", func(w http.ResponseWriter, r *http.Request) {
		s.submissions.Add(1)
		start := time.Now()
		defer func() { s.latencyNS.Add(uint64(time.Since(start))) }()
		var c ledger.Command
		if err := decode(w, r, &c, 64<<10); err != nil {
			s.rejected.Add(1)
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if err := c.Validate(true); err != nil {
			s.rejected.Add(1)
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		result, err := s.Node.Submit(ctx, c)
		if err != nil {
			s.rejected.Add(1)
			s.failure(w, err)
			return
		}
		code := http.StatusOK
		if !result.OK {
			s.rejected.Add(1)
			code = http.StatusUnprocessableEntity
		}
		writeJSON(w, code, result)
	})
	mux.HandleFunc("GET /v1/state", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		state, _, err := s.Node.Read(ctx, 0, 0)
		if err != nil {
			s.failure(w, err)
			return
		}
		w.Header().Set("X-Ledger-Consistency", "linearizable")
		writeJSON(w, http.StatusOK, state)
	})
	mux.HandleFunc("GET /v1/audit", func(w http.ResponseWriter, r *http.Request) {
		after, limit := 0, 100
		var err error
		if raw := r.URL.Query().Get("after"); raw != "" {
			after, err = strconv.Atoi(raw)
		}
		if err != nil || after < 0 {
			writeJSON(w, 400, map[string]string{"error": "after must be a nonnegative integer"})
			return
		}
		if raw := r.URL.Query().Get("limit"); raw != "" {
			limit, err = strconv.Atoi(raw)
		}
		if err != nil || limit < 1 || limit > 1000 {
			writeJSON(w, 400, map[string]string{"error": "limit must be between 1 and 1000"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		state, events, err := s.Node.Read(ctx, after, limit)
		if err != nil {
			s.failure(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"snapshot": state, "events": events})
	})
	mux.HandleFunc("POST /internal/{method}", func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+s.Config.Token)) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid peer token"})
			return
		}
		method := r.PathValue("method")
		if method != "prepare" && method != "accept" && method != "heartbeat" && method != "learn" {
			http.NotFound(w, r)
			return
		}
		var req consensus.Request
		if err := decode(w, r, &req, 1<<20); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		reply, err := s.Node.Handle(method, req)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, reply)
	})
	mux.HandleFunc("GET /metrics", s.metrics)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests.Add(1)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	status, state := s.Node.Status(), s.Node.Local()
	leader := 0
	if status.Leader {
		leader = 1
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	labels := fmt.Sprintf("{node=%q}", status.ID)
	values := []struct {
		name, kind, help string
		value            any
	}{
		{"ledger_leader", "gauge", "Local belief that this member is leader.", leader},
		{"ledger_applied_slot", "gauge", "Highest contiguous applied slot.", status.Applied},
		{"ledger_ballot_round", "gauge", "Durable promised ballot round.", status.Promise.Round},
		{"ledger_accepted_slots", "gauge", "Number of retained accepted slots.", status.AcceptedSlots},
		{"ledger_total_cents", "gauge", "Conserved supply across all accounts.", state.Total},
		{"ledger_accounts", "gauge", "Number of bank accounts including treasury.", len(state.Accounts)},
		{"ledger_http_requests_total", "counter", "All HTTP requests including metrics scrapes.", s.requests.Load()},
		{"ledger_submissions_total", "counter", "Transaction submission attempts.", s.submissions.Load()},
		{"ledger_rejections_total", "counter", "Invalid, unavailable, or business rejected submissions.", s.rejected.Load()},
		{"ledger_submission_duration_seconds_total", "counter", "Total submission handling time in seconds.", float64(s.latencyNS.Load()) / 1e9},
		{"ledger_uptime_seconds", "gauge", "Time since this HTTP server started.", time.Since(s.started).Seconds()},
	}
	for _, v := range values {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s%s %v\n", v.name, strings.TrimSpace(v.help), v.name, v.kind, v.name, labels, v.value)
	}
}
