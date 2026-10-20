package consensus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"quorumledger/internal/ledger"
	"quorumledger/internal/storage"
)

type Node struct {
	mu          sync.Mutex
	proposeMu   sync.Mutex
	cfg         Config
	id          string
	transport   Transport
	wal         *storage.WAL
	promise     Ballot
	observed    Ballot
	term        Ballot
	leader      bool
	lastContact time.Time
	accepted    map[int]Accepted
	decided     map[int]ledger.Command
	machine     *ledger.Machine
	failed      error
	closed      bool
}

func New(cfg Config, id, dir string, transport Transport) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if _, ok := cfg.Nodes[id]; !ok {
		return nil, fmt.Errorf("unknown member %q", id)
	}
	n := &Node{cfg: cfg, id: id, transport: transport, accepted: make(map[int]Accepted), decided: make(map[int]ledger.Command), machine: ledger.New(), lastContact: time.Now()}
	identity := ledger.Digest(struct{ Node, Cluster string }{id, cfg.Fingerprint()})
	seenIdentity := false
	w, err := storage.Open(dir, func(raw json.RawMessage) error {
		var r record
		if err := json.Unmarshal(raw, &r); err != nil {
			return err
		}
		if !seenIdentity && r.Kind != "identity" {
			return fmt.Errorf("missing WAL identity")
		}
		switch r.Kind {
		case "identity":
			if seenIdentity || r.Identity != identity {
				return fmt.Errorf("WAL member/cluster identity mismatch")
			}
			seenIdentity = true
		case "promise":
			if r.Ballot.Less(n.promise) {
				return fmt.Errorf("promise regressed")
			}
			n.promise = r.Ballot
		case "accept":
			if r.Slot < 1 || r.Ballot.Less(n.promise) {
				return fmt.Errorf("invalid accepted record")
			}
			if err := r.Command.Validate(false); err != nil {
				return err
			}
			if a, ok := n.accepted[r.Slot]; ok && a.Ballot == r.Ballot && ledger.Digest(a.Command) != ledger.Digest(r.Command) {
				return fmt.Errorf("same-ballot value conflict")
			}
			n.promise = r.Ballot
			n.accepted[r.Slot] = Accepted{Ballot: r.Ballot, Command: r.Command}
		case "decide":
			if r.Slot < 1 {
				return fmt.Errorf("invalid decided slot")
			}
			if err := r.Command.Validate(false); err != nil {
				return err
			}
			if old, ok := n.decided[r.Slot]; ok && ledger.Digest(old) != ledger.Digest(r.Command) {
				return fmt.Errorf("conflicting decision")
			}
			n.decided[r.Slot] = r.Command
		default:
			return fmt.Errorf("unknown WAL record %q", r.Kind)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	n.wal = w
	if !seenIdentity {
		if err := w.Append(record{Kind: "identity", Identity: identity}); err != nil {
			w.Close()
			return nil, err
		}
	}
	if err := n.applyLocked(); err != nil {
		w.Close()
		return nil, err
	}
	return n, nil
}

func (n *Node) persistLocked(r record) error {
	if n.failed != nil {
		return n.failed
	}
	if n.closed {
		return fmt.Errorf("node closed")
	}
	if err := n.wal.Append(r); err != nil {
		n.failed = err
		n.leader = false
		return err
	}
	return nil
}

func (n *Node) applyLocked() error {
	for slot := n.machine.Applied() + 1; ; slot++ {
		c, ok := n.decided[slot]
		if !ok {
			return nil
		}
		if _, err := n.machine.Apply(slot, c); err != nil {
			n.failed = err
			n.leader = false
			return err
		}
	}
}

// Handle runs acceptor and learner transitions under one lock. No positive
// promise/accept/learn acknowledgment is returned before durable storage syncs.
func (n *Node) Handle(method string, req Request) (Reply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	response := func(ok bool) Reply { return Reply{OK: ok, Promise: n.promise, Applied: n.machine.Applied()} }
	if n.failed != nil {
		return response(false), n.failed
	}
	if n.closed {
		return response(false), fmt.Errorf("node closed")
	}
	if _, ok := n.cfg.Nodes[req.Ballot.Node]; !ok || req.Ballot.IsZero() {
		return response(false), fmt.Errorf("invalid sender ballot")
	}
	if method == "learn" {
		if len(req.Entries) > 128 {
			return response(false), fmt.Errorf("learn batch exceeds 128 slots")
		}
		for _, e := range req.Entries {
			e.Command = e.Command.Clone()
			if e.Slot < 1 {
				return response(false), fmt.Errorf("invalid slot")
			}
			if err := e.Command.Validate(false); err != nil {
				return response(false), err
			}
			if old, ok := n.decided[e.Slot]; ok {
				if ledger.Digest(old) != ledger.Digest(e.Command) {
					n.failed = fmt.Errorf("safety violation: conflicting decision at %d", e.Slot)
					n.leader = false
					return response(false), n.failed
				}
				continue
			}
			if err := n.persistLocked(record{Kind: "decide", Slot: e.Slot, Command: e.Command}); err != nil {
				return response(false), err
			}
			n.decided[e.Slot] = e.Command
		}
		if err := n.applyLocked(); err != nil {
			return response(false), err
		}
		return response(true), nil
	}
	if method != "prepare" && method != "accept" && method != "heartbeat" {
		return response(false), fmt.Errorf("unknown consensus method")
	}
	if req.Ballot.Less(n.promise) {
		return response(false), nil
	}
	if method == "accept" {
		req.Command = req.Command.Clone()
		if req.Slot < 1 {
			return response(false), fmt.Errorf("invalid slot")
		}
		if err := req.Command.Validate(false); err != nil {
			return response(false), err
		}
		if old, ok := n.decided[req.Slot]; ok && ledger.Digest(old) != ledger.Digest(req.Command) {
			return response(false), fmt.Errorf("attempt to overwrite chosen slot")
		}
		if old, ok := n.accepted[req.Slot]; ok && old.Ballot == req.Ballot && ledger.Digest(old.Command) != ledger.Digest(req.Command) {
			return response(false), fmt.Errorf("attempt to change value within ballot")
		}
		if old, ok := n.accepted[req.Slot]; !ok || old.Ballot != req.Ballot {
			if err := n.persistLocked(record{Kind: "accept", Ballot: req.Ballot, Slot: req.Slot, Command: req.Command}); err != nil {
				return response(false), err
			}
			n.accepted[req.Slot] = Accepted{Ballot: req.Ballot, Command: req.Command}
		}
	} else if n.promise != req.Ballot {
		if err := n.persistLocked(record{Kind: "promise", Ballot: req.Ballot}); err != nil {
			return response(false), err
		}
	}
	n.promise = req.Ballot
	if n.term != req.Ballot {
		n.leader = false
	}
	n.lastContact = time.Now()
	r := response(true)
	if method == "prepare" {
		r.Accepted = make(map[int]Accepted, len(n.accepted))
		for s, a := range n.accepted {
			a.Command = a.Command.Clone()
			r.Accepted[s] = a
		}
		r.Decided = make(map[int]ledger.Command, len(n.decided))
		for s, c := range n.decided {
			r.Decided[s] = c.Clone()
		}
	}
	return r, nil
}

type rpcResult struct {
	peer  string
	reply Reply
	err   error
}

func (n *Node) call(ctx context.Context, peer, method string, req Request) (Reply, error) {
	ctx, cancel := context.WithTimeout(ctx, n.cfg.Timeout())
	defer cancel()
	if peer == n.id {
		if err := ctx.Err(); err != nil {
			return Reply{}, err
		}
		return n.Handle(method, req)
	}
	return n.transport.Call(ctx, peer, method, req)
}

func (n *Node) observe(b Ballot) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.observed.Less(b) {
		n.observed = b
	}
	if n.term.Less(b) {
		n.leader = false
	}
}

func (n *Node) collect(ctx context.Context, method string, req Request) ([]Reply, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch := make(chan rpcResult, len(n.cfg.Nodes))
	for _, peer := range n.cfg.IDs() {
		go func(peer string) { r, err := n.call(ctx, peer, method, req); ch <- rpcResult{peer, r, err} }(peer)
	}
	replies := make([]Reply, 0, n.quorum())
	for remaining := len(n.cfg.Nodes); remaining > 0; remaining-- {
		select {
		case <-ctx.Done():
			return nil, ErrNoQuorum
		case result := <-ch:
			if result.err == nil {
				n.observe(result.reply.Promise)
				if result.reply.OK {
					replies = append(replies, result.reply)
				}
			}
			if len(replies) >= n.quorum() {
				return replies, nil
			}
			if len(replies)+remaining-1 < n.quorum() {
				return nil, ErrNoQuorum
			}
		}
	}
	return nil, ErrNoQuorum
}

func (n *Node) quorum() int { return len(n.cfg.Nodes)/2 + 1 }
func (n *Node) demote()     { n.mu.Lock(); n.leader = false; n.mu.Unlock() }

// Campaign prepares the entire untrimmed log once, adopts the highest accepted
// value per slot from a quorum, and fills gaps before accepting new client work.
func (n *Node) Campaign(ctx context.Context) error {
	n.proposeMu.Lock()
	defer n.proposeMu.Unlock()
	n.mu.Lock()
	if n.closed || n.failed != nil {
		n.mu.Unlock()
		return fmt.Errorf("node unavailable")
	}
	base := n.promise
	if base.Less(n.observed) {
		base = n.observed
	}
	if base.Round == ^uint64(0) {
		n.mu.Unlock()
		return fmt.Errorf("ballot space exhausted")
	}
	b := Ballot{Round: base.Round + 1, Node: n.id}
	n.leader = false
	n.term = b
	n.lastContact = time.Now()
	n.mu.Unlock()
	// Persist the candidate ballot locally even if the remote prepare quorum
	// excludes this member. This prevents reuse after a process restart.
	r, err := n.Handle("prepare", Request{Ballot: b})
	if err != nil {
		return err
	}
	if !r.OK {
		return ErrNotLeader
	}
	replies, err := n.collect(ctx, "prepare", Request{Ballot: b})
	if err != nil {
		return err
	}
	best := make(map[int]Accepted)
	chosen := make(map[int]ledger.Command)
	maxSlot := 0
	n.mu.Lock()
	for s, c := range n.decided {
		chosen[s] = c
		if s > maxSlot {
			maxSlot = s
		}
	}
	n.mu.Unlock()
	for _, reply := range replies {
		for s, a := range reply.Accepted {
			if old, ok := best[s]; !ok || old.Ballot.Less(a.Ballot) {
				best[s] = a
			}
			if s > maxSlot {
				maxSlot = s
			}
		}
		for s, c := range reply.Decided {
			if old, ok := chosen[s]; ok && ledger.Digest(old) != ledger.Digest(c) {
				return fmt.Errorf("conflicting learned values in prepare")
			}
			chosen[s] = c
			if s > maxSlot {
				maxSlot = s
			}
		}
	}
	for s := 1; s <= maxSlot; s++ {
		c := ledger.Command{Kind: "noop"}
		if a, ok := best[s]; ok {
			c = a.Command
		}
		if decided, ok := chosen[s]; ok {
			c = decided
		}
		if err := n.choose(ctx, b, s, c); err != nil {
			return err
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.promise != b || n.term != b || b.Less(n.observed) || n.failed != nil {
		return ErrNotLeader
	}
	n.leader = true
	n.lastContact = time.Now()
	return nil
}

func (n *Node) choose(ctx context.Context, b Ballot, slot int, c ledger.Command) error {
	_, err := n.collect(ctx, "accept", Request{Ballot: b, Slot: slot, Command: c})
	if err != nil {
		n.demote()
		return err
	}
	learn := Request{Ballot: b, Entries: []Entry{{Slot: slot, Command: c}}}
	if _, err = n.Handle("learn", learn); err != nil {
		n.demote()
		return err
	}
	// Acknowledgment depends on durable acceptances, not delivery of learn
	// messages. Heartbeats repair replicas that miss this best-effort broadcast.
	_, _ = n.collect(ctx, "learn", learn)
	return nil
}

func (n *Node) proposeLocked(ctx context.Context, c ledger.Command) (ledger.Result, error) {
	if ctx.Err() != nil {
		return ledger.Result{}, ErrNoQuorum
	}
	n.mu.Lock()
	if !n.leader || n.failed != nil || n.closed {
		n.mu.Unlock()
		return ledger.Result{}, ErrNotLeader
	}
	b := n.term
	slot := n.machine.Applied() + 1
	n.mu.Unlock()
	if err := n.choose(ctx, b, slot, c); err != nil {
		return ledger.Result{}, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if c.Kind == "noop" {
		return ledger.Result{OK: true, Code: "noop", Slot: slot}, nil
	}
	r, ok := n.machine.Lookup(c)
	if !ok {
		return ledger.Result{}, fmt.Errorf("chosen command not applied")
	}
	return r, nil
}

func (n *Node) Submit(ctx context.Context, c ledger.Command) (ledger.Result, error) {
	if err := c.Validate(true); err != nil {
		return ledger.Result{}, err
	}
	n.proposeMu.Lock()
	defer n.proposeMu.Unlock()
	return n.proposeLocked(ctx, c)
}

// Read commits a barrier instead of assuming clock-based leader leases. An
// isolated former leader cannot serve a successful linearizable read.
func (n *Node) Read(ctx context.Context, after, limit int) (ledger.Snapshot, []ledger.Event, error) {
	n.proposeMu.Lock()
	defer n.proposeMu.Unlock()
	if _, err := n.proposeLocked(ctx, ledger.Command{Kind: "noop"}); err != nil {
		return ledger.Snapshot{}, nil, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.machine.Snapshot(), n.machine.Audit(after, limit), nil
}

func (n *Node) Local() ledger.Snapshot { n.mu.Lock(); defer n.mu.Unlock(); return n.machine.Snapshot() }
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := Status{ID: n.id, ClusterID: n.cfg.ClusterID, Leader: n.leader, LeaderID: n.promise.Node, Promise: n.promise, Applied: n.machine.Applied(), AcceptedSlots: len(n.accepted), DecidedSlots: len(n.decided), Quorum: n.quorum()}
	if n.failed != nil {
		s.Failure = n.failed.Error()
	}
	if n.closed {
		s.Failure = "node closed"
	}
	return s
}

// Pulse probes every peer, including slow replicas, and sends bounded catch-up
// batches. The leader steps down if it cannot contact a majority.
func (n *Node) Pulse(ctx context.Context) error {
	n.proposeMu.Lock()
	defer n.proposeMu.Unlock()
	n.mu.Lock()
	if !n.leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	b := n.term
	n.mu.Unlock()
	ch := make(chan rpcResult, len(n.cfg.Nodes))
	for _, peer := range n.cfg.IDs() {
		go func(peer string) {
			r, err := n.call(ctx, peer, "heartbeat", Request{Ballot: b})
			if err == nil && r.OK {
				n.mu.Lock()
				entries := make([]Entry, 0, 128)
				for s := r.Applied + 1; len(entries) < 128; s++ {
					c, ok := n.decided[s]
					if !ok {
						break
					}
					entries = append(entries, Entry{Slot: s, Command: c})
				}
				n.mu.Unlock()
				if len(entries) > 0 {
					_, _ = n.call(ctx, peer, "learn", Request{Ballot: b, Entries: entries})
				}
			}
			ch <- rpcResult{peer, r, err}
		}(peer)
	}
	votes := 0
	for range n.cfg.Nodes {
		select {
		case <-ctx.Done():
			n.demote()
			return ErrNoQuorum
		case r := <-ch:
			if r.err == nil {
				n.observe(r.reply.Promise)
				if r.reply.OK {
					votes++
				}
			}
		}
	}
	if votes < n.quorum() {
		n.demote()
		return ErrNoQuorum
	}
	return nil
}

func (n *Node) Run(ctx context.Context) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	nextElection := func() time.Duration {
		return time.Duration(n.cfg.ElectionMinMS+rng.Intn(n.cfg.ElectionMinMS)) * time.Millisecond
	}
	timeout := nextElection()
	ticker := time.NewTicker(time.Duration(n.cfg.HeartbeatMS) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.mu.Lock()
			leader := n.leader
			elapsed := time.Since(n.lastContact)
			unavailable := n.closed || n.failed != nil
			n.mu.Unlock()
			if unavailable {
				return
			}
			if leader {
				_ = n.Pulse(ctx)
			} else if elapsed >= timeout {
				_ = n.Campaign(ctx)
				timeout = nextElection()
			}
		}
	}
}

func (n *Node) Close() error {
	n.proposeMu.Lock()
	defer n.proposeMu.Unlock()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil
	}
	n.closed = true
	n.leader = false
	return n.wal.Close()
}

func IsUnavailable(err error) bool {
	return errors.Is(err, ErrNotLeader) || errors.Is(err, ErrNoQuorum)
}
