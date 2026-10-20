package consensus

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"quorumledger/internal/ledger"
)

// The in-process network exercises real acceptor transitions and real disk WALs.
// It can isolate members independently of their local leadership belief.
type network struct {
	mu       sync.Mutex
	nodes    map[string]*Node
	isolated map[string]bool
}
type endpoint struct {
	net  *network
	from string
}

func (e endpoint) Call(ctx context.Context, peer, method string, req Request) (Reply, error) {
	if err := ctx.Err(); err != nil {
		return Reply{}, err
	}
	e.net.mu.Lock()
	n := e.net.nodes[peer]
	blocked := e.net.isolated[peer] || e.net.isolated[e.from]
	e.net.mu.Unlock()
	if blocked || n == nil {
		return Reply{}, fmt.Errorf("link unavailable")
	}
	return n.Handle(method, req)
}
func (net *network) isolate(id string, yes bool) {
	net.mu.Lock()
	net.isolated[id] = yes
	net.mu.Unlock()
}

type cluster struct {
	cfg   Config
	net   *network
	dirs  map[string]string
	nodes map[string]*Node
}

func newCluster(t *testing.T) *cluster {
	t.Helper()
	c := &cluster{cfg: Config{ClusterID: "test", Token: "test-token-123456789", Nodes: map[string]string{"n1": "http://127.0.0.1:9101", "n2": "http://127.0.0.1:9102", "n3": "http://127.0.0.1:9103"}, HeartbeatMS: 20, RPCTimeoutMS: 20, ElectionMinMS: 200}, dirs: make(map[string]string), nodes: make(map[string]*Node), net: &network{nodes: make(map[string]*Node), isolated: make(map[string]bool)}}
	for _, id := range c.cfg.IDs() {
		c.dirs[id] = t.TempDir()
		c.open(t, id)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			_ = n.Close()
		}
	})
	return c
}
func (c *cluster) open(t *testing.T, id string) {
	t.Helper()
	n, err := New(c.cfg, id, c.dirs[id], endpoint{c.net, id})
	if err != nil {
		t.Fatal(err)
	}
	c.nodes[id] = n
	c.net.mu.Lock()
	c.net.nodes[id] = n
	c.net.mu.Unlock()
}
func campaign(t *testing.T, n *Node) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Campaign(ctx); err != nil {
		t.Fatal(err)
	}
}
func submit(t *testing.T, n *Node, c ledger.Command) ledger.Result {
	t.Helper()
	r, err := n.Submit(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func openAccount(id, account string) ledger.Command {
	return ledger.Command{ID: id, Kind: "open", Account: account}
}
func payment(id, from, to string, amount int64) ledger.Command {
	return ledger.Command{ID: id, Kind: "transfer", Transfers: []ledger.Transfer{{From: from, To: to, Amount: amount}}}
}
func converge(t *testing.T, c *cluster, leader string) {
	t.Helper()
	if err := c.nodes[leader].Pulse(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := c.nodes[leader].Local()
	for id, n := range c.nodes {
		if got := n.Local(); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s diverged:\ngot %+v\nwant %+v", id, got, want)
		}
	}
}

func TestPartitionFailoverAndLinearizableReads(t *testing.T) {
	c := newCluster(t)
	n1, n2 := c.nodes["n1"], c.nodes["n2"]
	campaign(t, n1)
	submit(t, n1, openAccount("open-a", "alice"))
	submit(t, n1, openAccount("open-b", "bob"))
	submit(t, n1, payment("fund", "treasury", "alice", 1000))
	c.net.isolate("n1", true)
	if _, err := n1.Submit(context.Background(), payment("isolated", "alice", "bob", 1)); err == nil {
		t.Fatal("minority acknowledged a new write")
	}
	if _, _, err := n1.Read(context.Background(), 0, 10); err == nil {
		t.Fatal("minority served a linearizable read")
	}
	campaign(t, n2)
	r := submit(t, n2, payment("payment", "alice", "bob", 300))
	if !r.OK {
		t.Fatal(r)
	}
	c.net.isolate("n1", false)
	converge(t, c, "n2")
	if got := n2.Local().Accounts["bob"]; got != 300 {
		t.Fatalf("unexpected bob balance %d", got)
	}
}

func TestRecoverChosenButUnlearnedValueAndHole(t *testing.T) {
	c := newCluster(t)
	campaign(t, c.nodes["n1"])
	b := c.nodes["n1"].Status().Promise
	// Slot two is accepted by a quorum, but the leader dies before any learner
	// learns it. Slot one is a hole and must be filled, never skipped.
	value := openAccount("orphan", "alice")
	for _, id := range []string{"n1", "n2"} {
		r, err := c.nodes[id].Handle("accept", Request{Ballot: b, Slot: 2, Command: value})
		if err != nil || !r.OK {
			t.Fatalf("accept: %+v %v", r, err)
		}
	}
	c.net.isolate("n1", true)
	campaign(t, c.nodes["n3"])
	s := c.nodes["n3"].Local()
	if s.Applied != 2 {
		t.Fatal(s)
	}
	if _, ok := s.Accounts["alice"]; !ok {
		t.Fatal("lost chosen value")
	}
	c.net.isolate("n1", false)
	converge(t, c, "n3")
}

func TestHighestAcceptedValueWins(t *testing.T) {
	c := newCluster(t)
	low := Ballot{1, "n1"}
	high := Ballot{2, "n2"}
	if r, err := c.nodes["n1"].Handle("accept", Request{Ballot: low, Slot: 1, Command: openAccount("old", "old")}); err != nil || !r.OK {
		t.Fatal(r, err)
	}
	for _, id := range []string{"n2", "n3"} {
		if r, err := c.nodes[id].Handle("prepare", Request{Ballot: high}); err != nil || !r.OK {
			t.Fatal(r, err)
		}
		if r, err := c.nodes[id].Handle("accept", Request{Ballot: high, Slot: 1, Command: openAccount("new", "new")}); err != nil || !r.OK {
			t.Fatal(r, err)
		}
	}
	campaign(t, c.nodes["n3"])
	converge(t, c, "n3")
	s := c.nodes["n3"].Local()
	if _, ok := s.Accounts["new"]; !ok {
		t.Fatal("lost highest accepted value")
	}
	if _, ok := s.Accounts["old"]; ok {
		t.Fatal("adopted lower ballot")
	}
}

func TestRestartPreservesBankingAndBallots(t *testing.T) {
	c := newCluster(t)
	campaign(t, c.nodes["n1"])
	submit(t, c.nodes["n1"], openAccount("open", "alice"))
	receipt := submit(t, c.nodes["n1"], payment("fund", "treasury", "alice", 900))
	converge(t, c, "n1")
	before := c.nodes["n1"].Status().Promise
	for _, id := range c.cfg.IDs() {
		if err := c.nodes[id].Close(); err != nil {
			t.Fatal(err)
		}
		c.open(t, id)
	}
	campaign(t, c.nodes["n1"])
	if !before.Less(c.nodes["n1"].Status().Promise) {
		t.Fatal("reused a pre-crash ballot")
	}
	if retry := submit(t, c.nodes["n1"], payment("fund", "treasury", "alice", 900)); retry != receipt {
		t.Fatal("receipt changed across restart")
	}
	converge(t, c, "n1")
	if c.nodes["n1"].Local().Accounts["alice"] != 900 {
		t.Fatal("duplicate funding after restart")
	}
}

func TestConcurrentRetriesMoveMoneyOnce(t *testing.T) {
	c := newCluster(t)
	n := c.nodes["n1"]
	campaign(t, n)
	submit(t, n, openAccount("open", "alice"))
	submit(t, n, payment("fund", "treasury", "alice", 1000))
	var wg sync.WaitGroup
	results := make(chan ledger.Result, 16)
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := n.Submit(context.Background(), payment("same-id", "alice", "treasury", 10))
			if err != nil {
				errs <- err
			} else {
				results <- r
			}
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var first ledger.Result
	for r := range results {
		if !r.OK {
			t.Fatal(r)
		}
		if first.ID == "" {
			first = r
		}
		if r != first {
			t.Fatal("retry receipt changed")
		}
	}
	converge(t, c, "n1")
	if n.Local().Accounts["alice"] != 990 {
		t.Fatal("concurrent retries moved money multiple times")
	}
}

func TestDurablePromisesAndSameBallotConflicts(t *testing.T) {
	c := newCluster(t)
	n := c.nodes["n1"]
	b := Ballot{9, "n2"}
	if r, err := n.Handle("accept", Request{Ballot: b, Slot: 1, Command: openAccount("a", "alice")}); err != nil || !r.OK {
		t.Fatal(r, err)
	}
	if _, err := n.Handle("accept", Request{Ballot: b, Slot: 1, Command: openAccount("b", "bob")}); err == nil {
		t.Fatal("same-ballot conflicting value accepted")
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	c.open(t, "n1")
	n = c.nodes["n1"]
	if r, err := n.Handle("prepare", Request{Ballot: Ballot{8, "n3"}}); err != nil || r.OK {
		t.Fatal("forgot durable promise", r, err)
	}
	if r, err := n.Handle("prepare", Request{Ballot: Ballot{10, "n3"}}); err != nil || !r.OK || r.Accepted[1].Command.Account != "alice" {
		t.Fatal("forgot accepted value", r, err)
	}
}

func TestWrongMemberDataDirectoryRejected(t *testing.T) {
	c := newCluster(t)
	c.nodes["n1"].Close()
	if n, err := New(c.cfg, "n2", c.dirs["n1"], endpoint{c.net, "n2"}); err == nil {
		n.Close()
		t.Fatal("opened another member's WAL")
	}
}

func TestAutomaticElectionAndLeaderLoss(t *testing.T) {
	c := newCluster(t)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, n := range c.nodes {
		wg.Add(1)
		go func(n *Node) { defer wg.Done(); n.Run(ctx) }(n)
	}
	defer func() { cancel(); wg.Wait() }()
	writeThroughLeader := func(except string, command ledger.Command) string {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			for id, n := range c.nodes {
				if id != except && n.Status().Leader {
					if r, err := n.Submit(ctx, command); err == nil && r.OK {
						return id
					}
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("automatic election did not stabilize")
		return ""
	}
	first := writeThroughLeader("", openAccount("open", "alice"))
	c.net.isolate(first, true)
	writeThroughLeader(first, payment("fund", "treasury", "alice", 100))
}
