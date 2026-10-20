package consensus

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"time"

	"quorumledger/internal/ledger"
)

type Config struct {
	ClusterID     string            `json:"cluster_id"`
	Token         string            `json:"internal_token"`
	Nodes         map[string]string `json:"nodes"`
	HeartbeatMS   int               `json:"heartbeat_ms"`
	ElectionMinMS int               `json:"election_min_ms"`
	RPCTimeoutMS  int               `json:"rpc_timeout_ms"`
}

func LoadConfig(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var c Config
	if err = json.Unmarshal(b, &c); err != nil {
		return c, err
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.ClusterID == "" || len(c.Token) < 16 {
		return fmt.Errorf("cluster_id and an internal_token of at least 16 characters are required")
	}
	if len(c.Nodes) == 0 || len(c.Nodes)%2 == 0 {
		return fmt.Errorf("use an odd, nonzero number of fixed members")
	}
	seen := make(map[string]bool)
	for id, raw := range c.Nodes {
		u, err := url.Parse(raw)
		if err != nil || id == "" || u.Scheme != "http" || u.Host == "" || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil || seen[raw] {
			return fmt.Errorf("invalid or duplicate member %q: use a distinct http://host:port URL", id)
		}
		seen[raw] = true
	}
	if c.HeartbeatMS < 20 || c.RPCTimeoutMS < 20 || c.ElectionMinMS < 3*(c.HeartbeatMS+c.RPCTimeoutMS) {
		return fmt.Errorf("election_min_ms must be >= 3 * (heartbeat_ms + rpc_timeout_ms), with both >= 20ms")
	}
	return nil
}

func (c Config) IDs() []string {
	ids := make([]string, 0, len(c.Nodes))
	for id := range c.Nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
func (c Config) Timeout() time.Duration { return time.Duration(c.RPCTimeoutMS) * time.Millisecond }
func (c Config) Fingerprint() string {
	return ledger.Digest(struct {
		Version string
		Cluster string
		Nodes   map[string]string
		Supply  int64
	}{"quorum-ledger-v1", c.ClusterID, c.Nodes, ledger.Supply})
}

// Ballots are totally ordered by (round, node ID). A candidate fsyncs its own
// promise before sending prepares, so a restarted process cannot reuse a ballot.
type Ballot struct {
	Round uint64 `json:"round"`
	Node  string `json:"node"`
}

func (b Ballot) Less(o Ballot) bool {
	return b.Round < o.Round || (b.Round == o.Round && b.Node < o.Node)
}
func (b Ballot) IsZero() bool { return b.Round == 0 }

type Accepted struct {
	Ballot  Ballot         `json:"ballot"`
	Command ledger.Command `json:"command"`
}
type Entry struct {
	Slot    int            `json:"slot"`
	Command ledger.Command `json:"command"`
}
type Request struct {
	Ballot  Ballot         `json:"ballot"`
	Slot    int            `json:"slot,omitempty"`
	Command ledger.Command `json:"command,omitempty"`
	Entries []Entry        `json:"entries,omitempty"`
}
type Reply struct {
	OK       bool                   `json:"ok"`
	Promise  Ballot                 `json:"promise"`
	Accepted map[int]Accepted       `json:"accepted,omitempty"`
	Decided  map[int]ledger.Command `json:"decided,omitempty"`
	Applied  int                    `json:"applied_slot"`
}
type Transport interface {
	Call(context.Context, string, string, Request) (Reply, error)
}

type Status struct {
	ID            string `json:"id"`
	ClusterID     string `json:"cluster_id"`
	Leader        bool   `json:"leader"`
	LeaderID      string `json:"leader_id"`
	Promise       Ballot `json:"promise"`
	Applied       int    `json:"applied_slot"`
	AcceptedSlots int    `json:"accepted_slots"`
	DecidedSlots  int    `json:"decided_slots"`
	Quorum        int    `json:"quorum"`
	Failure       string `json:"failure,omitempty"`
}

var ErrNotLeader = fmt.Errorf("not the active leader; discover the leader and retry with the same transaction ID")
var ErrNoQuorum = fmt.Errorf("majority unavailable; outcome may be unknown; retry with the same transaction ID")

type record struct {
	Kind     string         `json:"kind"`
	Identity string         `json:"identity,omitempty"`
	Ballot   Ballot         `json:"ballot,omitempty"`
	Slot     int            `json:"slot,omitempty"`
	Command  ledger.Command `json:"command,omitempty"`
}
