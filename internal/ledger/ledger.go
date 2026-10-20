// Package ledger implements the deterministic banking state machine. Amounts are
// integer cents; the fixed supply enters circulation through treasury transfers.
package ledger

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
)

const Supply int64 = 1_000_000_000_000
const MaxBatch = 64

var namePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$`)

type Transfer struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Amount int64  `json:"amount_cents"`
}

type Command struct {
	ID        string     `json:"id,omitempty"`
	Kind      string     `json:"kind"`
	Account   string     `json:"account,omitempty"`
	Transfers []Transfer `json:"transfers,omitempty"`
}

func (c Command) Clone() Command { c.Transfers = append([]Transfer(nil), c.Transfers...); return c }

func (c Command) Validate(client bool) error {
	if !client && c.Kind == "noop" && c.ID == "" && c.Account == "" && len(c.Transfers) == 0 {
		return nil
	}
	if !namePattern.MatchString(c.ID) {
		return fmt.Errorf("id must contain 1–64 letters, digits, dots, dashes, or underscores")
	}
	switch c.Kind {
	case "open":
		if !namePattern.MatchString(c.Account) || len(c.Transfers) != 0 {
			return fmt.Errorf("open requires a valid account and no transfers")
		}
	case "transfer", "batch":
		if c.Account != "" || len(c.Transfers) == 0 || len(c.Transfers) > MaxBatch || (c.Kind == "transfer" && len(c.Transfers) != 1) {
			return fmt.Errorf("transfer requires one leg; batch requires 1–64 legs")
		}
		for _, t := range c.Transfers {
			if !namePattern.MatchString(t.From) || !namePattern.MatchString(t.To) || t.From == t.To || t.Amount <= 0 || t.Amount > Supply {
				return fmt.Errorf("invalid transfer leg or amount")
			}
		}
	default:
		return fmt.Errorf("kind must be open, transfer, or batch")
	}
	return nil
}

func Digest(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

type Result struct {
	ID        string `json:"id,omitempty"`
	OK        bool   `json:"ok"`
	Code      string `json:"code"`
	Slot      int    `json:"slot"`
	AuditHash string `json:"audit_hash"`
}

type Receipt struct {
	Fingerprint string
	Result      Result
}

type Event struct {
	Slot      int     `json:"slot"`
	Command   Command `json:"command"`
	Code      string  `json:"code"`
	Previous  string  `json:"previous"`
	StateHash string  `json:"state_hash"`
	Hash      string  `json:"hash"`
}

type Snapshot struct {
	Applied   int              `json:"applied_slot"`
	Accounts  map[string]int64 `json:"accounts"`
	Total     int64            `json:"total_cents"`
	AuditHead string           `json:"audit_head"`
	Receipts  int              `json:"receipt_count"`
}

type Machine struct {
	balances map[string]int64
	receipts map[string]Receipt
	events   []Event
}

func New() *Machine {
	return &Machine{balances: map[string]int64{"treasury": Supply}, receipts: make(map[string]Receipt)}
}

func (m *Machine) Applied() int { return len(m.events) }

// Apply records every slot, including no-ops and duplicate requests, in the audit
// chain. A retry returns the original receipt and never moves money twice.
func (m *Machine) Apply(slot int, c Command) (Result, error) {
	if slot != len(m.events)+1 {
		return Result{}, fmt.Errorf("noncontiguous apply: slot %d after %d", slot, len(m.events))
	}
	if err := c.Validate(false); err != nil {
		return Result{}, err
	}
	c = c.Clone()
	r := Result{ID: c.ID, OK: true, Code: "applied", Slot: slot}
	fingerprint := Digest(c)
	old, duplicate := m.receipts[c.ID]
	if c.Kind == "noop" {
		r.Code = "noop"
	} else if duplicate {
		if old.Fingerprint == fingerprint {
			r = old.Result
		} else {
			r.OK = false
			r.Code = "id_conflict"
		}
	} else if c.Kind == "open" {
		if _, exists := m.balances[c.Account]; exists {
			r.OK = false
			r.Code = "account_exists"
		} else {
			m.balances[c.Account] = 0
		}
	} else {
		delta := make(map[string]int64)
		for _, t := range c.Transfers {
			if _, ok := m.balances[t.From]; !ok {
				r.OK = false
				r.Code = "account_missing"
			}
			if _, ok := m.balances[t.To]; !ok {
				r.OK = false
				r.Code = "account_missing"
			}
			delta[t.From] -= t.Amount
			delta[t.To] += t.Amount
		}
		// Batch transfers settle net changes atomically, so cycles are allowed.
		if r.OK {
			for a, d := range delta {
				if m.balances[a]+d < 0 {
					r.OK = false
					r.Code = "insufficient_funds"
					break
				}
			}
		}
		if r.OK {
			for a, d := range delta {
				m.balances[a] += d
			}
		}
	}
	previous := "genesis"
	if len(m.events) > 0 {
		previous = m.events[len(m.events)-1].Hash
	}
	e := Event{Slot: slot, Command: c, Code: r.Code, Previous: previous, StateHash: Digest(m.balances)}
	e.Hash = Digest(e) // Hash is empty while calculating the digest.
	m.events = append(m.events, e)
	if !duplicate {
		r.AuditHash = e.Hash
		if c.ID != "" {
			m.receipts[c.ID] = Receipt{Fingerprint: fingerprint, Result: r}
		}
	} else if old.Fingerprint != fingerprint {
		r.AuditHash = e.Hash
	}
	return r, nil
}

func (m *Machine) Lookup(c Command) (Result, bool) {
	x, ok := m.receipts[c.ID]
	if !ok {
		return Result{}, false
	}
	if x.Fingerprint != Digest(c) {
		return Result{ID: c.ID, Code: "id_conflict", Slot: x.Result.Slot, AuditHash: x.Result.AuditHash}, true
	}
	return x.Result, true
}

func (m *Machine) Snapshot() Snapshot {
	s := Snapshot{Applied: len(m.events), Accounts: make(map[string]int64), Receipts: len(m.receipts)}
	for a, b := range m.balances {
		s.Accounts[a] = b
		s.Total += b
	}
	if len(m.events) > 0 {
		s.AuditHead = m.events[len(m.events)-1].Hash
	}
	return s
}

func (m *Machine) Audit(after, limit int) []Event {
	if limit < 0 {
		limit = 0
	}
	if after < 0 {
		after = 0
	}
	if after > len(m.events) {
		after = len(m.events)
	}
	end := after + limit
	if end > len(m.events) {
		end = len(m.events)
	}
	events := append([]Event{}, m.events[after:end]...)
	for i := range events {
		events[i].Command = events[i].Command.Clone()
	}
	return events
}
