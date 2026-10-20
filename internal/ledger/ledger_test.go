package ledger

import (
	"math/rand"
	"reflect"
	"strconv"
	"testing"
)

func apply(t *testing.T, m *Machine, c Command) Result {
	t.Helper()
	r, err := m.Apply(m.Snapshot().Applied+1, c)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func transfer(id, from, to string, amount int64) Command {
	return Command{ID: id, Kind: "transfer", Transfers: []Transfer{{From: from, To: to, Amount: amount}}}
}

func TestAtomicNetSettlementAndRetry(t *testing.T) {
	m := New()
	apply(t, m, Command{ID: "open-a", Kind: "open", Account: "alice"})
	apply(t, m, Command{ID: "open-b", Kind: "open", Account: "bob"})
	apply(t, m, transfer("fund", "treasury", "alice", 1000))
	c := Command{ID: "payroll", Kind: "batch", Transfers: []Transfer{{"alice", "bob", 1200}, {"bob", "alice", 700}}}
	r := apply(t, m, c)
	if !r.OK {
		t.Fatal(r)
	}
	state := m.Snapshot()
	if state.Accounts["alice"] != 500 || state.Accounts["bob"] != 500 || state.Total != Supply {
		t.Fatal(state)
	}
	if retry := apply(t, m, c); retry != r {
		t.Fatalf("receipt changed: %+v vs %+v", r, retry)
	}
	if !reflect.DeepEqual(state.Accounts, m.Snapshot().Accounts) {
		t.Fatal("retry moved funds")
	}
	c.Transfers[0].Amount++
	if conflict := apply(t, m, c); conflict.Code != "id_conflict" || conflict.OK {
		t.Fatal(conflict)
	}
	if !reflect.DeepEqual(state.Accounts, m.Snapshot().Accounts) {
		t.Fatal("conflicting ID moved funds")
	}
	bad := Command{ID: "bad-batch", Kind: "batch", Transfers: []Transfer{{"alice", "bob", 100}, {"bob", "alice", 900}}}
	if r := apply(t, m, bad); r.OK || r.Code != "insufficient_funds" {
		t.Fatal(r)
	}
	if !reflect.DeepEqual(state.Accounts, m.Snapshot().Accounts) {
		t.Fatal("failed batch partially applied")
	}
	previous := "genesis"
	for _, event := range m.Audit(0, 100) {
		hash := event.Hash
		event.Hash = ""
		if Digest(event) != hash || event.Previous != previous {
			t.Fatal("invalid audit chain")
		}
		previous = hash
	}
}

func TestRejectedReceiptIsStable(t *testing.T) {
	m := New()
	apply(t, m, Command{ID: "open", Kind: "open", Account: "alice"})
	c := transfer("empty", "alice", "treasury", 10)
	denied := apply(t, m, c)
	apply(t, m, transfer("fund", "treasury", "alice", 100))
	if retried := apply(t, m, c); retried != denied || retried.OK {
		t.Fatalf("a rejected retry changed outcome: %+v", retried)
	}
	if m.Snapshot().Accounts["alice"] != 100 {
		t.Fatal("rejected retry moved funds")
	}
}

func TestConservationAcrossRandomTransfers(t *testing.T) {
	m := New()
	names := []string{"treasury", "alice", "bob", "carol"}
	for _, a := range names[1:] {
		apply(t, m, Command{ID: "open-" + a, Kind: "open", Account: a})
		apply(t, m, transfer("fund-"+a, "treasury", a, 10000))
	}
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 1000; i++ {
		from, to := names[rng.Intn(len(names))], names[rng.Intn(len(names))]
		if from == to {
			continue
		}
		c := transfer("random-"+strconv.Itoa(i), from, to, int64(rng.Intn(25000)+1))
		apply(t, m, c)
		s := m.Snapshot()
		if s.Total != Supply {
			t.Fatalf("supply changed at %d", i)
		}
		for _, b := range s.Accounts {
			if b < 0 {
				t.Fatal("negative balance")
			}
		}
	}
}

func TestCommandValidation(t *testing.T) {
	for _, c := range []Command{{Kind: "noop"}, {ID: "x", Kind: "open", Account: "../escape"}, transfer("x", "a", "b", -1), transfer("x", "a", "a", 1), transfer("x", "a", "b", Supply+1)} {
		if c.Validate(true) == nil {
			t.Fatalf("accepted malformed client command %+v", c)
		}
	}
}
