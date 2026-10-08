package funnel

import (
	"fmt"
	"testing"
)

// Format pins "<AccountID>:<seq>" exactly (BROKER_PHASE5 §1.4 P5-D4).
// Settlement's accountOf (account_of.go) relies on the shape — the LastIndex
// of ":" is the account/seq split, so any shape drift here would make
// EngineResult routing fail silently.
func TestNextOrderID_Format(t *testing.T) {
	shardUnderTest := newShard("B-nextid", 1, nil, nil)
	got := shardUnderTest.nextOrderID("acct-42")
	want := "acct-42:1"
	if got != want {
		t.Errorf("nextOrderID(acct-42) first mint = %q, want %q", got, want)
	}
}

// Monotonic within a single account: seq bumps by one per call.
func TestNextOrderID_MonotonicPerShard(t *testing.T) {
	shardUnderTest := newShard("B-nextid", 1, nil, nil)
	for callIndex := 1; callIndex <= 5; callIndex++ {
		got := shardUnderTest.nextOrderID("acct-A")
		want := fmt.Sprintf("acct-A:%d", callIndex)
		if got != want {
			t.Errorf("call #%d: nextOrderID = %q, want %q", callIndex, got, want)
		}
	}
}

// Cross-account uniqueness on one shard: seq is shared across accounts (not
// reset per account), so the AccountID prefix is what disambiguates — but
// the seq numbers themselves still interleave strictly monotonically. This
// property guarantees settlement's accountOf parses the right owner on an
// echoed OrderID.
func TestNextOrderID_SharedSeqAcrossAccounts(t *testing.T) {
	shardUnderTest := newShard("B-nextid", 1, nil, nil)

	first := shardUnderTest.nextOrderID("acct-A")  // acct-A:1
	second := shardUnderTest.nextOrderID("acct-B") // acct-B:2
	third := shardUnderTest.nextOrderID("acct-A")  // acct-A:3

	if first != "acct-A:1" {
		t.Errorf("first mint = %q, want acct-A:1", first)
	}
	if second != "acct-B:2" {
		t.Errorf("second mint = %q, want acct-B:2", second)
	}
	if third != "acct-A:3" {
		t.Errorf("third mint = %q, want acct-A:3", third)
	}
}

// A fresh shard always starts seq at 0; the first mint is seq=1 (post-
// increment). Two shards built back-to-back must each start fresh so there is
// no accidental cross-shard sharing.
func TestNextOrderID_FreshShardsStartIndependent(t *testing.T) {
	shardOne := newShard("B-one", 1, nil, nil)
	shardTwo := newShard("B-two", 1, nil, nil)

	if got := shardOne.nextOrderID("acct-A"); got != "acct-A:1" {
		t.Errorf("shardOne first mint = %q, want acct-A:1", got)
	}
	if got := shardTwo.nextOrderID("acct-A"); got != "acct-A:1" {
		t.Errorf("shardTwo first mint = %q, want acct-A:1 (independent counter)", got)
	}
}

// Round-trip: a minted OrderID parses back to its originating AccountID via
// accountOf (the settlement consumer's parse). This is the whole reason the
// shape exists — if it ever breaks, settlement routing breaks.
func TestNextOrderID_RoundTripsThroughAccountOf(t *testing.T) {
	shardUnderTest := newShard("B-roundtrip", 1, nil, nil)
	orderID := shardUnderTest.nextOrderID("acct-rt")

	parsedAccount, parsed := accountOf(orderID)
	if !parsed {
		t.Fatalf("accountOf(%q) parsed=false, want true", orderID)
	}
	if parsedAccount != "acct-rt" {
		t.Errorf("accountOf(%q) = %q, want acct-rt", orderID, parsedAccount)
	}
}
