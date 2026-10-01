package funnel

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/capstone"
	"simulator/economy/lifecycle"
	"simulator/economy/realize"
	"simulator/economy/reserve"
)

// ---- construction shape ----------------------------------------------

// NewBroker must build one shard per bundle, cache n = len(bundles),
// stamp brokerID on every shard, and hold the store — the Phase-1
// signature (BROKER_PHASE1 §1.4).
func TestNewBroker_ShapeMatchesBundles(t *testing.T) {
	bundles := make([]*capstone.Bundle, 4)
	// bundles[i] == nil is legal in Phase 0 (per-shard L5 wiring is
	// skipped when the bundle is nil; see newShard).
	brokerUnderTest := NewBroker("B-shape", 8, bundles, nil)

	if brokerUnderTest.brokerID != "B-shape" {
		t.Fatalf("brokerID = %q, want %q", brokerUnderTest.brokerID, "B-shape")
	}
	if brokerUnderTest.shardCount != 4 {
		t.Fatalf("broker.shardCount = %d, want 4", brokerUnderTest.shardCount)
	}
	if len(brokerUnderTest.shards) != 4 {
		t.Fatalf("len(shards) = %d, want 4", len(brokerUnderTest.shards))
	}
	for shardIndex, shardOwner := range brokerUnderTest.shards {
		if shardOwner == nil {
			t.Fatalf("shards[%d] is nil", shardIndex)
		}
		if shardOwner.brokerID != "B-shape" {
			t.Fatalf("shards[%d].brokerID = %q, want %q", shardIndex, shardOwner.brokerID, "B-shape")
		}
		if cap(shardOwner.inbox) != 8 {
			t.Fatalf("shards[%d].inbox cap = %d, want 8", shardIndex, cap(shardOwner.inbox))
		}
	}
}

// Empty bundles is a construction-time invariant violation — a
// zero-shard broker cannot route (hash modulo zero is undefined). Per
// the root error/panic policy this is a programming error, not a
// runtime condition, so NewBroker panics.
func TestNewBroker_PanicsOnEmptyBundles(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatalf("expected panic on empty bundles; got none")
		}
	}()
	_ = NewBroker("B-empty", 8, nil, nil)
}

// ---- Start / Stop ordering -------------------------------------------

// Start spawns one goroutine per shard; Stop closes each inbox and
// waits for those goroutines to drain and return. Stop returning IS the
// signal that owners have exited — Phase-1 shutdown depends on this
// synchronization (BROKER_PHASE1 §1.5).
func TestBroker_StartStopIsClean(t *testing.T) {
	brokerUnderTest := NewBroker("B-startstop", 4, make([]*capstone.Bundle, 2), nil)
	brokerUnderTest.Start()

	done := make(chan struct{})
	go func() {
		brokerUnderTest.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("Broker.Stop did not return within 2s after clean Start")
	}
}

// Stop must wait for pending commands to finish dispatching. Enqueue N
// commands, Start, immediately Stop; every reply must arrive by the time
// Stop returns (buffered replies still land in caller-owned channels).
func TestBroker_StopDrainsInFlightCommands(t *testing.T) {
	const commandCount = 32
	brokerUnderTest := NewBroker("B-drain", commandCount, make([]*capstone.Bundle, 2), nil)

	replies := make([]chan PlaceResult, commandCount)
	for commandIndex := 0; commandIndex < commandCount; commandIndex++ {
		replies[commandIndex] = make(chan PlaceResult, 1)
		brokerUnderTest.route(PlaceOrder{
			AccountID: AccountID("acct-drain"),
			ReplyTo:   replies[commandIndex],
		})
	}

	brokerUnderTest.Start()
	brokerUnderTest.Stop()

	for commandIndex, replyChannel := range replies {
		select {
		case <-replyChannel:
		default:
			t.Fatalf("reply %d did not arrive by Stop return", commandIndex)
		}
	}
}

// ---- routing determinism (G1) ----------------------------------------

// Every command for a given account must land on the same shard —
// exactly the one hashAccount picks for that account. Two accounts,
// interleaved commands, drained shard by shard. Any leak = spine item
// 2 violation (account state split).
func TestBroker_Route_AccountAlwaysSameShard(t *testing.T) {
	const shardCount = 4
	brokerUnderTest := NewBroker("B-route", 128, make([]*capstone.Bundle, shardCount), nil)

	// Find two accountIDs whose hash lands on two DIFFERENT shards, so
	// the "no cross-shard leak" assertion below is meaningful. Fixed
	// name templates + hashAccount are deterministic, so this is stable
	// across runs.
	firstAccount, firstExpectedShard := findAccountHashingTo(0, shardCount, "acct-first-")
	secondAccount, secondExpectedShard := findAccountHashingTo(1, shardCount, "acct-second-")

	const commandsPerAccount = 20
	for round := 0; round < commandsPerAccount; round++ {
		brokerUnderTest.route(ApplyResult{AccountID: firstAccount})
		brokerUnderTest.route(ApplyResult{AccountID: secondAccount})
	}

	// Drain each shard's inbox (Broker.Stop closes them). No goroutines
	// were started, so Stop returns immediately after close.
	brokerUnderTest.Stop()

	perShardFirst := make([]int, shardCount)
	perShardSecond := make([]int, shardCount)
	for shardIndex, shardOwner := range brokerUnderTest.shards {
		for cmd := range shardOwner.inbox {
			switch cmd.Account() {
			case firstAccount:
				perShardFirst[shardIndex]++
			case secondAccount:
				perShardSecond[shardIndex]++
			default:
				t.Fatalf("shard %d saw unexpected accountID %q", shardIndex, cmd.Account())
			}
		}
	}

	if perShardFirst[firstExpectedShard] != commandsPerAccount {
		t.Fatalf("first account: shard %d got %d commands, want %d",
			firstExpectedShard, perShardFirst[firstExpectedShard], commandsPerAccount)
	}
	if perShardSecond[secondExpectedShard] != commandsPerAccount {
		t.Fatalf("second account: shard %d got %d commands, want %d",
			secondExpectedShard, perShardSecond[secondExpectedShard], commandsPerAccount)
	}

	for shardIndex := 0; shardIndex < shardCount; shardIndex++ {
		if shardIndex != firstExpectedShard && perShardFirst[shardIndex] != 0 {
			t.Fatalf("first account leaked to shard %d: %d commands",
				shardIndex, perShardFirst[shardIndex])
		}
		if shardIndex != secondExpectedShard && perShardSecond[shardIndex] != 0 {
			t.Fatalf("second account leaked to shard %d: %d commands",
				shardIndex, perShardSecond[shardIndex])
		}
	}
}

// ---- reply mechanism (G3 / G4) ---------------------------------------

// G3 happy path: with the broker Started, PlaceOrder returns the
// zero-value PlaceResult (the Phase-0 echo) and a nil error.
func TestBroker_PlaceOrder_ReplyHappyPath(t *testing.T) {
	brokerUnderTest := NewBroker("B-happy", 8, make([]*capstone.Bundle, 2), nil)
	brokerUnderTest.Start()
	defer brokerUnderTest.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := brokerUnderTest.PlaceOrder(ctx, "acct-happy", "session-1", OrderSpec{})
	if err != nil {
		t.Fatalf("PlaceOrder happy path returned error: %v", err)
	}
}

// G4 timeout path: with a pre-canceled ctx and the broker NOT Started,
// PlaceOrder must return ErrBrokerTimeout. The command still sits in
// the shard's inbox; after Start it is processed (proven via the reply
// channel we constructed ourselves via route).
func TestBroker_PlaceOrder_ReplyTimeout(t *testing.T) {
	brokerUnderTest := NewBroker("B-timeout", 8, make([]*capstone.Bundle, 2), nil)
	defer brokerUnderTest.Stop()

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := brokerUnderTest.PlaceOrder(canceledCtx, "acct-timeout", "session-1", OrderSpec{})
	if !errors.Is(err, ErrBrokerTimeout) {
		t.Fatalf("PlaceOrder with canceled ctx: err = %v, want ErrBrokerTimeout", err)
	}
}

// A command already in the inbox is processed AFTER a caller times out
// (BROKER_TIER0 §Point-7 Decisions). Prove this by routing directly
// (owning the reply channel), asserting nothing before Start, then
// asserting arrival after Start.
func TestBroker_ShardProcessesCommand_AfterCallerTimeout(t *testing.T) {
	brokerUnderTest := NewBroker("B-post-timeout", 8, make([]*capstone.Bundle, 2), nil)

	ownedReply := make(chan PlaceResult, 1)
	brokerUnderTest.route(PlaceOrder{AccountID: "acct-post-timeout", ReplyTo: ownedReply})

	// Not started yet — nothing should have processed.
	select {
	case <-ownedReply:
		t.Fatalf("reply arrived before Broker.Start")
	case <-time.After(50 * time.Millisecond):
	}

	brokerUnderTest.Start()
	defer brokerUnderTest.Stop()

	select {
	case <-ownedReply:
	case <-time.After(2 * time.Second):
		t.Fatalf("reply did not arrive after Start (the shard did not process the queued command)")
	}
}

// ---- push entry points -----------------------------------------------

// Push helpers route the appropriate command onto the shard's inbox and
// do not block on a reply.
func TestBroker_PushHelpers_RouteAndReturn(t *testing.T) {
	brokerUnderTest := NewBroker("B-push", 16, make([]*capstone.Bundle, 2), nil)

	// Push before Start — commands accumulate; Stop closes inboxes and
	// we can drain to prove they routed to the correct shard.
	brokerUnderTest.PushApplyResult("acct-push-a", contract.EngineResult{OrderID: "acct-push-a:1"})
	brokerUnderTest.PushFireStop("acct-push-b", StopRecord{})

	brokerUnderTest.Stop()

	seenApply := false
	seenFire := false
	for _, shardOwner := range brokerUnderTest.shards {
		for cmd := range shardOwner.inbox {
			switch typedCmd := cmd.(type) {
			case ApplyResult:
				if typedCmd.AccountID != "acct-push-a" {
					t.Fatalf("ApplyResult on wrong account: %q", typedCmd.AccountID)
				}
				seenApply = true
			case FireStop:
				if typedCmd.AccountID != "acct-push-b" {
					t.Fatalf("FireStop on wrong account: %q", typedCmd.AccountID)
				}
				seenFire = true
			default:
				t.Fatalf("shard drained unexpected command type %T", cmd)
			}
		}
	}
	if !seenApply {
		t.Fatalf("PushApplyResult never landed on any shard")
	}
	if !seenFire {
		t.Fatalf("PushFireStop never landed on any shard")
	}
}

// ---- Point-6 per-shard L5 wiring (F5) --------------------------------

// The shard is the AccountLedger for both reserve and realize (structural
// typing; identical single-method interface). Compile-time assignments
// enforce the satisfaction — a regression that renamed / dropped Get
// would fail build here before any runtime test.
var (
	_ reserve.AccountLedger = (*shard)(nil)
	_ realize.AccountLedger = (*shard)(nil)
)

func TestShard_Get_MissingAccountReturnsNil(t *testing.T) {
	testShard := newShard("B-get", 4, nil)
	if resolvedAccount := testShard.Get(account.AccountID("acct-missing")); resolvedAccount != nil {
		t.Fatalf("shard.Get on empty sessions: got %v, want nil", resolvedAccount)
	}
}

func TestShard_Get_ReturnsAccountFromLoadedSession(t *testing.T) {
	testShard := newShard("B-get", 4, nil)
	loadedAccount := &account.Account{}
	testShard.sessions[account.AccountID("acct-loaded")] = &ownedSession{
		account: lifecycle.LoadedAccount{Account: loadedAccount},
	}

	resolvedAccount := testShard.Get(account.AccountID("acct-loaded"))
	if resolvedAccount != loadedAccount {
		t.Fatalf("shard.Get: got %p, want %p", resolvedAccount, loadedAccount)
	}
}

// Per-shard bundle discipline (BROKER_PHASE1 §1.2 / decision P1-D1):
// each shard owns its OWN reserve + realizer, never shared with another
// shard. Two shards constructed with distinct fake bundles must have
// distinct reserve/realizer pointers.
func TestNewBroker_PerShardWiringIsDistinct(t *testing.T) {
	firstBundle := &capstone.Bundle{}
	secondBundle := &capstone.Bundle{}
	brokerUnderTest := NewBroker("B-distinct", 4, []*capstone.Bundle{firstBundle, secondBundle}, nil)

	firstShard := brokerUnderTest.shards[0]
	secondShard := brokerUnderTest.shards[1]

	if firstShard.resv == nil || secondShard.resv == nil {
		t.Fatalf("shards missing per-shard resv (first=%v second=%v)",
			firstShard.resv, secondShard.resv)
	}
	if firstShard.realiz == nil || secondShard.realiz == nil {
		t.Fatalf("shards missing per-shard realiz (first=%v second=%v)",
			firstShard.realiz, secondShard.realiz)
	}
	if firstShard.resv == secondShard.resv {
		t.Fatalf("shards share the SAME reserve service — per-shard-bundle discipline violated")
	}
	if firstShard.realiz == secondShard.realiz {
		t.Fatalf("shards share the SAME realizer — per-shard-bundle discipline violated")
	}
	if firstShard.bundle != firstBundle || secondShard.bundle != secondBundle {
		t.Fatalf("shards hold the wrong bundle references")
	}
}

// findAccountHashingTo searches a suffix-numbered name space for an
// AccountID that hashes to the requested shard. Deterministic (same
// prefix + same shardCount → same result across runs). Fails the test
// if no match found in a bounded search — should never happen for a
// well-distributed hash over a small shardCount.
func findAccountHashingTo(targetShard, shardCount int, prefix string) (AccountID, int) {
	for suffix := 0; suffix < 1000; suffix++ {
		candidate := AccountID(fmt.Sprintf("%s%d", prefix, suffix))
		if hashAccount(candidate, shardCount) == targetShard {
			return candidate, targetShard
		}
	}
	panic(fmt.Sprintf("findAccountHashingTo: no accountID with prefix %q hashes to shard %d out of %d in 1000 tries",
		prefix, targetShard, shardCount))
}

// Nil bundles are legal in Phase 0 — resv/realiz stay nil, but the
// shard is still fully constructed and dispatch works over its empty
// handlers.
func TestNewShard_NilBundleSkipsWiring(t *testing.T) {
	testShard := newShard("B-nilbundle", 4, nil)
	if testShard.resv != nil {
		t.Fatalf("nil bundle should leave resv nil; got %v", testShard.resv)
	}
	if testShard.realiz != nil {
		t.Fatalf("nil bundle should leave realiz nil; got %v", testShard.realiz)
	}
	if testShard.bundle != nil {
		t.Fatalf("nil bundle should leave bundle nil; got %v", testShard.bundle)
	}
}

