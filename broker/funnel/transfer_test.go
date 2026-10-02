package funnel_test

// Transfer integration tests (BROKER_PHASE3 §2.2 / §2.4). Each case
// traces to the Phase-3 status table — not the code's output — and
// probes both buckets of the dual-branched command:
//
//   F1 — live push (Available → Bank), sufficient              → Present:true, (Available-X, Bank+X)
//   F2 — live pull (Bank → Available), sufficient              → Present:true, (Available+X, Bank-X)
//   F3 — live push, Available < amount                         → Present:true, Err:ErrInsufficientFunds-category
//   F4 — live pull, Bank      < amount                         → Present:true, Err:ErrInsufficientFunds-category
//   F6 — no live session                                       → Present:false (internal only, HTTP falls to durable)
//   F7 — concurrent Transfer + PlaceOrder on one account       → both complete, -race oracle
//   F8 — determinism replay                                    → identical final balances across two fresh runtimes
//
// F5 (mid-position, Reserved untouched) is deferred to a later phase:
// PlaceOrder is still an empty echo at Stage B, so there is no honest
// path to Reserved > 0. The structural claim "MoveCash never writes
// Reserved" is already covered one layer down in economy/account/cash_test.go.
//
// Also deferred: the live-success path's MarkSnapshotDirty side-effect is
// not asserted here because the Persist component is per-shard-owned and
// unexported; the drainer-driven round-trip is covered by economy's
// persistence tests. The success return itself is the direct observation
// that the handler reached the mark-dirty step.

import (
	"context"
	"errors"
	"sync"
	"testing"

	"simulator/broker/bringup"
	"simulator/broker/funnel"
	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/lifecycle"
)

// liveSessionBrokerConfig returns a BrokerConfig with the per-test money
// dials; otherwise defaults to the shared test shape.
func liveSessionBrokerConfig(startingCapital, startingBank int64) bringup.BrokerConfig {
	cfg := defaultTestBrokerConfig()
	cfg.BrokerID = "B-transfer-integ"
	cfg.StartingCapital = startingCapital
	cfg.StartingBank = startingBank
	return cfg
}

// installLiveSession stands up an EnsureAccount + canonical StartSession
// for one account against the given runtime. Returns the AccountID so
// callers can drive Transfer / PlaceOrder against it.
func installLiveSession(t *testing.T, runtime *bringup.Runtime, ownerPrefix string) funnel.AccountID {
	t.Helper()
	accountID, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, ownerPrefix+"-owner")
	if err != nil {
		t.Fatalf("EnsureAccount: %v", err)
	}
	cmd := funnel.SessionStartCmd{
		AccountID:       accountID,
		SessionID:       ownerPrefix + "-sess",
		Mode:            lifecycle.Canonical,
		ExchangeID:      "X-transfer",
		ExchangeInbound: make(chan contract.InboundMessage, 1),
		Participant:     "P-transfer",
	}
	if err := runtime.Broker.StartSession(context.Background(), cmd); err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	return accountID
}

// F1 — live push (Available → Bank), sufficient Available.
func TestTransfer_LivePushSufficient_ReturnsNewBalances(t *testing.T) {
	const startingCapital int64 = 10_000
	const startingBank int64 = 5_000
	const pushAmount int64 = 3_000

	ownerPrefix := uniqueOwnerPrefix(t)
	runtime := requireRuntime(t, liveSessionBrokerConfig(startingCapital, startingBank))
	cleanupOwnerRows(t, runtime, ownerPrefix)

	accountID := installLiveSession(t, runtime, ownerPrefix)

	res, err := runtime.Broker.Transfer(context.Background(), accountID, account.Available, account.Bank, pushAmount)
	if err != nil {
		t.Fatalf("Transfer: unexpected transport error: %v", err)
	}
	if !res.Present {
		t.Fatalf("Transfer: got Present=false, want Present=true (live session installed)")
	}
	if res.Err != nil {
		t.Fatalf("Transfer: got Err=%v, want nil", res.Err)
	}
	if got, want := res.Available, startingCapital-pushAmount; got != want {
		t.Errorf("post-push Available: got %d, want %d", got, want)
	}
	if got, want := res.Bank, startingBank+pushAmount; got != want {
		t.Errorf("post-push Bank: got %d, want %d", got, want)
	}
}

// F2 — live pull (Bank → Available), sufficient Bank. Symmetric to F1;
// explicit, not inferred from parity ([[feedback_tests_probe_dont_assume]]).
func TestTransfer_LivePullSufficient_ReturnsNewBalances(t *testing.T) {
	const startingCapital int64 = 10_000
	const startingBank int64 = 5_000
	const pullAmount int64 = 2_000

	ownerPrefix := uniqueOwnerPrefix(t)
	runtime := requireRuntime(t, liveSessionBrokerConfig(startingCapital, startingBank))
	cleanupOwnerRows(t, runtime, ownerPrefix)

	accountID := installLiveSession(t, runtime, ownerPrefix)

	res, err := runtime.Broker.Transfer(context.Background(), accountID, account.Bank, account.Available, pullAmount)
	if err != nil {
		t.Fatalf("Transfer: unexpected transport error: %v", err)
	}
	if !res.Present {
		t.Fatalf("Transfer: got Present=false, want Present=true (live session installed)")
	}
	if res.Err != nil {
		t.Fatalf("Transfer: got Err=%v, want nil", res.Err)
	}
	if got, want := res.Available, startingCapital+pullAmount; got != want {
		t.Errorf("post-pull Available: got %d, want %d", got, want)
	}
	if got, want := res.Bank, startingBank-pullAmount; got != want {
		t.Errorf("post-pull Bank: got %d, want %d", got, want)
	}
}

// F3 — live push with insufficient Available → Present:true + Err; no
// mutation. The follow-up probe (a legitimate 1-coin push) asserts the
// post-reject balances are untouched: if the failed call had somehow
// decremented Available, the probe's reported Available would be off
// by the failed amount.
func TestTransfer_LivePushInsufficient_ErrAndNoMutation(t *testing.T) {
	const startingCapital int64 = 1_000
	const startingBank int64 = 0
	const overflowAmount int64 = 5_000
	const probeAmount int64 = 1

	ownerPrefix := uniqueOwnerPrefix(t)
	runtime := requireRuntime(t, liveSessionBrokerConfig(startingCapital, startingBank))
	cleanupOwnerRows(t, runtime, ownerPrefix)

	accountID := installLiveSession(t, runtime, ownerPrefix)

	res, err := runtime.Broker.Transfer(context.Background(), accountID, account.Available, account.Bank, overflowAmount)
	if err != nil {
		t.Fatalf("Transfer: unexpected transport error: %v", err)
	}
	if !res.Present {
		t.Fatalf("Transfer: got Present=false, want Present=true (live session installed)")
	}
	if res.Err == nil || !errors.Is(res.Err, account.ErrInsufficientFunds) {
		t.Fatalf("Transfer: got Err=%v, want errors.Is ErrInsufficientFunds", res.Err)
	}

	// Probe: a legitimate 1-coin push proves nothing moved on the failed
	// call. The reported Available must be exactly starting-1 (not
	// starting-1-overflow and not starting-overflow+something).
	probe, err := runtime.Broker.Transfer(context.Background(), accountID, account.Available, account.Bank, probeAmount)
	if err != nil {
		t.Fatalf("probe Transfer: unexpected transport error: %v", err)
	}
	if probe.Err != nil {
		t.Fatalf("probe Transfer: got Err=%v, want nil (should succeed on 1 coin)", probe.Err)
	}
	if got, want := probe.Available, startingCapital-probeAmount; got != want {
		t.Errorf("probe Available: got %d, want %d (prior reject mutated state)", got, want)
	}
	if got, want := probe.Bank, startingBank+probeAmount; got != want {
		t.Errorf("probe Bank: got %d, want %d (prior reject mutated state)", got, want)
	}
}

// F4 — live pull with insufficient Bank → Present:true + Err; no
// mutation. Symmetric to F3; explicit, not inferred.
func TestTransfer_LivePullInsufficient_ErrAndNoMutation(t *testing.T) {
	const startingCapital int64 = 0
	const startingBank int64 = 1_000
	const overflowAmount int64 = 5_000
	const probeAmount int64 = 1

	ownerPrefix := uniqueOwnerPrefix(t)
	runtime := requireRuntime(t, liveSessionBrokerConfig(startingCapital, startingBank))
	cleanupOwnerRows(t, runtime, ownerPrefix)

	accountID := installLiveSession(t, runtime, ownerPrefix)

	res, err := runtime.Broker.Transfer(context.Background(), accountID, account.Bank, account.Available, overflowAmount)
	if err != nil {
		t.Fatalf("Transfer: unexpected transport error: %v", err)
	}
	if !res.Present {
		t.Fatalf("Transfer: got Present=false, want Present=true (live session installed)")
	}
	if res.Err == nil || !errors.Is(res.Err, account.ErrInsufficientFunds) {
		t.Fatalf("Transfer: got Err=%v, want errors.Is ErrInsufficientFunds", res.Err)
	}

	probe, err := runtime.Broker.Transfer(context.Background(), accountID, account.Bank, account.Available, probeAmount)
	if err != nil {
		t.Fatalf("probe Transfer: unexpected transport error: %v", err)
	}
	if probe.Err != nil {
		t.Fatalf("probe Transfer: got Err=%v, want nil (should succeed on 1 coin)", probe.Err)
	}
	if got, want := probe.Available, startingCapital+probeAmount; got != want {
		t.Errorf("probe Available: got %d, want %d (prior reject mutated state)", got, want)
	}
	if got, want := probe.Bank, startingBank-probeAmount; got != want {
		t.Errorf("probe Bank: got %d, want %d (prior reject mutated state)", got, want)
	}
}

// F6 — no live session. The account exists durably (EnsureAccount), but
// no StartSession has been called, so the shard's sessions map has no
// entry. The handler returns Present:false — a routine idle miss that
// the HTTP layer routes to Store.DurableTransfer. Never a client-visible
// 409 (P3-D8).
func TestTransfer_NoLiveSession_ReturnsPresentFalse(t *testing.T) {
	ownerPrefix := uniqueOwnerPrefix(t)
	runtime := requireRuntime(t, liveSessionBrokerConfig(10_000, 5_000))
	cleanupOwnerRows(t, runtime, ownerPrefix)

	accountID, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, ownerPrefix+"-owner")
	if err != nil {
		t.Fatalf("EnsureAccount: %v", err)
	}
	// NO StartSession — account is durable-only.

	res, err := runtime.Broker.Transfer(context.Background(), accountID, account.Available, account.Bank, 1_000)
	if err != nil {
		t.Fatalf("Transfer: unexpected transport error: %v", err)
	}
	if res.Present {
		t.Fatalf("Transfer: got Present=true, want Present=false (no live session)")
	}
	if res.Err != nil {
		t.Fatalf("Transfer: got Err=%v, want nil on Present:false path", res.Err)
	}
	if res.Available != 0 || res.Bank != 0 {
		t.Errorf("Transfer: got (Available=%d, Bank=%d), want (0, 0) on Present:false", res.Available, res.Bank)
	}
}

// F7 — concurrent Transfer + PlaceOrder on one account. Both commands
// hash to the same shard (one AccountID → one shard); the shard's serial
// loop dispatches them one at a time. The oracle is -race (the standing
// gate); correctness here is "both calls return without a data race."
// PlaceOrder is still an empty echo at this stage, so we only observe
// that it returned — not that it did anything.
func TestTransfer_ConcurrentWithPlaceOrder_SerializedOnShard(t *testing.T) {
	ownerPrefix := uniqueOwnerPrefix(t)
	runtime := requireRuntime(t, liveSessionBrokerConfig(10_000, 5_000))
	cleanupOwnerRows(t, runtime, ownerPrefix)

	accountID := installLiveSession(t, runtime, ownerPrefix)
	const iterations = 50

	var waitGroup sync.WaitGroup
	waitGroup.Add(2)

	transferErrs := make([]error, 0, iterations)
	orderErrs := make([]error, 0, iterations)
	var collectMu sync.Mutex

	go func() {
		defer waitGroup.Done()
		for iteration := 0; iteration < iterations; iteration++ {
			res, err := runtime.Broker.Transfer(context.Background(), accountID, account.Available, account.Bank, 1)
			collectMu.Lock()
			if err != nil {
				transferErrs = append(transferErrs, err)
			} else if res.Err != nil {
				transferErrs = append(transferErrs, res.Err)
			}
			collectMu.Unlock()
		}
	}()

	go func() {
		defer waitGroup.Done()
		for iteration := 0; iteration < iterations; iteration++ {
			if _, err := runtime.Broker.PlaceOrder(context.Background(), accountID, ownerPrefix+"-sess", funnel.OrderSpec{}); err != nil {
				collectMu.Lock()
				orderErrs = append(orderErrs, err)
				collectMu.Unlock()
			}
		}
	}()

	waitGroup.Wait()

	if len(transferErrs) != 0 {
		t.Fatalf("Transfer: %d errors under concurrent dispatch, first=%v", len(transferErrs), transferErrs[0])
	}
	if len(orderErrs) != 0 {
		t.Fatalf("PlaceOrder: %d errors under concurrent dispatch, first=%v", len(orderErrs), orderErrs[0])
	}
	// Final balance: pushed `iterations` coins Available→Bank; verify the
	// shard saw all `iterations` Transfer commands (and processed them in
	// some order consistent with the shard loop's serial dispatch).
	finalProbe, err := runtime.Broker.Transfer(context.Background(), accountID, account.Available, account.Bank, 0+1)
	if err != nil {
		t.Fatalf("final probe Transfer: %v", err)
	}
	if got, want := finalProbe.Available, int64(10_000-iterations-1); got != want {
		t.Errorf("final Available: got %d, want %d (some concurrent Transfer was lost)", got, want)
	}
	if got, want := finalProbe.Bank, int64(5_000+iterations+1); got != want {
		t.Errorf("final Bank: got %d, want %d (some concurrent Transfer was lost)", got, want)
	}
}

// F8 — determinism replay. Two fresh runtimes, two different accounts
// (one each — the runtime scopes the shard's sessions map), identical
// Transfer sequences applied to each → identical final (Available, Bank).
// Different accounts because reusing one owner across runtimes would
// collide on the UNIQUE (owner_type, owner_id) constraint and the second
// EnsureAccount would fall back to the first run's row, which carries
// post-mutation balances.
func TestTransfer_DeterministicReplay_IdenticalFinalBalances(t *testing.T) {
	const startingCapital int64 = 20_000
	const startingBank int64 = 10_000

	// A fixed, non-trivial command stream: a mix of pushes and pulls, some
	// of which the handler will accept and some may not depending on the
	// running balance. Determinism is "same input → same output," so we
	// care that the SEQUENCE is identical both runs, not that every
	// element succeeds.
	type command struct {
		from, to account.Bucket
		amount   int64
	}
	stream := []command{
		{account.Available, account.Bank, 1_000},
		{account.Bank, account.Available, 500},
		{account.Available, account.Bank, 7_500},
		{account.Bank, account.Available, 2_000},
		{account.Available, account.Bank, 100},
	}

	runOnce := func(subT *testing.T, suffix string) (int64, int64) {
		subT.Helper()
		ownerPrefix := uniqueOwnerPrefix(subT) + "-" + suffix
		runtime := requireRuntime(subT, liveSessionBrokerConfig(startingCapital, startingBank))
		cleanupOwnerRows(subT, runtime, ownerPrefix)
		accountID := installLiveSession(subT, runtime, ownerPrefix)

		var lastAvailable, lastBank int64 = startingCapital, startingBank
		for _, cmd := range stream {
			res, err := runtime.Broker.Transfer(context.Background(), accountID, cmd.from, cmd.to, cmd.amount)
			if err != nil {
				subT.Fatalf("Transfer transport error: %v", err)
			}
			if res.Present && res.Err == nil {
				lastAvailable, lastBank = res.Available, res.Bank
			}
			// Rejected (Err:set) or Present:false (impossible here — session
			// is live) → balances unchanged for this command.
		}
		return lastAvailable, lastBank
	}

	availableA, bankA := runOnce(t, "runA")
	availableB, bankB := runOnce(t, "runB")

	if availableA != availableB {
		t.Errorf("determinism: final Available diverged: runA=%d runB=%d", availableA, availableB)
	}
	if bankA != bankB {
		t.Errorf("determinism: final Bank diverged: runA=%d runB=%d", bankA, bankB)
	}
}
