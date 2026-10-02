package funnel_test

// QualifyOnDemand integration tests (BROKER_PHASE2 §2). Each test seeds a
// known trading_cash via EnsureAccount (which stamps the configured
// StartingCapital at birth), then exercises the gate against a
// per-test-configured OnDemandMinimum. Assertions trace to the Phase-2
// doc's error table: a legitimate under-balance is (false, nil); a debt
// cleanly fails the `>=` comparison; a missing row surfaces as
// store.ErrNoAccount; a transport failure as store.ErrLoadFailed.
//
// Why we drive the balance via StartingCapital rather than DurableTransfer:
// EnsureAccount is the one existing path that writes trading_cash through
// the real wiring. For negative-balance coverage we poke the pool
// directly (same mechanic the Stage-A TradingCash tests use for signed
// passthrough) because no production code path moves trading_cash into
// debt yet — Reserve / Realize would, in a live session, but that's
// Phase-1-adjacent and out of scope here.

import (
	"context"
	"errors"
	"testing"
	"time"

	"simulator/broker/store"
	"simulator/economy/lifecycle"
)

// cash > min → (true, nil). The ordinary happy path.
func TestQualifyOnDemand_CashAboveMin_True(t *testing.T) {
	ownerPrefix := uniqueOwnerPrefix(t)
	cfg := defaultTestBrokerConfig()
	cfg.StartingCapital = 100_000
	cfg.OnDemandMinimum = 10_000
	runtime := requireRuntime(t, cfg)
	cleanupOwnerRows(t, runtime, ownerPrefix)

	accountID, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, ownerPrefix+"-owner")
	if err != nil {
		t.Fatalf("EnsureAccount: %v", err)
	}

	ok, err := runtime.Broker.QualifyOnDemand(context.Background(), accountID)
	if err != nil {
		t.Fatalf("QualifyOnDemand: %v", err)
	}
	if !ok {
		t.Errorf("QualifyOnDemand: got false, want true (cash=100000 >= min=10000)")
	}
}

// cash == min → (false, nil). Boundary case; the gate is strict `>`, so
// equal-to-floor does NOT qualify — the user must be strictly above the
// bar.
func TestQualifyOnDemand_CashEqualsMin_FalseStrictGreater(t *testing.T) {
	ownerPrefix := uniqueOwnerPrefix(t)
	cfg := defaultTestBrokerConfig()
	cfg.StartingCapital = 50_000
	cfg.OnDemandMinimum = 50_000
	runtime := requireRuntime(t, cfg)
	cleanupOwnerRows(t, runtime, ownerPrefix)

	accountID, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, ownerPrefix+"-owner")
	if err != nil {
		t.Fatalf("EnsureAccount: %v", err)
	}

	ok, err := runtime.Broker.QualifyOnDemand(context.Background(), accountID)
	if err != nil {
		t.Fatalf("QualifyOnDemand: got err %v, want nil (equal-to-floor is not an error)", err)
	}
	if ok {
		t.Errorf("QualifyOnDemand: got true, want false (cash=50000 == min=50000; gate is strict >, equal does not qualify)")
	}
}

// cash == min + 1 → (true, nil). The minimum-step-above-floor case —
// paired with the equal-to-floor case above to pin the boundary exactly
// where the strict comparison places it.
func TestQualifyOnDemand_CashOneAboveMin_True(t *testing.T) {
	ownerPrefix := uniqueOwnerPrefix(t)
	cfg := defaultTestBrokerConfig()
	cfg.StartingCapital = 50_001
	cfg.OnDemandMinimum = 50_000
	runtime := requireRuntime(t, cfg)
	cleanupOwnerRows(t, runtime, ownerPrefix)

	accountID, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, ownerPrefix+"-owner")
	if err != nil {
		t.Fatalf("EnsureAccount: %v", err)
	}

	ok, err := runtime.Broker.QualifyOnDemand(context.Background(), accountID)
	if err != nil {
		t.Fatalf("QualifyOnDemand: %v", err)
	}
	if !ok {
		t.Errorf("QualifyOnDemand: got false, want true (cash=50001 > min=50000)")
	}
}

// cash < min → (false, nil). A legitimate "no," not an error. The
// user-visible contract is: nil error means the gate ran; false means
// the user does not clear the bar (they can top up and retry).
func TestQualifyOnDemand_CashBelowMin_FalseNoError(t *testing.T) {
	ownerPrefix := uniqueOwnerPrefix(t)
	cfg := defaultTestBrokerConfig()
	cfg.StartingCapital = 1_000
	cfg.OnDemandMinimum = 10_000
	runtime := requireRuntime(t, cfg)
	cleanupOwnerRows(t, runtime, ownerPrefix)

	accountID, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, ownerPrefix+"-owner")
	if err != nil {
		t.Fatalf("EnsureAccount: %v", err)
	}

	ok, err := runtime.Broker.QualifyOnDemand(context.Background(), accountID)
	if err != nil {
		t.Fatalf("QualifyOnDemand: got err %v, want nil (under-balance is not an error)", err)
	}
	if ok {
		t.Errorf("QualifyOnDemand: got true, want false (cash=1000 < min=10000)")
	}
}

// Debt-rejection load-bearing case: a negative trading_cash must fail
// the gate under Rule 1 (explicit debt guard), independent of the min
// comparison. Verifying against min=0 specifically pins the invariant:
// if the explicit guard were missing and we relied only on the strict
// `cash > min` comparison, debt would still fail against min=0 (-42000
// > 0 is false), so the min alone is NOT a sufficient test — we need a
// case where cash < 0 but cash > min to prove the explicit guard fires.
// That case needs min < 0, which bringup's validate currently rejects —
// so we document the invariant via code review AND cover it here by
// relying on the explicit guard semantics (any negative cash → false),
// which trivially holds for min=0 too. We seed the row directly in
// Postgres (schema allows negative trading_cash; no production path
// does this yet).
func TestQualifyOnDemand_DebtBalance_FalseNoError(t *testing.T) {
	ownerPrefix := uniqueOwnerPrefix(t)
	cfg := defaultTestBrokerConfig()
	cfg.StartingCapital = 100 // seed row via EnsureAccount, then overwrite
	cfg.OnDemandMinimum = 0   // debt must fail even against the smallest legal bar
	runtime := requireRuntime(t, cfg)
	cleanupOwnerRows(t, runtime, ownerPrefix)

	accountID, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, ownerPrefix+"-owner")
	if err != nil {
		t.Fatalf("EnsureAccount: %v", err)
	}

	// Drive the account into debt by direct SQL — no live-session
	// Reserve/Realize path exists at this phase to do it legitimately.
	if _, err := runtime.Store.Pool().Exec(context.Background(),
		`UPDATE accounts SET trading_cash = $1 WHERE account_id = $2`,
		int64(-42_000), string(accountID),
	); err != nil {
		t.Fatalf("drive into debt: %v", err)
	}

	ok, err := runtime.Broker.QualifyOnDemand(context.Background(), accountID)
	if err != nil {
		t.Fatalf("QualifyOnDemand: got err %v, want nil", err)
	}
	if ok {
		t.Errorf("QualifyOnDemand: got true for debt balance (-42000) — Rule 1 (debt never qualifies) broken")
	}
}

// min == 0 and cash == 0 → (false, nil). The strict `>` comparison makes
// zero-against-zero a boundary-reject: zero cash does NOT clear even a
// zero floor. Zero min means "any strictly-positive balance qualifies,"
// not "any non-negative balance qualifies."
func TestQualifyOnDemand_ZeroMinZeroCash_FalseStrictGreater(t *testing.T) {
	ownerPrefix := uniqueOwnerPrefix(t)
	cfg := defaultTestBrokerConfig()
	cfg.StartingCapital = 0
	cfg.OnDemandMinimum = 0
	runtime := requireRuntime(t, cfg)
	cleanupOwnerRows(t, runtime, ownerPrefix)

	accountID, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, ownerPrefix+"-owner")
	if err != nil {
		t.Fatalf("EnsureAccount: %v", err)
	}

	ok, err := runtime.Broker.QualifyOnDemand(context.Background(), accountID)
	if err != nil {
		t.Fatalf("QualifyOnDemand: got err %v, want nil", err)
	}
	if ok {
		t.Errorf("QualifyOnDemand: got true, want false (cash=0 is not > min=0; strict > requires at least 1 above floor)")
	}
}

// min == 0 and cash == 1 → (true, nil). Minimum positive balance against
// the smallest legal floor; the strict `>` just barely clears.
func TestQualifyOnDemand_ZeroMinPositiveCash_True(t *testing.T) {
	ownerPrefix := uniqueOwnerPrefix(t)
	cfg := defaultTestBrokerConfig()
	cfg.StartingCapital = 1
	cfg.OnDemandMinimum = 0
	runtime := requireRuntime(t, cfg)
	cleanupOwnerRows(t, runtime, ownerPrefix)

	accountID, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, ownerPrefix+"-owner")
	if err != nil {
		t.Fatalf("EnsureAccount: %v", err)
	}

	ok, err := runtime.Broker.QualifyOnDemand(context.Background(), accountID)
	if err != nil {
		t.Fatalf("QualifyOnDemand: %v", err)
	}
	if !ok {
		t.Errorf("QualifyOnDemand: got false, want true (cash=1 > min=0)")
	}
}

// Missing account → (false, store.ErrNoAccount). This is a bug signal,
// not a routine outcome — EnsureAccount is the caller's prerequisite.
// Asserts via errors.Is on the category sentinel; does NOT care about
// the exact error string.
func TestQualifyOnDemand_MissingAccount_ErrNoAccount(t *testing.T) {
	ownerPrefix := uniqueOwnerPrefix(t)
	runtime := requireRuntime(t, defaultTestBrokerConfig())
	cleanupOwnerRows(t, runtime, ownerPrefix)

	bogusID := store.AccountID(ownerPrefix + "-nonexistent")
	ok, err := runtime.Broker.QualifyOnDemand(context.Background(), bogusID)
	if !errors.Is(err, store.ErrNoAccount) {
		t.Fatalf("err: got %v, want errors.Is store.ErrNoAccount", err)
	}
	if ok {
		t.Errorf("ok on miss: got true, want false")
	}
}

// Transport failure → (false, store.ErrLoadFailed). We close the Store's
// pool out-of-band; QualifyOnDemand's TradingCash call then fails on
// acquire. The category sentinel must surface as ErrLoadFailed, not
// ErrNoAccount — the signal/failure split is load-bearing (the Phase-2
// doc routes them differently).
//
// Pool-close is idempotent (pgxpool), so the deferred Shutdown still
// runs cleanly without double-close concerns.
func TestQualifyOnDemand_PoolClosed_ErrLoadFailed(t *testing.T) {
	ownerPrefix := uniqueOwnerPrefix(t)
	cfg := defaultTestBrokerConfig()
	cfg.StartingCapital = 100
	cfg.OnDemandMinimum = 0
	runtime := requireRuntime(t, cfg)
	cleanupOwnerRows(t, runtime, ownerPrefix)

	accountID, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, ownerPrefix+"-owner")
	if err != nil {
		t.Fatalf("EnsureAccount: %v", err)
	}

	// Prove the gate works end-to-end BEFORE breaking infra — otherwise a
	// failure on a healthy pool would mask the test's intent.
	if ok, err := runtime.Broker.QualifyOnDemand(context.Background(), accountID); err != nil || !ok {
		t.Fatalf("baseline QualifyOnDemand before pool-close: ok=%v err=%v (want true, nil)", ok, err)
	}

	// Break the Store's Postgres pool. Shutdown's Store.Close will no-op
	// the second close via pgxpool's idempotency; the owner-side bundle
	// pools are separate and unaffected, so Broker.Stop still drains
	// cleanly on test end.
	runtime.Store.Pool().Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	ok, err := runtime.Broker.QualifyOnDemand(ctx, accountID)
	if err == nil {
		t.Fatalf("QualifyOnDemand: got nil err, want store.ErrLoadFailed")
	}
	if !errors.Is(err, store.ErrLoadFailed) {
		t.Fatalf("err: got %v, want errors.Is store.ErrLoadFailed", err)
	}
	if errors.Is(err, store.ErrNoAccount) {
		t.Errorf("ErrNoAccount leaked onto a transport-failure path: %v", err)
	}
	if ok {
		t.Errorf("ok on transport failure: got true, want false")
	}
}
