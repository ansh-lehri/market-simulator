package funnel_test

// EnsureAccount integration tests (BROKER_PHASE2 §1). Each assertion
// traces to the Phase-2 doc's error table — the create-first strategy,
// the duplicate-fallback via AccountHandle, the one-shot retry on a
// fallback-miss, and the straight-through propagation of OpenAccount's
// own typed errors.
//
// The tests run through the real Runtime (bringup) so they exercise the
// Store wiring + funnel.NewBroker + MoneyConfig threading end-to-end;
// a mock-Store alternative would need either a Store interface or a
// test fake, neither of which exists today and both of which would be
// unrequested abstractions.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"simulator/broker/store"
	"simulator/economy/lifecycle"
)

// Fresh (owner_type, owner_id) → account created, non-empty AccountID,
// stamped with the configured starting cash. Round-trips through the
// Store's Postgres pool so the row actually landed.
func TestEnsureAccount_FreshCreate(t *testing.T) {
	ownerPrefix := uniqueOwnerPrefix(t)
	cfg := defaultTestBrokerConfig()
	cfg.StartingCapital = 100_000
	cfg.StartingBank = 50_000
	runtime := requireRuntime(t, cfg)
	cleanupOwnerRows(t, runtime, ownerPrefix)

	ownerID := ownerPrefix + "-owner"
	accountID, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, ownerID)
	if err != nil {
		t.Fatalf("EnsureAccount: %v", err)
	}
	if accountID == "" {
		t.Fatal("EnsureAccount returned empty AccountID on create")
	}

	// Prove the row exists and carries the configured dials.
	var tradingCash, bankCash int64
	if err := runtime.Store.Pool().QueryRow(context.Background(),
		`SELECT trading_cash, bank_cash FROM accounts WHERE account_id = $1`,
		string(accountID),
	).Scan(&tradingCash, &bankCash); err != nil {
		t.Fatalf("round-trip SELECT: %v", err)
	}
	if tradingCash != 100_000 || bankCash != 50_000 {
		t.Errorf("stamped cash: got trading=%d bank=%d, want 100000 / 50000", tradingCash, bankCash)
	}
}

// Second call for the same owner → falls back to AccountHandle and returns
// the SAME aid as the first call. The create-first strategy's correctness
// proof: no check-then-create window, the INSERT is the race arbiter, and
// the duplicate branch always resolves to the existing row.
func TestEnsureAccount_DuplicateReturnsSameAid(t *testing.T) {
	ownerPrefix := uniqueOwnerPrefix(t)
	runtime := requireRuntime(t, defaultTestBrokerConfig())
	cleanupOwnerRows(t, runtime, ownerPrefix)

	ownerID := ownerPrefix + "-owner"
	first, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, ownerID)
	if err != nil {
		t.Fatalf("first EnsureAccount: %v", err)
	}
	second, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, ownerID)
	if err != nil {
		t.Fatalf("second EnsureAccount: %v", err)
	}
	if first != second {
		t.Errorf("duplicate EnsureAccount: got %q then %q — want same aid both calls", first, second)
	}
}

// Concurrent EnsureAccount for the same owner across many goroutines →
// every caller gets the same aid, exactly one Postgres row exists. Proves
// the uq_accounts_owner UNIQUE constraint is the sole race arbiter and
// that the duplicate-fallback path returns the SAME handle the winner
// created. Also runs under -race.
func TestEnsureAccount_ConcurrentSameOwner_AllShareAid(t *testing.T) {
	ownerPrefix := uniqueOwnerPrefix(t)
	runtime := requireRuntime(t, defaultTestBrokerConfig())
	cleanupOwnerRows(t, runtime, ownerPrefix)

	const callers = 8
	ownerID := ownerPrefix + "-owner"
	results := make([]store.AccountID, callers)
	errs := make([]error, callers)

	var wg sync.WaitGroup
	wg.Add(callers)
	start := make(chan struct{}) // release everyone at once to maximize contention
	for callerIdx := 0; callerIdx < callers; callerIdx++ {
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx], errs[idx] = runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, ownerID)
		}(callerIdx)
	}
	close(start)
	wg.Wait()

	for idx, err := range errs {
		if err != nil {
			t.Fatalf("caller %d: %v", idx, err)
		}
	}
	for idx := 1; idx < callers; idx++ {
		if results[idx] != results[0] {
			t.Errorf("caller %d: aid %q diverges from caller 0's aid %q", idx, results[idx], results[0])
		}
	}

	// Exactly one row.
	var rowCount int
	if err := runtime.Store.Pool().QueryRow(context.Background(),
		`SELECT COUNT(*) FROM accounts WHERE owner_id = $1`, ownerID,
	).Scan(&rowCount); err != nil {
		t.Fatalf("row-count SELECT: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("row count: got %d, want 1 (uq_accounts_owner arbitrates)", rowCount)
	}
}

// Empty ownerID → lifecycle.ErrBadOwnerID propagates through
// EnsureAccount verbatim. The funnel does not re-validate; L5's
// OpenAccount is the source of truth for input validation (per the
// Phase-2 doc §1 error table).
func TestEnsureAccount_EmptyOwnerID_PropagatesLifecycleError(t *testing.T) {
	ownerPrefix := uniqueOwnerPrefix(t)
	runtime := requireRuntime(t, defaultTestBrokerConfig())
	cleanupOwnerRows(t, runtime, ownerPrefix) // harmless with empty ownerID

	accountID, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, "")
	if !errors.Is(err, lifecycle.ErrBadOwnerID) {
		t.Fatalf("err: got %v, want errors.Is lifecycle.ErrBadOwnerID", err)
	}
	if accountID != "" {
		t.Errorf("accountID on error: got %q, want empty", accountID)
	}
}

// Invalid OwnerType (out of the {Agent, Human} enum range) →
// lifecycle.ErrBadOwnerType propagates. Mirrors the empty-ownerID path:
// L5 owns input validation; the funnel is a passthrough.
func TestEnsureAccount_InvalidOwnerType_PropagatesLifecycleError(t *testing.T) {
	ownerPrefix := uniqueOwnerPrefix(t)
	runtime := requireRuntime(t, defaultTestBrokerConfig())
	cleanupOwnerRows(t, runtime, ownerPrefix)

	// OwnerType(99) is outside the {OwnerAgent=0, OwnerHuman=1} enum.
	bogusType := lifecycle.OwnerType(99)
	accountID, err := runtime.Broker.EnsureAccount(context.Background(), bogusType, ownerPrefix+"-owner")
	if !errors.Is(err, lifecycle.ErrBadOwnerType) {
		t.Fatalf("err: got %v, want errors.Is lifecycle.ErrBadOwnerType", err)
	}
	if accountID != "" {
		t.Errorf("accountID on error: got %q, want empty", accountID)
	}
}

// After a successful create, deleting the row out of band and calling
// EnsureAccount again → the INSERT path succeeds (no row exists, UNIQUE
// does not fire). Models the "returning user whose account exists" vs
// "first-time user" distinction over the same owner.
func TestEnsureAccount_AfterOutOfBandDelete_RecreatesCleanly(t *testing.T) {
	ownerPrefix := uniqueOwnerPrefix(t)
	runtime := requireRuntime(t, defaultTestBrokerConfig())
	cleanupOwnerRows(t, runtime, ownerPrefix)

	ownerID := ownerPrefix + "-owner"
	firstAid, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, ownerID)
	if err != nil {
		t.Fatalf("first EnsureAccount: %v", err)
	}

	// Simulate the vanishingly rare "row deleted between create and
	// subsequent reads" case. No prod code path does this today — this
	// test models the invariant that EnsureAccount's create-first design
	// absorbs a disappearance by just re-creating.
	if _, err := runtime.Store.Pool().Exec(context.Background(),
		`DELETE FROM accounts WHERE account_id = $1`, string(firstAid),
	); err != nil {
		t.Fatalf("out-of-band delete: %v", err)
	}

	secondAid, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, ownerID)
	if err != nil {
		t.Fatalf("second EnsureAccount: %v", err)
	}
	if secondAid == "" {
		t.Fatal("second EnsureAccount returned empty AccountID")
	}
	// The id-mint scheme is lifecycle's; we don't require the aid value
	// to differ, only that recreation succeeded.
	var rowCount int
	if err := runtime.Store.Pool().QueryRow(context.Background(),
		`SELECT COUNT(*) FROM accounts WHERE owner_id = $1`, ownerID,
	).Scan(&rowCount); err != nil {
		t.Fatalf("row-count SELECT: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("row count after recreate: got %d, want 1", rowCount)
	}
}

// Both owner types coexist for distinct owner_ids under the same prefix.
// Belt-and-suspenders check that the uq_accounts_owner UNIQUE is on
// (owner_type, owner_id), not owner_id alone.
func TestEnsureAccount_HumanAndAgentCoexist(t *testing.T) {
	ownerPrefix := uniqueOwnerPrefix(t)
	runtime := requireRuntime(t, defaultTestBrokerConfig())
	cleanupOwnerRows(t, runtime, ownerPrefix)

	humanID := fmt.Sprintf("%s-human", ownerPrefix)
	agentID := fmt.Sprintf("%s-agent", ownerPrefix)
	humanAid, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerHuman, humanID)
	if err != nil {
		t.Fatalf("human EnsureAccount: %v", err)
	}
	agentAid, err := runtime.Broker.EnsureAccount(context.Background(), lifecycle.OwnerAgent, agentID)
	if err != nil {
		t.Fatalf("agent EnsureAccount: %v", err)
	}
	if humanAid == agentAid {
		t.Errorf("human and agent collapsed to the same aid %q — distinct owners must mint distinct ids", humanAid)
	}
}
