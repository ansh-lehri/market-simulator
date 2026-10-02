package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TradingCash integration tests. Each assertion traces to BROKER_PHASE2
// §2 — the gate consumes a raw signed value and errors.Is on category
// sentinels; everything below mirrors that contract against a live Postgres.
//
// TradingCash is a single-column pool read; it does not touch the Store's
// redis or lifecycle handles. These tests build the Store with
// newPoolOnlyStore for that reason — the helper documents which handles
// the method under test actually uses.

// Positive balance round-trip: seed a known trading_cash, read it back.
func TestTradingCash_PositiveBalance(t *testing.T) {
	pool := requirePostgresPool(t)
	ownerPrefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, ownerPrefix)

	accountID := fmt.Sprintf("%s-acc", ownerPrefix)
	seedAccount(t, pool, accountID, "human", ownerPrefix+"-owner", 123_456 /*trading*/, 50_000 /*bank*/)

	store := newPoolOnlyStore(pool)
	cash, err := store.TradingCash(context.Background(), AccountID(accountID))
	if err != nil {
		t.Fatalf("TradingCash: %v", err)
	}
	if cash != 123_456 {
		t.Errorf("TradingCash: got %d, want 123456", cash)
	}
}

// Zero balance is a legitimate state (boundary case for QualifyOnDemand
// when OnDemandMinimum is 0). Must return cleanly, not as a miss.
func TestTradingCash_ZeroBalance(t *testing.T) {
	pool := requirePostgresPool(t)
	ownerPrefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, ownerPrefix)

	accountID := fmt.Sprintf("%s-acc", ownerPrefix)
	seedAccount(t, pool, accountID, "human", ownerPrefix+"-owner", 0, 0)

	store := newPoolOnlyStore(pool)
	cash, err := store.TradingCash(context.Background(), AccountID(accountID))
	if err != nil {
		t.Fatalf("TradingCash: %v", err)
	}
	if cash != 0 {
		t.Errorf("TradingCash: got %d, want 0", cash)
	}
}

// Negative balance is legal in trading_cash (signed column; no CHECK >= 0).
// This is load-bearing for QualifyOnDemand: a user in debt must cleanly
// fail the `>= OnDemandMinimum` comparison, which requires the raw signed
// value coming back from this method.
func TestTradingCash_NegativeBalance_SignedPassthrough(t *testing.T) {
	pool := requirePostgresPool(t)
	ownerPrefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, ownerPrefix)

	accountID := fmt.Sprintf("%s-acc", ownerPrefix)
	// seedAccount uses a direct INSERT; schema allows negative trading_cash.
	seedAccount(t, pool, accountID, "human", ownerPrefix+"-owner", -42_000 /*debt*/, 1_000 /*bank*/)

	store := newPoolOnlyStore(pool)
	cash, err := store.TradingCash(context.Background(), AccountID(accountID))
	if err != nil {
		t.Fatalf("TradingCash: %v", err)
	}
	if cash != -42_000 {
		t.Errorf("TradingCash: got %d, want -42000 (signed passthrough)", cash)
	}
}

// Missing row → ErrNoAccount via errors.Is on the category sentinel.
// Not a failure — the Phase-2 doc §2 maps this to a bug signal
// (QualifyOnDemand's caller must have run EnsureAccount first), not a
// transport error.
func TestTradingCash_MissingAccount_ErrNoAccount(t *testing.T) {
	pool := requirePostgresPool(t)
	ownerPrefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, ownerPrefix) // scheduled; harmless even with no rows

	store := newPoolOnlyStore(pool)
	cash, err := store.TradingCash(context.Background(), AccountID(ownerPrefix+"-does-not-exist"))
	if !errors.Is(err, ErrNoAccount) {
		t.Fatalf("err: got %v, want errors.Is ErrNoAccount", err)
	}
	if cash != 0 {
		t.Errorf("cash on miss: got %d, want 0", cash)
	}
}

// Pool closed before the call → ErrLoadFailed via errors.Is on the
// category sentinel. Proves non-ErrNoRows driver failures route to the
// "transport failure" bucket, keeping the signal/failure split clean.
func TestTradingCash_PoolClosed_ErrLoadFailed(t *testing.T) {
	pool := requirePostgresPool(t)
	ownerPrefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, ownerPrefix)

	// Seed a real account so a successful read is possible on a healthy
	// pool — we are testing the transport failure, not the "missing row"
	// path.
	accountID := fmt.Sprintf("%s-acc", ownerPrefix)
	seedAccount(t, pool, accountID, "human", ownerPrefix+"-owner", 100, 0)

	store := newPoolOnlyStore(pool)
	pool.Close() // simulate pool-wide infra failure

	// Use a short-timeout ctx so the test does not stall on retries.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	cash, err := store.TradingCash(ctx, AccountID(accountID))
	if err == nil {
		t.Fatalf("TradingCash: got nil err, want ErrLoadFailed")
	}
	if !errors.Is(err, ErrLoadFailed) {
		t.Fatalf("err: got %v, want errors.Is ErrLoadFailed", err)
	}
	if errors.Is(err, ErrNoAccount) {
		t.Errorf("ErrNoAccount leaked onto a transport-failure path: %v", err)
	}
	if cash != 0 {
		t.Errorf("cash on failure: got %d, want 0", cash)
	}
}
