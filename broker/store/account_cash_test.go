package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// AccountCash integration tests. Mirrors TradingCash's structure since
// AccountCash reuses the same single-row-trip pattern — same error
// contract, same transport failure routing — but reads two columns
// instead of one. Each case traces to:
//
//   - the pre-session cash view requirement: a signed-in user whose
//     account has not joined a market yet must still see both cash
//     columns;
//   - the Store's "zero writes, no shaping" rule: raw int64 pair out,
//     ErrNoAccount on miss, ErrLoadFailed on transport.
//
// AccountCash is a single-pool read; the Store under test is built with
// newPoolOnlyStore for that reason — the helper names which handles the
// method under test actually uses.

// Positive balance round-trip: seed trading + bank, read both back.
func TestAccountCash_PositiveBalances(t *testing.T) {
	pool := requirePostgresPool(t)
	ownerPrefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, ownerPrefix)

	accountID := fmt.Sprintf("%s-acc", ownerPrefix)
	seedAccount(t, pool, accountID, "human", ownerPrefix+"-owner", 1_234_567 /*trading*/, 500_000 /*bank*/)

	store := newPoolOnlyStore(pool)
	tradingCash, bankCash, err := store.AccountCash(context.Background(), AccountID(accountID))
	if err != nil {
		t.Fatalf("AccountCash: %v", err)
	}
	if tradingCash != 1_234_567 {
		t.Errorf("trading_cash: got %d, want 1234567", tradingCash)
	}
	if bankCash != 500_000 {
		t.Errorf("bank_cash: got %d, want 500000", bankCash)
	}
}

// Zero on both columns is a legitimate state (fresh account with zero
// starting-capital config, boundary case for the pre-session view).
// Must return cleanly, not as a miss.
func TestAccountCash_ZeroBalances(t *testing.T) {
	pool := requirePostgresPool(t)
	ownerPrefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, ownerPrefix)

	accountID := fmt.Sprintf("%s-acc", ownerPrefix)
	seedAccount(t, pool, accountID, "human", ownerPrefix+"-owner", 0, 0)

	store := newPoolOnlyStore(pool)
	tradingCash, bankCash, err := store.AccountCash(context.Background(), AccountID(accountID))
	if err != nil {
		t.Fatalf("AccountCash: %v", err)
	}
	if tradingCash != 0 {
		t.Errorf("trading_cash: got %d, want 0", tradingCash)
	}
	if bankCash != 0 {
		t.Errorf("bank_cash: got %d, want 0", bankCash)
	}
}

// Signed trading_cash passthrough. trading_cash is a signed BIGINT
// (migration 0001: no CHECK >= 0; debt is legal post-fill). The pair
// read must preserve the sign so a client in debt renders truthfully
// — zeroing a negative value would silently hide the loss.
func TestAccountCash_NegativeTradingSignedPassthrough(t *testing.T) {
	pool := requirePostgresPool(t)
	ownerPrefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, ownerPrefix)

	accountID := fmt.Sprintf("%s-acc", ownerPrefix)
	seedAccount(t, pool, accountID, "human", ownerPrefix+"-owner", -42_000 /*debt*/, 1_000 /*bank*/)

	store := newPoolOnlyStore(pool)
	tradingCash, bankCash, err := store.AccountCash(context.Background(), AccountID(accountID))
	if err != nil {
		t.Fatalf("AccountCash: %v", err)
	}
	if tradingCash != -42_000 {
		t.Errorf("trading_cash: got %d, want -42000 (signed passthrough)", tradingCash)
	}
	if bankCash != 1_000 {
		t.Errorf("bank_cash: got %d, want 1000", bankCash)
	}
}

// Missing row → ErrNoAccount via errors.Is on the category sentinel.
// The HTTP handler maps this to 404 (singular object, missing really
// means not-found; see the handler's status-map comment).
func TestAccountCash_MissingAccount_ErrNoAccount(t *testing.T) {
	pool := requirePostgresPool(t)
	ownerPrefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, ownerPrefix) // scheduled; harmless even with no rows

	store := newPoolOnlyStore(pool)
	tradingCash, bankCash, err := store.AccountCash(context.Background(), AccountID(ownerPrefix+"-does-not-exist"))
	if !errors.Is(err, ErrNoAccount) {
		t.Fatalf("err: got %v, want errors.Is ErrNoAccount", err)
	}
	if tradingCash != 0 || bankCash != 0 {
		t.Errorf("cash on miss: got (%d, %d), want (0, 0)", tradingCash, bankCash)
	}
}

// Pool closed before the call → ErrLoadFailed via errors.Is on the
// category sentinel. Proves non-ErrNoRows driver failures route to the
// transport-failure bucket (not ErrNoAccount), which keeps the
// signal/failure split clean at the HTTP boundary (503 vs 404).
func TestAccountCash_PoolClosed_ErrLoadFailed(t *testing.T) {
	pool := requirePostgresPool(t)
	ownerPrefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, ownerPrefix)

	accountID := fmt.Sprintf("%s-acc", ownerPrefix)
	seedAccount(t, pool, accountID, "human", ownerPrefix+"-owner", 100, 0)

	store := newPoolOnlyStore(pool)
	pool.Close() // simulate pool-wide infra failure

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	tradingCash, bankCash, err := store.AccountCash(ctx, AccountID(accountID))
	if err == nil {
		t.Fatalf("AccountCash: got nil err, want ErrLoadFailed")
	}
	if !errors.Is(err, ErrLoadFailed) {
		t.Fatalf("err: got %v, want errors.Is ErrLoadFailed", err)
	}
	if errors.Is(err, ErrNoAccount) {
		t.Errorf("ErrNoAccount leaked onto a transport-failure path: %v", err)
	}
	if tradingCash != 0 || bankCash != 0 {
		t.Errorf("cash on failure: got (%d, %d), want (0, 0)", tradingCash, bankCash)
	}
}
