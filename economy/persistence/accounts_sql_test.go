package persistence

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// buildStore returns a store bound to the given tx-wrapped Postgres path.
// Redis+adapter are unused by accounts tests, so we pass nil / IndiaCostAdapter{}.
func buildStore(tx dbConn) *store {
	return newStore(tx, nil, IndiaCostAdapter{}, Config{}.withDefaults())
}

func TestLoadAccountRoundTripPositive(t *testing.T) {
	tx := beginTestTx(t)
	accountID := fmt.Sprintf("acct-load-%d", time.Now().UnixNano())
	seedAccount(t, tx, accountID, 1_000_000, 250_000)

	got, err := buildStore(tx).loadAccount(context.Background(), accountID)
	if err != nil {
		t.Fatalf("loadAccount: %v", err)
	}
	if got.AccountID != accountID {
		t.Errorf("AccountID: got %q, want %q", got.AccountID, accountID)
	}
	if got.OwnerType != "agent" {
		t.Errorf("OwnerType: got %q, want \"agent\"", got.OwnerType)
	}
	if got.TradingCash != 1_000_000 {
		t.Errorf("TradingCash: got %d, want 1_000_000", got.TradingCash)
	}
	if got.BankCash != 250_000 {
		t.Errorf("BankCash: got %d, want 250_000", got.BankCash)
	}
	if got.DripActive {
		t.Errorf("DripActive: default was DEFAULT FALSE, got true")
	}
	if got.DripProgress != nil {
		t.Errorf("DripProgress: default was NULL, got %v", *got.DripProgress)
	}
}

func TestLoadAccountMissingReturnsErrNoAccount(t *testing.T) {
	tx := beginTestTx(t)
	_, err := buildStore(tx).loadAccount(context.Background(), "does-not-exist")
	if !errors.Is(err, ErrNoAccount) {
		t.Fatalf("expected ErrNoAccount, got %v", err)
	}
}

func TestSaveAccountRoundTripsSignedTradingCash(t *testing.T) {
	// Option B / MON-7: trading_cash is signed; debt is a NEGATIVE value with
	// no clamp. Round-trip via saveAccount → loadAccount.
	tx := beginTestTx(t)
	accountID := fmt.Sprintf("acct-signed-%d", time.Now().UnixNano())
	seedAccount(t, tx, accountID, 0, 0)

	updated := DurableAccount{
		AccountID:   accountID,
		OwnerType:   "agent",
		OwnerID:     accountID,
		TradingCash: -777_777, // debt — signed
		BankCash:    12345,
		DripActive:  true,
	}
	if err := buildStore(tx).saveAccount(context.Background(), updated); err != nil {
		t.Fatalf("saveAccount: %v", err)
	}

	got, err := buildStore(tx).loadAccount(context.Background(), accountID)
	if err != nil {
		t.Fatalf("loadAccount: %v", err)
	}
	if got.TradingCash != -777_777 {
		t.Errorf("TradingCash: got %d, want -777_777 (signed, no clamp)", got.TradingCash)
	}
	if got.BankCash != 12345 {
		t.Errorf("BankCash: got %d, want 12345", got.BankCash)
	}
	if !got.DripActive {
		t.Errorf("DripActive: got false, want true")
	}
}

func TestSaveAccountRoundTripsDripProgress(t *testing.T) {
	// DripProgress is nullable: nil ↔ NULL; non-nil round-trips.
	tx := beginTestTx(t)
	accountID := fmt.Sprintf("acct-drip-%d", time.Now().UnixNano())
	seedAccount(t, tx, accountID, 0, 0)

	dripValue := int64(4200)
	updated := DurableAccount{
		AccountID:    accountID,
		OwnerType:    "agent",
		OwnerID:      accountID,
		DripActive:   true,
		DripProgress: &dripValue,
	}
	if err := buildStore(tx).saveAccount(context.Background(), updated); err != nil {
		t.Fatalf("saveAccount (non-nil drip): %v", err)
	}

	got, err := buildStore(tx).loadAccount(context.Background(), accountID)
	if err != nil {
		t.Fatalf("loadAccount: %v", err)
	}
	if got.DripProgress == nil {
		t.Fatalf("DripProgress was nil after saving 4200")
	}
	if *got.DripProgress != 4200 {
		t.Errorf("DripProgress: got %d, want 4200", *got.DripProgress)
	}

	// Now flip back to NULL.
	updated.DripProgress = nil
	updated.DripActive = false
	if err := buildStore(tx).saveAccount(context.Background(), updated); err != nil {
		t.Fatalf("saveAccount (nil drip): %v", err)
	}
	got, err = buildStore(tx).loadAccount(context.Background(), accountID)
	if err != nil {
		t.Fatalf("loadAccount after nil drip: %v", err)
	}
	if got.DripProgress != nil {
		t.Errorf("DripProgress: expected nil after clearing, got %v", *got.DripProgress)
	}
}

func TestSaveAccountMissingRowReturnsErrNoAccount(t *testing.T) {
	tx := beginTestTx(t)
	err := buildStore(tx).saveAccount(context.Background(), DurableAccount{
		AccountID:   "does-not-exist",
		OwnerType:   "agent",
		OwnerID:     "does-not-exist",
		TradingCash: 100,
	})
	if !errors.Is(err, ErrNoAccount) {
		t.Fatalf("expected ErrNoAccount, got %v", err)
	}
}

func TestSaveAccountBankCashNonNegConstraint(t *testing.T) {
	// ck_bank_nonneg (schema §2.1) rejects bank_cash < 0. Only trading_cash
	// is deliberately unbounded.
	tx := beginTestTx(t)
	accountID := fmt.Sprintf("acct-neg-bank-%d", time.Now().UnixNano())
	seedAccount(t, tx, accountID, 0, 0)

	err := buildStore(tx).saveAccount(context.Background(), DurableAccount{
		AccountID: accountID,
		OwnerType: "agent",
		OwnerID:   accountID,
		BankCash:  -1,
	})
	if err == nil {
		t.Fatalf("expected constraint violation on negative bank_cash, got nil")
	}
	// The tx is now aborted; further calls in this outer tx would fail,
	// but rollback on cleanup restores a clean baseline.
}

func TestOpenAccountFallbackGeneratesIDWhenEmpty(t *testing.T) {
	// Empty AccountID → persistence mints a 128-bit hex fallback and
	// returns it. Round-trip: loadAccount reads back what was written.
	tx := beginTestTx(t)
	ownerID := fmt.Sprintf("owner-open-%d", time.Now().UnixNano())

	sto := buildStore(tx)
	accountID, err := sto.openAccount(context.Background(), DurableAccount{
		OwnerType:   "agent",
		OwnerID:     ownerID,
		TradingCash: 5_000_000,
		BankCash:    100_000,
		DripActive:  false,
		// AccountID: "" (empty) → generator kicks in
		// DripProgress: nil (birth default)
	})
	if err != nil {
		t.Fatalf("openAccount: %v", err)
	}
	if accountID == "" {
		t.Fatalf("openAccount returned empty AccountID (generator did not fire)")
	}
	if len(accountID) != 32 {
		t.Errorf("generated AccountID: len %d, want 32 hex chars", len(accountID))
	}

	got, err := sto.loadAccount(context.Background(), accountID)
	if err != nil {
		t.Fatalf("loadAccount %s: %v", accountID, err)
	}
	if got.AccountID != accountID {
		t.Errorf("AccountID: got %q, want %q", got.AccountID, accountID)
	}
	if got.OwnerType != "agent" {
		t.Errorf("OwnerType: got %q, want \"agent\"", got.OwnerType)
	}
	if got.OwnerID != ownerID {
		t.Errorf("OwnerID: got %q, want %q", got.OwnerID, ownerID)
	}
	if got.TradingCash != 5_000_000 {
		t.Errorf("TradingCash: got %d, want 5_000_000", got.TradingCash)
	}
	if got.BankCash != 100_000 {
		t.Errorf("BankCash: got %d, want 100_000", got.BankCash)
	}
	if got.DripActive {
		t.Errorf("DripActive: got true, want false (birth default)")
	}
	if got.DripProgress != nil {
		t.Errorf("DripProgress: got %v, want nil (birth default)", *got.DripProgress)
	}
}

func TestOpenAccountAcceptsCallerSuppliedID(t *testing.T) {
	// Caller-supplied AccountID (any unique TEXT — scheme-prefixed,
	// deterministic test id, upstream-generated) is used as-is; the
	// generator does NOT override it. This covers callers that need a
	// specific id format their own layer cares about.
	tx := beginTestTx(t)
	ownerID := fmt.Sprintf("owner-supplied-%d", time.Now().UnixNano())
	callerID := fmt.Sprintf("agent_custom_%d", time.Now().UnixNano())

	sto := buildStore(tx)
	returnedID, err := sto.openAccount(context.Background(), DurableAccount{
		AccountID:   callerID,
		OwnerType:   "agent",
		OwnerID:     ownerID,
		TradingCash: 250_000,
		BankCash:    0,
	})
	if err != nil {
		t.Fatalf("openAccount with caller id: %v", err)
	}
	if returnedID != callerID {
		t.Fatalf("openAccount returned %q, want caller-supplied %q", returnedID, callerID)
	}

	got, err := sto.loadAccount(context.Background(), callerID)
	if err != nil {
		t.Fatalf("loadAccount %s: %v", callerID, err)
	}
	if got.AccountID != callerID {
		t.Errorf("AccountID in DB: got %q, want %q", got.AccountID, callerID)
	}
	if got.TradingCash != 250_000 {
		t.Errorf("TradingCash: got %d, want 250_000", got.TradingCash)
	}
}

func TestOpenAccountUniqueOwnerReturnsErrAccountExists(t *testing.T) {
	// UNIQUE-as-arbiter (LAYER_5_2_LIFECYCLE_FLOWS §2): a second openAccount
	// for the same (owner_type, owner_id) surfaces the uq_accounts_owner
	// violation as ErrAccountExists — the caller (agent re-spawn / human
	// re-login) treats it as "look up the existing handle," not fatal.
	//
	// This test uses a savepoint pattern: the first insert succeeds on the
	// outer tx, then a nested tx wraps the second insert so the constraint
	// violation aborts only the savepoint, leaving the outer tx usable
	// (otherwise cleanup rollback still works, but the assertion path is
	// cleaner).
	tx := beginTestTx(t)
	ownerID := fmt.Sprintf("owner-dup-%d", time.Now().UnixNano())
	sto := buildStore(tx)

	firstID, err := sto.openAccount(context.Background(), DurableAccount{
		OwnerType:   "agent",
		OwnerID:     ownerID,
		TradingCash: 100,
		BankCash:    0,
	})
	if err != nil {
		t.Fatalf("first openAccount: %v", err)
	}
	if firstID == "" {
		t.Fatalf("first openAccount returned empty id")
	}

	// Second insert for the same owner must return ErrAccountExists.
	// The outer tx aborts on the constraint fire; that's OK because
	// cleanup rolls it back and this is the last store call in the test.
	_, err = sto.openAccount(context.Background(), DurableAccount{
		OwnerType:   "agent",
		OwnerID:     ownerID,
		TradingCash: 200,
		BankCash:    0,
	})
	if !errors.Is(err, ErrAccountExists) {
		t.Fatalf("duplicate openAccount: err = %v, want wraps ErrAccountExists", err)
	}
}

func TestOpenAccountThenSaveRoundTrip(t *testing.T) {
	// End-to-end lifecycle-shaped scenario: openAccount at birth → mutate
	// TradingCash / DripActive via saveAccount → loadAccount returns the
	// updated durable state. Confirms Open and Save cooperate on the same
	// row across the two SQL statements.
	tx := beginTestTx(t)
	ownerID := fmt.Sprintf("owner-open-save-%d", time.Now().UnixNano())
	sto := buildStore(tx)

	accountID, err := sto.openAccount(context.Background(), DurableAccount{
		OwnerType:   "agent",
		OwnerID:     ownerID,
		TradingCash: 1_000_000,
		BankCash:    50_000,
	})
	if err != nil {
		t.Fatalf("openAccount: %v", err)
	}

	// Simulate a lifecycle save (drives TradingCash negative — signed round-trip).
	updated := DurableAccount{
		AccountID:   accountID,
		OwnerType:   "agent",
		OwnerID:     ownerID,
		TradingCash: -50_000, // debt (Option B)
		BankCash:    50_000,
		DripActive:  true,
	}
	if err := sto.saveAccount(context.Background(), updated); err != nil {
		t.Fatalf("saveAccount: %v", err)
	}

	got, err := sto.loadAccount(context.Background(), accountID)
	if err != nil {
		t.Fatalf("loadAccount: %v", err)
	}
	if got.TradingCash != -50_000 {
		t.Errorf("TradingCash after save: got %d, want -50_000 (signed, no clamp)", got.TradingCash)
	}
	if !got.DripActive {
		t.Errorf("DripActive after save: got false, want true")
	}
}

func TestSeedAccountUniqueOwnerConstraint(t *testing.T) {
	// uq_accounts_owner (schema §2.1): only one account per (owner_type,
	// owner_id) pair. seedAccount uses account_id == owner_id so a duplicate
	// insert exercises the constraint directly.
	tx := beginTestTx(t)
	accountID := fmt.Sprintf("acct-dup-%d", time.Now().UnixNano())
	seedAccount(t, tx, accountID, 0, 0)

	_, err := tx.Exec(context.Background(),
		`INSERT INTO accounts (account_id, owner_type, owner_id, trading_cash, bank_cash)
		 VALUES ($1, 'agent', $2, 0, 0)`,
		accountID+"-different-id", accountID,
	)
	if err == nil {
		t.Fatalf("expected uq_accounts_owner violation, got nil")
	}
}
