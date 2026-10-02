package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// ---------- happy paths (both directions) ----------

// ToTrading: Bank → Available (trading_cash). Returned pair and the DB
// row must both reflect the move; conservation (sum of the two columns)
// must hold.
func TestDurableTransfer_ToTrading_Success(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := prefix + "-acc"
	seedAccount(t, pool, accountID, "agent", prefix+"-owner", 100, 50)

	store := newPoolOnlyStore(pool)
	available, bank, err := store.DurableTransfer(context.Background(), accountID, ToTrading, 20)
	if err != nil {
		t.Fatalf("DurableTransfer: %v", err)
	}
	if available != 120 || bank != 30 {
		t.Errorf("returned pair: got (%d,%d), want (120,30)", available, bank)
	}
	gotTrading, gotBank := readCashColumns(t, pool, accountID)
	if gotTrading != 120 || gotBank != 30 {
		t.Errorf("db state: got (trading=%d,bank=%d), want (120,30)", gotTrading, gotBank)
	}
	if gotTrading+gotBank != 100+50 {
		t.Errorf("conservation break: before=%d after=%d", 100+50, gotTrading+gotBank)
	}
}

// ToBank: Available (trading_cash) → Bank. Symmetric to ToTrading.
func TestDurableTransfer_ToBank_Success(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := prefix + "-acc"
	seedAccount(t, pool, accountID, "agent", prefix+"-owner", 100, 50)

	store := newPoolOnlyStore(pool)
	available, bank, err := store.DurableTransfer(context.Background(), accountID, ToBank, 20)
	if err != nil {
		t.Fatalf("DurableTransfer: %v", err)
	}
	if available != 80 || bank != 70 {
		t.Errorf("returned pair: got (%d,%d), want (80,70)", available, bank)
	}
	gotTrading, gotBank := readCashColumns(t, pool, accountID)
	if gotTrading != 80 || gotBank != 70 {
		t.Errorf("db state: got (trading=%d,bank=%d), want (80,70)", gotTrading, gotBank)
	}
	if gotTrading+gotBank != 100+50 {
		t.Errorf("conservation break: before=%d after=%d", 100+50, gotTrading+gotBank)
	}
}

// ---------- insufficient-funds guard (both directions) ----------

// ToTrading guard fail: bank < amount — ErrInsufficient; row UNTOUCHED.
func TestDurableTransfer_ToTrading_Insufficient(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := prefix + "-acc"
	seedAccount(t, pool, accountID, "agent", prefix+"-owner", 100, 10)

	store := newPoolOnlyStore(pool)
	_, _, err := store.DurableTransfer(context.Background(), accountID, ToTrading, 20)
	if !errors.Is(err, ErrInsufficient) {
		t.Fatalf("err: got %v, want ErrInsufficient", err)
	}
	gotTrading, gotBank := readCashColumns(t, pool, accountID)
	if gotTrading != 100 || gotBank != 10 {
		t.Errorf("db state after rejected transfer: got (%d,%d), want untouched (100,10)", gotTrading, gotBank)
	}
}

// ToBank guard fail: trading < amount — ErrInsufficient; row UNTOUCHED.
// Note trading_cash is SIGNED, so this covers the "signed guard still
// rejects when the value is lower than amount" case.
func TestDurableTransfer_ToBank_Insufficient(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := prefix + "-acc"
	seedAccount(t, pool, accountID, "agent", prefix+"-owner", 10, 100)

	store := newPoolOnlyStore(pool)
	_, _, err := store.DurableTransfer(context.Background(), accountID, ToBank, 20)
	if !errors.Is(err, ErrInsufficient) {
		t.Fatalf("err: got %v, want ErrInsufficient", err)
	}
	gotTrading, gotBank := readCashColumns(t, pool, accountID)
	if gotTrading != 10 || gotBank != 100 {
		t.Errorf("db state after rejected transfer: got (%d,%d), want untouched (10,100)", gotTrading, gotBank)
	}
}

// ToBank guard fail when trading_cash is NEGATIVE (debt case, Option 4):
// trading=-100, amount=10 — -100 >= 10 is false → ErrInsufficient. Proves
// debt accounts can't be "banked against" (BROKER_PHASE1 §4.4).
func TestDurableTransfer_ToBank_InsufficientWhenInDebt(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := prefix + "-acc"
	seedAccount(t, pool, accountID, "agent", prefix+"-owner", -100, 50)

	store := newPoolOnlyStore(pool)
	_, _, err := store.DurableTransfer(context.Background(), accountID, ToBank, 10)
	if !errors.Is(err, ErrInsufficient) {
		t.Fatalf("err: got %v, want ErrInsufficient (debt is not spendable)", err)
	}
}

// Debt-reduction via ToTrading: trading_cash is signed (Option 4), so
// moving bank → trading on a debt account REDUCES the debt. Proves the
// signed-column semantics round-trip through this method.
func TestDurableTransfer_ToTrading_ReducesDebt(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := prefix + "-acc"
	seedAccount(t, pool, accountID, "agent", prefix+"-owner", -500, 1000)

	store := newPoolOnlyStore(pool)
	available, bank, err := store.DurableTransfer(context.Background(), accountID, ToTrading, 200)
	if err != nil {
		t.Fatalf("DurableTransfer: %v", err)
	}
	if available != -300 || bank != 800 {
		t.Errorf("returned pair: got (%d,%d), want (-300,800)", available, bank)
	}
}

// ---------- missing account (both directions) ----------

func TestDurableTransfer_ToTrading_MissingAccount(t *testing.T) {
	pool := requirePostgresPool(t)
	store := newPoolOnlyStore(pool)

	_, _, err := store.DurableTransfer(context.Background(), "nonexistent-acc", ToTrading, 1)
	if !errors.Is(err, ErrNoAccount) {
		t.Fatalf("err: got %v, want ErrNoAccount", err)
	}
}

func TestDurableTransfer_ToBank_MissingAccount(t *testing.T) {
	pool := requirePostgresPool(t)
	store := newPoolOnlyStore(pool)

	_, _, err := store.DurableTransfer(context.Background(), "nonexistent-acc", ToBank, 1)
	if !errors.Is(err, ErrNoAccount) {
		t.Fatalf("err: got %v, want ErrNoAccount", err)
	}
}

// ---------- bad arguments (no SQL issued) ----------

// amount == 0 is rejected before any SQL. We prove "no SQL" indirectly:
// use a non-pool store with an obviously-wrong account_id — if any SQL
// ran we'd see ErrLoadFailed (query against unknown DSN) or ErrNoAccount,
// not ErrBadTransfer.
func TestDurableTransfer_ZeroAmount(t *testing.T) {
	pool := requirePostgresPool(t)
	store := newPoolOnlyStore(pool)

	_, _, err := store.DurableTransfer(context.Background(), "any-acc", ToTrading, 0)
	if !errors.Is(err, ErrBadTransfer) {
		t.Fatalf("err: got %v, want ErrBadTransfer", err)
	}
}

// Negative amount, both branches of the sign guard.
func TestDurableTransfer_NegativeAmount(t *testing.T) {
	pool := requirePostgresPool(t)
	store := newPoolOnlyStore(pool)

	_, _, err := store.DurableTransfer(context.Background(), "any-acc", ToTrading, -5)
	if !errors.Is(err, ErrBadTransfer) {
		t.Fatalf("err: got %v, want ErrBadTransfer", err)
	}
}

// Zero-value direction (the sentinel between iota 0 and the first named
// constant) — ErrBadTransfer. Proves the "unset direction" trap.
func TestDurableTransfer_ZeroDirection(t *testing.T) {
	pool := requirePostgresPool(t)
	store := newPoolOnlyStore(pool)

	_, _, err := store.DurableTransfer(context.Background(), "any-acc", TransferDirection(0), 10)
	if !errors.Is(err, ErrBadTransfer) {
		t.Fatalf("err: got %v, want ErrBadTransfer", err)
	}
}

// Out-of-range direction — any value past ToBank. Same ErrBadTransfer.
func TestDurableTransfer_OutOfRangeDirection(t *testing.T) {
	pool := requirePostgresPool(t)
	store := newPoolOnlyStore(pool)

	_, _, err := store.DurableTransfer(context.Background(), "any-acc", TransferDirection(99), 10)
	if !errors.Is(err, ErrBadTransfer) {
		t.Fatalf("err: got %v, want ErrBadTransfer", err)
	}
}

// ---------- context cancellation ----------

func TestDurableTransfer_CancelledContext(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := prefix + "-acc"
	seedAccount(t, pool, accountID, "agent", prefix+"-owner", 100, 50)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	store := newPoolOnlyStore(pool)
	_, _, err := store.DurableTransfer(ctx, accountID, ToTrading, 10)
	if err == nil {
		t.Fatal("DurableTransfer(cancelled): got nil err, want infra failure")
	}
	if errors.Is(err, ErrBadTransfer) || errors.Is(err, ErrNoAccount) || errors.Is(err, ErrInsufficient) {
		t.Fatalf("DurableTransfer(cancelled): got business err %v, want infra failure", err)
	}
	if !errors.Is(err, ErrLoadFailed) {
		t.Fatalf("DurableTransfer(cancelled): got %v, want ErrLoadFailed-wrapped", err)
	}
	// DB untouched by a cancelled call.
	gotTrading, gotBank := readCashColumns(t, pool, accountID)
	if gotTrading != 100 || gotBank != 50 {
		t.Errorf("db state after cancelled transfer: got (%d,%d), want untouched (100,50)", gotTrading, gotBank)
	}
}

// ---------- ck_bank_nonneg backstop ----------

// Prove the DB's CHECK constraint actively rejects a would-be
// bank_cash-negative write, independent of the Go guard. The test
// issues a hand-crafted "evil" UPDATE that omits the guard predicate —
// this represents a hypothetical buggy production SQL. Postgres must
// reject it with SQLSTATE 23514 (check_violation).
func TestDurableTransfer_CkBankNonNegBackstop(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := prefix + "-acc"
	seedAccount(t, pool, accountID, "agent", prefix+"-owner", 0, 10)

	// Omits the `AND bank_cash >= $2` guard — the DB CHECK must catch it.
	_, err := pool.Exec(context.Background(),
		`UPDATE accounts SET bank_cash = bank_cash - 999 WHERE account_id = $1`,
		accountID,
	)
	if err == nil {
		t.Fatal("buggy UPDATE drove bank_cash negative but ck_bank_nonneg didn't fire")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected *pgconn.PgError, got %T: %v", err, err)
	}
	// 23514 = check_violation (PostgreSQL error codes appendix).
	if pgErr.Code != "23514" {
		t.Errorf("expected SQLSTATE 23514 (check_violation), got %s (%s)", pgErr.Code, pgErr.Message)
	}
	if !strings.Contains(pgErr.ConstraintName, "ck_bank_nonneg") {
		t.Errorf("expected constraint ck_bank_nonneg, got %q", pgErr.ConstraintName)
	}
	// And bank_cash is still 10 — the aborted statement left the row untouched.
	_, gotBank := readCashColumns(t, pool, accountID)
	if gotBank != 10 {
		t.Errorf("bank_cash after rejected UPDATE: got %d, want 10 (untouched)", gotBank)
	}
}

// ---------- conservation property ----------

// Conservation: trading_cash + bank_cash is invariant across any legal
// transfer (both directions, several amounts and starting balances).
// Not full rapid because each iteration is a round-trip to Postgres;
// a representative fixed table is enough to catch sign/accounting bugs.
func TestDurableTransfer_Conservation(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	cases := []struct {
		name         string
		trading      int64
		bank         int64
		direction    TransferDirection
		amount       int64
	}{
		{"to_trading_small", 100, 500, ToTrading, 50},
		{"to_trading_all_bank", 100, 500, ToTrading, 500},
		{"to_bank_small", 500, 100, ToBank, 50},
		{"to_bank_all_trading", 500, 100, ToBank, 500},
		{"to_trading_debt_reduces", -200, 1000, ToTrading, 150},
	}
	store := newPoolOnlyStore(pool)
	for caseIndex, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(subtest *testing.T) {
			accountID := prefix + "-conserv-" + testCase.name
			seedAccount(subtest, pool, accountID, "agent", prefix+"-owner-"+testCase.name, testCase.trading, testCase.bank)
			sumBefore := testCase.trading + testCase.bank

			_, _, err := store.DurableTransfer(context.Background(), accountID, testCase.direction, testCase.amount)
			if err != nil {
				subtest.Fatalf("case %d (%s): %v", caseIndex, testCase.name, err)
			}
			gotTrading, gotBank := readCashColumns(subtest, pool, accountID)
			if gotTrading+gotBank != sumBefore {
				subtest.Errorf("conservation break: before=%d after=%d (trading=%d bank=%d)",
					sumBefore, gotTrading+gotBank, gotTrading, gotBank)
			}
		})
	}
}
