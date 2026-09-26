package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"simulator/economy/account"
	"simulator/economy/persistence"
)

// helper: build a DurableAccount for load tests. Kept in one place so
// changes to the persistence DTO surface here first.
func durableFor(accountID, ownerID string, tradingCash, bankCash int64) persistence.DurableAccount {
	return persistence.DurableAccount{
		AccountID:   accountID,
		OwnerType:   "agent",
		OwnerID:     ownerID,
		TradingCash: tradingCash,
		BankCash:    bankCash,
	}
}

// TestSessionLoadHydrationSynthesis covers the split synthesis for
// canonical loads under every signed-cash case (positive / zero / negative
// TradingCash), plus non-nil DripProgress. Proves the ONE-and-only
// synthesis point matches doc §3 step 3 field-for-field, and that the
// resulting *Account tags PersistOnClose=true.
func TestSessionLoadHydrationSynthesis(t *testing.T) {
	dripProgress := int64(4200)
	cases := []struct {
		name                string
		tradingCash         int64
		bankCash            int64
		dripActive          bool
		dripProgress        *int64
		wantAvailable       int64 // must equal TradingCash (signed, no clamp)
	}{
		{"positive trading cash", 5_000_000, 100_000, false, nil, 5_000_000},
		{"zero trading cash (must not be confused with missing)", 0, 0, false, nil, 0},
		{"negative trading cash (Option B — debt at load)", -777_777, 50_000, false, nil, -777_777},
		{"drip active + non-nil progress", 1_000_000, 250_000, true, &dripProgress, 1_000_000},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(subtest *testing.T) {
			durable := durableFor("acct-1", "owner-1", testCase.tradingCash, testCase.bankCash)
			durable.DripActive = testCase.dripActive
			durable.DripProgress = testCase.dripProgress

			fake := &fakeStore{
				LoadAccountFn: func(ctx context.Context, accountID string) (persistence.DurableAccount, error) {
					if accountID != "acct-1" {
						subtest.Fatalf("LoadAccount arg: got %q, want acct-1", accountID)
					}
					return durable, nil
				},
			}
			service := NewService(fake)

			loaded, err := service.SessionLoad(context.Background(), "sess-1", account.AccountID("acct-1"))
			if err != nil {
				subtest.Fatalf("SessionLoad: %v", err)
			}

			// Cash split synthesis — Available signed, others zero.
			wantCash := account.Cash{
				Available: testCase.wantAvailable,
				Reserved:  0,
				Settled:   0,
				Bank:      testCase.bankCash,
			}
			if loaded.Account.Cash != wantCash {
				subtest.Errorf("Cash split:\n got  %+v\n want %+v", loaded.Account.Cash, wantCash)
			}
			if loaded.Account.ID != "acct-1" {
				subtest.Errorf("Account.ID: got %q, want acct-1", loaded.Account.ID)
			}
			if !loaded.Account.PersistOnClose {
				subtest.Errorf("PersistOnClose: got false, want true (canonical)")
			}
			if len(loaded.Account.Positions) != 0 {
				subtest.Errorf("Positions: got %d entries, want 0 (born flat)", len(loaded.Account.Positions))
			}
			if loaded.Account.DripActive != testCase.dripActive {
				subtest.Errorf("DripActive: got %v, want %v", loaded.Account.DripActive, testCase.dripActive)
			}
			// Drip progress must be the SAME pointer / same value semantics.
			switch {
			case testCase.dripProgress == nil && loaded.Account.DripProgress != nil:
				subtest.Errorf("DripProgress: got %v, want nil", *loaded.Account.DripProgress)
			case testCase.dripProgress != nil && loaded.Account.DripProgress == nil:
				subtest.Errorf("DripProgress: got nil, want %v", *testCase.dripProgress)
			case testCase.dripProgress != nil && *loaded.Account.DripProgress != *testCase.dripProgress:
				subtest.Errorf("DripProgress: got %d, want %d", *loaded.Account.DripProgress, *testCase.dripProgress)
			}

			// LoadedAccount identity carry (needed for session_save later).
			if loaded.OwnerType != OwnerAgent {
				subtest.Errorf("LoadedAccount.OwnerType: got %s, want agent", loaded.OwnerType)
			}
			if loaded.OwnerID != "owner-1" {
				subtest.Errorf("LoadedAccount.OwnerID: got %q, want owner-1", loaded.OwnerID)
			}

			// Store contract: exactly one LoadAccount + exactly one
			// WriteSnapshot, in that order. No other store method touched.
			if len(fake.LoadAccountCalls) != 1 {
				subtest.Errorf("LoadAccount call count: got %d, want 1", len(fake.LoadAccountCalls))
			}
			if len(fake.WriteSnapshotCalls) != 1 {
				subtest.Fatalf("WriteSnapshot call count: got %d, want 1", len(fake.WriteSnapshotCalls))
			}
			wantSnap := persistence.SnapshotPayload{
				SessionID: "sess-1",
				AccountID: "acct-1",
				Cash: persistence.CashSnap{
					Available:    testCase.wantAvailable,
					Reserved:     0,
					Settled:      0,
					BankCash:     testCase.bankCash,
					DripActive:   testCase.dripActive,
					DripProgress: testCase.dripProgress,
				},
			}
			gotSnap := fake.WriteSnapshotCalls[0]
			if !snapshotEqual(gotSnap, wantSnap) {
				subtest.Errorf("WriteSnapshot payload:\n got  %+v\n want %+v", gotSnap, wantSnap)
			}
			assertNoOtherStoreCallsBeyondLoadAndWrite(subtest, fake)
		})
	}
}

func TestSessionLoadMapsErrNoAccount(t *testing.T) {
	// persistence.ErrNoAccount → ErrNoAccountFor (unwraps to ErrNoAccount
	// for category dispatch). WriteSnapshot must NOT be called — a
	// missing durable row means no session state to seed.
	fake := &fakeStore{
		LoadAccountFn: func(ctx context.Context, accountID string) (persistence.DurableAccount, error) {
			return persistence.DurableAccount{}, fmt.Errorf("%w: %s", persistence.ErrNoAccount, accountID)
		},
	}
	service := NewService(fake)

	loaded, err := service.SessionLoad(context.Background(), "sess-x", account.AccountID("acct-missing"))
	if loaded.Account != nil {
		t.Errorf("LoadedAccount.Account: got non-nil on error path")
	}
	if !errors.Is(err, ErrNoAccount) {
		t.Fatalf("errors.Is(err, ErrNoAccount): got err = %v", err)
	}
	var typed ErrNoAccountFor
	if !errors.As(err, &typed) {
		t.Fatalf("errors.As(err, *ErrNoAccountFor): failed, got %v", err)
	}
	if typed.AccountID != account.AccountID("acct-missing") {
		t.Errorf("ErrNoAccountFor.AccountID: got %s, want acct-missing", typed.AccountID)
	}
	if len(fake.WriteSnapshotCalls) != 0 {
		t.Errorf("WriteSnapshot must NOT run when LoadAccount returns ErrNoAccount; got %d calls", len(fake.WriteSnapshotCalls))
	}
}

func TestSessionLoadMapsTransportFailure(t *testing.T) {
	// A non-ErrNoAccount error from LoadAccount → ErrLoadFailedFor,
	// wrapping both ErrLoadFailed and the injected cause. WriteSnapshot
	// must NOT run.
	injected := errors.New("pg: connection reset by peer")
	fake := &fakeStore{
		LoadAccountFn: func(ctx context.Context, accountID string) (persistence.DurableAccount, error) {
			return persistence.DurableAccount{}, injected
		},
	}
	service := NewService(fake)

	loaded, err := service.SessionLoad(context.Background(), "sess-x", account.AccountID("acct-x"))
	if loaded.Account != nil {
		t.Errorf("LoadedAccount.Account: got non-nil on error path")
	}
	if !errors.Is(err, ErrLoadFailed) {
		t.Fatalf("errors.Is(err, ErrLoadFailed): got err = %v", err)
	}
	if !errors.Is(err, injected) {
		t.Fatalf("errors.Is(err, injected): got err = %v", err)
	}
	// Must NOT be ErrNoAccount.
	if errors.Is(err, ErrNoAccount) {
		t.Errorf("transport failure must NOT map to ErrNoAccount: err = %v", err)
	}
	if len(fake.WriteSnapshotCalls) != 0 {
		t.Errorf("WriteSnapshot must NOT run on LoadAccount transport failure; got %d", len(fake.WriteSnapshotCalls))
	}
}

func TestSessionLoadCanonicalHardFailsOnSnapshotWriteFailure(t *testing.T) {
	// Canonical: WriteSnapshot failure hard-fails the session start (doc
	// §3 step 6 — refuse THIS participant's session rather than run
	// silently-unrecoverable). The returned LoadedAccount MUST be zero so
	// the caller can't accidentally use a session that has no snapshot.
	// The durable row was already read (no rollback needed — read-only).
	injected := errors.New("redis: EXEC failed")
	fake := &fakeStore{
		LoadAccountFn: func(ctx context.Context, accountID string) (persistence.DurableAccount, error) {
			return durableFor(accountID, "owner-hf", 1_000_000, 0), nil
		},
		WriteSnapshotFn: func(ctx context.Context, payload persistence.SnapshotPayload) error {
			return injected
		},
	}
	service := NewService(fake)

	loaded, err := service.SessionLoad(context.Background(), "sess-hf", account.AccountID("acct-hf"))
	if loaded.Account != nil {
		t.Errorf("LoadedAccount.Account: got non-nil on canonical snapshot-write failure")
	}
	if !errors.Is(err, ErrSnapshotWriteFailed) {
		t.Fatalf("errors.Is(err, ErrSnapshotWriteFailed): got err = %v", err)
	}
	if !errors.Is(err, injected) {
		t.Fatalf("errors.Is(err, injected): got err = %v", err)
	}
	var typed ErrSnapshotWriteFailedFor
	if !errors.As(err, &typed) {
		t.Fatalf("errors.As(err, *ErrSnapshotWriteFailedFor): failed, got %v", err)
	}
	if typed.SessionID != "sess-hf" || typed.AccountID != account.AccountID("acct-hf") {
		t.Errorf("ErrSnapshotWriteFailedFor: got %+v, want {sess-hf, acct-hf}", typed)
	}

	// LoadAccount ran; WriteSnapshot was attempted; no SaveAccount / no
	// DeleteSnapshot (defense-in-depth check).
	if len(fake.LoadAccountCalls) != 1 {
		t.Errorf("LoadAccount call count: got %d, want 1", len(fake.LoadAccountCalls))
	}
	if len(fake.WriteSnapshotCalls) != 1 {
		t.Errorf("WriteSnapshot call count: got %d, want 1 (attempted)", len(fake.WriteSnapshotCalls))
	}
	if len(fake.SaveAccountCalls) != 0 || len(fake.DeleteSnapshotCalls) != 0 {
		t.Errorf("unexpected save/delete calls: save=%d delete=%d",
			len(fake.SaveAccountCalls), len(fake.DeleteSnapshotCalls))
	}
}

// snapshotEqual compares two SnapshotPayloads by value with nil-safe
// DripProgress handling (a pointer field's addresses differ but values may
// still match).
func snapshotEqual(got, want persistence.SnapshotPayload) bool {
	if got.SessionID != want.SessionID || got.AccountID != want.AccountID {
		return false
	}
	if got.Cash.Available != want.Cash.Available ||
		got.Cash.Reserved != want.Cash.Reserved ||
		got.Cash.Settled != want.Cash.Settled ||
		got.Cash.BankCash != want.Cash.BankCash ||
		got.Cash.DripActive != want.Cash.DripActive {
		return false
	}
	switch {
	case got.Cash.DripProgress == nil && want.Cash.DripProgress != nil:
		return false
	case got.Cash.DripProgress != nil && want.Cash.DripProgress == nil:
		return false
	case got.Cash.DripProgress != nil && *got.Cash.DripProgress != *want.Cash.DripProgress:
		return false
	}
	// Initial snapshot has no positions or reservations.
	return len(got.Positions) == 0 && len(want.Positions) == 0 &&
		len(got.Reservations) == 0 && len(want.Reservations) == 0
}

func assertNoOtherStoreCallsBeyondLoadAndWrite(t *testing.T, fake *fakeStore) {
	t.Helper()
	if len(fake.OpenAccountCalls) != 0 {
		t.Errorf("unexpected OpenAccount calls: %d", len(fake.OpenAccountCalls))
	}
	if len(fake.SaveAccountCalls) != 0 {
		t.Errorf("unexpected SaveAccount calls: %d", len(fake.SaveAccountCalls))
	}
	if len(fake.ReadSnapshotCalls) != 0 {
		t.Errorf("unexpected ReadSnapshot calls: %d", len(fake.ReadSnapshotCalls))
	}
	if len(fake.DeleteSnapshotCalls) != 0 {
		t.Errorf("unexpected DeleteSnapshot calls: %d", len(fake.DeleteSnapshotCalls))
	}
}
