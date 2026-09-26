package lifecycle

import (
	"context"
	"errors"
	"testing"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/persistence"
)

// flatCanonicalLoaded builds a LoadedAccount ready for a clean save:
// PersistOnClose=true, Reserved=0, no live positions.
func flatCanonicalLoaded(accountID account.AccountID, available, settled, bank int64) LoadedAccount {
	return LoadedAccount{
		Account: &account.Account{
			ID: accountID,
			Cash: account.Cash{
				Available: available, // signed
				Reserved:  0,
				Settled:   settled,
				Bank:      bank,
			},
			Positions:      make(map[contract.InstrumentID]*account.Position),
			PersistOnClose: true,
		},
		OwnerType: OwnerAgent,
		OwnerID:   "owner-save",
	}
}

// TestSessionSaveReconciliation covers Available + Settled → TradingCash
// across positive / zero / negative results (signed round-trip, Option 4).
func TestSessionSaveReconciliation(t *testing.T) {
	cases := []struct {
		name             string
		available        int64
		settled          int64
		bank             int64
		wantTradingCash  int64
	}{
		{"positive available + positive settled", 1_000_000, 250_000, 100_000, 1_250_000},
		{"positive available + zero settled", 750_000, 0, 50_000, 750_000},
		{"zero available + positive settled (all realized)", 0, 500_000, 42, 500_000},
		{"available zero, settled zero (flat zero account)", 0, 0, 0, 0},
		{"negative available, positive settled netting to positive", -100_000, 300_000, 50, 200_000},
		{"negative available, positive settled netting to zero", -500_000, 500_000, 0, 0},
		{"negative available, small settled netting to negative (debt)", -1_000_000, 100_000, 0, -900_000},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(subtest *testing.T) {
			loaded := flatCanonicalLoaded(account.AccountID("acct-recon"), testCase.available, testCase.settled, testCase.bank)
			fake := &fakeStore{}
			service := NewService(fake)

			if err := service.SessionSave(context.Background(), "sess-recon", loaded); err != nil {
				subtest.Fatalf("SessionSave: %v", err)
			}
			if len(fake.SaveAccountCalls) != 1 {
				subtest.Fatalf("SaveAccount call count: got %d, want 1", len(fake.SaveAccountCalls))
			}
			saved := fake.SaveAccountCalls[0]
			if saved.TradingCash != testCase.wantTradingCash {
				subtest.Errorf("TradingCash: got %d, want %d (Available %d + Settled %d)",
					saved.TradingCash, testCase.wantTradingCash, testCase.available, testCase.settled)
			}
			if saved.BankCash != testCase.bank {
				subtest.Errorf("BankCash: got %d, want %d", saved.BankCash, testCase.bank)
			}
			if saved.OwnerType != "agent" || saved.OwnerID != "owner-save" {
				subtest.Errorf("identity: got (%s,%s), want (agent, owner-save)", saved.OwnerType, saved.OwnerID)
			}
			if saved.AccountID != "acct-recon" {
				subtest.Errorf("AccountID: got %q, want acct-recon", saved.AccountID)
			}
			// DeleteSnapshot must be called (Postgres committed).
			if len(fake.DeleteSnapshotCalls) != 1 {
				subtest.Errorf("DeleteSnapshot call count: got %d, want 1", len(fake.DeleteSnapshotCalls))
			}
		})
	}
}

func TestSessionSaveRefusesOnDemand(t *testing.T) {
	// PersistOnClose=false must return ErrNotCanonical with NO store call
	// (defense-in-depth per doc §5 step 1).
	loaded := flatCanonicalLoaded(account.AccountID("acct-od"), 100, 0, 0)
	loaded.Account.PersistOnClose = false

	fake := &fakeStore{}
	service := NewService(fake)

	err := service.SessionSave(context.Background(), "sess-od", loaded)
	if !errors.Is(err, ErrNotCanonical) {
		t.Fatalf("errors.Is(err, ErrNotCanonical): got err = %v", err)
	}
	if len(fake.SaveAccountCalls) != 0 || len(fake.DeleteSnapshotCalls) != 0 {
		t.Errorf("no store call must happen on ErrNotCanonical; got save=%d delete=%d",
			len(fake.SaveAccountCalls), len(fake.DeleteSnapshotCalls))
	}
}

func TestSessionSavePanicsOnReservedCash(t *testing.T) {
	// Reserved != 0 at save = ordering bug (auto-square did not release
	// reservations). Dev-fatal panic per doc §5 step 2. Neither
	// SaveAccount NOR DeleteSnapshot may run.
	loaded := flatCanonicalLoaded(account.AccountID("acct-resv"), 100, 0, 0)
	loaded.Account.Cash.Reserved = 42 // <- the invariant break

	fake := &fakeStore{}
	service := NewService(fake)

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on reserved cash, got none")
		}
		if len(fake.SaveAccountCalls) != 0 {
			t.Errorf("SaveAccount must NOT run when flat assertion fires; got %d calls", len(fake.SaveAccountCalls))
		}
		if len(fake.DeleteSnapshotCalls) != 0 {
			t.Errorf("DeleteSnapshot must NOT run when flat assertion fires; got %d calls", len(fake.DeleteSnapshotCalls))
		}
	}()
	_ = service.SessionSave(context.Background(), "sess-resv", loaded)
}

func TestSessionSavePanicsOnLivePositionAvailable(t *testing.T) {
	// A position with Available > 0 means auto-square did not zero it.
	// Dev-fatal panic.
	loaded := flatCanonicalLoaded(account.AccountID("acct-pos"), 100, 0, 0)
	loaded.Account.Positions[contract.InstrumentID("ACME")] = &account.Position{Available: 5, Reserved: 0}

	fake := &fakeStore{}
	service := NewService(fake)

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on unsquared position (Available > 0), got none")
		}
		if len(fake.SaveAccountCalls) != 0 || len(fake.DeleteSnapshotCalls) != 0 {
			t.Errorf("no store call on flat-assert panic; got save=%d delete=%d",
				len(fake.SaveAccountCalls), len(fake.DeleteSnapshotCalls))
		}
	}()
	_ = service.SessionSave(context.Background(), "sess-pos", loaded)
}

func TestSessionSavePanicsOnLivePositionReserved(t *testing.T) {
	// Symmetric to the Available case — Reserved > 0 also panics. Covers
	// the second branch of the position flat-check per your
	// [[feedback_tests_probe_dont_assume]] rule.
	loaded := flatCanonicalLoaded(account.AccountID("acct-pos-r"), 100, 0, 0)
	loaded.Account.Positions[contract.InstrumentID("ACME")] = &account.Position{Available: 0, Reserved: 3}

	fake := &fakeStore{}
	service := NewService(fake)

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on unsquared position (Reserved > 0), got none")
		}
		if len(fake.SaveAccountCalls) != 0 || len(fake.DeleteSnapshotCalls) != 0 {
			t.Errorf("no store call on flat-assert panic; got save=%d delete=%d",
				len(fake.SaveAccountCalls), len(fake.DeleteSnapshotCalls))
		}
	}()
	_ = service.SessionSave(context.Background(), "sess-pos-r", loaded)
}

func TestSessionSaveAllowsAllZeroPositionEntries(t *testing.T) {
	// A residual position ENTRY with Available=0 AND Reserved=0 is
	// allowed — 5.4's RemovePosition may leave the map key after
	// draining. Only non-zero counts panic. This proves the flat-assert
	// isn't over-strict (would break auto-square teardown otherwise).
	loaded := flatCanonicalLoaded(account.AccountID("acct-zero-pos"), 500, 100, 0)
	loaded.Account.Positions[contract.InstrumentID("ACME")] = &account.Position{Available: 0, Reserved: 0}
	loaded.Account.Positions[contract.InstrumentID("BETA")] = nil // defensive: nil entry tolerated

	fake := &fakeStore{}
	service := NewService(fake)

	if err := service.SessionSave(context.Background(), "sess-zp", loaded); err != nil {
		t.Fatalf("SessionSave: %v", err)
	}
	if len(fake.SaveAccountCalls) != 1 {
		t.Errorf("SaveAccount call count: got %d, want 1", len(fake.SaveAccountCalls))
	}
}

func TestSessionSaveDoesNotDiscardOnSaveFailure(t *testing.T) {
	// The load-bearing rule (doc §5 step 5): if SaveAccount fails, do
	// NOT DeleteSnapshot — the snapshot is the only surviving copy of
	// the final state. Returns ErrSaveFailedFor wrapping both
	// ErrSaveFailed and the driver cause.
	injected := errors.New("pg: connection dropped mid-txn")
	loaded := flatCanonicalLoaded(account.AccountID("acct-fail"), 100_000, 50_000, 0)

	fake := &fakeStore{
		SaveAccountFn: func(ctx context.Context, durable persistence.DurableAccount) error {
			return injected
		},
	}
	service := NewService(fake)

	err := service.SessionSave(context.Background(), "sess-fail", loaded)
	if !errors.Is(err, ErrSaveFailed) {
		t.Fatalf("errors.Is(err, ErrSaveFailed): got err = %v", err)
	}
	if !errors.Is(err, injected) {
		t.Fatalf("errors.Is(err, injected): got err = %v", err)
	}
	var typed ErrSaveFailedFor
	if !errors.As(err, &typed) {
		t.Fatalf("errors.As(err, *ErrSaveFailedFor): failed, got %v", err)
	}
	if typed.AccountID != account.AccountID("acct-fail") {
		t.Errorf("ErrSaveFailedFor.AccountID: got %s, want acct-fail", typed.AccountID)
	}

	// THE critical assertion: snapshot RETAINED.
	if len(fake.DeleteSnapshotCalls) != 0 {
		t.Fatalf("DeleteSnapshot MUST NOT run on SaveAccount failure; got %d calls", len(fake.DeleteSnapshotCalls))
	}
	// SaveAccount was attempted exactly once (fake returned the error).
	if len(fake.SaveAccountCalls) != 1 {
		t.Errorf("SaveAccount call count: got %d, want 1 (attempted)", len(fake.SaveAccountCalls))
	}
}

func TestSessionSaveDeleteSnapshotFailureIsNonFatal(t *testing.T) {
	// After SaveAccount commits, a DeleteSnapshot failure is a cleanup
	// nit (TTL cleans stale keys). Session-level return is nil; the
	// warning goes to the log. Doc §5 step 6.
	loaded := flatCanonicalLoaded(account.AccountID("acct-del-fail"), 1_000_000, 0, 0)
	fake := &fakeStore{
		DeleteSnapshotFn: func(ctx context.Context, sessionID, accountID string) error {
			return errors.New("redis: DEL timeout")
		},
	}
	service := NewService(fake)

	if err := service.SessionSave(context.Background(), "sess-del", loaded); err != nil {
		t.Fatalf("SessionSave: got err = %v, want nil (DeleteSnapshot failure is non-fatal)", err)
	}
	if len(fake.SaveAccountCalls) != 1 {
		t.Errorf("SaveAccount call count: got %d, want 1", len(fake.SaveAccountCalls))
	}
	if len(fake.DeleteSnapshotCalls) != 1 {
		t.Errorf("DeleteSnapshot call count: got %d, want 1 (attempted)", len(fake.DeleteSnapshotCalls))
	}
}

func TestSessionSaveDripCarriesOver(t *testing.T) {
	// Drip pair (active + progress) must round-trip verbatim on save.
	dripProgress := int64(7777)
	loaded := flatCanonicalLoaded(account.AccountID("acct-drip"), 500_000, 0, 0)
	loaded.Account.DripActive = true
	loaded.Account.DripProgress = &dripProgress

	fake := &fakeStore{}
	service := NewService(fake)

	if err := service.SessionSave(context.Background(), "sess-drip", loaded); err != nil {
		t.Fatalf("SessionSave: %v", err)
	}
	if len(fake.SaveAccountCalls) != 1 {
		t.Fatalf("SaveAccount call count: got %d, want 1", len(fake.SaveAccountCalls))
	}
	saved := fake.SaveAccountCalls[0]
	if !saved.DripActive {
		t.Errorf("DripActive: got false, want true")
	}
	if saved.DripProgress == nil {
		t.Fatalf("DripProgress: got nil, want 7777")
	}
	if *saved.DripProgress != 7777 {
		t.Errorf("DripProgress: got %d, want 7777", *saved.DripProgress)
	}
}
