package lifecycle

import (
	"context"
	"errors"
	"testing"

	"simulator/contract"
	"simulator/economy/account"
)

// flatOndemandLoaded builds a LoadedAccount ready for a clean discard:
// PersistOnClose=false, Reserved=0, no live positions.
func flatOndemandLoaded(accountID account.AccountID) LoadedAccount {
	return LoadedAccount{
		Account: &account.Account{
			ID:             accountID,
			Cash:           account.Cash{Available: 100, Reserved: 0, Settled: 0, Bank: 0},
			Positions:      make(map[contract.InstrumentID]*account.Position),
			PersistOnClose: false, // the defining on-demand tag
		},
		OwnerType: OwnerAgent,
		OwnerID:   "owner-discard",
	}
}

func TestSessionDiscardHappyPathDeletesSnapshotOnly(t *testing.T) {
	// The on-demand no-footprint promise: DeleteSnapshot called exactly
	// once with the caller's ids; SaveAccount NEVER called. No other
	// store method touched either.
	loaded := flatOndemandLoaded(account.AccountID("acct-od"))
	fake := &fakeStore{}
	service := NewService(fake)

	if err := service.SessionDiscard(context.Background(), "sess-od", loaded); err != nil {
		t.Fatalf("SessionDiscard: %v", err)
	}
	if len(fake.DeleteSnapshotCalls) != 1 {
		t.Fatalf("DeleteSnapshot call count: got %d, want 1", len(fake.DeleteSnapshotCalls))
	}
	call := fake.DeleteSnapshotCalls[0]
	if call.SessionID != "sess-od" || call.AccountID != "acct-od" {
		t.Errorf("DeleteSnapshot args: got (%s,%s), want (sess-od,acct-od)", call.SessionID, call.AccountID)
	}
	// THE structural guarantee: on-demand leaves zero durable footprint.
	if len(fake.SaveAccountCalls) != 0 {
		t.Errorf("SaveAccount MUST NEVER run in discard; got %d calls", len(fake.SaveAccountCalls))
	}
	if len(fake.OpenAccountCalls) != 0 || len(fake.LoadAccountCalls) != 0 {
		t.Errorf("no Postgres touch allowed; got open=%d load=%d",
			len(fake.OpenAccountCalls), len(fake.LoadAccountCalls))
	}
	if len(fake.WriteSnapshotCalls) != 0 || len(fake.ReadSnapshotCalls) != 0 {
		t.Errorf("no other Redis touch allowed; got write=%d read=%d",
			len(fake.WriteSnapshotCalls), len(fake.ReadSnapshotCalls))
	}
}

func TestSessionDiscardPanicsOnCanonicalMisroute(t *testing.T) {
	// The deliberately-asymmetric guard (doc §6): a canonical account
	// reaching discard is dev-fatal — silently discarding would throw
	// away real money progress with no recovery. Panic is the loudest
	// possible signal.
	//
	// Symmetric to session_save's ErrNotCanonical guard (which RETURNS
	// rather than panics because refusing to write is safe).
	loaded := flatOndemandLoaded(account.AccountID("acct-canonical-in-discard"))
	loaded.Account.PersistOnClose = true // <- the mis-route

	fake := &fakeStore{}
	service := NewService(fake)

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("canonical mis-route in SessionDiscard: expected panic, got none")
		}
		// No store call may have happened.
		if len(fake.DeleteSnapshotCalls) != 0 {
			t.Errorf("DeleteSnapshot must NOT run on canonical mis-route; got %d calls", len(fake.DeleteSnapshotCalls))
		}
		if len(fake.SaveAccountCalls) != 0 {
			t.Errorf("SaveAccount must NEVER run in discard; got %d calls", len(fake.SaveAccountCalls))
		}
	}()
	_ = service.SessionDiscard(context.Background(), "sess-x", loaded)
}

func TestSessionDiscardSurfacesDeleteError(t *testing.T) {
	// Unlike session_save's post-commit DeleteSnapshot failure (which is
	// non-fatal because Postgres already committed correct state), here
	// the delete failure is the ONLY meaningful signal — the snapshot
	// still lives in Redis, and the caller may want to retry or route.
	// Surface as-is (not wrapped) so the caller can inspect via
	// errors.Is on the driver's own sentinels.
	loaded := flatOndemandLoaded(account.AccountID("acct-del-err"))
	injected := errors.New("redis: DEL failed")
	fake := &fakeStore{
		DeleteSnapshotFn: func(ctx context.Context, sessionID, accountID string) error {
			return injected
		},
	}
	service := NewService(fake)

	err := service.SessionDiscard(context.Background(), "sess-del-err", loaded)
	if err == nil {
		t.Fatalf("SessionDiscard: got nil err, want the injected DeleteSnapshot error")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("errors.Is(err, injected): got err = %v", err)
	}
	if len(fake.DeleteSnapshotCalls) != 1 {
		t.Errorf("DeleteSnapshot call count: got %d, want 1 (attempted)", len(fake.DeleteSnapshotCalls))
	}
	// Still no SaveAccount ever.
	if len(fake.SaveAccountCalls) != 0 {
		t.Errorf("SaveAccount MUST NEVER run in discard; got %d calls", len(fake.SaveAccountCalls))
	}
}
