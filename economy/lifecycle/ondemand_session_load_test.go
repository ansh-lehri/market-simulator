package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"simulator/economy/account"
	"simulator/economy/persistence"
)

func TestOndemandSessionLoadTagsPersistOnCloseFalse(t *testing.T) {
	// The defining on-demand property (doc §4 step 4): PersistOnClose is
	// false on the returned Account. Everything else (split synthesis,
	// snapshot seeding) is identical to canonical — one boolean routes
	// the whole downstream policy.
	durable := durableFor("acct-od", "owner-od", 2_000_000, 100_000)
	fake := &fakeStore{
		LoadAccountFn: func(ctx context.Context, accountID string) (persistence.DurableAccount, error) {
			return durable, nil
		},
	}
	service := NewService(fake)

	loaded, err := service.OndemandSessionLoad(context.Background(), "sess-od", account.AccountID("acct-od"))
	if err != nil {
		t.Fatalf("OndemandSessionLoad: %v", err)
	}
	if loaded.Account == nil {
		t.Fatalf("LoadedAccount.Account: got nil on happy path")
	}
	if loaded.Account.PersistOnClose {
		t.Errorf("PersistOnClose: got true, want false (on-demand)")
	}

	// Split synthesis identical to canonical (same helper).
	wantCash := account.Cash{Available: 2_000_000, Reserved: 0, Settled: 0, Bank: 100_000}
	if loaded.Account.Cash != wantCash {
		t.Errorf("Cash split:\n got  %+v\n want %+v", loaded.Account.Cash, wantCash)
	}

	// LoadAccount + WriteSnapshot both called — on-demand IS snapshotted
	// (Redis-recoverable within life; doc §4 step 5).
	if len(fake.LoadAccountCalls) != 1 {
		t.Errorf("LoadAccount call count: got %d, want 1", len(fake.LoadAccountCalls))
	}
	if len(fake.WriteSnapshotCalls) != 1 {
		t.Errorf("WriteSnapshot call count: got %d, want 1 (on-demand still snapshots)", len(fake.WriteSnapshotCalls))
	}
	// No SaveAccount ever (the defining on-demand promise).
	if len(fake.SaveAccountCalls) != 0 {
		t.Errorf("SaveAccount MUST NOT run on on-demand load; got %d calls", len(fake.SaveAccountCalls))
	}
	// LoadedAccount identity carry.
	if loaded.OwnerType != OwnerAgent || loaded.OwnerID != "owner-od" {
		t.Errorf("identity: got (%s, %s), want (agent, owner-od)", loaded.OwnerType, loaded.OwnerID)
	}
}

func TestOndemandSessionLoadMapsErrNoAccount(t *testing.T) {
	// On-demand REQUIRES an existing durable account to snapshot from
	// (doc §4 step 2: "can't practice-trade an account that was never
	// created"). No lazy-create — ErrNoAccount hard-fails identically
	// to canonical. WriteSnapshot must NOT run.
	fake := &fakeStore{
		LoadAccountFn: func(ctx context.Context, accountID string) (persistence.DurableAccount, error) {
			return persistence.DurableAccount{}, fmt.Errorf("%w: %s", persistence.ErrNoAccount, accountID)
		},
	}
	service := NewService(fake)

	loaded, err := service.OndemandSessionLoad(context.Background(), "sess-x", account.AccountID("acct-missing"))
	if loaded.Account != nil {
		t.Errorf("LoadedAccount.Account: got non-nil on error path")
	}
	if !errors.Is(err, ErrNoAccount) {
		t.Fatalf("errors.Is(err, ErrNoAccount): got err = %v", err)
	}
	if len(fake.WriteSnapshotCalls) != 0 {
		t.Errorf("WriteSnapshot must NOT run when LoadAccount returns ErrNoAccount; got %d", len(fake.WriteSnapshotCalls))
	}
}

func TestOndemandSessionLoadToleratesSnapshotWriteFailure(t *testing.T) {
	// The deliberate divergence from canonical (doc §4 step 6): on-demand
	// LOG-AND-CONTINUES on WriteSnapshot failure. Reason: an on-demand
	// crash-loss costs nothing (never writes back, isn't a record), so
	// refusing the session start would be over-strict.
	//
	// The return contract on this path:
	//   - err == nil
	//   - loaded.Account is populated and usable
	//   - LoadAccount ran; WriteSnapshot was ATTEMPTED (call recorded)
	injected := errors.New("redis: EXEC failed")
	fake := &fakeStore{
		LoadAccountFn: func(ctx context.Context, accountID string) (persistence.DurableAccount, error) {
			return durableFor(accountID, "owner-tol", 500_000, 0), nil
		},
		WriteSnapshotFn: func(ctx context.Context, payload persistence.SnapshotPayload) error {
			return injected
		},
	}
	service := NewService(fake)

	loaded, err := service.OndemandSessionLoad(context.Background(), "sess-tol", account.AccountID("acct-tol"))
	if err != nil {
		t.Fatalf("on-demand snapshot-write failure must be tolerated: got err = %v", err)
	}
	if loaded.Account == nil {
		t.Fatalf("LoadedAccount.Account: got nil (should be usable in-memory copy)")
	}
	if loaded.Account.PersistOnClose {
		t.Errorf("PersistOnClose: got true, want false (still on-demand after tolerance)")
	}
	if loaded.Account.Cash.Available != 500_000 {
		t.Errorf("Cash.Available: got %d, want 500_000 (in-memory copy still valid)", loaded.Account.Cash.Available)
	}
	// The store was consulted in the right order — LoadAccount + WriteSnapshot both attempted.
	if len(fake.LoadAccountCalls) != 1 {
		t.Errorf("LoadAccount call count: got %d, want 1", len(fake.LoadAccountCalls))
	}
	if len(fake.WriteSnapshotCalls) != 1 {
		t.Errorf("WriteSnapshot call count: got %d, want 1 (attempted then tolerated)", len(fake.WriteSnapshotCalls))
	}
}

func TestOndemandSessionLoadMapsTransportFailure(t *testing.T) {
	// A LoadAccount transport error hard-fails on-demand too (there's
	// nothing to seed from). Symmetric with canonical.
	injected := errors.New("pg: connection reset")
	fake := &fakeStore{
		LoadAccountFn: func(ctx context.Context, accountID string) (persistence.DurableAccount, error) {
			return persistence.DurableAccount{}, injected
		},
	}
	service := NewService(fake)

	loaded, err := service.OndemandSessionLoad(context.Background(), "sess-x", account.AccountID("acct-x"))
	if loaded.Account != nil {
		t.Errorf("LoadedAccount.Account: got non-nil on transport failure")
	}
	if !errors.Is(err, ErrLoadFailed) {
		t.Fatalf("errors.Is(err, ErrLoadFailed): got err = %v", err)
	}
	if !errors.Is(err, injected) {
		t.Fatalf("errors.Is(err, injected): got err = %v", err)
	}
}
