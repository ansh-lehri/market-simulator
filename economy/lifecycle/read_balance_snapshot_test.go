package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"simulator/economy/account"
	"simulator/economy/persistence"
)

// happy-path table: proves the payload → *Cash mapping is field-exact and
// that signed Available (Option B) passes through unchanged in both signs.
// Covers your [[feedback_tests_probe_dont_assume]] rule — both signs of
// the sign-carrying field are covered explicitly.
func TestReadBalanceSnapshotHappyPathMapping(t *testing.T) {
	dripProgress := int64(4200)
	cases := []struct {
		name string
		cash persistence.CashSnap
	}{
		{
			name: "positive available",
			cash: persistence.CashSnap{Available: 1_500_000, Reserved: 200_000, Settled: 50_000, BankCash: 100_000, DripActive: true, DripProgress: &dripProgress},
		},
		{
			name: "zero available (brand-new session, must NOT look like missing)",
			cash: persistence.CashSnap{Available: 0, Reserved: 0, Settled: 0, BankCash: 0, DripActive: false, DripProgress: nil},
		},
		{
			name: "negative available (debt, Option B)",
			cash: persistence.CashSnap{Available: -777_777, Reserved: 100, Settled: 33, BankCash: 42, DripActive: false, DripProgress: nil},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(subtest *testing.T) {
			expected := testCase.cash
			fake := &fakeStore{
				ReadSnapshotFn: func(ctx context.Context, sessionID, accountID string) (persistence.SnapshotPayload, error) {
					return persistence.SnapshotPayload{
						SessionID: sessionID,
						AccountID: accountID,
						Cash:      expected,
					}, nil
				},
			}
			service := NewService(fake)

			got, err := service.ReadBalanceSnapshot(context.Background(), "sess-1", account.AccountID("acct-1"))
			if err != nil {
				subtest.Fatalf("ReadBalanceSnapshot: %v", err)
			}
			if got == nil {
				subtest.Fatalf("ReadBalanceSnapshot: got nil *Cash on success")
			}
			// Field-exact: Available (signed), Reserved, Settled from Cash;
			// Bank from CashSnap.BankCash (the field-name is different at
			// the persistence layer). Drip fields are NOT part of *Cash.
			wantCash := account.Cash{
				Available: expected.Available,
				Reserved:  expected.Reserved,
				Settled:   expected.Settled,
				Bank:      expected.BankCash,
			}
			if *got != wantCash {
				subtest.Errorf("Cash mismatch:\n got  %+v\n want %+v", *got, wantCash)
			}

			// Store contract: exactly ONE ReadSnapshot call, with the
			// exact ids the caller passed. No other store method touched.
			if len(fake.ReadSnapshotCalls) != 1 {
				subtest.Errorf("ReadSnapshot call count: got %d, want 1", len(fake.ReadSnapshotCalls))
			}
			if len(fake.ReadSnapshotCalls) > 0 {
				call := fake.ReadSnapshotCalls[0]
				if call.SessionID != "sess-1" || call.AccountID != "acct-1" {
					subtest.Errorf("ReadSnapshot args: got (%s,%s), want (sess-1,acct-1)", call.SessionID, call.AccountID)
				}
			}
			assertOnlyReadSnapshotCalled(subtest, fake)
		})
	}
}

func TestReadBalanceSnapshotMapsErrSnapshotMissing(t *testing.T) {
	// A persistence.ErrSnapshotMissing MUST surface as
	// ErrSnapshotMissingFor{sessionID, accountID}, unwrapping via
	// errors.Is to lifecycle.ErrSnapshotMissing so the caller can dispatch
	// on category (per [[feedback_error_category_sentinels]]).
	fake := &fakeStore{
		ReadSnapshotFn: func(ctx context.Context, sessionID, accountID string) (persistence.SnapshotPayload, error) {
			return persistence.SnapshotPayload{SessionID: sessionID, AccountID: accountID}, persistence.ErrSnapshotMissing
		},
	}
	service := NewService(fake)

	got, err := service.ReadBalanceSnapshot(context.Background(), "sess-gone", account.AccountID("acct-gone"))
	if got != nil {
		t.Errorf("expected nil *Cash on ErrSnapshotMissing, got %+v", *got)
	}
	if !errors.Is(err, ErrSnapshotMissing) {
		t.Fatalf("errors.Is(err, ErrSnapshotMissing): got err = %v", err)
	}
	// Struct-typed error carries the ids for the caller to log / correlate.
	var typed ErrSnapshotMissingFor
	if !errors.As(err, &typed) {
		t.Fatalf("errors.As(err, *ErrSnapshotMissingFor): failed, got %v", err)
	}
	if typed.SessionID != "sess-gone" || typed.AccountID != account.AccountID("acct-gone") {
		t.Errorf("ErrSnapshotMissingFor: got %+v, want {sess-gone, acct-gone}", typed)
	}
}

func TestReadBalanceSnapshotMapsTransportFailure(t *testing.T) {
	// A non-ErrSnapshotMissing error from persistence surfaces as
	// ErrLoadFailedFor{accountID, cause}. Callers dispatch via errors.Is
	// on ErrLoadFailed AND on the wrapped cause.
	injected := errors.New("redis timeout after 5s")
	fake := &fakeStore{
		ReadSnapshotFn: func(ctx context.Context, sessionID, accountID string) (persistence.SnapshotPayload, error) {
			return persistence.SnapshotPayload{}, fmt.Errorf("persistence: readSnapshot cash: %w", injected)
		},
	}
	service := NewService(fake)

	got, err := service.ReadBalanceSnapshot(context.Background(), "sess-x", account.AccountID("acct-x"))
	if got != nil {
		t.Errorf("expected nil *Cash on transport failure, got %+v", *got)
	}
	if !errors.Is(err, ErrLoadFailed) {
		t.Fatalf("errors.Is(err, ErrLoadFailed): got err = %v", err)
	}
	if !errors.Is(err, injected) {
		t.Fatalf("errors.Is(err, injected cause): got err = %v", err)
	}
	// And critically NOT ErrSnapshotMissing — the two must be
	// distinguishable at the lifecycle boundary.
	if errors.Is(err, ErrSnapshotMissing) {
		t.Errorf("transport failure must NOT map to ErrSnapshotMissing: err = %v", err)
	}
	var typed ErrLoadFailedFor
	if !errors.As(err, &typed) {
		t.Fatalf("errors.As(err, *ErrLoadFailedFor): failed, got %v", err)
	}
	if typed.AccountID != account.AccountID("acct-x") {
		t.Errorf("ErrLoadFailedFor.AccountID: got %s, want acct-x", typed.AccountID)
	}
}

// assertOnlyReadSnapshotCalled proves ReadBalanceSnapshot doesn't leak
// into any other store method — it's a pure Redis GET per doc §5.
func assertOnlyReadSnapshotCalled(t *testing.T, fake *fakeStore) {
	t.Helper()
	if len(fake.OpenAccountCalls) != 0 {
		t.Errorf("unexpected OpenAccount calls: %d", len(fake.OpenAccountCalls))
	}
	if len(fake.LoadAccountCalls) != 0 {
		t.Errorf("unexpected LoadAccount calls: %d", len(fake.LoadAccountCalls))
	}
	if len(fake.SaveAccountCalls) != 0 {
		t.Errorf("unexpected SaveAccount calls: %d", len(fake.SaveAccountCalls))
	}
	if len(fake.WriteSnapshotCalls) != 0 {
		t.Errorf("unexpected WriteSnapshot calls: %d", len(fake.WriteSnapshotCalls))
	}
	if len(fake.DeleteSnapshotCalls) != 0 {
		t.Errorf("unexpected DeleteSnapshot calls: %d", len(fake.DeleteSnapshotCalls))
	}
}
