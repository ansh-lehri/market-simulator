package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"simulator/economy/persistence"
)

// TestOpenAccountValidation covers each input-validation branch in the
// order doc §2 step 1 lists them. Every branch must fail BEFORE the
// store is touched (otherwise a caller bug could still hit the DB).
func TestOpenAccountValidation(t *testing.T) {
	cases := []struct {
		name            string
		ownerType       OwnerType
		ownerID         string
		startingCapital int64
		startingBank    int64
		wantSentinel    error
	}{
		{"bad owner type (below range)", OwnerType(-1), "owner-1", 100, 0, ErrBadOwnerType},
		{"bad owner type (above range)", OwnerType(2), "owner-1", 100, 0, ErrBadOwnerType},
		{"empty owner id", OwnerAgent, "", 100, 0, ErrBadOwnerID},
		{"negative starting_capital", OwnerAgent, "owner-1", -1, 0, ErrNegativeStartingCapital},
		{"negative starting_bank", OwnerAgent, "owner-1", 0, -1, ErrNegativeStartingBank},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(subtest *testing.T) {
			fake := &fakeStore{}
			service := NewService(fake)

			gotID, err := service.OpenAccount(context.Background(),
				testCase.ownerType, testCase.ownerID,
				testCase.startingCapital, testCase.startingBank)

			if !errors.Is(err, testCase.wantSentinel) {
				subtest.Fatalf("errors.Is(err, %v): got err = %v", testCase.wantSentinel, err)
			}
			if gotID != "" {
				subtest.Errorf("AccountID: got %q, want empty on validation error", gotID)
			}
			// Structural: no store call on any validation branch — that
			// keeps a caller bug from hitting the DB.
			if len(fake.OpenAccountCalls) != 0 {
				subtest.Errorf("OpenAccount MUST NOT be called on validation error; got %d calls", len(fake.OpenAccountCalls))
			}
		})
	}
}

// TestOpenAccountValidationBoundaryZeros verifies the >= 0 birth-time
// floors admit zero (a legitimate "zero-capital signup" case). Confirms
// the validation is > 0 vs >= 0 correct at both boundaries.
func TestOpenAccountValidationBoundaryZeros(t *testing.T) {
	cases := []struct {
		name            string
		startingCapital int64
		startingBank    int64
	}{
		{"both zero (legal — poorest starting user)", 0, 0},
		{"capital zero, bank positive", 0, 100},
		{"capital positive, bank zero", 100, 0},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(subtest *testing.T) {
			fake := &fakeStore{
				OpenAccountFn: func(ctx context.Context, durable persistence.DurableAccount) (string, error) {
					return "acct-generated", nil
				},
			}
			service := NewService(fake)

			gotID, err := service.OpenAccount(context.Background(),
				OwnerAgent, "owner-boundary",
				testCase.startingCapital, testCase.startingBank)
			if err != nil {
				subtest.Fatalf("boundary zero must be legal: got err = %v", err)
			}
			if gotID != "acct-generated" {
				subtest.Errorf("AccountID: got %q, want acct-generated", gotID)
			}
		})
	}
}

func TestOpenAccountHappyPath(t *testing.T) {
	// Happy path: persistence.OpenAccount is called with the correct DTO
	// (empty AccountID → persistence mints; owner-type string mapping;
	// signed TradingCash passthrough for the caller's config figure).
	fake := &fakeStore{
		OpenAccountFn: func(ctx context.Context, durable persistence.DurableAccount) (string, error) {
			// DTO shape checks (this is the critical assertion — the
			// contract between lifecycle and persistence).
			if durable.AccountID != "" {
				t.Errorf("DurableAccount.AccountID: got %q, want empty (persistence mints)", durable.AccountID)
			}
			if durable.OwnerType != "agent" {
				t.Errorf("OwnerType: got %q, want \"agent\"", durable.OwnerType)
			}
			if durable.OwnerID != "owner-happy" {
				t.Errorf("OwnerID: got %q, want owner-happy", durable.OwnerID)
			}
			if durable.TradingCash != 5_000_000 {
				t.Errorf("TradingCash: got %d, want 5_000_000", durable.TradingCash)
			}
			if durable.BankCash != 100_000 {
				t.Errorf("BankCash: got %d, want 100_000", durable.BankCash)
			}
			if durable.DripActive {
				t.Errorf("DripActive: got true, want false (birth default)")
			}
			if durable.DripProgress != nil {
				t.Errorf("DripProgress: got %v, want nil (birth default)", *durable.DripProgress)
			}
			return "acct-xyz-123", nil
		},
	}
	service := NewService(fake)

	gotID, err := service.OpenAccount(context.Background(),
		OwnerAgent, "owner-happy", 5_000_000, 100_000)
	if err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}
	if gotID != "acct-xyz-123" {
		t.Errorf("AccountID: got %q, want acct-xyz-123", gotID)
	}

	// Structural: exactly one OpenAccount call; no other store method touched.
	if len(fake.OpenAccountCalls) != 1 {
		t.Errorf("OpenAccount call count: got %d, want 1", len(fake.OpenAccountCalls))
	}
	if len(fake.LoadAccountCalls) != 0 ||
		len(fake.SaveAccountCalls) != 0 ||
		len(fake.WriteSnapshotCalls) != 0 ||
		len(fake.ReadSnapshotCalls) != 0 ||
		len(fake.DeleteSnapshotCalls) != 0 {
		t.Errorf("open_account touches Postgres ONLY; got load=%d save=%d write=%d read=%d delete=%d",
			len(fake.LoadAccountCalls), len(fake.SaveAccountCalls),
			len(fake.WriteSnapshotCalls), len(fake.ReadSnapshotCalls),
			len(fake.DeleteSnapshotCalls))
	}
}

func TestOpenAccountBothOwnerTypesMap(t *testing.T) {
	// Every legal OwnerType must map to its correct DB column string.
	// Covers the both-branches rule per [[feedback_tests_probe_dont_assume]].
	cases := []struct {
		ownerType OwnerType
		wantText  string
	}{
		{OwnerAgent, "agent"},
		{OwnerHuman, "human"},
	}
	for _, testCase := range cases {
		t.Run(testCase.wantText, func(subtest *testing.T) {
			var captured string
			fake := &fakeStore{
				OpenAccountFn: func(ctx context.Context, durable persistence.DurableAccount) (string, error) {
					captured = durable.OwnerType
					return "acct-1", nil
				},
			}
			service := NewService(fake)
			if _, err := service.OpenAccount(context.Background(), testCase.ownerType, "owner-1", 100, 0); err != nil {
				subtest.Fatalf("OpenAccount: %v", err)
			}
			if captured != testCase.wantText {
				subtest.Errorf("OwnerType column: got %q, want %q", captured, testCase.wantText)
			}
		})
	}
}

func TestOpenAccountMapsErrAccountExists(t *testing.T) {
	// persistence.ErrAccountExists → ErrAccountExistsFor{OwnerType,
	// OwnerID}, unwraps via errors.Is to lifecycle.ErrAccountExists.
	// Callers dispatch on category; the struct-typed error carries the
	// identity for the caller to look up the existing handle.
	fake := &fakeStore{
		OpenAccountFn: func(ctx context.Context, durable persistence.DurableAccount) (string, error) {
			return "", fmt.Errorf("%w: owner_type=%s owner_id=%s",
				persistence.ErrAccountExists, durable.OwnerType, durable.OwnerID)
		},
	}
	service := NewService(fake)

	gotID, err := service.OpenAccount(context.Background(),
		OwnerHuman, "human-42", 100_000, 0)
	if gotID != "" {
		t.Errorf("AccountID: got %q, want empty on ErrAccountExists", gotID)
	}
	if !errors.Is(err, ErrAccountExists) {
		t.Fatalf("errors.Is(err, ErrAccountExists): got err = %v", err)
	}
	var typed ErrAccountExistsFor
	if !errors.As(err, &typed) {
		t.Fatalf("errors.As(err, *ErrAccountExistsFor): failed, got %v", err)
	}
	if typed.OwnerType != OwnerHuman || typed.OwnerID != "human-42" {
		t.Errorf("ErrAccountExistsFor: got %+v, want {OwnerHuman, human-42}", typed)
	}
}

func TestOpenAccountMapsGenericError(t *testing.T) {
	// Any non-ErrAccountExists error from persistence → ErrCreateFailedFor
	// with the cause wrapped. Must NOT map to ErrAccountExists.
	injected := errors.New("pg: 42501 permission denied on accounts")
	fake := &fakeStore{
		OpenAccountFn: func(ctx context.Context, durable persistence.DurableAccount) (string, error) {
			return "", injected
		},
	}
	service := NewService(fake)

	gotID, err := service.OpenAccount(context.Background(),
		OwnerAgent, "owner-perm", 100, 0)
	if gotID != "" {
		t.Errorf("AccountID: got %q, want empty on ErrCreateFailed", gotID)
	}
	if !errors.Is(err, ErrCreateFailed) {
		t.Fatalf("errors.Is(err, ErrCreateFailed): got err = %v", err)
	}
	if !errors.Is(err, injected) {
		t.Fatalf("errors.Is(err, injected cause): got err = %v", err)
	}
	if errors.Is(err, ErrAccountExists) {
		t.Errorf("generic error must NOT map to ErrAccountExists; got %v", err)
	}
	var typed ErrCreateFailedFor
	if !errors.As(err, &typed) {
		t.Fatalf("errors.As(err, *ErrCreateFailedFor): failed, got %v", err)
	}
	if typed.OwnerType != OwnerAgent || typed.OwnerID != "owner-perm" {
		t.Errorf("ErrCreateFailedFor: got %+v, want {OwnerAgent, owner-perm}", typed)
	}
}
