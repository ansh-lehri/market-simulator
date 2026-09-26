package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/persistence"
)

// evolvedPayloadFor builds a SnapshotPayload representing a MID-SESSION
// crash: cash split has non-zero Reserved / Settled (proving no synthesis),
// two instruments held with multiple lots (proving FIFO order), and one
// open reservation with all cost-lifecycle flags set (proving the full
// ReservationSnap round-trips).
//
// The values are picked so every field is DISTINCT — a swapped field
// during hydration would produce a wrong number, not a legal-looking value.
func evolvedPayloadFor(sessionID, accountID string) persistence.SnapshotPayload {
	drip := int64(9_100)
	entryAtEarlier := time.Date(2026, 3, 4, 9, 30, 15, 0, time.UTC)
	entryAtLater := time.Date(2026, 3, 4, 10, 45, 22, 0, time.UTC)

	return persistence.SnapshotPayload{
		SessionID: sessionID,
		AccountID: accountID,
		Cash: persistence.CashSnap{
			Available:    -12_345, // NEGATIVE — Option B mid-session debt survives restore
			Reserved:     50_000,
			Settled:      7_777,
			BankCash:     900_000,
			DripActive:   true,
			DripProgress: &drip,
		},
		Positions: []persistence.PositionSnap{
			{
				InstrumentID:   "ACME",
				AvailableUnits: 40,
				ReservedUnits:  10,
				OpenLots: []persistence.LotSnap{
					// FIFO: earlier lot first, then later.
					{EntryPrice: 100, EntryAt: entryAtEarlier, RemainingQty: 30},
					{EntryPrice: 110, EntryAt: entryAtLater, RemainingQty: 20},
				},
			},
			{
				InstrumentID:   "BETA",
				AvailableUnits: 5,
				ReservedUnits:  0,
				OpenLots: []persistence.LotSnap{
					{EntryPrice: 250, EntryAt: entryAtEarlier, RemainingQty: 5},
				},
			},
		},
		Reservations: []persistence.ReservationSnap{
			{
				OrderID:          contract.OrderID("ord-A"),
				AccountID:        account.AccountID(accountID),
				SessionID:        sessionID,
				InstrumentID:     contract.InstrumentID("ACME"),
				Side:             contract.Buy,
				IsMarket:         false,
				ReservedCash:     50_000,
				ReservedUnits:    0,
				RemainingQty:     10,
				OrigQty:          25,
				EverFilled:       true, // partial fill mid-session — brokerage-once state MUST survive
				BrokerageCharged: false,
			},
		},
	}
}

// TestCrashRecoveryReloadCanonicalRestoresVerbatim covers the defining
// property of item 23 (doc §7): the cash split, positions, and reservation
// records come back BIT-FOR-BIT identical to what the snapshot held. No
// synthesis (contrast with SessionLoad, which zeroes Reserved / Settled).
func TestCrashRecoveryReloadCanonicalRestoresVerbatim(t *testing.T) {
	payload := evolvedPayloadFor("sess-crash", "acct-crash")
	fake := &fakeStore{
		ReadSnapshotFn: func(ctx context.Context, sessionID, accountID string) (persistence.SnapshotPayload, error) {
			if sessionID != "sess-crash" || accountID != "acct-crash" {
				t.Errorf("ReadSnapshot args: got (%s, %s), want (sess-crash, acct-crash)", sessionID, accountID)
			}
			return payload, nil
		},
	}
	service := NewService(fake)

	restored, err := service.CrashRecoveryReload(context.Background(), "sess-crash",
		account.AccountID("acct-crash"), Canonical)
	if err != nil {
		t.Fatalf("CrashRecoveryReload: %v", err)
	}
	if restored.Account == nil {
		t.Fatalf("RestoredSession.Account: got nil on happy path")
	}

	// Cash split VERBATIM — the load-bearing anti-synthesis assertion. Note
	// the negative Available and non-zero Reserved / Settled: SessionLoad
	// would zero those; CrashRecoveryReload must not.
	wantCash := account.Cash{
		Available: -12_345,
		Reserved:  50_000,
		Settled:   7_777,
		Bank:      900_000,
	}
	if restored.Account.Cash != wantCash {
		t.Errorf("Cash split:\n got  %+v\n want %+v", restored.Account.Cash, wantCash)
	}
	if restored.Account.ID != account.AccountID("acct-crash") {
		t.Errorf("Account.ID: got %q, want acct-crash", restored.Account.ID)
	}
	// Canonical → PersistOnClose=true (doc §7 step 6, same rule as load).
	if !restored.Account.PersistOnClose {
		t.Errorf("PersistOnClose: got false, want true (canonical resume)")
	}
	// Drip carry-through.
	if !restored.Account.DripActive {
		t.Errorf("DripActive: got false, want true")
	}
	if restored.Account.DripProgress == nil || *restored.Account.DripProgress != 9_100 {
		t.Errorf("DripProgress: got %v, want *9100", restored.Account.DripProgress)
	}

	// Positions VERBATIM — both instruments, in the map, with the right
	// buckets and full lot lists.
	if len(restored.Account.Positions) != 2 {
		t.Fatalf("Positions count: got %d, want 2", len(restored.Account.Positions))
	}
	acme := restored.Account.Positions[contract.InstrumentID("ACME")]
	if acme == nil {
		t.Fatalf("Positions[ACME]: missing")
	}
	if acme.Available != 40 || acme.Reserved != 10 {
		t.Errorf("Positions[ACME] buckets: got Available=%d Reserved=%d, want 40 / 10",
			acme.Available, acme.Reserved)
	}
	// FIFO order preserved (doc §7 step 4).
	if len(acme.OpenLots) != 2 {
		t.Fatalf("Positions[ACME] lot count: got %d, want 2", len(acme.OpenLots))
	}
	if acme.OpenLots[0].EntryPrice != 100 || acme.OpenLots[0].RemainingQty != 30 {
		t.Errorf("Lot[0] (earlier arrival): got price=%d qty=%d, want 100 / 30",
			acme.OpenLots[0].EntryPrice, acme.OpenLots[0].RemainingQty)
	}
	if acme.OpenLots[1].EntryPrice != 110 || acme.OpenLots[1].RemainingQty != 20 {
		t.Errorf("Lot[1] (later arrival): got price=%d qty=%d, want 110 / 20",
			acme.OpenLots[1].EntryPrice, acme.OpenLots[1].RemainingQty)
	}
	if !acme.OpenLots[0].EntryAt.Before(acme.OpenLots[1].EntryAt) {
		t.Errorf("FIFO order: lot[0].EntryAt (%v) must precede lot[1].EntryAt (%v)",
			acme.OpenLots[0].EntryAt, acme.OpenLots[1].EntryAt)
	}
	beta := restored.Account.Positions[contract.InstrumentID("BETA")]
	if beta == nil || beta.Available != 5 || beta.Reserved != 0 || len(beta.OpenLots) != 1 {
		t.Errorf("Positions[BETA]: got %+v, want Available=5 Reserved=0 with 1 lot", beta)
	}

	// ReservationSnaps travels through RAW — lifecycle cannot import reserve
	// (per CLAUDE.md), so the caller hydrates the reservation book. The
	// slice must be preserved with all cost-lifecycle flags intact.
	if len(restored.ReservationSnaps) != 1 {
		t.Fatalf("ReservationSnaps count: got %d, want 1", len(restored.ReservationSnaps))
	}
	snap := restored.ReservationSnaps[0]
	if snap.OrderID != contract.OrderID("ord-A") {
		t.Errorf("Reservation OrderID: got %q, want ord-A", snap.OrderID)
	}
	if snap.OrigQty != 25 || snap.RemainingQty != 10 || snap.ReservedCash != 50_000 {
		t.Errorf("Reservation numbers: got orig=%d rem=%d resvCash=%d, want 25 / 10 / 50000",
			snap.OrigQty, snap.RemainingQty, snap.ReservedCash)
	}
	// The whole reason ReservationSnap carries the full record (per
	// persistence/CLAUDE.md flag): brokerage-once flags must survive a crash.
	if !snap.EverFilled {
		t.Errorf("EverFilled: got false, want true (brokerage-once flag must survive crash)")
	}
	if snap.BrokerageCharged {
		t.Errorf("BrokerageCharged: got true, want false (still unpaid at crash)")
	}

	// Structural: exactly ONE ReadSnapshot call; no other store method
	// touched. Item 23 is Redis-only — never a Postgres hop.
	if len(fake.ReadSnapshotCalls) != 1 {
		t.Errorf("ReadSnapshot call count: got %d, want 1", len(fake.ReadSnapshotCalls))
	}
	if len(fake.LoadAccountCalls) != 0 {
		t.Errorf("LoadAccount MUST NOT be called on crash recovery; got %d", len(fake.LoadAccountCalls))
	}
	if len(fake.SaveAccountCalls) != 0 ||
		len(fake.OpenAccountCalls) != 0 ||
		len(fake.WriteSnapshotCalls) != 0 ||
		len(fake.DeleteSnapshotCalls) != 0 {
		t.Errorf("crash_recovery_reload is READ-ONLY; got open=%d save=%d write=%d delete=%d",
			len(fake.OpenAccountCalls), len(fake.SaveAccountCalls),
			len(fake.WriteSnapshotCalls), len(fake.DeleteSnapshotCalls))
	}
}

// TestCrashRecoveryReloadOnDemandTagsPersistOnCloseFalse — same restore,
// on-demand flavor. Only the one boolean differs (doc §7 step 6, mirrors
// the canonical / on-demand rule at load).
func TestCrashRecoveryReloadOnDemandTagsPersistOnCloseFalse(t *testing.T) {
	payload := evolvedPayloadFor("sess-od-crash", "acct-od-crash")
	fake := &fakeStore{
		ReadSnapshotFn: func(ctx context.Context, sessionID, accountID string) (persistence.SnapshotPayload, error) {
			return payload, nil
		},
	}
	service := NewService(fake)

	restored, err := service.CrashRecoveryReload(context.Background(), "sess-od-crash",
		account.AccountID("acct-od-crash"), OnDemand)
	if err != nil {
		t.Fatalf("CrashRecoveryReload (on-demand): %v", err)
	}
	if restored.Account.PersistOnClose {
		t.Errorf("PersistOnClose: got true, want false (on-demand resume)")
	}
	// Split still verbatim — the on-demand flavor changes ONLY the boolean.
	if restored.Account.Cash.Reserved != 50_000 || restored.Account.Cash.Settled != 7_777 {
		t.Errorf("Cash split (on-demand): got Reserved=%d Settled=%d, want 50000 / 7777 (still verbatim)",
			restored.Account.Cash.Reserved, restored.Account.Cash.Settled)
	}
}

// TestCrashRecoveryReloadEmptySnapshotRestoresFlatSession covers the case
// where the crash happened before ANY activity: cash split is all zeros
// (a legitimate seed for a zero-capital session), no positions, no
// reservations. Must still succeed — an empty snapshot is a valid state,
// distinct from a missing snapshot.
func TestCrashRecoveryReloadEmptySnapshotRestoresFlatSession(t *testing.T) {
	fake := &fakeStore{
		ReadSnapshotFn: func(ctx context.Context, sessionID, accountID string) (persistence.SnapshotPayload, error) {
			return persistence.SnapshotPayload{
				SessionID: sessionID,
				AccountID: accountID,
				Cash:      persistence.CashSnap{}, // all zeros — a valid seed
				// Positions, Reservations: nil
			}, nil
		},
	}
	service := NewService(fake)

	restored, err := service.CrashRecoveryReload(context.Background(), "sess-empty",
		account.AccountID("acct-empty"), Canonical)
	if err != nil {
		t.Fatalf("empty snapshot must be legal: %v", err)
	}
	if restored.Account == nil {
		t.Fatalf("Account: got nil on empty snapshot")
	}
	if restored.Account.Cash != (account.Cash{}) {
		t.Errorf("Cash: got %+v, want zero-value", restored.Account.Cash)
	}
	if restored.Account.Positions == nil {
		t.Errorf("Positions: got nil map, want non-nil empty (contract with hands)")
	}
	if len(restored.Account.Positions) != 0 {
		t.Errorf("Positions count: got %d, want 0", len(restored.Account.Positions))
	}
	if len(restored.ReservationSnaps) != 0 {
		t.Errorf("ReservationSnaps: got %d, want 0", len(restored.ReservationSnaps))
	}
}

// TestCrashRecoveryReloadMapsErrNoSnapshot — missing snapshot key from
// persistence maps to ErrNoSnapshotFor (item 23's distinct sentinel). NOT
// ErrSnapshotMissingFor (that's item 16's shape) — different callers need
// different decisions on the same underlying event, so the lifecycle
// sentinel is different.
func TestCrashRecoveryReloadMapsErrNoSnapshot(t *testing.T) {
	fake := &fakeStore{
		ReadSnapshotFn: func(ctx context.Context, sessionID, accountID string) (persistence.SnapshotPayload, error) {
			return persistence.SnapshotPayload{}, fmt.Errorf("%w: session=%s account=%s",
				persistence.ErrSnapshotMissing, sessionID, accountID)
		},
	}
	service := NewService(fake)

	restored, err := service.CrashRecoveryReload(context.Background(), "sess-missing",
		account.AccountID("acct-missing"), Canonical)
	if restored.Account != nil {
		t.Errorf("Account: got non-nil on missing-snapshot path")
	}
	if !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("errors.Is(err, ErrNoSnapshot): got err = %v", err)
	}
	// The item-16 sentinel is a DIFFERENT event — recovery has no in-flight
	// state to restore, whereas item 16's caller (bootstrap read) has yet
	// to write the seed. Confirm the two do not collide.
	if errors.Is(err, ErrSnapshotMissing) {
		t.Errorf("crash-recovery MUST NOT map to ErrSnapshotMissing (item-16 sentinel); got %v", err)
	}
	var typed ErrNoSnapshotFor
	if !errors.As(err, &typed) {
		t.Fatalf("errors.As(err, *ErrNoSnapshotFor): failed, got %v", err)
	}
	if typed.SessionID != "sess-missing" || typed.AccountID != account.AccountID("acct-missing") {
		t.Errorf("ErrNoSnapshotFor ids: got %+v, want {sess-missing, acct-missing}", typed)
	}
}

// TestCrashRecoveryReloadMapsTransportFailure — Redis / decode failure
// (anything not ErrSnapshotMissing) maps to ErrLoadFailedFor with the
// cause wrapped. Symmetric with SessionLoad's transport branch: never seed
// from a failed money read.
func TestCrashRecoveryReloadMapsTransportFailure(t *testing.T) {
	injected := errors.New("redis: EXEC failed on cash GET")
	fake := &fakeStore{
		ReadSnapshotFn: func(ctx context.Context, sessionID, accountID string) (persistence.SnapshotPayload, error) {
			return persistence.SnapshotPayload{}, injected
		},
	}
	service := NewService(fake)

	restored, err := service.CrashRecoveryReload(context.Background(), "sess-x",
		account.AccountID("acct-x"), Canonical)
	if restored.Account != nil {
		t.Errorf("Account: got non-nil on transport failure")
	}
	if !errors.Is(err, ErrLoadFailed) {
		t.Fatalf("errors.Is(err, ErrLoadFailed): got err = %v", err)
	}
	if !errors.Is(err, injected) {
		t.Fatalf("errors.Is(err, injected cause): got err = %v", err)
	}
	// Must NOT map to ErrNoSnapshot — a transport failure is not "no state
	// to recover from"; it's "we don't know."
	if errors.Is(err, ErrNoSnapshot) {
		t.Errorf("transport failure MUST NOT map to ErrNoSnapshot; got %v", err)
	}
	var typed ErrLoadFailedFor
	if !errors.As(err, &typed) {
		t.Fatalf("errors.As(err, *ErrLoadFailedFor): failed, got %v", err)
	}
	if typed.AccountID != account.AccountID("acct-x") {
		t.Errorf("ErrLoadFailedFor.AccountID: got %q, want acct-x", typed.AccountID)
	}
}

// TestCrashRecoveryReloadDoesNotSynthesize is the anti-synthesis property
// stated three ways in the doc (§7 status, §7 "difference from
// session_load", §7 step 3): items that would be zeroed by hydrateFreshAccount
// must NOT be zeroed here. Covers the case that would silently pass if
// someone accidentally routed through hydrateFreshAccount.
func TestCrashRecoveryReloadDoesNotSynthesize(t *testing.T) {
	// A snapshot whose Available equals TradingCash would happen to be
	// numerically-indistinguishable from a fresh synthesis IF the split
	// were being resynthesized. We defeat that by using values that ONLY
	// make sense mid-session (Reserved != 0, Settled != 0). Any codepath
	// that ran hydrateFreshAccount would zero those and the assertion
	// would fail immediately.
	payload := persistence.SnapshotPayload{
		SessionID: "sess-antisynth",
		AccountID: "acct-antisynth",
		Cash: persistence.CashSnap{
			Available: 200_000,
			Reserved:  30_000, // <-- would be zeroed by synthesis
			Settled:   40_000, // <-- would be zeroed by synthesis
			BankCash:  10_000,
		},
	}
	fake := &fakeStore{
		ReadSnapshotFn: func(ctx context.Context, sessionID, accountID string) (persistence.SnapshotPayload, error) {
			return payload, nil
		},
	}
	service := NewService(fake)

	restored, err := service.CrashRecoveryReload(context.Background(), "sess-antisynth",
		account.AccountID("acct-antisynth"), Canonical)
	if err != nil {
		t.Fatalf("CrashRecoveryReload: %v", err)
	}
	if restored.Account.Cash.Reserved != 30_000 {
		t.Errorf("Reserved: got %d, want 30000 (SYNTHESIS BUG — item 23 must not zero Reserved)",
			restored.Account.Cash.Reserved)
	}
	if restored.Account.Cash.Settled != 40_000 {
		t.Errorf("Settled: got %d, want 40000 (SYNTHESIS BUG — item 23 must not zero Settled)",
			restored.Account.Cash.Settled)
	}
}
