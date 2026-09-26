package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/persistence"
)

// TestIntegration_OpenLoadRead exercises the canonical bring-up sequence
// (item 18 → item 19 → item 16) against the real persistence.Component:
//
//	OpenAccount            (Postgres INSERT — durable birth)
//	SessionLoad            (Postgres SELECT, then Redis SEED)
//	ReadBalanceSnapshot    (Redis GET, off the Postgres path)
//
// Proves the read-model rule verbatim from economy/CLAUDE.md ("Redis-first
// for the snapshot ... seeded ONCE from Postgres at session_load"):
// ReadBalanceSnapshot returns THE seeded split without ever touching
// Postgres again. Also proves that hydration synthesizes the split
// correctly against a real DurableAccount (Available=trading_cash,
// Reserved=Settled=0, Bank=bank_cash).
func TestIntegration_OpenLoadRead(t *testing.T) {
	pool := requirePostgresPool(t)
	client, namespace := requireRedis(t)
	service, _ := buildLifecycleForIntegration(pool, client, namespace)

	ctx := context.Background()
	ownerID := uniqueOwnerID(t, "open-load-read")

	// Item 18 — durable birth.
	accountID, err := service.OpenAccount(ctx, OwnerAgent, ownerID, 5_000_000, 100_000)
	if err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}
	registerAccountCleanup(t, pool, string(accountID))

	// Item 19 — canonical load. Postgres → *Account + initial Redis seed.
	sessionID := "sess-" + ownerID
	loaded, err := service.SessionLoad(ctx, sessionID, accountID)
	if err != nil {
		t.Fatalf("SessionLoad: %v", err)
	}
	if !loaded.Account.PersistOnClose {
		t.Errorf("PersistOnClose: got false, want true (canonical)")
	}
	// Identity round-tripped through DurableAccount.
	if loaded.OwnerType != OwnerAgent || loaded.OwnerID != ownerID {
		t.Errorf("identity: got (%s, %s), want (agent, %s)", loaded.OwnerType, loaded.OwnerID, ownerID)
	}
	// Split synthesis check (only place this happens, per doc §3 step 3).
	wantHydrated := account.Cash{Available: 5_000_000, Reserved: 0, Settled: 0, Bank: 100_000}
	if loaded.Account.Cash != wantHydrated {
		t.Errorf("hydrated Cash:\n got  %+v\n want %+v", loaded.Account.Cash, wantHydrated)
	}

	// Item 16 — read the snapshot back from Redis. The load-bearing
	// assertion: the split we just synthesized is the one Redis returns.
	fromSnapshot, err := service.ReadBalanceSnapshot(ctx, sessionID, accountID)
	if err != nil {
		t.Fatalf("ReadBalanceSnapshot: %v", err)
	}
	if *fromSnapshot != wantHydrated {
		t.Errorf("snapshot Cash mismatch:\n got  %+v\n want %+v", *fromSnapshot, wantHydrated)
	}
}

// TestIntegration_MutateSaveSignedRoundTrip covers the item-21 canonical
// teardown against real Postgres, with the two properties the doc calls
// out most strongly:
//
//  1. Reconciliation: TradingCash(durable) = Available + Reserved
//     (Reserved asserted 0 at save → collapses to Available). Settled is
//     a session-only bucket and is ignored (see [[settled-semantics]]).
//  2. Signed round-trip (Option B): a NEGATIVE Available after mutation
//     survives SessionSave → LoadAccount unchanged — no floor, no clamp.
//
// After SessionSave the Redis snapshot MUST be gone (doc §5 step 5 —
// snapshot is discarded ONLY after Postgres commits).
func TestIntegration_MutateSaveSignedRoundTrip(t *testing.T) {
	pool := requirePostgresPool(t)
	client, namespace := requireRedis(t)
	service, component := buildLifecycleForIntegration(pool, client, namespace)

	ctx := context.Background()
	ownerID := uniqueOwnerID(t, "mutate-save")

	accountID, err := service.OpenAccount(ctx, OwnerHuman, ownerID, 1_000_000, 250_000)
	if err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}
	registerAccountCleanup(t, pool, string(accountID))

	sessionID := "sess-" + ownerID
	loaded, err := service.SessionLoad(ctx, sessionID, accountID)
	if err != nil {
		t.Fatalf("SessionLoad: %v", err)
	}

	// Simulate mid-session mutations that a real reserve/realize flow would
	// have made: drive Available negative (debt via DebitCash — Option B)
	// and park some cash in Settled. Per [[settled-semantics]], TradingCash
	// = Available + Reserved (Reserved asserted 0 → collapses to Available).
	// Settled is deliberately non-zero here to prove it is IGNORED at save.
	loaded.Account.Cash.Available = -500_000
	loaded.Account.Cash.Settled = 700_000 // intentional distractor — must NOT appear in TradingCash
	// Positions map already empty (born flat). Reserved stays 0.

	if err := service.SessionSave(ctx, sessionID, loaded); err != nil {
		t.Fatalf("SessionSave: %v", err)
	}

	// LoadAccount directly through the Component to prove the signed
	// figure hit Postgres. Not going through SessionLoad because that would
	// re-seed a new snapshot and mask the discard we're verifying next.
	durable, err := component.LoadAccount(ctx, string(accountID))
	if err != nil {
		t.Fatalf("LoadAccount after save: %v", err)
	}
	const wantTradingCash int64 = -500_000
	if durable.TradingCash != wantTradingCash {
		t.Errorf("reconciled TradingCash: got %d, want %d (Available + Reserved; Settled ignored)",
			durable.TradingCash, wantTradingCash)
	}

	// Drive the negative-round-trip case: mutate to a strictly-negative
	// final value and save again. This isolates the "no clamp" property —
	// Postgres must accept and echo back a negative bigint on trading_cash.
	loaded2, err := service.SessionLoad(ctx, sessionID+"-second", accountID)
	if err != nil {
		t.Fatalf("second SessionLoad: %v", err)
	}
	// After the first save the seed for this new session is
	// TradingCash=-500_000; hand-set Available directly to isolate the
	// signed round-trip. Settled left at 0 for a clean read.
	loaded2.Account.Cash.Available = -777_777
	loaded2.Account.Cash.Settled = 0
	if err := service.SessionSave(ctx, sessionID+"-second", loaded2); err != nil {
		t.Fatalf("second SessionSave: %v", err)
	}
	durable2, err := component.LoadAccount(ctx, string(accountID))
	if err != nil {
		t.Fatalf("LoadAccount after negative save: %v", err)
	}
	if durable2.TradingCash != -777_777 {
		t.Errorf("negative TradingCash round-trip: got %d, want -777777", durable2.TradingCash)
	}

	// Snapshot discard: after a successful SessionSave, ReadBalanceSnapshot
	// on the same (sessionID+"-second") key MUST fail with ErrSnapshotMissing
	// — the doc §5 order (Postgres commits → Redis DEL) leaves nothing to
	// read. This is the round-trip that proves Postgres-before-Redis.
	if _, readErr := service.ReadBalanceSnapshot(ctx, sessionID+"-second", accountID); !errors.Is(readErr, ErrSnapshotMissing) {
		t.Errorf("ReadBalanceSnapshot after save: got err = %v, want wraps ErrSnapshotMissing", readErr)
	}
}

// TestIntegration_OnDemandNoPostgresFootprint is THE on-demand promise:
// combined with 5.4's PersistOnClose gate on trades, an on-demand session
// leaves ZERO Postgres footprint. Here we exercise the lifecycle slice —
// OndemandSessionLoad + SessionDiscard — and prove the accounts row is
// untouched after the round-trip.
func TestIntegration_OnDemandNoPostgresFootprint(t *testing.T) {
	pool := requirePostgresPool(t)
	client, namespace := requireRedis(t)
	service, component := buildLifecycleForIntegration(pool, client, namespace)

	ctx := context.Background()
	ownerID := uniqueOwnerID(t, "on-demand-footprint")

	// Birth once.
	accountID, err := service.OpenAccount(ctx, OwnerAgent, ownerID, 3_000_000, 400_000)
	if err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}
	registerAccountCleanup(t, pool, string(accountID))

	// Snapshot the durable row right after birth so we can compare
	// byte-for-byte after the on-demand round-trip.
	pristine, err := component.LoadAccount(ctx, string(accountID))
	if err != nil {
		t.Fatalf("LoadAccount pristine: %v", err)
	}

	sessionID := "sess-od-" + ownerID
	loaded, err := service.OndemandSessionLoad(ctx, sessionID, accountID)
	if err != nil {
		t.Fatalf("OndemandSessionLoad: %v", err)
	}
	if loaded.Account.PersistOnClose {
		t.Errorf("PersistOnClose: got true, want false (on-demand)")
	}

	// Mutate in-memory as if the on-demand session traded. These changes
	// must NOT reach Postgres via any codepath.
	loaded.Account.Cash.Available = -99_999_999 // deliberately extreme
	loaded.Account.Cash.Settled = 12_345
	loaded.Account.Positions[contract.InstrumentID("ACME")] = &account.Position{
		Available: 100,
		Reserved:  0,
		OpenLots: []account.Lot{
			{EntryPrice: 500, EntryAt: time.Now().UTC(), RemainingQty: 100},
		},
	}

	if err := service.SessionDiscard(ctx, sessionID, loaded); err != nil {
		t.Fatalf("SessionDiscard: %v", err)
	}

	// THE assertion: Postgres accounts row is bit-for-bit unchanged.
	after, err := component.LoadAccount(ctx, string(accountID))
	if err != nil {
		t.Fatalf("LoadAccount after discard: %v", err)
	}
	if after.TradingCash != pristine.TradingCash {
		t.Errorf("on-demand leaked to Postgres: TradingCash %d, want %d (pristine)",
			after.TradingCash, pristine.TradingCash)
	}
	if after.BankCash != pristine.BankCash {
		t.Errorf("on-demand leaked to Postgres: BankCash %d, want %d (pristine)",
			after.BankCash, pristine.BankCash)
	}
	if after.DripActive != pristine.DripActive {
		t.Errorf("on-demand leaked to Postgres: DripActive %v, want %v (pristine)",
			after.DripActive, pristine.DripActive)
	}

	// Snapshot gone (SessionDiscard deleted it).
	if _, readErr := service.ReadBalanceSnapshot(ctx, sessionID, accountID); !errors.Is(readErr, ErrSnapshotMissing) {
		t.Errorf("ReadBalanceSnapshot after discard: got err = %v, want wraps ErrSnapshotMissing", readErr)
	}
}

// TestIntegration_ForcedSaveFailureRetainsSnapshot proves the doc §5 step 5
// rule verbatim: on SaveAccount failure, the Redis snapshot is RETAINED
// (never a DELETE) — it's the only surviving copy of the final state. We
// force the failure by DELETEing the accounts row out-of-band between
// SessionLoad and SessionSave, which makes the UPDATE touch 0 rows and
// surface as persistence.ErrNoAccount → lifecycle.ErrSaveFailedFor.
func TestIntegration_ForcedSaveFailureRetainsSnapshot(t *testing.T) {
	pool := requirePostgresPool(t)
	client, namespace := requireRedis(t)
	service, _ := buildLifecycleForIntegration(pool, client, namespace)

	ctx := context.Background()
	ownerID := uniqueOwnerID(t, "forced-save-fail")

	accountID, err := service.OpenAccount(ctx, OwnerAgent, ownerID, 800_000, 50_000)
	if err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}
	registerAccountCleanup(t, pool, string(accountID))

	sessionID := "sess-" + ownerID
	loaded, err := service.SessionLoad(ctx, sessionID, accountID)
	if err != nil {
		t.Fatalf("SessionLoad: %v", err)
	}
	// Mutate so reconciled TradingCash would be different from the seed —
	// makes the "snapshot survived" assertion meaningful (we can spot it
	// by its Available value).
	loaded.Account.Cash.Available = 555_555

	// The forced failure: DELETE the row so SaveAccount's UPDATE affects
	// zero rows and returns persistence.ErrNoAccount.
	if _, deleteErr := pool.Exec(ctx, `DELETE FROM accounts WHERE account_id = $1`, string(accountID)); deleteErr != nil {
		t.Fatalf("out-of-band DELETE: %v", deleteErr)
	}

	saveErr := service.SessionSave(ctx, sessionID, loaded)
	if !errors.Is(saveErr, ErrSaveFailed) {
		t.Fatalf("SessionSave after out-of-band DELETE: got err = %v, want wraps ErrSaveFailed", saveErr)
	}

	// THE assertion: snapshot survives — ReadBalanceSnapshot still returns
	// the last-good split (including the mutated Available). This is the
	// only surviving copy of the session's state; discarding it here would
	// be irreversible loss (doc §5 step 5).
	fromSnapshot, err := service.ReadBalanceSnapshot(ctx, sessionID, accountID)
	if err != nil {
		t.Fatalf("ReadBalanceSnapshot after failed save: %v (snapshot MUST have been retained)", err)
	}
	// The seed we WROTE was the SessionLoad synthesis (Available=800_000,
	// Reserved/Settled=0). The mutation to Available=555_555 was in-memory
	// only — a session_save that failed did not update the snapshot either.
	// So what survives is the SEEDED split, not the mutated one.
	wantSurvived := account.Cash{Available: 800_000, Reserved: 0, Settled: 0, Bank: 50_000}
	if *fromSnapshot != wantSurvived {
		t.Errorf("survived snapshot Cash:\n got  %+v\n want %+v (SessionLoad seed)",
			*fromSnapshot, wantSurvived)
	}
}

// TestIntegration_CrashRecoveryRoundTrip covers item 23 against the real
// snapshotter. Instead of relying on SessionLoad's initial seed (which is
// a fresh split — indistinguishable from a synthesis), we write a mid-
// session evolved snapshot directly via component.WriteSnapshot and then
// prove CrashRecoveryReload restores it VERBATIM (no synthesis, no
// reconciliation).
func TestIntegration_CrashRecoveryRoundTrip(t *testing.T) {
	pool := requirePostgresPool(t)
	client, namespace := requireRedis(t)
	service, component := buildLifecycleForIntegration(pool, client, namespace)

	ctx := context.Background()
	ownerID := uniqueOwnerID(t, "crash-recovery")

	accountID, err := service.OpenAccount(ctx, OwnerAgent, ownerID, 2_000_000, 300_000)
	if err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}
	registerAccountCleanup(t, pool, string(accountID))

	// Simulate an evolved mid-session state and mirror it to Redis directly.
	// Non-zero Reserved and Settled are the anti-synthesis markers — a
	// fresh-load synthesis would zero both.
	sessionID := "sess-crash-" + ownerID
	entryAt := time.Date(2026, 5, 20, 10, 15, 0, 0, time.UTC).Truncate(time.Second)
	evolved := persistence.SnapshotPayload{
		SessionID: sessionID,
		AccountID: string(accountID),
		Cash: persistence.CashSnap{
			Available: -50_000, // Option B mid-session debt
			Reserved:  120_000,
			Settled:   80_000,
			BankCash:  300_000,
		},
		Positions: []persistence.PositionSnap{
			{
				InstrumentID:   "ACME",
				AvailableUnits: 200,
				ReservedUnits:  50,
				OpenLots: []persistence.LotSnap{
					{EntryPrice: 100, EntryAt: entryAt, RemainingQty: 150},
					{EntryPrice: 110, EntryAt: entryAt.Add(30 * time.Minute), RemainingQty: 100},
				},
			},
		},
		Reservations: []persistence.ReservationSnap{
			{
				OrderID:          contract.OrderID("ord-crash-1"),
				AccountID:        account.AccountID(accountID),
				SessionID:        sessionID,
				InstrumentID:     contract.InstrumentID("ACME"),
				Side:             contract.Buy,
				IsMarket:         false,
				ReservedCash:     120_000,
				RemainingQty:     40,
				OrigQty:          100,
				EverFilled:       true,
				BrokerageCharged: false,
			},
		},
	}
	if err := component.WriteSnapshot(ctx, evolved); err != nil {
		t.Fatalf("seed evolved snapshot: %v", err)
	}

	restored, err := service.CrashRecoveryReload(ctx, sessionID, accountID, Canonical)
	if err != nil {
		t.Fatalf("CrashRecoveryReload: %v", err)
	}

	// Cash split VERBATIM — the anti-synthesis check.
	wantCash := account.Cash{Available: -50_000, Reserved: 120_000, Settled: 80_000, Bank: 300_000}
	if restored.Account.Cash != wantCash {
		t.Errorf("restored Cash:\n got  %+v\n want %+v", restored.Account.Cash, wantCash)
	}
	if !restored.Account.PersistOnClose {
		t.Errorf("PersistOnClose: got false, want true (canonical resume)")
	}

	// Position + lot fidelity (FIFO arrival order preserved).
	acme := restored.Account.Positions[contract.InstrumentID("ACME")]
	if acme == nil {
		t.Fatalf("Positions[ACME]: missing after restore")
	}
	if acme.Available != 200 || acme.Reserved != 50 {
		t.Errorf("Position[ACME] buckets: got Available=%d Reserved=%d, want 200 / 50",
			acme.Available, acme.Reserved)
	}
	if len(acme.OpenLots) != 2 {
		t.Fatalf("Position[ACME] lot count: got %d, want 2", len(acme.OpenLots))
	}
	if acme.OpenLots[0].EntryPrice != 100 || acme.OpenLots[1].EntryPrice != 110 {
		t.Errorf("FIFO order: got prices [%d, %d], want [100, 110]",
			acme.OpenLots[0].EntryPrice, acme.OpenLots[1].EntryPrice)
	}
	if !acme.OpenLots[0].EntryAt.Equal(entryAt) {
		t.Errorf("Lot[0].EntryAt: got %v, want %v (time-encoding round-trip)",
			acme.OpenLots[0].EntryAt, entryAt)
	}

	// Reservation raw passthrough — including the brokerage-once flags,
	// the whole reason ReservationSnap carries the full record.
	if len(restored.ReservationSnaps) != 1 {
		t.Fatalf("ReservationSnaps count: got %d, want 1", len(restored.ReservationSnaps))
	}
	snap := restored.ReservationSnaps[0]
	if snap.OrderID != "ord-crash-1" || snap.OrigQty != 100 || snap.RemainingQty != 40 {
		t.Errorf("Reservation numbers: got orderID=%s orig=%d rem=%d, want ord-crash-1 / 100 / 40",
			snap.OrderID, snap.OrigQty, snap.RemainingQty)
	}
	if !snap.EverFilled || snap.BrokerageCharged {
		t.Errorf("brokerage-once flags: got EverFilled=%v BrokerageCharged=%v, want true / false",
			snap.EverFilled, snap.BrokerageCharged)
	}
}
