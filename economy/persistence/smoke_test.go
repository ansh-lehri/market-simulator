package persistence

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"simulator/contract"
	"simulator/country/india"
	"simulator/economy/account"
	"simulator/economy/realize"
)

// TestSmokeEndToEndBothPersistencePaths exercises the two required paths
// from LAYER_5_PERSISTENCE §12 def-of-done in ONE scenario, driven from
// the owner's perspective (public Component API only):
//
//  1. EnqueueClosedTrade → Redis buffer → drainer goroutine → Postgres
//     `participant_trades` + `trade_costs`.
//  2. MarkSnapshotDirty → owner walks DrainDirty → WriteSnapshot → Redis
//     three-key-group snapshot. Verified via ReadSnapshot round-trip.
//
// Also touches the boundary loaders (LoadAccount / SaveAccount) so the
// full public surface is covered by a single smoke pass.
//
// Uses the full pool (not a rollback-tx), because the drainer's goroutine
// runs concurrently and pgx.Tx is not safe for concurrent use. Cleanup is
// manual: DELETE the seeded rows on t.Cleanup. Redis keys are namespaced
// and cleaned by requireRedis.
//
// FLOW (top-to-bottom of the test body):
//
//  A. Setup
//     - Acquire pool + Redis client; namespace scopes every Redis key so
//       parallel test runs cannot collide.
//     - Build Component with DrainerTick=20ms (fast enough to finish
//       inside a 2s deadline).
//     - Seed TWO accounts (persistence does not create accounts — §3),
//       generate two unique trade_ids. Register cleanup DELETEs.
//
//  B. Start the drainer
//     - component.Start(ctx) launches the goroutine; register Stop as a
//       t.Cleanup so it runs BEFORE the DELETE cleanup (LIFO order),
//       guaranteeing the goroutine is quiet before the schema is touched.
//
//  C. Path 1 — enqueue
//     - For each trade: EnqueueClosedTrade (validates via India adapter,
//       LPUSHes onto the buffer) + MarkSnapshotDirty (in-memory flag).
//     - After this: buffer has 2 rows, dirty map has 2 entries, Postgres
//       has 0 rows yet (drainer catches up asynchronously).
//
//  D. Path 2 — dirty → snapshot
//     - DrainDirty() → 2 keys (does NOT clear the set).
//     - For each key: assemble a SnapshotPayload (buildSmokePayload — a
//       fixed small shape) and call WriteSnapshot. Success removes that
//       key from the dirty map.
//     - Assert DrainDirty() is empty afterwards.
//     - ReadSnapshot each account back and reflect.DeepEqual against the
//       payload we wrote — this is the "three-key-group faithfulness"
//       check (cash + pos:{instrument} + resv:{order}).
//
//  E. Wait for the drainer, verify Postgres
//     - waitForBufferDrained polls LLEN → 0 (bounded 2s).
//     - Query participant_trades (universal columns) and trade_costs
//       (India-specific columns via adapter) for both trade_ids.
//
//  F. Boundary loaders round-trip
//     - LoadAccount → assert seed values.
//     - SaveAccount with a NEGATIVE trading_cash (Option B / debt), then
//       LoadAccount → assert the signed value survives Postgres.
//
//  G. Cleanup (LIFO): Stop the drainer → DELETE trades + accounts →
//     Redis SCAN+DEL of the namespace → close pool + client.
func TestSmokeEndToEndBothPersistencePaths(t *testing.T) {
	pool := requirePostgresPool(t)
	client, namespace := requireRedis(t)

	cfg := Config{
		KeyNamespace: namespace,
		DrainerTick:  20 * time.Millisecond,
		DrainerBatch: 8,
	}.withDefaults()
	component := NewComponent(pool, client, IndiaCostAdapter{}, cfg)

	// Two accounts so we can verify per-account dirty flag scoping.
	sessionID := realize.SessionID(fmt.Sprintf("sess-smoke-%d", time.Now().UnixNano()))
	accountIDs := []account.AccountID{
		account.AccountID(fmt.Sprintf("acct-smoke-a-%d", time.Now().UnixNano())),
		account.AccountID(fmt.Sprintf("acct-smoke-b-%d", time.Now().UnixNano())),
	}

	ctx := context.Background()
	for _, accountID := range accountIDs {
		_, err := pool.Exec(ctx,
			`INSERT INTO accounts (account_id, owner_type, owner_id, trading_cash, bank_cash)
			 VALUES ($1, 'agent', $1, 500000, 100000)`, string(accountID))
		if err != nil {
			t.Fatalf("seed account %s: %v", accountID, err)
		}
	}

	tradeIDs := []string{
		fmt.Sprintf("trd-smoke-a-%d", time.Now().UnixNano()),
		fmt.Sprintf("trd-smoke-b-%d", time.Now().UnixNano()),
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		for _, tradeID := range tradeIDs {
			_, _ = pool.Exec(cleanupCtx, `DELETE FROM trade_costs WHERE trade_id = $1`, tradeID)
			_, _ = pool.Exec(cleanupCtx, `DELETE FROM participant_trades WHERE trade_id = $1`, tradeID)
		}
		for _, accountID := range accountIDs {
			_, _ = pool.Exec(cleanupCtx, `DELETE FROM accounts WHERE account_id = $1`, string(accountID))
		}
	})

	// --- Path 1: enqueue path ------------------------------------------
	if err := component.Start(ctx); err != nil {
		t.Fatalf("Component.Start: %v", err)
	}
	// Stop before cleanup so the drainer goroutine is guaranteed to have
	// stopped touching the DB before we DELETE the rows it just inserted.
	t.Cleanup(component.Stop)

	entryTime := time.Date(2026, 5, 1, 9, 30, 0, 0, time.UTC)
	exitTime := entryTime.Add(3 * time.Hour)
	for tradeIndex, tradeID := range tradeIDs {
		closed := realize.ClosedTradeRow{
			Summary: realize.ParticipantTradeRow{
				TradeID:      realize.TradeID(tradeID),
				AccountID:    accountIDs[tradeIndex],
				SessionID:    sessionID,
				InstrumentID: "ACME",
				Direction:    "long",
				Quantity:     100,
				EntryPrice:   50000,
				ExitPrice:    51000,
				EntryAt:      entryTime,
				ExitAt:       exitTime,
				GrossPnl:     100000,
				TotalCosts:   34,
				NetPnl:       99966,
				PnlPct:       1.99932,
				ExitReason:   realize.ExitVoluntary,
			},
			Cost: realize.TradeCostRow{
				TradeID: realize.TradeID(tradeID),
				Lines: map[string]int64{
					india.LineSTT:      25,
					india.LineExchange: 3,
					india.LineSEBI:     1,
					india.LineGST:      5,
				},
				Total: 34,
			},
		}
		component.EnqueueClosedTrade(closed)
		component.MarkSnapshotDirty(sessionID, accountIDs[tradeIndex])
	}

	// --- Path 2: dirty → snapshot path ---------------------------------
	// The owner drains dirty and writes a snapshot for each account. In
	// production this happens on the owner's tick cadence; in the smoke
	// test we do it inline.
	dirtyKeys := component.DrainDirty()
	if len(dirtyKeys) != len(accountIDs) {
		t.Fatalf("DrainDirty: got %d keys, want %d", len(dirtyKeys), len(accountIDs))
	}
	payloadsByAccount := map[string]SnapshotPayload{}
	for _, key := range dirtyKeys {
		payload := buildSmokePayload(key.SessionID, key.AccountID)
		payloadsByAccount[key.AccountID] = payload
		if err := component.WriteSnapshot(ctx, payload); err != nil {
			t.Fatalf("WriteSnapshot %s: %v", key.AccountID, err)
		}
	}
	if len(component.DrainDirty()) != 0 {
		t.Fatalf("dirty set not cleared after WriteSnapshot")
	}

	// Snapshot faithfulness — read back and compare against what we wrote.
	for _, accountID := range accountIDs {
		got, err := component.ReadSnapshot(ctx, string(sessionID), string(accountID))
		if err != nil {
			t.Fatalf("ReadSnapshot %s: %v", accountID, err)
		}
		want := payloadsByAccount[string(accountID)]
		if !reflect.DeepEqual(got, want) {
			t.Errorf("snapshot round-trip mismatch for %s:\n got: %+v\nwant: %+v", accountID, got, want)
		}
	}

	// --- Wait for drainer to move buffer → Postgres --------------------
	drained := waitForBufferDrained(t, client, cfg.KeyNamespace, 2*time.Second)
	if !drained {
		t.Fatal("buffer did not drain within 2s")
	}

	// Postgres must hold BOTH trade rows and their cost children.
	for tradeIndex, tradeID := range tradeIDs {
		var (
			gotAccountID string
			gotNetPnl    int64
			gotTotal     int64
		)
		err := pool.QueryRow(ctx,
			`SELECT account_id, net_pnl, total_costs FROM participant_trades WHERE trade_id = $1`,
			tradeID).Scan(&gotAccountID, &gotNetPnl, &gotTotal)
		if err != nil {
			t.Fatalf("verify participant_trades %s: %v", tradeID, err)
		}
		if gotAccountID != string(accountIDs[tradeIndex]) {
			t.Errorf("account_id mismatch for %s: got %s, want %s", tradeID, gotAccountID, accountIDs[tradeIndex])
		}
		if gotNetPnl != 99966 || gotTotal != 34 {
			t.Errorf("figures mismatch for %s: net=%d total=%d", tradeID, gotNetPnl, gotTotal)
		}

		var (
			gotSTT      int64
			gotExchange int64
			gotSEBI     int64
			gotGST      int64
			gotTotalCol int64
		)
		err = pool.QueryRow(ctx,
			`SELECT stt, exchange, sebi, gst, total FROM trade_costs WHERE trade_id = $1`,
			tradeID).Scan(&gotSTT, &gotExchange, &gotSEBI, &gotGST, &gotTotalCol)
		if err != nil {
			t.Fatalf("verify trade_costs %s: %v", tradeID, err)
		}
		if gotSTT != 25 || gotExchange != 3 || gotSEBI != 1 || gotGST != 5 || gotTotalCol != 34 {
			t.Errorf("trade_costs mismatch for %s: stt=%d exchange=%d sebi=%d gst=%d total=%d",
				tradeID, gotSTT, gotExchange, gotSEBI, gotGST, gotTotalCol)
		}
	}

	// --- Boundary loaders round-trip -----------------------------------
	// LoadAccount pulls the seed values; SaveAccount writes a new signed
	// trading_cash figure (including going negative — Option B); LoadAccount
	// reads it back unchanged.
	loaded, err := component.LoadAccount(ctx, string(accountIDs[0]))
	if err != nil {
		t.Fatalf("LoadAccount: %v", err)
	}
	if loaded.TradingCash != 500_000 || loaded.BankCash != 100_000 {
		t.Errorf("LoadAccount seed values: got trading=%d bank=%d, want 500000/100000",
			loaded.TradingCash, loaded.BankCash)
	}

	reconciled := loaded
	reconciled.TradingCash = -12_345 // signed, may be negative (debt)
	reconciled.BankCash = 42
	if err := component.SaveAccount(ctx, reconciled); err != nil {
		t.Fatalf("SaveAccount: %v", err)
	}
	reloaded, err := component.LoadAccount(ctx, string(accountIDs[0]))
	if err != nil {
		t.Fatalf("LoadAccount after save: %v", err)
	}
	if reloaded.TradingCash != -12_345 || reloaded.BankCash != 42 {
		t.Errorf("Load→Save→Load round-trip failed: got trading=%d bank=%d, want -12345/42",
			reloaded.TradingCash, reloaded.BankCash)
	}
}

// buildSmokePayload produces a deterministic SnapshotPayload for a given
// (session, account). Kept small — one instrument, one lot, one
// reservation — so the round-trip comparison is easy to eyeball on
// failure.
func buildSmokePayload(sessionID, accountID string) SnapshotPayload {
	entryTime := time.Date(2026, 5, 1, 9, 30, 0, 0, time.UTC)
	return SnapshotPayload{
		SessionID: sessionID,
		AccountID: accountID,
		Cash: CashSnap{
			Available:  200_000,
			Reserved:   50_000,
			Settled:    10_000,
			BankCash:   100_000,
			DripActive: false,
		},
		Positions: []PositionSnap{
			{
				InstrumentID:   "ACME",
				AvailableUnits: 100,
				ReservedUnits:  0,
				OpenLots: []LotSnap{
					{EntryPrice: 50_000, EntryAt: entryTime, RemainingQty: 100},
				},
			},
		},
		Reservations: []ReservationSnap{
			{
				OrderID:      contract.OrderID(fmt.Sprintf("ord-smoke-%s", accountID)),
				AccountID:    account.AccountID(accountID),
				SessionID:    realize.SessionID(sessionID),
				InstrumentID: "ACME",
				Side:         contract.Buy,
				IsMarket:     false,
				ReservedCash: 50_000,
				RemainingQty: 10,
				OrigQty:      10,
			},
		},
	}
}
