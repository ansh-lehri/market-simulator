package capstone

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/lifecycle"
	"simulator/economy/persistence"
	"simulator/economy/reserve"
)

// TestCapstone_Ondemand_NoDurableFootprint drives a full buy → sell
// round-trip on an on-demand session and proves NOTHING durable is
// touched beyond the initial OpenAccount insert.
//
// The invariants under test:
//   - OndemandSessionLoad returns Account.PersistOnClose=false.
//   - realize.OnSellFill's PersistOnClose gate skips EnqueueClosedTrade
//     (buffer stays empty; no participant_trades / trade_costs rows).
//   - SessionDiscard removes the Redis snapshot and never calls SaveAccount.
//   - The accounts row post-discard is byte-identical to its post-OpenAccount
//     state (via reflect.DeepEqual on the DurableAccount DTO — DripProgress
//     is *int64, so struct-== would compare pointer addresses, not values).
func TestCapstone_Ondemand_NoDurableFootprint(t *testing.T) {
	bundle, _ := newBundle(t)
	ctx := context.Background()

	const (
		startingCapital int64                 = 1_000_000 // ₹10,000
		startingBank    int64                 = 250_000   // ₹2,500 — non-zero to prove Bank survives verbatim too
		buyPrice        int64                 = 5_000
		sellPrice       int64                 = 4_500 // small loss — direction doesn't matter for this test
		qty             int64                 = 10
		instrument      contract.InstrumentID = "OD"
	)

	// -------- birth ------------------------------------------------------
	ownerID := fmt.Sprintf("cap-ondemand-%d", time.Now().UnixNano())
	accountID, err := bundle.Lifecycle.OpenAccount(ctx, lifecycle.OwnerHuman, ownerID, startingCapital, startingBank)
	if err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}

	// Snapshot the post-OpenAccount durable row. Every field of this DTO
	// must survive the round-trip + discard unchanged.
	pristine, err := bundle.Persist.LoadAccount(ctx, string(accountID))
	if err != nil {
		t.Fatalf("LoadAccount (pristine snapshot): %v", err)
	}

	// -------- on-demand session load -------------------------------------
	sessionID := fmt.Sprintf("sess-od-%d", time.Now().UnixNano())
	loaded, err := bundle.Lifecycle.OndemandSessionLoad(ctx, sessionID, accountID)
	if err != nil {
		t.Fatalf("OndemandSessionLoad: %v", err)
	}
	if loaded.Account.PersistOnClose {
		t.Fatal("OndemandSessionLoad: PersistOnClose = true (must be false — routes teardown to Discard, gates realize's enqueue)")
	}
	bundle.Ledger.Put(loaded.Account)

	// -------- full round-trip --------------------------------------------
	participant := reserve.Participant{ID: ownerID}
	buyOrder := buildLimitOrder("ord-od-buy", accountID, sessionID, instrument, contract.Buy, qty, buyPrice)
	if err := bundle.Reserve.ReserveIfAvailable(participant, buyOrder); err != nil {
		t.Fatalf("ReserveIfAvailable(buy): %v", err)
	}
	if err := bundle.Reserve.Convert("ord-od-buy", qty, buyPrice, testExitAt, ""); err != nil {
		t.Fatalf("Convert(buy): %v", err)
	}

	sellOrder := buildLimitOrder("ord-od-sell", accountID, sessionID, instrument, contract.Sell, qty, sellPrice)
	if err := bundle.Reserve.ReserveIfAvailable(participant, sellOrder); err != nil {
		t.Fatalf("ReserveIfAvailable(sell): %v", err)
	}
	if err := bundle.Reserve.Convert("ord-od-sell", qty, sellPrice, testExitAt, ""); err != nil {
		t.Fatalf("Convert(sell): %v", err)
	}

	// -------- session discard --------------------------------------------
	if err := bundle.Lifecycle.SessionDiscard(ctx, sessionID, loaded); err != nil {
		t.Fatalf("SessionDiscard: %v", err)
	}

	// Give the drainer a beat to catch up in case (bug scenario) something
	// slipped into the buffer despite the gate — we want to see it if it
	// did, not race past it. 500 ms > drainer tick (100 ms) × several.
	time.Sleep(500 * time.Millisecond)

	// -------- assertions -------------------------------------------------
	// 1. participant_trades: zero rows for this account (belt) and this
	//    session (suspenders — proves it wasn't slotted under a different
	//    session id somehow).
	assertNoTradeRows(t, ctx, bundle, accountID, sessionID)

	// 2. trade_costs: zero rows joined against this account.
	assertNoCostRows(t, ctx, bundle, accountID)

	// 3. Accounts row byte-identical to pristine.
	after, err := bundle.Persist.LoadAccount(ctx, string(accountID))
	if err != nil {
		t.Fatalf("LoadAccount (post-discard): %v", err)
	}
	if !reflect.DeepEqual(after, pristine) {
		t.Errorf("accounts row leaked on-demand mutation:\n got  %+v\n want %+v (pristine)", after, pristine)
	}

	// 4. Redis snapshot gone (SessionDiscard's job).
	if _, err := bundle.Persist.ReadSnapshot(ctx, sessionID, string(accountID)); !errors.Is(err, persistence.ErrSnapshotMissing) {
		t.Errorf("post-Discard ReadSnapshot: got err = %v, want ErrSnapshotMissing", err)
	}
}

// -----------------------------------------------------------------------------
// On-demand-specific assertion helpers (kept in this file so cross-file
// coupling in tests stays minimal).
// -----------------------------------------------------------------------------

func assertNoTradeRows(
	t *testing.T,
	ctx context.Context,
	bundle *Bundle,
	accountID account.AccountID,
	sessionID string,
) {
	t.Helper()
	var count int64
	if err := bundle.Pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM participant_trades WHERE account_id = $1 AND session_id = $2`,
		string(accountID), sessionID,
	).Scan(&count); err != nil {
		t.Fatalf("participant_trades COUNT: %v", err)
	}
	if count != 0 {
		t.Errorf("participant_trades: got %d rows, want 0 (on-demand must not persist trades)", count)
	}
}

func assertNoCostRows(
	t *testing.T,
	ctx context.Context,
	bundle *Bundle,
	accountID account.AccountID,
) {
	t.Helper()
	var count int64
	if err := bundle.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM trade_costs tc
		JOIN participant_trades pt USING (trade_id)
		WHERE pt.account_id = $1`,
		string(accountID),
	).Scan(&count); err != nil {
		t.Fatalf("trade_costs COUNT: %v", err)
	}
	if count != 0 {
		t.Errorf("trade_costs: got %d rows, want 0", count)
	}
}
