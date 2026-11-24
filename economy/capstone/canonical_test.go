package capstone

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/cost"
	"simulator/economy/lifecycle"
	"simulator/economy/persistence"
	"simulator/economy/reserve"
)

// -----------------------------------------------------------------------------
// Canonical (PersistOnClose=true) end-to-end scenarios.
//
// TestCapstone_CanonicalLoss_RoundTripReconciles drives one full
// buy → sell → flat round-trip at a modest loss and verifies every seam
// that Stage 6 promised:
//   - session_load's Redis seed is readable via ReadBalanceSnapshot;
//   - reserve+realize's per-fill flow marks the account dirty;
//   - realize enqueues one ClosedTradeRow which the drainer writes into
//     participant_trades + trade_costs;
//   - session_save reconciles Available + Reserved (Reserved==0 → =Available)
//     into a signed trading_cash that reflects the round-trip P&L;
//   - the Redis snapshot is deleted post-save.
//
// TestCapstone_CanonicalUnderwater_TradingCashGoesNegative is the same
// shape at drastically different numbers, chosen so brokerage debits +
// realized loss push final trading_cash strictly below zero. Proves
// Option B (signed no-clamp) all the way through: durable trading_cash
// negative in Postgres, and a fresh SessionLoad seeds Available with
// that same negative value.
// -----------------------------------------------------------------------------

// TestCapstone_CanonicalLoss_RoundTripReconciles — see file header.
func TestCapstone_CanonicalLoss_RoundTripReconciles(t *testing.T) {
	bundle, namespace := newBundle(t)
	ctx := context.Background()

	const (
		startingCapital int64                 = 10_000_000 // ₹100,000
		startingBank    int64                 = 0
		buyPrice        int64                 = 50_000 // ₹500 / unit
		sellPrice       int64                 = 49_000 // ₹490 / unit — loss
		qty             int64                 = 100
		instrument      contract.InstrumentID = "ACME"
	)

	// -------- birth + load ------------------------------------------------
	ownerID := fmt.Sprintf("cap-loss-%d", time.Now().UnixNano())
	accountID, err := bundle.Lifecycle.OpenAccount(ctx, lifecycle.OwnerAgent, ownerID, startingCapital, startingBank)
	if err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}

	sessionID := fmt.Sprintf("sess-loss-%d", time.Now().UnixNano())
	loaded, err := bundle.Lifecycle.SessionLoad(ctx, sessionID, accountID)
	if err != nil {
		t.Fatalf("SessionLoad: %v", err)
	}
	if !loaded.Account.PersistOnClose {
		t.Fatal("SessionLoad: PersistOnClose = false on canonical session")
	}
	bundle.Ledger.Put(loaded.Account)

	// -------- ReadBalanceSnapshot round-trip ------------------------------
	seededCash, err := bundle.Lifecycle.ReadBalanceSnapshot(ctx, sessionID, accountID)
	if err != nil {
		t.Fatalf("ReadBalanceSnapshot: %v", err)
	}
	wantSeed := account.Cash{Available: startingCapital, Reserved: 0, Settled: 0, Bank: startingBank}
	if *seededCash != wantSeed {
		t.Errorf("ReadBalanceSnapshot seed:\n got  %+v\n want %+v", *seededCash, wantSeed)
	}

	// -------- buy leg -----------------------------------------------------
	participant := reserve.Participant{ID: ownerID}
	buyOrder := buildLimitOrder("ord-buy-1", accountID, sessionID, instrument, contract.Buy, qty, buyPrice)
	if err := bundle.Reserve.ReserveIfAvailable(participant, buyOrder); err != nil {
		t.Fatalf("ReserveIfAvailable(buy): %v", err)
	}
	if err := bundle.Reserve.Convert("ord-buy-1", qty, buyPrice, testExitAt, ""); err != nil {
		t.Fatalf("Convert(buy): %v", err)
	}

	// -------- sell leg (loss) --------------------------------------------
	sellOrder := buildLimitOrder("ord-sell-1", accountID, sessionID, instrument, contract.Sell, qty, sellPrice)
	if err := bundle.Reserve.ReserveIfAvailable(participant, sellOrder); err != nil {
		t.Fatalf("ReserveIfAvailable(sell): %v", err)
	}
	if err := bundle.Reserve.Convert("ord-sell-1", qty, sellPrice, testExitAt, ""); err != nil {
		t.Fatalf("Convert(sell): %v", err)
	}

	// -------- wait for drainer to flush -----------------------------------
	if !waitForBufferDrained(t, bundle.Redis, namespace, 2*time.Second) {
		t.Fatal("closed-trade buffer did not drain within 2 s")
	}

	// -------- compute expected numbers dynamically ------------------------
	// Recomputing via bundle.Cost keeps the test rate-agnostic — if the
	// India rulebook changes, this test still checks the RIGHT thing.
	buyFillCost := bundle.Cost.ComputeFillCost(cost.FillCostInput{
		Side: contract.Buy, FillQty: qty, MakerPrice: buyPrice, IsAutosquare: false,
	})
	sellFillCost := bundle.Cost.ComputeFillCost(cost.FillCostInput{
		Side: contract.Sell, FillQty: qty, MakerPrice: sellPrice, IsAutosquare: false,
	})
	buyOrderCost := bundle.Cost.ComputeOrderCost(cost.OrderCostInput{
		EverFilled: true, TotalQty: qty, Side: contract.Buy,
	})
	sellOrderCost := bundle.Cost.ComputeOrderCost(cost.OrderCostInput{
		EverFilled: true, TotalQty: qty, Side: contract.Sell,
	})

	// realize's accumulator folds sell-fill costs only (see
	// realize/accumulator.go). Gross is (exit-entry)*qty; net = gross -
	// costs; total_costs = sellFillCost.Total.
	wantGrossPnl := (sellPrice - buyPrice) * qty
	wantTotalCosts := sellFillCost.Total
	wantNetPnl := wantGrossPnl - wantTotalCosts

	// -------- participant_trades assertions ------------------------------
	assertOneTradeRow(t, ctx, bundle, accountID, sessionID,
		expectedTradeRow{
			InstrumentID: string(instrument),
			Direction:    "long",
			Quantity:     qty,
			EntryPrice:   buyPrice,
			ExitPrice:    sellPrice,
			GrossPnl:     wantGrossPnl,
			TotalCosts:   wantTotalCosts,
			NetPnl:       wantNetPnl,
			ExitReason:   string(realizeExitVoluntary),
		})

	// -------- trade_costs assertions -------------------------------------
	assertOneCostRow(t, ctx, bundle, accountID, sessionID, sellFillCost)

	// -------- DrainDirty saw this account --------------------------------
	dirtyKeys := bundle.Persist.DrainDirty()
	if !containsDirtyKey(dirtyKeys, sessionID, string(accountID)) {
		t.Errorf("DrainDirty missing (%q, %q); got %+v", sessionID, accountID, dirtyKeys)
	}

	// -------- SessionSave ------------------------------------------------
	// Snapshot the pre-save Available for the reconciliation cross-check.
	availableBeforeSave := loaded.Account.Cash.Available
	reservedBeforeSave := loaded.Account.Cash.Reserved
	if reservedBeforeSave != 0 {
		t.Fatalf("Reserved before save = %d, want 0 (round-trip should have left flat)", reservedBeforeSave)
	}

	if err := bundle.Lifecycle.SessionSave(ctx, sessionID, loaded); err != nil {
		t.Fatalf("SessionSave: %v", err)
	}

	// -------- durable trading_cash reflects round-trip -------------------
	wantFinalTradingCash := startingCapital -
		(buyPrice * qty) + (sellPrice * qty) - // net proceeds delta
		buyFillCost.Total - sellFillCost.Total - // per-fill costs
		buyOrderCost.Total - sellOrderCost.Total // brokerages
	if wantFinalTradingCash != availableBeforeSave {
		t.Fatalf("reconciliation cross-check: hand-derived (%d) vs. observed Available before save (%d) disagree — test is confused",
			wantFinalTradingCash, availableBeforeSave)
	}
	if wantFinalTradingCash >= startingCapital {
		t.Fatalf("test setup bug: canonical LOSS scenario expected < starting (%d >= %d)",
			wantFinalTradingCash, startingCapital)
	}
	assertDurableTradingCash(t, ctx, bundle, accountID, wantFinalTradingCash)

	// -------- snapshot deleted post-save ---------------------------------
	if _, err := bundle.Persist.ReadSnapshot(ctx, sessionID, string(accountID)); !errors.Is(err, persistence.ErrSnapshotMissing) {
		t.Errorf("post-SessionSave ReadSnapshot: got err = %v, want ErrSnapshotMissing", err)
	}
}

// TestCapstone_CanonicalUnderwater_TradingCashGoesNegative — see file header.
func TestCapstone_CanonicalUnderwater_TradingCashGoesNegative(t *testing.T) {
	bundle, _ := newBundle(t)
	ctx := context.Background()

	// Numbers chosen so buy_brokerage + sell_brokerage (both ₹20 = 2360 paise
	// including 18% GST — so 4720 paise total) alone exceeds the gross
	// proceeds from a 99% loss on a tiny principal. This forces Available
	// strictly negative at close.
	const (
		startingCapital int64                 = 5_000 // ₹50
		startingBank    int64                 = 0
		buyPrice        int64                 = 5_000 // ₹50 / unit
		sellPrice       int64                 = 100   // ₹1 / unit (98% loss)
		qty             int64                 = 1
		instrument      contract.InstrumentID = "TINY"
	)

	ownerID := fmt.Sprintf("cap-under-%d", time.Now().UnixNano())
	accountID, err := bundle.Lifecycle.OpenAccount(ctx, lifecycle.OwnerAgent, ownerID, startingCapital, startingBank)
	if err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}

	sessionID := fmt.Sprintf("sess-under-%d", time.Now().UnixNano())
	loaded, err := bundle.Lifecycle.SessionLoad(ctx, sessionID, accountID)
	if err != nil {
		t.Fatalf("SessionLoad: %v", err)
	}
	bundle.Ledger.Put(loaded.Account)

	participant := reserve.Participant{ID: ownerID}
	buyOrder := buildLimitOrder("ord-buy-u", accountID, sessionID, instrument, contract.Buy, qty, buyPrice)
	if err := bundle.Reserve.ReserveIfAvailable(participant, buyOrder); err != nil {
		t.Fatalf("ReserveIfAvailable(buy): %v", err)
	}
	if err := bundle.Reserve.Convert("ord-buy-u", qty, buyPrice, testExitAt, ""); err != nil {
		t.Fatalf("Convert(buy): %v", err)
	}

	sellOrder := buildLimitOrder("ord-sell-u", accountID, sessionID, instrument, contract.Sell, qty, sellPrice)
	if err := bundle.Reserve.ReserveIfAvailable(participant, sellOrder); err != nil {
		t.Fatalf("ReserveIfAvailable(sell): %v", err)
	}
	if err := bundle.Reserve.Convert("ord-sell-u", qty, sellPrice, testExitAt, ""); err != nil {
		t.Fatalf("Convert(sell): %v", err)
	}

	// Pre-save cross-check: Available must already be negative.
	if loaded.Account.Cash.Available >= 0 {
		t.Fatalf("test setup bug: underwater scenario expected Available < 0 pre-save, got %d",
			loaded.Account.Cash.Available)
	}
	availableBeforeSave := loaded.Account.Cash.Available

	if err := bundle.Lifecycle.SessionSave(ctx, sessionID, loaded); err != nil {
		t.Fatalf("SessionSave: %v", err)
	}

	// Durable figure must be negative and equal to the pre-save Available
	// (Reserved was 0, Settled is ignored per [[settled-semantics]]).
	assertDurableTradingCash(t, ctx, bundle, accountID, availableBeforeSave)
	if availableBeforeSave >= 0 {
		t.Fatalf("underwater assertion: expected negative durable trading_cash, got %d", availableBeforeSave)
	}

	// Reload proves Option B: SessionLoad must seed Available with the
	// negative durable value verbatim — no floor, no clamp.
	reloadSession := sessionID + "-reload"
	reloaded, err := bundle.Lifecycle.SessionLoad(ctx, reloadSession, accountID)
	if err != nil {
		t.Fatalf("SessionLoad after underwater save: %v", err)
	}
	if reloaded.Account.Cash.Available != availableBeforeSave {
		t.Errorf("reload Available: got %d, want %d (Option B — no clamp)",
			reloaded.Account.Cash.Available, availableBeforeSave)
	}
}

// -----------------------------------------------------------------------------
// Assertion helpers — used by canonical + underwater tests.
// -----------------------------------------------------------------------------

// realizeExitVoluntary mirrors realize.ExitVoluntary. Locally aliased so
// this test file does not import the realize package just for one string
// constant.
const realizeExitVoluntary = "voluntary"

// expectedTradeRow is the shape assertOneTradeRow compares against. Only
// the fields that don't have a fixed EntryAt/ExitAt time appear here;
// timestamps are asserted separately as "not zero" so the test does not
// tie itself to the fixed testExitAt.
type expectedTradeRow struct {
	InstrumentID string
	Direction    string
	Quantity     int64
	EntryPrice   int64
	ExitPrice    int64
	GrossPnl     int64
	TotalCosts   int64
	NetPnl       int64
	ExitReason   string
}

func assertOneTradeRow(
	t *testing.T,
	ctx context.Context,
	bundle *Bundle,
	accountID account.AccountID,
	sessionID string,
	want expectedTradeRow,
) {
	t.Helper()

	var rowCount int64
	if err := bundle.Pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM participant_trades WHERE account_id = $1 AND session_id = $2`,
		string(accountID), sessionID,
	).Scan(&rowCount); err != nil {
		t.Fatalf("participant_trades COUNT: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("participant_trades count: got %d, want 1", rowCount)
	}

	var (
		gotInstrumentID string
		gotDirection    string
		gotQuantity     int64
		gotEntryPrice   int64
		gotExitPrice    int64
		gotEntryAt      time.Time
		gotExitAt       time.Time
		gotGrossPnl     int64
		gotTotalCosts   int64
		gotNetPnl       int64
		gotExitReason   string
	)
	if err := bundle.Pool.QueryRow(ctx, `
		SELECT instrument_id, direction, quantity, entry_price, exit_price,
		       entry_at, exit_at, gross_pnl, total_costs, net_pnl, exit_reason
		FROM participant_trades
		WHERE account_id = $1 AND session_id = $2`,
		string(accountID), sessionID,
	).Scan(&gotInstrumentID, &gotDirection, &gotQuantity, &gotEntryPrice, &gotExitPrice,
		&gotEntryAt, &gotExitAt, &gotGrossPnl, &gotTotalCosts, &gotNetPnl, &gotExitReason,
	); err != nil {
		t.Fatalf("participant_trades SELECT: %v", err)
	}

	got := expectedTradeRow{
		InstrumentID: gotInstrumentID,
		Direction:    gotDirection,
		Quantity:     gotQuantity,
		EntryPrice:   gotEntryPrice,
		ExitPrice:    gotExitPrice,
		GrossPnl:     gotGrossPnl,
		TotalCosts:   gotTotalCosts,
		NetPnl:       gotNetPnl,
		ExitReason:   gotExitReason,
	}
	if got != want {
		t.Errorf("participant_trades row:\n got  %+v\n want %+v", got, want)
	}
	if gotEntryAt.IsZero() {
		t.Error("participant_trades.entry_at is zero")
	}
	if gotExitAt.IsZero() {
		t.Error("participant_trades.exit_at is zero")
	}
}

// assertOneCostRow verifies the trade_costs row for the (account,
// session) matches the India-flattened view of the sell-fill breakdown.
// Brokerage MUST be 0 (order-level; D2 of the persistence spec).
func assertOneCostRow(
	t *testing.T,
	ctx context.Context,
	bundle *Bundle,
	accountID account.AccountID,
	sessionID string,
	sellFillCost cost.CostBreakdown,
) {
	t.Helper()

	adapter := persistence.IndiaCostAdapter{}
	wantColumns, err := adapter.FlattenCostColumns(sellFillCost.Lines, sellFillCost.Total)
	if err != nil {
		t.Fatalf("adapter.FlattenCostColumns: %v", err)
	}

	var (
		gotBrokerage     int64
		gotSTT           int64
		gotExchange      int64
		gotSEBI          int64
		gotStamp         int64
		gotGST           int64
		gotAutosquareFee int64
		gotTotal         int64
	)
	if err := bundle.Pool.QueryRow(ctx, `
		SELECT tc.brokerage, tc.stt, tc.exchange, tc.sebi, tc.stamp, tc.gst, tc.autosquare_fee, tc.total
		FROM trade_costs tc
		JOIN participant_trades pt USING (trade_id)
		WHERE pt.account_id = $1 AND pt.session_id = $2`,
		string(accountID), sessionID,
	).Scan(&gotBrokerage, &gotSTT, &gotExchange, &gotSEBI, &gotStamp, &gotGST, &gotAutosquareFee, &gotTotal); err != nil {
		t.Fatalf("trade_costs SELECT: %v", err)
	}

	if gotBrokerage != 0 {
		t.Errorf("trade_costs.brokerage: got %d, want 0 (order-level, D2)", gotBrokerage)
	}
	if gotSTT != wantColumns["stt"] {
		t.Errorf("trade_costs.stt: got %d, want %d", gotSTT, wantColumns["stt"])
	}
	if gotExchange != wantColumns["exchange"] {
		t.Errorf("trade_costs.exchange: got %d, want %d", gotExchange, wantColumns["exchange"])
	}
	if gotSEBI != wantColumns["sebi"] {
		t.Errorf("trade_costs.sebi: got %d, want %d", gotSEBI, wantColumns["sebi"])
	}
	if gotStamp != wantColumns["stamp"] {
		t.Errorf("trade_costs.stamp: got %d, want %d", gotStamp, wantColumns["stamp"])
	}
	if gotGST != wantColumns["gst"] {
		t.Errorf("trade_costs.gst: got %d, want %d", gotGST, wantColumns["gst"])
	}
	if gotAutosquareFee != wantColumns["autosquare_fee"] {
		t.Errorf("trade_costs.autosquare_fee: got %d, want %d (no autosquare)", gotAutosquareFee, wantColumns["autosquare_fee"])
	}
	if gotTotal != sellFillCost.Total {
		t.Errorf("trade_costs.total: got %d, want %d", gotTotal, sellFillCost.Total)
	}
}

// assertDurableTradingCash loads the account back through the same
// Component that Wire built and compares trading_cash to want.
func assertDurableTradingCash(
	t *testing.T,
	ctx context.Context,
	bundle *Bundle,
	accountID account.AccountID,
	want int64,
) {
	t.Helper()
	durable, err := bundle.Persist.LoadAccount(ctx, string(accountID))
	if err != nil {
		t.Fatalf("LoadAccount for reconciliation check: %v", err)
	}
	if durable.TradingCash != want {
		t.Errorf("durable trading_cash: got %d, want %d", durable.TradingCash, want)
	}
}

// containsDirtyKey checks whether DrainDirty's slice includes a match.
// Reused by both canonical tests.
func containsDirtyKey(keys []persistence.DirtyKey, sessionID, accountID string) bool {
	for _, key := range keys {
		if key.SessionID == sessionID && key.AccountID == accountID {
			return true
		}
	}
	return false
}

