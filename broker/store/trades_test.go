package store

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// A fixed base time keeps exit_at ordering deterministic across tests.
// Each seeded trade shifts off this base in whole hours so a
// newest-first sort is unambiguous.
var tradesBaseTime = time.Date(2026, 3, 15, 9, 30, 0, 0, time.UTC)

// makeTradeSeed is a tiny builder: it defaults the per-trade fields that
// the tests don't care about (session, instrument, direction, prices,
// quantity, exit reason) and lets the caller pin the ones that drive the
// test's assertion.
func makeTradeSeed(accountID, tradeID string, exitOffsetHours int, grossPnl, totalCosts, netPnl int64) tradeSeed {
	entryAt := tradesBaseTime.Add(time.Duration(exitOffsetHours) * time.Hour)
	exitAt := entryAt.Add(2 * time.Hour)
	return tradeSeed{
		TradeID:      tradeID,
		AccountID:    accountID,
		SessionID:    "sess-" + tradeID,
		InstrumentID: "ACME",
		Direction:    "long",
		Quantity:     10,
		EntryPrice:   50_000,
		ExitPrice:    51_000,
		EntryAt:      entryAt,
		ExitAt:       exitAt,
		GrossPnl:     grossPnl,
		TotalCosts:   totalCosts,
		NetPnl:       netPnl,
		PnlPct:       1.99,
		ExitReason:   "voluntary",
	}
}

// seedAccountAndThreeTrades installs one accounts row + three
// participant_trades rows ordered oldest→newest by exit_offset (0h, 1h,
// 2h). Returns the account_id so the test can query by it.
func seedAccountAndThreeTrades(t *testing.T, pool *pgxpool.Pool, prefix string, grossA, grossB, grossC int64) string {
	t.Helper()
	accountID := prefix + "-acc"
	ownerID := prefix + "-owner"
	seedAccount(t, pool, accountID, "agent", ownerID, 100_000, 0)
	seedTrade(t, pool, makeTradeSeed(accountID, prefix+"-trd-A", 0, grossA, 100, grossA-100))
	seedTrade(t, pool, makeTradeSeed(accountID, prefix+"-trd-B", 1, grossB, 100, grossB-100))
	seedTrade(t, pool, makeTradeSeed(accountID, prefix+"-trd-C", 2, grossC, 100, grossC-100))
	return accountID
}

// ---------- RecentTrades ----------

// Happy path: 3 trades seeded, newest-first order (exit_at DESC) is
// observed.
func TestRecentTrades_HappyPath_NewestFirst(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := seedAccountAndThreeTrades(t, pool, prefix, 500, 1000, 1500)

	store := newPoolOnlyStore(pool)
	trades, err := store.RecentTrades(context.Background(), accountID, 10)
	if err != nil {
		t.Fatalf("RecentTrades: %v", err)
	}
	if len(trades) != 3 {
		t.Fatalf("len: got %d, want 3", len(trades))
	}
	// Newest exit_at first: trd-C (offset 2h) → trd-B → trd-A.
	wantOrder := []string{prefix + "-trd-C", prefix + "-trd-B", prefix + "-trd-A"}
	for index, want := range wantOrder {
		if trades[index].TradeID != want {
			t.Errorf("position %d: got %q, want %q", index, trades[index].TradeID, want)
		}
	}
	// Spot-check a field survives the Scan round-trip.
	if trades[0].GrossPnl != 1500 || trades[0].NetPnl != 1400 || trades[0].Direction != "long" {
		t.Errorf("Scan drift on newest row: %+v", trades[0])
	}
}

// n > seeded rows: natural ceiling (we return what exists, no error).
func TestRecentTrades_NAboveSeededReturnsAll(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := seedAccountAndThreeTrades(t, pool, prefix, 100, 200, 300)

	store := newPoolOnlyStore(pool)
	trades, err := store.RecentTrades(context.Background(), accountID, 100)
	if err != nil {
		t.Fatalf("RecentTrades: %v", err)
	}
	if len(trades) != 3 {
		t.Errorf("len: got %d, want 3 (seeded)", len(trades))
	}
}

// Empty account: zero trades is NOT an error (BROKER_PHASE1 §3.6).
func TestRecentTrades_EmptyAccountReturnsEmpty(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := prefix + "-acc"
	seedAccount(t, pool, accountID, "agent", prefix+"-owner", 0, 0)

	store := newPoolOnlyStore(pool)
	trades, err := store.RecentTrades(context.Background(), accountID, 10)
	if err != nil {
		t.Fatalf("RecentTrades: %v", err)
	}
	if len(trades) != 0 {
		t.Errorf("len: got %d, want 0", len(trades))
	}
}

// n == 0 shortcut: no SQL issued. We prove "no SQL" indirectly by
// seeding 3 trades and asking for 0 — the result MUST be empty (if any
// SQL ran, LIMIT 0 would also be empty, but the slice must be
// non-nil and the call must not error).
func TestRecentTrades_ZeroN_Shortcut(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := seedAccountAndThreeTrades(t, pool, prefix, 100, 200, 300)

	store := newPoolOnlyStore(pool)
	trades, err := store.RecentTrades(context.Background(), accountID, 0)
	if err != nil {
		t.Fatalf("RecentTrades(n=0): %v", err)
	}
	if trades == nil {
		t.Error("RecentTrades(n=0): nil slice, want empty non-nil")
	}
	if len(trades) != 0 {
		t.Errorf("len: got %d, want 0", len(trades))
	}
}

// Negative n: same shortcut as n == 0 (both branches of the guard).
func TestRecentTrades_NegativeN_Shortcut(t *testing.T) {
	pool := requirePostgresPool(t)
	store := newPoolOnlyStore(pool)

	trades, err := store.RecentTrades(context.Background(), "any", -5)
	if err != nil {
		t.Fatalf("RecentTrades(n=-5): %v", err)
	}
	if len(trades) != 0 {
		t.Errorf("len: got %d, want 0", len(trades))
	}
}

// Account filter: seed two accounts, each with one trade; a RecentTrades
// for one account returns ONLY its trade.
func TestRecentTrades_FiltersByAccount(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accA := prefix + "-acc-A"
	accB := prefix + "-acc-B"
	seedAccount(t, pool, accA, "agent", prefix+"-ownerA", 0, 0)
	seedAccount(t, pool, accB, "agent", prefix+"-ownerB", 0, 0)
	seedTrade(t, pool, makeTradeSeed(accA, prefix+"-trdA1", 0, 100, 10, 90))
	seedTrade(t, pool, makeTradeSeed(accB, prefix+"-trdB1", 0, 200, 20, 180))

	store := newPoolOnlyStore(pool)
	tradesA, err := store.RecentTrades(context.Background(), accA, 10)
	if err != nil {
		t.Fatalf("RecentTrades(A): %v", err)
	}
	if len(tradesA) != 1 || tradesA[0].TradeID != prefix+"-trdA1" {
		t.Errorf("account A got %+v, want one trade with ID %q", tradesA, prefix+"-trdA1")
	}
	tradesB, err := store.RecentTrades(context.Background(), accB, 10)
	if err != nil {
		t.Fatalf("RecentTrades(B): %v", err)
	}
	if len(tradesB) != 1 || tradesB[0].TradeID != prefix+"-trdB1" {
		t.Errorf("account B got %+v, want one trade with ID %q", tradesB, prefix+"-trdB1")
	}
}

// ---------- TradePage ----------

// before == nil → newest page (same as RecentTrades on the newest end).
func TestTradePage_NewestPage_BeforeNil(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := seedAccountAndThreeTrades(t, pool, prefix, 100, 200, 300)

	store := newPoolOnlyStore(pool)
	trades, err := store.TradePage(context.Background(), accountID, 10, nil)
	if err != nil {
		t.Fatalf("TradePage: %v", err)
	}
	if len(trades) != 3 {
		t.Fatalf("len: got %d, want 3", len(trades))
	}
	if trades[0].TradeID != prefix+"-trd-C" {
		t.Errorf("newest first: got %q, want %q", trades[0].TradeID, prefix+"-trd-C")
	}
}

// Walking pages: seed 5 trades, limit 2 — walking returns 2 + 2 + 1 then
// empty.
func TestTradePage_WalksPagesWithCursor(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := prefix + "-acc"
	seedAccount(t, pool, accountID, "agent", prefix+"-owner", 0, 0)
	for tradeIndex := 0; tradeIndex < 5; tradeIndex++ {
		trade := makeTradeSeed(accountID, prefix+"-trd-"+string(rune('A'+tradeIndex)), tradeIndex, int64(100*(tradeIndex+1)), 10, int64(100*(tradeIndex+1))-10)
		seedTrade(t, pool, trade)
	}

	store := newPoolOnlyStore(pool)

	var pages [][]TradeRow
	var cursor *time.Time
	for pageIndex := 0; pageIndex < 10 /* safety bound */; pageIndex++ {
		page, err := store.TradePage(context.Background(), accountID, 2, cursor)
		if err != nil {
			t.Fatalf("TradePage page %d: %v", pageIndex, err)
		}
		if len(page) == 0 {
			break
		}
		pages = append(pages, page)
		lastExitAt := page[len(page)-1].ExitAt
		cursor = &lastExitAt
	}
	if len(pages) != 3 {
		t.Fatalf("pages: got %d, want 3 (2+2+1)", len(pages))
	}
	if len(pages[0]) != 2 || len(pages[1]) != 2 || len(pages[2]) != 1 {
		t.Errorf("page lens: got %d/%d/%d, want 2/2/1", len(pages[0]), len(pages[1]), len(pages[2]))
	}
	// Across pages, strictly descending exit_at — proves the cursor
	// predicate is strict (<) and the ORDER BY matches.
	seen := make([]time.Time, 0, 5)
	for _, page := range pages {
		for _, trade := range page {
			seen = append(seen, trade.ExitAt)
		}
	}
	for index := 1; index < len(seen); index++ {
		if !seen[index].Before(seen[index-1]) {
			t.Errorf("not strictly descending at %d: %v then %v", index, seen[index-1], seen[index])
		}
	}
}

// Determinism: two identical TradePage calls over the same seeds return
// identical results (relied on at Phase-6 agent-snapshot replay).
func TestTradePage_Determinism(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := seedAccountAndThreeTrades(t, pool, prefix, 100, 200, 300)

	store := newPoolOnlyStore(pool)
	first, err := store.TradePage(context.Background(), accountID, 10, nil)
	if err != nil {
		t.Fatalf("TradePage first: %v", err)
	}
	second, err := store.TradePage(context.Background(), accountID, 10, nil)
	if err != nil {
		t.Fatalf("TradePage second: %v", err)
	}
	if len(first) != len(second) {
		t.Fatalf("len drift: %d vs %d", len(first), len(second))
	}
	for index := range first {
		if first[index].TradeID != second[index].TradeID || first[index].ExitAt != second[index].ExitAt {
			t.Errorf("row %d drift: %+v vs %+v", index, first[index], second[index])
		}
	}
}

// limit <= 0 shortcut, both branches.
func TestTradePage_ZeroLimit_Shortcut(t *testing.T) {
	pool := requirePostgresPool(t)
	store := newPoolOnlyStore(pool)
	trades, err := store.TradePage(context.Background(), "any", 0, nil)
	if err != nil || len(trades) != 0 {
		t.Errorf("TradePage(limit=0): len=%d err=%v, want len=0 err=nil", len(trades), err)
	}
}

func TestTradePage_NegativeLimit_Shortcut(t *testing.T) {
	pool := requirePostgresPool(t)
	store := newPoolOnlyStore(pool)
	trades, err := store.TradePage(context.Background(), "any", -1, nil)
	if err != nil || len(trades) != 0 {
		t.Errorf("TradePage(limit=-1): len=%d err=%v, want len=0 err=nil", len(trades), err)
	}
}

// ---------- PnLSummary ----------

// Mixed wins / losses / breakeven: assert each projection field.
func TestPnLSummary_HappyPath_MixedWinsLosses(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := prefix + "-acc"
	seedAccount(t, pool, accountID, "agent", prefix+"-owner", 0, 0)
	// Three trades: one win, one loss, one breakeven.
	seedTrade(t, pool, makeTradeSeed(accountID, prefix+"-win", 0, 1000, 100, 900))      // net 900
	seedTrade(t, pool, makeTradeSeed(accountID, prefix+"-loss", 1, -500, 100, -600))    // net -600
	seedTrade(t, pool, makeTradeSeed(accountID, prefix+"-breakeven", 2, 100, 100, 0))   // net 0 (counted in neither)

	store := newPoolOnlyStore(pool)
	summary, err := store.PnLSummary(context.Background(), accountID)
	if err != nil {
		t.Fatalf("PnLSummary: %v", err)
	}
	if summary.TradeCount != 3 {
		t.Errorf("TradeCount: got %d, want 3", summary.TradeCount)
	}
	if summary.GrossPnl != 1000-500+100 {
		t.Errorf("GrossPnl: got %d, want %d", summary.GrossPnl, 1000-500+100)
	}
	if summary.TotalCosts != 300 {
		t.Errorf("TotalCosts: got %d, want 300", summary.TotalCosts)
	}
	if summary.NetPnl != 900-600+0 {
		t.Errorf("NetPnl: got %d, want %d", summary.NetPnl, 900-600+0)
	}
	if summary.WinCount != 1 {
		t.Errorf("WinCount: got %d, want 1", summary.WinCount)
	}
	if summary.LossCount != 1 {
		t.Errorf("LossCount: got %d, want 1 (breakeven in neither)", summary.LossCount)
	}
}

// Empty account: zero-filled summary, NOT an error.
func TestPnLSummary_EmptyAccountReturnsZeros(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := prefix + "-acc"
	seedAccount(t, pool, accountID, "agent", prefix+"-owner", 0, 0)

	store := newPoolOnlyStore(pool)
	summary, err := store.PnLSummary(context.Background(), accountID)
	if err != nil {
		t.Fatalf("PnLSummary: %v", err)
	}
	if summary.TradeCount != 0 || summary.GrossPnl != 0 || summary.TotalCosts != 0 ||
		summary.NetPnl != 0 || summary.WinCount != 0 || summary.LossCount != 0 {
		t.Errorf("empty summary: got %+v, want all zeros", summary)
	}
}
