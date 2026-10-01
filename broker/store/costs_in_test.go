package store

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"simulator/country/india"
)

// seedIndiaTradeWithCosts seeds one accounts row (if the first time for
// this account), one participant_trades row, and one trade_costs row
// via the real IndiaCostAdapter. The cost `lines` map is the India line
// → amount shape the adapter accepts; `total` lands in trade_costs.total.
func seedIndiaTradeWithCosts(t *testing.T, pool *pgxpool.Pool, accountID, tradeID string, exitOffsetHours int, lines map[string]int64, total int64) {
	t.Helper()
	seed := makeTradeSeed(accountID, tradeID, exitOffsetHours, 1000, total, 1000-total)
	seedTrade(t, pool, seed)
	seedIndiaCostRow(t, pool, tradeID, lines, total)
}

// findLine returns the Amount for the named line, or (0, false) if absent.
func findLine(lines []Line, name string) (int64, bool) {
	for _, line := range lines {
		if line.Name == name {
			return line.Amount, true
		}
	}
	return 0, false
}

// ---------- CostsPerTradeIN ----------

// Happy path: a trade with every India line set — each one surfaces as
// a non-zero Line entry, in the mapper's stable order.
func TestCostsPerTradeIN_HappyPath_AllLinesSurface(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := prefix + "-acc"
	seedAccount(t, pool, accountID, "agent", prefix+"-owner", 0, 0)
	seedIndiaTradeWithCosts(t, pool, accountID, prefix+"-trd-1", 0,
		map[string]int64{
			india.LineSTT:           25,
			india.LineExchange:      3,
			india.LineSEBI:          1,
			india.LineStamp:         15,
			india.LineGST:           5,
			india.LineAutosquareFee: 5000,
		}, 5049)

	store := NewStore(pool)
	rows, err := store.CostsPerTradeIN(context.Background(), accountID, 10, nil)
	if err != nil {
		t.Fatalf("CostsPerTradeIN: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("len: got %d, want 1", len(rows))
	}
	row := rows[0]
	if row.TradeID != prefix+"-trd-1" || row.Total != 5049 {
		t.Errorf("row header: got {%q,%d}, want {%q,5049}", row.TradeID, row.Total, prefix+"-trd-1")
	}
	// Every India line must appear with its exact amount; no "brokerage"
	// line (D2 — never emitted at the per-trade level).
	wantAmounts := map[string]int64{
		india.LineSTT:           25,
		india.LineExchange:      3,
		india.LineSEBI:          1,
		india.LineStamp:         15,
		india.LineGST:           5,
		india.LineAutosquareFee: 5000,
	}
	for lineName, wantAmount := range wantAmounts {
		gotAmount, present := findLine(row.Lines, lineName)
		if !present {
			t.Errorf("line %q missing", lineName)
			continue
		}
		if gotAmount != wantAmount {
			t.Errorf("line %q: got %d, want %d", lineName, gotAmount, wantAmount)
		}
	}
	if _, present := findLine(row.Lines, "brokerage"); present {
		t.Error("brokerage line unexpectedly present (D2 — not an India Line* name)")
	}
}

// Zero-value drops: a trade without an auto-square fee — the line is
// absent from Lines (the mapper's drop-zero rule).
func TestCostsPerTradeIN_DropsZeroLines(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := prefix + "-acc"
	seedAccount(t, pool, accountID, "agent", prefix+"-owner", 0, 0)
	// A voluntary close — no auto-square fee, no stamp (sell-side only is
	// a cost-engine detail; here we just omit).
	seedIndiaTradeWithCosts(t, pool, accountID, prefix+"-trd-vol", 0,
		map[string]int64{
			india.LineSTT:      25,
			india.LineExchange: 3,
			india.LineSEBI:     1,
			india.LineGST:      5,
		}, 34)

	store := NewStore(pool)
	rows, err := store.CostsPerTradeIN(context.Background(), accountID, 10, nil)
	if err != nil {
		t.Fatalf("CostsPerTradeIN: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("len: got %d, want 1", len(rows))
	}
	// LineAutosquareFee was never set → zero → dropped.
	if _, present := findLine(rows[0].Lines, india.LineAutosquareFee); present {
		t.Error("autosquare_fee line present but amount should be zero → dropped")
	}
	if _, present := findLine(rows[0].Lines, india.LineStamp); present {
		t.Error("stamp line present but amount should be zero → dropped")
	}
	// Lines that WERE set remain.
	if amount, present := findLine(rows[0].Lines, india.LineSTT); !present || amount != 25 {
		t.Errorf("stt: present=%v amount=%d, want present=true amount=25", present, amount)
	}
}

// Account filter via the mandatory participant_trades join: a trade with
// a cost row belonging to another account must NOT appear.
func TestCostsPerTradeIN_FiltersByAccountThroughJoin(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accMine := prefix + "-mine"
	accOther := prefix + "-other"
	seedAccount(t, pool, accMine, "agent", prefix+"-ownerM", 0, 0)
	seedAccount(t, pool, accOther, "agent", prefix+"-ownerO", 0, 0)

	seedIndiaTradeWithCosts(t, pool, accMine, prefix+"-trd-mine", 0,
		map[string]int64{india.LineSTT: 10}, 10)
	seedIndiaTradeWithCosts(t, pool, accOther, prefix+"-trd-other", 0,
		map[string]int64{india.LineSTT: 99}, 99)

	store := NewStore(pool)
	rows, err := store.CostsPerTradeIN(context.Background(), accMine, 10, nil)
	if err != nil {
		t.Fatalf("CostsPerTradeIN: %v", err)
	}
	if len(rows) != 1 || rows[0].TradeID != prefix+"-trd-mine" {
		t.Fatalf("got %+v, want exactly one trade %q (the other account's trade must be filtered)", rows, prefix+"-trd-mine")
	}
}

// Pagination walks pages identically to TradePage (same keyset
// mechanic; same ORDER BY).
func TestCostsPerTradeIN_WalksPagesWithCursor(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := prefix + "-acc"
	seedAccount(t, pool, accountID, "agent", prefix+"-owner", 0, 0)
	for tradeIndex := 0; tradeIndex < 5; tradeIndex++ {
		tradeID := prefix + "-trd-" + string(rune('A'+tradeIndex))
		seedIndiaTradeWithCosts(t, pool, accountID, tradeID, tradeIndex,
			map[string]int64{india.LineSTT: int64(10 * (tradeIndex + 1))}, int64(10*(tradeIndex+1)))
	}

	store := NewStore(pool)
	var pages [][]TradeCostBreakdown
	var cursor *time.Time
	for pageIndex := 0; pageIndex < 10; pageIndex++ {
		page, err := store.CostsPerTradeIN(context.Background(), accountID, 2, cursor)
		if err != nil {
			t.Fatalf("page %d: %v", pageIndex, err)
		}
		if len(page) == 0 {
			break
		}
		pages = append(pages, page)
		// cursor = last row's exit_at — need to look it up (not in the
		// cost row). Fetch via a TradePage with the same tradeIDs set.
		// Simpler: pull from participant_trades via a quick lookup.
		var lastExit time.Time
		if err := pool.QueryRow(context.Background(),
			`SELECT exit_at FROM participant_trades WHERE trade_id = $1`,
			page[len(page)-1].TradeID,
		).Scan(&lastExit); err != nil {
			t.Fatalf("fetch exit_at for cursor: %v", err)
		}
		cursor = &lastExit
	}
	if len(pages) != 3 || len(pages[0]) != 2 || len(pages[1]) != 2 || len(pages[2]) != 1 {
		t.Errorf("pages: got sizes %v, want 2/2/1", pageSizes(pages))
	}
}

func pageSizes(pages [][]TradeCostBreakdown) []int {
	out := make([]int, len(pages))
	for pageIndex, page := range pages {
		out[pageIndex] = len(page)
	}
	return out
}

// Shortcut on limit <= 0 — both branches.
func TestCostsPerTradeIN_ZeroLimit_Shortcut(t *testing.T) {
	pool := requirePostgresPool(t)
	store := NewStore(pool)
	rows, err := store.CostsPerTradeIN(context.Background(), "any", 0, nil)
	if err != nil || len(rows) != 0 {
		t.Errorf("CostsPerTradeIN(limit=0): len=%d err=%v, want 0/nil", len(rows), err)
	}
}

func TestCostsPerTradeIN_NegativeLimit_Shortcut(t *testing.T) {
	pool := requirePostgresPool(t)
	store := NewStore(pool)
	rows, err := store.CostsPerTradeIN(context.Background(), "any", -1, nil)
	if err != nil || len(rows) != 0 {
		t.Errorf("CostsPerTradeIN(limit=-1): len=%d err=%v, want 0/nil", len(rows), err)
	}
}

// ---------- CostsAggregateIN ----------

// Happy path: three trades, each with the same India line set — Lines
// amounts are per-line sums, Total is the sum of trade_costs.total,
// TradeCount is count(*).
func TestCostsAggregateIN_HappyPath(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := prefix + "-acc"
	seedAccount(t, pool, accountID, "agent", prefix+"-owner", 0, 0)
	// Each trade: STT=25, GST=5, autosquare=5000, total=5030.
	for tradeIndex := 0; tradeIndex < 3; tradeIndex++ {
		tradeID := prefix + "-trd-" + string(rune('A'+tradeIndex))
		seedIndiaTradeWithCosts(t, pool, accountID, tradeID, tradeIndex,
			map[string]int64{
				india.LineSTT:           25,
				india.LineGST:           5,
				india.LineAutosquareFee: 5000,
			}, 5030)
	}

	store := NewStore(pool)
	agg, err := store.CostsAggregateIN(context.Background(), accountID)
	if err != nil {
		t.Fatalf("CostsAggregateIN: %v", err)
	}
	if agg.TradeCount != 3 {
		t.Errorf("TradeCount: got %d, want 3", agg.TradeCount)
	}
	if agg.Total != 5030*3 {
		t.Errorf("Total: got %d, want %d", agg.Total, 5030*3)
	}
	wantSums := map[string]int64{
		india.LineSTT:           75,
		india.LineGST:           15,
		india.LineAutosquareFee: 15000,
	}
	for lineName, wantSum := range wantSums {
		amount, present := findLine(agg.Lines, lineName)
		if !present {
			t.Errorf("aggregate missing %q", lineName)
			continue
		}
		if amount != wantSum {
			t.Errorf("aggregate %q: got %d, want %d", lineName, amount, wantSum)
		}
	}
	// Lines never charged for this account (sum 0) → absent.
	if _, present := findLine(agg.Lines, india.LineStamp); present {
		t.Error("stamp aggregate present but should be dropped (sum=0)")
	}
}

// Empty account: zero-filled aggregate, NOT an error.
func TestCostsAggregateIN_EmptyAccountReturnsZeros(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accountID := prefix + "-acc"
	seedAccount(t, pool, accountID, "agent", prefix+"-owner", 0, 0)

	store := NewStore(pool)
	agg, err := store.CostsAggregateIN(context.Background(), accountID)
	if err != nil {
		t.Fatalf("CostsAggregateIN: %v", err)
	}
	if agg.TradeCount != 0 {
		t.Errorf("TradeCount: got %d, want 0", agg.TradeCount)
	}
	if agg.Total != 0 {
		t.Errorf("Total: got %d, want 0", agg.Total)
	}
	if len(agg.Lines) != 0 {
		t.Errorf("Lines: got %+v, want []", agg.Lines)
	}
}

// Account filter via the mandatory join: another account's trade must
// NOT contribute to this account's aggregate.
func TestCostsAggregateIN_FiltersByAccountThroughJoin(t *testing.T) {
	pool := requirePostgresPool(t)
	prefix := testOwnerPrefix(t)
	cleanupOwnersWithPrefix(t, pool, prefix)

	accMine := prefix + "-mine"
	accOther := prefix + "-other"
	seedAccount(t, pool, accMine, "agent", prefix+"-ownerM", 0, 0)
	seedAccount(t, pool, accOther, "agent", prefix+"-ownerO", 0, 0)

	seedIndiaTradeWithCosts(t, pool, accMine, prefix+"-trd-mine", 0,
		map[string]int64{india.LineSTT: 10}, 10)
	seedIndiaTradeWithCosts(t, pool, accOther, prefix+"-trd-other", 0,
		map[string]int64{india.LineSTT: 999}, 999)

	store := NewStore(pool)
	agg, err := store.CostsAggregateIN(context.Background(), accMine)
	if err != nil {
		t.Fatalf("CostsAggregateIN: %v", err)
	}
	if agg.TradeCount != 1 || agg.Total != 10 {
		t.Errorf("aggregate contaminated by other account: got {count=%d, total=%d}, want {1, 10}", agg.TradeCount, agg.Total)
	}
}
