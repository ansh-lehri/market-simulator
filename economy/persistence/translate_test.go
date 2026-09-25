package persistence

import (
	"testing"
	"time"

	"simulator/economy/realize"
)

// baseSummary returns a fully-populated ParticipantTradeRow so tests focused
// on Cost / CostLines behavior can reuse a canonical summary.
func baseSummary() realize.ParticipantTradeRow {
	entry := time.Date(2026, 3, 15, 9, 30, 0, 0, time.UTC)
	exit := entry.Add(4 * time.Hour)
	return realize.ParticipantTradeRow{
		TradeID:      "trd-1",
		AccountID:    "acct-1",
		SessionID:    "sess-1",
		InstrumentID: "ACME",
		Direction:    "long",
		Quantity:     100,
		EntryPrice:   50000,
		ExitPrice:    51000,
		EntryAt:      entry,
		ExitAt:       exit,
		GrossPnl:     100000,
		TotalCosts:   543,
		NetPnl:       99457,
		PnlPct:       1.98914,
		ExitReason:   realize.ExitVoluntary,
	}
}

func TestTranslatePassesUniversalFieldsThrough(t *testing.T) {
	summary := baseSummary()
	closed := realize.ClosedTradeRow{
		Summary: summary,
		Cost:    realize.TradeCostRow{TradeID: summary.TradeID, Lines: nil, Total: summary.TotalCosts},
	}
	insert := translateClosedTrade(closed)

	if insert.TradeID != summary.TradeID ||
		insert.AccountID != summary.AccountID ||
		insert.SessionID != summary.SessionID ||
		insert.InstrumentID != summary.InstrumentID ||
		insert.Direction != summary.Direction ||
		insert.Quantity != summary.Quantity ||
		insert.EntryPrice != summary.EntryPrice ||
		insert.ExitPrice != summary.ExitPrice ||
		!insert.EntryAt.Equal(summary.EntryAt) ||
		!insert.ExitAt.Equal(summary.ExitAt) ||
		insert.GrossPnl != summary.GrossPnl ||
		insert.TotalCosts != summary.TotalCosts ||
		insert.NetPnl != summary.NetPnl ||
		insert.PnlPct != summary.PnlPct ||
		insert.ExitReason != string(summary.ExitReason) {
		t.Errorf("participant_trades passthrough incomplete: got %+v, want summary %+v", insert, summary)
	}
	if insert.CostTotal != summary.TotalCosts {
		t.Errorf("CostTotal: got %d, want %d", insert.CostTotal, summary.TotalCosts)
	}
	if insert.CostLines != nil {
		t.Errorf("nil Lines should stay nil, got %v", insert.CostLines)
	}
}

func TestTranslateCarriesCostLinesOpaquely(t *testing.T) {
	// translate is country-agnostic: it never inspects line keys. It carries
	// whatever the caller put in, exactly. Validation (unknown-key rejection,
	// India column mapping) is the CountryCostAdapter's job at INSERT time.
	someCountryLines := map[string]int64{
		"india_line":    100,
		"us_line":       200,
		"anything_else": 300,
	}
	closed := realize.ClosedTradeRow{
		Summary: baseSummary(),
		Cost:    realize.TradeCostRow{Lines: someCountryLines, Total: 600},
	}
	insert := translateClosedTrade(closed)

	if len(insert.CostLines) != len(someCountryLines) {
		t.Fatalf("CostLines length: got %d, want %d", len(insert.CostLines), len(someCountryLines))
	}
	for lineName, want := range someCountryLines {
		if got := insert.CostLines[lineName]; got != want {
			t.Errorf("line %q: got %d, want %d", lineName, got, want)
		}
	}
	if insert.CostTotal != 600 {
		t.Errorf("CostTotal: got %d, want 600", insert.CostTotal)
	}
}

func TestTranslateCostLinesAreDefensivelyCloned(t *testing.T) {
	// A buffered row must not be corrupted by an owner mutating the source
	// map after enqueue. Assert translate returns a distinct map.
	source := map[string]int64{"stt": 100}
	closed := realize.ClosedTradeRow{
		Summary: baseSummary(),
		Cost:    realize.TradeCostRow{Lines: source, Total: 100},
	}
	insert := translateClosedTrade(closed)

	source["stt"] = 999
	source["injected"] = 42

	if insert.CostLines["stt"] != 100 {
		t.Errorf("mutating source changed insert: got %d, want 100 (defensive clone)", insert.CostLines["stt"])
	}
	if _, present := insert.CostLines["injected"]; present {
		t.Errorf("key added to source after translate leaked into insert (defensive clone)")
	}
}

func TestTranslateEmptyLinesRoundTripAsEmpty(t *testing.T) {
	// An explicitly-empty Lines map stays empty on the way through — distinct
	// from nil.
	closed := realize.ClosedTradeRow{
		Summary: baseSummary(),
		Cost:    realize.TradeCostRow{Lines: map[string]int64{}, Total: 0},
	}
	insert := translateClosedTrade(closed)
	if insert.CostLines == nil {
		t.Errorf("empty (non-nil) map became nil on the way through")
	}
	if len(insert.CostLines) != 0 {
		t.Errorf("empty map gained entries: %v", insert.CostLines)
	}
}
