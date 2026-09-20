package realize

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"simulator/economy/account"
)

// G19 — determinism replay: two independent Realizers, identical fill
// sequences across two accounts × two instruments, with a deterministic
// TradeID counter injected. Emitted ClosedTradeRow slices must be byte-
// equal — proves realize is a pure function of its inputs + the injected
// generator (no hidden state, no clock, no map-iteration order dependence).
func TestRealizer_G19_DeterminismReplay(t *testing.T) {
	entry := time.Date(2026, 1, 15, 9, 0, 0, 0, time.UTC)

	// Fixed fill sequence. Two accounts, two instruments each; some fills
	// are partial, some flush. Written to exercise every code path
	// deterministically.
	fills := []RealizationInput{
		buildSellFill("a1", "X", 100, entry, 4, 110,
			time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
			map[string]int64{"stt": 2, "gst": 1}, 3, false),
		buildSellFill("a2", "X", 200, entry, 5, 210,
			time.Date(2026, 1, 15, 10, 5, 0, 0, time.UTC),
			map[string]int64{"stt": 3}, 3, true),
		buildSellFill("a1", "Y", 50, entry, 10, 55,
			time.Date(2026, 1, 15, 10, 10, 0, 0, time.UTC),
			map[string]int64{"stt": 5, "gst": 1}, 6, true),
		buildSellFill("a1", "X", 100, entry, 6, 120,
			time.Date(2026, 1, 15, 10, 15, 0, 0, time.UTC),
			map[string]int64{"stt": 3, "gst": 2}, 5, true),
		buildSellFill("a2", "Y", 80, entry, 3, 75,
			time.Date(2026, 1, 15, 10, 20, 0, 0, time.UTC),
			map[string]int64{"stt": 1}, 1, true),
	}

	runReplay := func() []ClosedTradeRow {
		ledger := newSpyLedger()
		ledger.put(&account.Account{ID: "a1", PersistOnClose: true})
		ledger.put(&account.Account{ID: "a2", PersistOnClose: true})

		persist := &spyPersistence{}
		realizer := NewRealizer(ledger, persist)

		counter := 0
		realizer.nextTradeID = func() TradeID {
			counter++
			return TradeID(fmt.Sprintf("det-%03d", counter))
		}

		for _, fill := range fills {
			realizer.OnSellFill(fill)
		}
		return persist.enqueued
	}

	replayA := runReplay()
	replayB := runReplay()

	if len(replayA) != 4 {
		t.Fatalf("expected 4 enqueues (one per flat sell), got %d", len(replayA))
	}
	if !reflect.DeepEqual(replayA, replayB) {
		t.Errorf("determinism replay mismatch:\nA=%+v\nB=%+v", replayA, replayB)
	}
}

// DeterminismReplayCostBreakdownIsolated — a fresh CostBreakdown per fill
// (rather than reused maps) confirms the accumulator doesn't alias its
// input's Lines map. A regression here would show as different runs
// producing different Lines totals depending on map iteration order.
func TestRealizer_DeterminismReplay_CostBreakdownIsolated(t *testing.T) {
	entry := time.Date(2026, 1, 15, 9, 0, 0, 0, time.UTC)

	buildFills := func() []RealizationInput {
		return []RealizationInput{
			buildSellFill("a1", "X", 100, entry, 4, 110,
				time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
				map[string]int64{"stt": 2, "gst": 1, "exchange": 1, "sebi": 0}, 4, false),
			buildSellFill("a1", "X", 100, entry, 6, 120,
				time.Date(2026, 1, 15, 10, 15, 0, 0, time.UTC),
				map[string]int64{"stt": 3, "gst": 2, "exchange": 2, "sebi": 0}, 7, true),
		}
	}

	runReplay := func() []ClosedTradeRow {
		ledger := newSpyLedger()
		ledger.put(&account.Account{ID: "a1", PersistOnClose: true})
		persist := &spyPersistence{}
		realizer := NewRealizer(ledger, persist)
		counter := 0
		realizer.nextTradeID = func() TradeID {
			counter++
			return TradeID(fmt.Sprintf("iso-%02d", counter))
		}
		for _, fill := range buildFills() {
			realizer.OnSellFill(fill)
		}
		return persist.enqueued
	}

	if !reflect.DeepEqual(runReplay(), runReplay()) {
		t.Errorf("map-aliased CostBreakdown would break replay")
	}
}
