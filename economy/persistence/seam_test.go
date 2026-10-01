package persistence

import (
	"context"
	"strings"
	"testing"
	"time"

	"simulator/country/india"
	"simulator/economy/realize"
)

// buildComponentRedisOnly builds a Component that only touches Redis (no
// Postgres). Used by seam tests that exercise MarkSnapshotDirty /
// EnqueueClosedTrade / DrainDirty — no pool required.
func buildComponentRedisOnly(t *testing.T) *Component {
	client, namespace := requireRedis(t)
	cfg := Config{
		KeyNamespace: namespace,
		// Tighter enqueue timeout in tests so a Redis hang surfaces fast.
		EnqueueTimeout: 500 * time.Millisecond,
	}.withDefaults()
	return &Component{
		store: newStore(nil, client, IndiaCostAdapter{}, cfg),
		dirty: make(map[dirtyKey]struct{}),
		cfg:   cfg,
	}
}

func TestMarkSnapshotDirtyDedupesSameKey(t *testing.T) {
	// A Component built without Redis suffices — MarkSnapshotDirty does no I/O.
	component := &Component{
		store: newStore(nil, nil, IndiaCostAdapter{}, Config{}.withDefaults()),
		dirty: make(map[dirtyKey]struct{}),
		cfg:   Config{}.withDefaults(),
	}
	component.MarkSnapshotDirty("sess-1", "acct-1")
	component.MarkSnapshotDirty("sess-1", "acct-1")
	component.MarkSnapshotDirty("sess-1", "acct-1")

	dirty := component.DrainDirty()
	if len(dirty) != 1 {
		t.Errorf("dedup broken: got %d dirty keys, want 1", len(dirty))
	}
	if dirty[0].SessionID != "sess-1" || dirty[0].AccountID != "acct-1" {
		t.Errorf("dirty key wrong: %+v", dirty[0])
	}
}

func TestMarkSnapshotDirtyDistinctKeysAllTracked(t *testing.T) {
	component := &Component{
		store: newStore(nil, nil, IndiaCostAdapter{}, Config{}.withDefaults()),
		dirty: make(map[dirtyKey]struct{}),
		cfg:   Config{}.withDefaults(),
	}
	pairs := []DirtyKey{
		{SessionID: "sess-a", AccountID: "acct-1"},
		{SessionID: "sess-a", AccountID: "acct-2"},
		{SessionID: "sess-b", AccountID: "acct-1"},
	}
	for _, pair := range pairs {
		component.MarkSnapshotDirty(pair.SessionID, pair.AccountID)
	}
	drained := component.DrainDirty()
	if len(drained) != 3 {
		t.Errorf("distinct keys: got %d, want 3", len(drained))
	}
	// DrainDirty does NOT clear — same entries still present on a second call.
	if len(component.DrainDirty()) != 3 {
		t.Errorf("DrainDirty must not clear; count changed on second call")
	}
}

func TestEnqueueClosedTradeLandsOnBuffer(t *testing.T) {
	component := buildComponentRedisOnly(t)

	closed := realize.ClosedTradeRow{
		Summary: realize.ParticipantTradeRow{
			TradeID:      "trd-enq-1",
			AccountID:    "acct-enq",
			SessionID:    "sess-enq",
			InstrumentID: "ACME",
			Direction:    "long",
			Quantity:     100,
			EntryPrice:   50000,
			ExitPrice:    51000,
			EntryAt:      time.Now().UTC(),
			ExitAt:       time.Now().UTC().Add(1 * time.Hour),
			GrossPnl:     100000,
			TotalCosts:   34,
			NetPnl:       99966,
			PnlPct:       1.99,
			ExitReason:   realize.ExitVoluntary,
		},
		Cost: realize.TradeCostRow{
			TradeID: "trd-enq-1",
			Lines:   map[string]int64{india.LineSTT: 25, india.LineGST: 5, india.LineExchange: 3, india.LineSEBI: 1},
			Total:   34,
		},
	}

	// EnqueueClosedTrade returns nothing; success = no panic.
	component.EnqueueClosedTrade(closed)

	// Drain and verify the row is on the buffer, well-formed.
	drained, err := component.store.drainClosedTrades(context.Background(), 5)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(drained) != 1 {
		t.Fatalf("expected 1 buffered row, got %d", len(drained))
	}
	if drained[0].TradeID != "trd-enq-1" {
		t.Errorf("TradeID mismatch: got %s, want trd-enq-1", drained[0].TradeID)
	}
	if drained[0].CostLines[india.LineSTT] != 25 {
		t.Errorf("CostLines.stt: got %d, want 25", drained[0].CostLines[india.LineSTT])
	}
	if drained[0].CostTotal != 34 {
		t.Errorf("CostTotal: got %d, want 34", drained[0].CostTotal)
	}
}

func TestEnqueueClosedTradeMultipleAreFIFO(t *testing.T) {
	component := buildComponentRedisOnly(t)

	firstID := "trd-A"
	secondID := "trd-B"
	thirdID := "trd-C"
	for _, tradeID := range []string{firstID, secondID, thirdID} {
		closed := realize.ClosedTradeRow{
			Summary: realize.ParticipantTradeRow{TradeID: tradeID, Quantity: 1, ExitReason: realize.ExitVoluntary},
			Cost:    realize.TradeCostRow{TradeID: tradeID, Total: 0},
		}
		component.EnqueueClosedTrade(closed)
	}

	drained, err := component.store.drainClosedTrades(context.Background(), 10)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(drained) != 3 {
		t.Fatalf("expected 3, got %d", len(drained))
	}
	if drained[0].TradeID != firstID || drained[1].TradeID != secondID || drained[2].TradeID != thirdID {
		t.Errorf("FIFO order broken: %s %s %s", drained[0].TradeID, drained[1].TradeID, drained[2].TradeID)
	}
}

func TestEnqueueClosedTradeUnknownCostLinePanics(t *testing.T) {
	// D1: unknown cost line is a programming bug. Panic BEFORE the row hits
	// the buffer so no poison entry lingers. Redis is not needed — the
	// panic fires during adapter validation, before any push.
	component := &Component{
		store: newStore(nil, nil, IndiaCostAdapter{}, Config{}.withDefaults()),
		dirty: make(map[dirtyKey]struct{}),
		cfg:   Config{}.withDefaults(),
	}

	closed := realize.ClosedTradeRow{
		Summary: realize.ParticipantTradeRow{TradeID: "trd-bad", Quantity: 1, ExitReason: realize.ExitVoluntary},
		Cost:    realize.TradeCostRow{Lines: map[string]int64{"nse_special_fee": 100}, Total: 100},
	}

	defer func() {
		panicValue := recover()
		if panicValue == nil {
			t.Fatalf("EnqueueClosedTrade did not panic on unknown cost line")
		}
		message, ok := panicValue.(string)
		if !ok {
			t.Fatalf("panic value not a string: %T %v", panicValue, panicValue)
		}
		if !strings.Contains(message, "unknown cost line") {
			t.Errorf("panic message: got %q, expected to mention 'unknown cost line'", message)
		}
	}()
	component.EnqueueClosedTrade(closed)
}

func TestEnqueueClosedTradePanicLeavesBufferEmpty(t *testing.T) {
	// After a validation panic, the buffer must still be empty — no poison
	// row leaked.
	component := buildComponentRedisOnly(t)

	closed := realize.ClosedTradeRow{
		Summary: realize.ParticipantTradeRow{TradeID: "trd-bad2", Quantity: 1, ExitReason: realize.ExitVoluntary},
		Cost:    realize.TradeCostRow{Lines: map[string]int64{"unknown": 100}, Total: 100},
	}

	func() {
		defer func() { _ = recover() }()
		component.EnqueueClosedTrade(closed)
	}()

	drained, err := component.store.drainClosedTrades(context.Background(), 5)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(drained) != 0 {
		t.Errorf("buffer polluted despite validation panic: got %d rows", len(drained))
	}
}

func TestWriteSnapshotClearsDirtyFlag(t *testing.T) {
	// WriteSnapshot success → dirty entry for that account removed. Other
	// accounts unchanged.
	client, namespace := requireRedis(t)
	cfg := Config{KeyNamespace: namespace}.withDefaults()
	component := &Component{
		store: newStore(nil, client, IndiaCostAdapter{}, cfg),
		dirty: make(map[dirtyKey]struct{}),
		cfg:   cfg,
	}

	component.MarkSnapshotDirty("sess-1", "acct-A")
	component.MarkSnapshotDirty("sess-1", "acct-B")

	payload := SnapshotPayload{
		SessionID: "sess-1",
		AccountID: "acct-A",
		Cash:      CashSnap{Available: 100},
	}
	if err := component.WriteSnapshot(context.Background(), payload); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}

	remaining := component.DrainDirty()
	if len(remaining) != 1 {
		t.Fatalf("dirty count after clear: got %d, want 1", len(remaining))
	}
	if remaining[0].AccountID != "acct-B" {
		t.Errorf("wrong account cleared: leftover is %s, expected acct-B", remaining[0].AccountID)
	}
}

func TestDeleteSnapshotClearsDirtyFlag(t *testing.T) {
	// Clean-session close: DeleteSnapshot also drops any lingering dirty flag.
	client, namespace := requireRedis(t)
	cfg := Config{KeyNamespace: namespace}.withDefaults()
	component := &Component{
		store: newStore(nil, client, IndiaCostAdapter{}, cfg),
		dirty: make(map[dirtyKey]struct{}),
		cfg:   cfg,
	}

	component.MarkSnapshotDirty("sess-clean", "acct-1")

	if err := component.DeleteSnapshot(context.Background(), "sess-clean", "acct-1"); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}
	if len(component.DrainDirty()) != 0 {
		t.Errorf("dirty flag not cleared after DeleteSnapshot")
	}
}

func TestNewComponentAppliesDefaults(t *testing.T) {
	// Zero-value Config gets defaults applied via withDefaults. Component
	// itself is nil-safe for pool/redis/adapter at construction (nil-deref
	// only if the method that needs them is called).
	component := NewComponent(nil, nil, IndiaCostAdapter{}, Config{})
	if component.cfg.DrainerTick != 100*time.Millisecond {
		t.Errorf("DrainerTick default not applied: %v", component.cfg.DrainerTick)
	}
	if component.cfg.EnqueueTimeout != time.Second {
		t.Errorf("EnqueueTimeout default not applied: %v", component.cfg.EnqueueTimeout)
	}
	if component.cfg.DrainerBatch != 64 {
		t.Errorf("DrainerBatch default not applied: %d", component.cfg.DrainerBatch)
	}
	if len(component.DrainDirty()) != 0 {
		t.Errorf("fresh Component has non-empty dirty set")
	}
}
