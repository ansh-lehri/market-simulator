package persistence

import (
	"context"
	"testing"
	"time"
)

func TestBufferPushDrainFIFO(t *testing.T) {
	// LPUSH producer + RPOP consumer → FIFO. First pushed comes out first.
	sto, _ := buildStoreRedis(t, TimeRFC3339, SideString)

	firstTradeID := "trd-A"
	secondTradeID := "trd-B"
	thirdTradeID := "trd-C"

	for _, tradeID := range []string{firstTradeID, secondTradeID, thirdTradeID} {
		insert := baseTradeInsert(tradeID, "acct-buf")
		if err := sto.pushClosedTrade(context.Background(), insert); err != nil {
			t.Fatalf("push %s: %v", tradeID, err)
		}
	}

	drained, err := sto.drainClosedTrades(context.Background(), 10)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(drained) != 3 {
		t.Fatalf("drain count: got %d, want 3", len(drained))
	}
	if drained[0].TradeID != firstTradeID ||
		drained[1].TradeID != secondTradeID ||
		drained[2].TradeID != thirdTradeID {
		t.Errorf("FIFO order broken: got %s %s %s",
			drained[0].TradeID, drained[1].TradeID, drained[2].TradeID)
	}
}

func TestBufferPushDrainRoundTripsAllFields(t *testing.T) {
	// A pushed row must come back with every field intact — including
	// time.Time, map[string]int64, and signed values.
	sto, _ := buildStoreRedis(t, TimeRFC3339, SideString)

	original := baseTradeInsert("trd-full", "acct-full")
	original.GrossPnl = -1234 // signed
	if err := sto.pushClosedTrade(context.Background(), original); err != nil {
		t.Fatalf("push: %v", err)
	}
	drained, err := sto.drainClosedTrades(context.Background(), 1)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(drained) != 1 {
		t.Fatalf("drain count: got %d, want 1", len(drained))
	}
	got := drained[0]
	if got.TradeID != original.TradeID ||
		got.AccountID != original.AccountID ||
		got.SessionID != original.SessionID ||
		got.InstrumentID != original.InstrumentID ||
		got.Direction != original.Direction ||
		got.Quantity != original.Quantity ||
		got.EntryPrice != original.EntryPrice ||
		got.ExitPrice != original.ExitPrice ||
		got.GrossPnl != original.GrossPnl ||
		got.TotalCosts != original.TotalCosts ||
		got.NetPnl != original.NetPnl ||
		got.PnlPct != original.PnlPct ||
		got.ExitReason != original.ExitReason ||
		got.CostTotal != original.CostTotal {
		t.Errorf("round-trip mismatch:\n got  %+v\n want %+v", got, original)
	}
	if !got.EntryAt.Equal(original.EntryAt) || !got.ExitAt.Equal(original.ExitAt) {
		t.Errorf("timestamps: got %v/%v, want %v/%v", got.EntryAt, got.ExitAt, original.EntryAt, original.ExitAt)
	}
	// CostLines map compare — keys and values.
	if len(got.CostLines) != len(original.CostLines) {
		t.Fatalf("CostLines len: got %d, want %d", len(got.CostLines), len(original.CostLines))
	}
	for key, wantValue := range original.CostLines {
		if got.CostLines[key] != wantValue {
			t.Errorf("CostLines[%q]: got %d, want %d", key, got.CostLines[key], wantValue)
		}
	}
}

func TestBufferDrainEmptyBufferReturnsNil(t *testing.T) {
	sto, _ := buildStoreRedis(t, TimeRFC3339, SideString)
	drained, err := sto.drainClosedTrades(context.Background(), 5)
	if err != nil {
		t.Fatalf("drain empty: %v", err)
	}
	if len(drained) != 0 {
		t.Errorf("drain empty: got %d rows, want 0", len(drained))
	}
}

func TestBufferDrainRespectsMax(t *testing.T) {
	// Drain with max=3 on a buffer of 5 → returns 3, leaves 2 in the buffer.
	sto, _ := buildStoreRedis(t, TimeRFC3339, SideString)

	for i := 0; i < 5; i++ {
		insert := baseTradeInsert("trd-batch", "acct-batch")
		insert.TradeID = insert.TradeID + "-" + time.Now().Format("150405.000000000")
		if err := sto.pushClosedTrade(context.Background(), insert); err != nil {
			t.Fatalf("push: %v", err)
		}
	}
	firstBatch, err := sto.drainClosedTrades(context.Background(), 3)
	if err != nil {
		t.Fatalf("drain first: %v", err)
	}
	if len(firstBatch) != 3 {
		t.Fatalf("first drain: got %d, want 3", len(firstBatch))
	}
	secondBatch, err := sto.drainClosedTrades(context.Background(), 10)
	if err != nil {
		t.Fatalf("drain second: %v", err)
	}
	if len(secondBatch) != 2 {
		t.Errorf("second drain: got %d, want 2 (remaining after batch of 3)", len(secondBatch))
	}
}

func TestBufferDrainZeroMaxReturnsNil(t *testing.T) {
	sto, _ := buildStoreRedis(t, TimeRFC3339, SideString)

	insert := baseTradeInsert("trd-zero", "acct-zero")
	if err := sto.pushClosedTrade(context.Background(), insert); err != nil {
		t.Fatalf("push: %v", err)
	}
	drained, err := sto.drainClosedTrades(context.Background(), 0)
	if err != nil {
		t.Fatalf("drain(0): %v", err)
	}
	if len(drained) != 0 {
		t.Errorf("drain(0): got %d rows, want 0 (no-op)", len(drained))
	}
	// Buffer still has the row.
	drained, err = sto.drainClosedTrades(context.Background(), 10)
	if err != nil {
		t.Fatalf("drain after zero: %v", err)
	}
	if len(drained) != 1 {
		t.Errorf("row survived zero-drain: got %d, want 1", len(drained))
	}
}

func TestBufferRequeueRetriesFirst(t *testing.T) {
	// requeueClosedTrade RPUSHes so the requeued row is popped FIRST on the
	// next drain (the tail is where RPOP looks).
	sto, _ := buildStoreRedis(t, TimeRFC3339, SideString)

	firstInsert := baseTradeInsert("trd-first", "acct-req")
	secondInsert := baseTradeInsert("trd-second", "acct-req")
	if err := sto.pushClosedTrade(context.Background(), firstInsert); err != nil {
		t.Fatalf("push first: %v", err)
	}
	if err := sto.pushClosedTrade(context.Background(), secondInsert); err != nil {
		t.Fatalf("push second: %v", err)
	}

	// Drain 1 — get firstInsert.
	drained, err := sto.drainClosedTrades(context.Background(), 1)
	if err != nil {
		t.Fatalf("first drain: %v", err)
	}
	if drained[0].TradeID != firstInsert.TradeID {
		t.Fatalf("drain order: got %s, want %s", drained[0].TradeID, firstInsert.TradeID)
	}
	// Simulate insert failure: requeue firstInsert.
	if err := sto.requeueClosedTrade(context.Background(), firstInsert); err != nil {
		t.Fatalf("requeue: %v", err)
	}

	// Next drain should return firstInsert AGAIN (retry-first behavior),
	// then secondInsert.
	drained, err = sto.drainClosedTrades(context.Background(), 10)
	if err != nil {
		t.Fatalf("drain after requeue: %v", err)
	}
	if len(drained) != 2 {
		t.Fatalf("drain count: got %d, want 2", len(drained))
	}
	if drained[0].TradeID != firstInsert.TradeID {
		t.Errorf("requeued row should pop first: got %s, want %s", drained[0].TradeID, firstInsert.TradeID)
	}
	if drained[1].TradeID != secondInsert.TradeID {
		t.Errorf("second row: got %s, want %s", drained[1].TradeID, secondInsert.TradeID)
	}
}
