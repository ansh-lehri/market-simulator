package wire

import (
	"testing"
	"time"

	"simulator/contract"
	"simulator/exchange/emit"
)

// buildShardWithInstruments returns a single-shard exchange whose shard
// holds the named instruments (all specs share the standard makeSpec so
// their OHLCV intervals and starts line up). Handy for handleTick unit
// tests that don't need a phase transition or ingress.
func buildShardWithInstruments(t *testing.T, instruments ...contract.InstrumentID) *Exchange {
	t.Helper()
	specs := make([]InstrumentSpec, 0, len(instruments))
	for _, id := range instruments {
		specs = append(specs, makeSpec(id))
	}
	return NewExchange(1, makeSchedule(), specs)
}

// TestHandleTick_RollsAllInstrumentsWhenIntervalElapsed — with three
// instruments on one shard and one trade ingested into each, a tick past
// the interval rolls all three and emits three candles on the public tape.
func TestHandleTick_RollsAllInstrumentsWhenIntervalElapsed(t *testing.T) {
	exchange := buildShardWithInstruments(t, "AAA", "BBB", "CCC")
	shard := exchange.shards[0]
	for _, id := range shard.instrIDs {
		shard.instrs[id].ohlcv.IngestTrade(1000, 5)
	}
	tickCh := make(chan time.Time, 1)
	shard.ticker = tickCh

	publicEvents, _ := runShardOnly(t, exchange, func() {
		tickCh <- time.Unix(0, 0).Add(2 * time.Minute)
	})

	if len(publicEvents) != 3 {
		t.Fatalf("public events: got %d, want 3 candles (one per instrument)", len(publicEvents))
	}
	for i, event := range publicEvents {
		if event.Kind != emit.KindCandle {
			t.Fatalf("public[%d].Kind = %v, want KindCandle", i, event.Kind)
		}
	}
	// Sorted-instrument order: AAA, BBB, CCC.
	wantOrder := []contract.InstrumentID{"AAA", "BBB", "CCC"}
	for i, event := range publicEvents {
		if event.Instrument != wantOrder[i] {
			t.Fatalf("public[%d].Instrument = %q, want %q (sorted iteration)",
				i, event.Instrument, wantOrder[i])
		}
	}
}

// TestHandleTick_DoesNotRollBeforeIntervalElapses — a tick before the
// bucket's end must emit no candles.
func TestHandleTick_DoesNotRollBeforeIntervalElapses(t *testing.T) {
	exchange := buildShardWithInstruments(t, "AAA")
	shard := exchange.shards[0]
	shard.instrs["AAA"].ohlcv.IngestTrade(1000, 5)
	tickCh := make(chan time.Time, 1)
	shard.ticker = tickCh

	publicEvents, _ := runShardOnly(t, exchange, func() {
		tickCh <- time.Unix(0, 0).Add(30 * time.Second)
	})

	if len(publicEvents) != 0 {
		t.Fatalf("public events: got %d, want 0 (interval not elapsed)", len(publicEvents))
	}
}

// TestHandleTick_MultipleRollsAcrossTicks — driving successive ticks past
// multiple interval boundaries produces one candle per instrument per
// crossed interval; subsequent buckets with no new trades are flat.
func TestHandleTick_MultipleRollsAcrossTicks(t *testing.T) {
	exchange := buildShardWithInstruments(t, "AAA")
	shard := exchange.shards[0]
	shard.instrs["AAA"].ohlcv.IngestTrade(1000, 5)
	tickCh := make(chan time.Time, 3)
	shard.ticker = tickCh

	publicEvents, _ := runShardOnly(t, exchange, func() {
		start := time.Unix(0, 0)
		tickCh <- start.Add(1 * time.Minute) // rolls bucket 1 (Volume 5)
		tickCh <- start.Add(2 * time.Minute) // flat bucket 2
		tickCh <- start.Add(3 * time.Minute) // flat bucket 3
	})

	if len(publicEvents) != 3 {
		t.Fatalf("public events: got %d, want 3 candles", len(publicEvents))
	}
	if publicEvents[0].Candle.Volume != 5 {
		t.Fatalf("first candle Volume = %d, want 5", publicEvents[0].Candle.Volume)
	}
	if publicEvents[1].Candle.Volume != 0 {
		t.Fatalf("second candle Volume = %d, want 0 (flat)", publicEvents[1].Candle.Volume)
	}
	if publicEvents[2].Candle.Volume != 0 {
		t.Fatalf("third candle Volume = %d, want 0 (flat)", publicEvents[2].Candle.Volume)
	}
}

// runShardOnly starts one shard's run() and public+private collectors,
// invokes the caller's work (which typically writes to the shard's
// ticker/control), then shuts down and returns the collected events.
// Used by handleTick/handleControl unit tests that don't need ingress.
//
// The 50ms sleep after work() is a compromise: unlike inbound (which we
// close to signal drain), the ticker/control arms are read by select
// without a natural "I've processed everything" signal. Sleeping gives
// the shard time to drain any buffered items before we close inbound and
// tear down.
func runShardOnly(t *testing.T, exchange *Exchange, work func()) ([]emit.TapeEvent, []contract.EngineResult) {
	t.Helper()

	publicChan := make(chan []emit.TapeEvent, 1)
	privateChan := make(chan []contract.EngineResult, 1)
	go func() {
		var collected []emit.TapeEvent
		for event := range exchange.Public {
			collected = append(collected, event)
		}
		publicChan <- collected
	}()
	go func() {
		var collected []contract.EngineResult
		for result := range exchange.Private {
			collected = append(collected, result)
		}
		privateChan <- collected
	}()

	shardDone := make(chan struct{})
	go func() {
		defer close(shardDone)
		exchange.shards[0].run()
	}()

	work()
	time.Sleep(50 * time.Millisecond)

	close(exchange.shards[0].inbound)
	select {
	case <-shardDone:
	case <-time.After(time.Second):
		t.Fatal("shard did not exit within 1s of inbound close")
	}
	close(exchange.public)
	close(exchange.private)
	return <-publicChan, <-privateChan
}
