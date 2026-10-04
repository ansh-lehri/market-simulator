// Package main is a runnable, production-shaped end-to-end example of the
// exchange in-process.
//
// It demonstrates every step a real caller (broker + session manager) would
// perform:
//
//  1. Compose a SessionSchedule (exchange-wide phase boundaries) from
//     wall-clock times + today's date.
//  2. Compose per-instrument specs — reference price, circuit band width,
//     rulebook (tick size), OHLCV cadence.
//  3. Construct the Exchange (assembles clock, shards, per-instrument
//     state, ingress, monitor — but starts no goroutines yet).
//  4. Start consumers on the public tape and private stream (the two "out"
//     edges the broker and tape subscribers listen on).
//  5. Start the Exchange (launches monitor + shard loops + ingress).
//  6. Send some traffic: a resting limit, a matching market order, a
//     resting limit that is then cancelled.
//  7. Stop the Exchange (orderly shutdown; closes edges; consumers drain
//     and exit).
//
// Run with:  go run ./exchange/example/
//
// Everything below the "broker activity" comment corresponds to what a real
// broker goroutine would do. The two consumers correspond to what a broker
// (private) and tape subscribers (public) would do. Nothing in this file is
// test-only — it's the shape production code should take.
package main

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"simulator/contract"
	"simulator/exchange/book"
	"simulator/exchange/clock"
	"simulator/exchange/emit"
	"simulator/exchange/gate"
	"simulator/exchange/wire"
)

func main() {
	// ---------- (0) Logging ---------------------------------------------
	// Structured text logs to stdout at Info level. Every boundary in the
	// exchange emits slog records with correlation ids (BrokerID, OrderID,
	// Instrument), so this handler is enough to trace a full order.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	slog.Info("=== exchange demo starting ===")

	// ---------- (1) Session schedule ------------------------------------
	// SessionSchedule is exchange-wide (all instruments share one clock).
	// Layer 7 / session manager would normally build this by composing
	// rulebook wall-clock times ("09:15", "15:30") with today's date in
	// UTC. For the demo we build it inline so PhaseContinuous is active
	// from a moment before start until an hour later, then Closed.
	//
	// The clock is passive: it holds the boundaries verbatim and reports
	// transitions when the monitor asks. Note that PhaseClosingAuction is
	// deliberately absent — the auction package is deferred, and any order
	// routed there today would panic. A future schedule adds it back.
	now := time.Now()
	schedule := clock.SessionSchedule{
		Boundaries: []clock.PhaseBoundary{
			{At: now.Add(-time.Second), Phase: clock.PhaseContinuous},
			{At: now.Add(time.Hour), Phase: clock.PhaseClosed},
		},
	}

	// ---------- (2) Per-instrument specs --------------------------------
	// Each instrument carries its own reference price (prior close / seed),
	// circuit band width (in basis points; 1000 = ±10%), rulebook (tick
	// size — future: lot size, min qty), and OHLCV cadence.
	//
	// The rulebook is per-instrument because different stocks can trade at
	// different tick sizes. The schedule is global because all instruments
	// open/close together in the prototype.
	instruments := []wire.InstrumentSpec{
		{
			Instrument:    "ACME",
			Reference:     10000, // ₹100.00 (at 1-paise ticks)
			BandBps:       1000,  // ±10% → band [9000, 11000]
			Rules:         gate.Rulebook{TickSize: 1},
			OHLCVInterval: time.Minute,
			OHLCVStart:    now,
		},
		{
			Instrument:    "BETA",
			Reference:     50000, // ₹500.00
			BandBps:       500,   // ±5% → band [47500, 52500]
			Rules:         gate.Rulebook{TickSize: 5}, // BETA trades in ticks of 5
			OHLCVInterval: time.Minute,
			OHLCVStart:    now,
		},
	}

	// ---------- (3) Construct the exchange ------------------------------
	// numShards = 2 spreads the two instruments across two goroutines. In
	// production this is sized to CPU cores; instruments grow into shard
	// maps, goroutine count stays fixed at P.
	//
	// NewExchange builds state and wiring — but starts nothing. Every
	// goroutine is launched by Start (step 5).
	exchange := wire.NewExchange(2, schedule, instruments)

	// ---------- (4) Start consumers -------------------------------------
	// The two out edges (Public tape, Private stream) must have readers by
	// the time the exchange starts writing to them, otherwise emitter
	// writes block the shard loop. Real deployments have broker goroutines
	// on Private and one or more subscribers on Public.
	//
	// The WaitGroup lets main wait for both consumers to finish draining
	// after Stop closes their channels.
	var consumersWG sync.WaitGroup
	consumersWG.Add(2)
	go publicTapeConsumer(exchange.Public, &consumersWG)
	go privateStreamConsumer(exchange.Private, &consumersWG)

	// ---------- (5) Start the exchange ----------------------------------
	// tickInterval drives both the monitor's phase-transition check and
	// each shard's OHLCV roll cadence. Production uses time.Second per doc
	// §4; this demo uses 100ms so SessionOpened fires promptly.
	exchange.Start(100 * time.Millisecond)

	// Give the monitor one tick to observe Closed→Continuous and fan out
	// SessionOpened so downstream sees "session is open" before the first
	// order lands. A real broker would wait on a SessionOpened event
	// rather than a fixed sleep.
	time.Sleep(300 * time.Millisecond)

	// ---------- (6) Broker activity -------------------------------------
	// Simulate a broker sending three transactions:
	//
	//   B1 posts a resting limit-sell on ACME (rests on the book).
	//   B2 posts a market-buy on ACME that fully consumes B1's rest.
	//   B1 posts a resting limit-buy on BETA, then cancels it.
	//
	// Each send is exactly one Go channel send. In a cross-process split
	// these become network sends; the internal exchange logic is unchanged.

	slog.Info(">>> broker B1: rest a limit-sell on ACME @ 10050 qty 50")
	exchange.Inbound <- inboundSubmit("B1", &contract.FundedOrder{
		OrderID:     "o-1",
		Participant: "p-a", // opaque; STP uses this
		Instrument:  "ACME",
		Side:        contract.Sell,
		Type:        contract.Limit,
		TIF:         contract.GTC,
		Qty:         50,
		LimitPrice:  10050,
	})

	time.Sleep(150 * time.Millisecond)

	slog.Info(">>> broker B2: market-buy ACME qty 50 (should consume o-1)")
	exchange.Inbound <- inboundSubmit("B2", &contract.FundedOrder{
		OrderID:     "o-2",
		Participant: "p-b",
		Instrument:  "ACME",
		Side:        contract.Buy,
		Type:        contract.Market,
		Qty:         50,
		MaxSpend:    100_000_000, // broker has already reserved this cash
	})

	time.Sleep(150 * time.Millisecond)

	slog.Info(">>> broker B1: rest a limit-buy on BETA @ 49500 qty 10")
	exchange.Inbound <- inboundSubmit("B1", &contract.FundedOrder{
		OrderID:     "o-3",
		Participant: "p-a",
		Instrument:  "BETA",
		Side:        contract.Buy,
		Type:        contract.Limit,
		TIF:         contract.GTC,
		Qty:         10,
		LimitPrice:  49500,
	})

	time.Sleep(150 * time.Millisecond)

	slog.Info(">>> broker B1: cancel o-3")
	exchange.Inbound <- inboundCancel("B1", "BETA", "o-3")

	time.Sleep(150 * time.Millisecond)

	// ---------- (7) Stop the exchange -----------------------------------
	// Stop closes the edge inbound, waits for every internal goroutine
	// (monitor + shards + ingress) to exit, then closes the two outbound
	// channels. That last close makes the consumer for-range loops exit.
	//
	// After Stop returns, sending on exchange.Inbound panics ("send on
	// closed channel"). Real broker code gates its send loop on its own
	// shutdown signal to avoid that.
	slog.Info("=== stopping exchange ===")
	exchange.Stop()

	// ---------- (8) Wait for consumers to drain -------------------------
	consumersWG.Wait()
	slog.Info("=== demo complete ===")
}

// publicTapeConsumer reads TapeEvents until the channel closes. In
// production this would fan out to broker relays, chart feeds, OHLCV
// writers, persistence, Layer 6, etc. Here it just logs each event.
//
// Public events carry no participant identity (invariant 1): PublicTrade
// has price/qty only. Downstream consumers cannot deanonymise trades.
func publicTapeConsumer(tape <-chan emit.TapeEvent, wg *sync.WaitGroup) {
	defer wg.Done()
	for event := range tape {
		switch event.Kind {
		case emit.KindTrade:
			slog.Info("PUBLIC trade",
				"instrument", event.Instrument,
				"seq", event.Seq,
				"price", event.Trade.Price,
				"qty", event.Trade.Qty,
			)
		case emit.KindQuote:
			slog.Info("PUBLIC quote",
				"instrument", event.Instrument,
				"seq", event.Seq,
				"bid", formatBid(event.Quote),
				"ask", formatAsk(event.Quote),
				"last", formatLast(event.Quote),
			)
		case emit.KindDepth:
			slog.Info("PUBLIC depth",
				"instrument", event.Instrument,
				"seq", event.Seq,
				"bidLevels", len(event.Depth.Bids),
				"askLevels", len(event.Depth.Asks),
			)
		case emit.KindCandle:
			slog.Info("PUBLIC candle",
				"instrument", event.Instrument,
				"seq", event.Seq,
				"open", event.Candle.Open,
				"high", event.Candle.High,
				"low", event.Candle.Low,
				"close", event.Candle.Close,
				"volume", event.Candle.Volume,
			)
		case emit.KindSession:
			slog.Info("PUBLIC session",
				"instrument", event.Instrument,
				"seq", event.Seq,
				"kind", sessionKindName(event.Session.Kind),
			)
		}
	}
	slog.Info("public tape consumer stopped")
}

// privateStreamConsumer reads EngineResults for every broker. In
// production this is the broker's ledger update path:
//
//   - OutcomeMatched   → Convert filled qty into a position; Release any
//                        capped / cancelled remainder from the reservation.
//   - OutcomeCancelled → Release the CancelledQty from the reservation.
//   - OutcomeRejected  → Release the entire reservation (order never took).
//   - OutcomeMakerFill → Convert (a resting order of yours got hit).
//
// With multiple brokers, each broker filters by BrokerID and processes
// only its own results. Public tape is broadcast and carries no BrokerID.
func privateStreamConsumer(private <-chan contract.EngineResult, wg *sync.WaitGroup) {
	defer wg.Done()
	for result := range private {
		switch result.Outcome {
		case contract.OutcomeMatched:
			trades := 0
			var lastPrice contract.Ticks
			rested := false
			if result.Match != nil {
				trades = len(result.Match.Trades)
				lastPrice = result.Match.LastPrice
				rested = result.Match.Rested != nil
			}
			slog.Info("PRIVATE matched",
				"brokerID", result.BrokerID,
				"orderID", result.OrderID,
				"trades", trades,
				"lastPrice", lastPrice,
				"rested", rested,
			)
		case contract.OutcomeRejected:
			slog.Warn("PRIVATE rejected",
				"brokerID", result.BrokerID,
				"orderID", result.OrderID,
				"reason", rejectReasonName(result.Rejection.Reason),
				"stage", result.Rejection.Stage,
			)
		case contract.OutcomeCancelled:
			slog.Info("PRIVATE cancelled",
				"brokerID", result.BrokerID,
				"orderID", result.OrderID,
				"cancelledQty", result.CancelledQty,
			)
		case contract.OutcomeCancelRejected:
			slog.Warn("PRIVATE cancel-rejected",
				"brokerID", result.BrokerID,
				"orderID", result.OrderID,
			)
		case contract.OutcomeMakerFill:
			slog.Info("PRIVATE maker-fill",
				"brokerID", result.BrokerID,
				"orderID", result.OrderID,
			)
		}
	}
	slog.Info("private stream consumer stopped")
}

// ---------- Message-builder helpers -------------------------------------
// Thin wrappers so main.go reads as broker intent rather than plumbing.
// A real broker library would provide equivalents.

func inboundSubmit(brokerID contract.BrokerID, order *contract.FundedOrder) contract.InboundMessage {
	return contract.InboundMessage{
		Kind:       contract.SubmitOrder,
		BrokerID:   brokerID,
		Instrument: order.Instrument,
		Submit:     order,
	}
}

func inboundCancel(brokerID contract.BrokerID, instrument contract.InstrumentID, orderID contract.OrderID) contract.InboundMessage {
	return contract.InboundMessage{
		Kind:       contract.CancelOrder,
		BrokerID:   brokerID,
		Instrument: instrument,
		Cancel:     &contract.CancelRequest{OrderID: orderID},
	}
}

// ---------- Small formatting helpers ------------------------------------

func formatBid(quote *book.QuoteState) string {
	if !quote.HasBid {
		return "-"
	}
	return fmt.Sprintf("%d@%d", quote.BidQty, quote.BestBid)
}

func formatAsk(quote *book.QuoteState) string {
	if !quote.HasAsk {
		return "-"
	}
	return fmt.Sprintf("%d@%d", quote.AskQty, quote.BestAsk)
}

func formatLast(quote *book.QuoteState) string {
	if !quote.HasLastPrice {
		return "-"
	}
	return fmt.Sprintf("%d", quote.LastPrice)
}

func sessionKindName(kind emit.SessionEventKind) string {
	switch kind {
	case emit.SessionOpened:
		return "SessionOpened"
	case emit.AuctionWindowOpened:
		return "AuctionWindowOpened"
	case emit.SessionClosed:
		return "SessionClosed"
	default:
		return fmt.Sprintf("SessionEventKind(%d)", kind)
	}
}

func rejectReasonName(reason contract.RejectReason) string {
	switch reason {
	case contract.RejectMalformed:
		return "Malformed"
	case contract.RejectMarketClosed:
		return "MarketClosed"
	case contract.RejectNoLiquidity:
		return "NoLiquidity"
	case contract.RejectBadTick:
		return "BadTick"
	case contract.RejectBeyondCircuit:
		return "BeyondCircuit"
	default:
		return fmt.Sprintf("RejectReason(%d)", reason)
	}
}
