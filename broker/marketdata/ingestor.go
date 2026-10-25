package marketdata

import (
	"context"
	"log/slog"
	"time"

	"simulator/exchange/emit"
)

// runMarketDataIngestor is one exchange instance's public-tape
// consumer. Spawned once per instance by WireInstance; the sole
// consumer of this instance's Exchange.Public channel.
//
// House pattern it mirrors (BODIES §1.0 — public-tape sibling of
// broker/funnel/settlement_consumer.go):
//
//   - one goroutine per exchange instance
//   - sole consumer of that instance's tape
//   - self-terminating: the for-range exits when the exchange closes
//     Public in Stop(). No stop flag, no per-event recover.
//   - slog.Info on start and on exit, with the correlation ids
//     (exchangeID, market).
//   - data problems are skip-and-log, NEVER panic — killing the loop
//     would halt all market data for that instance.
//
// Teardown (§1.8): after the for-range exits, deregister this
// instance's MarketSnapshot from the shared CacheRegistry so the
// account side stops reading a dead snapshot. forming / snapshot
// state falls out of scope with the goroutine exit.
//
// Routing table (§1.5, Model-A): see routeTapeEvent. Trades touch
// only the snapshot's lastPrice; the forming bar is built solely from
// KindCandle (the exchange emits continuous flat candles even on
// zero-trade minutes, EXCHANGE_19 §5).
func runMarketDataIngestor(
	tape <-chan emit.TapeEvent,
	exchangeID ExchangeID,
	market string,
	mode Mode,
	candleStore candleInserter,
	marketSnapshot *MarketSnapshot,
	rollup *streamRollup,
	fanOut *FanOut,
	registry *CacheRegistry,
) {
	slog.Info("broker/marketdata: ingestor started",
		"exch", exchangeID,
		"market", market,
		"mode", mode,
	)
	// A per-ingestor context derived from Background — the Store's
	// Insert call takes a ctx so a slow pool doesn't block the drain
	// forever. Today we pass Background; a future refinement could
	// bound the insert deadline per call.
	ingestCtx := context.Background()

	for event := range tape {
		routeTapeEvent(
			event,
			ingestCtx,
			exchangeID,
			market,
			mode,
			candleStore,
			marketSnapshot,
			rollup,
			fanOut,
		)
	}

	// Teardown — the exchange closed Public (BODIES §1.8). Remove
	// our snapshot from the shared registry so the account side's
	// BestAsk/LastPrice stops hitting a dead snapshot. Idempotent on
	// unknown exch; here we always registered at WireInstance.
	registry.Remove(exchangeID)
	slog.Info("broker/marketdata: ingestor exited (Public closed)",
		"exch", exchangeID,
		"market", market,
	)
}

// routeTapeEvent dispatches ONE tape event to the right destinations
// (BODIES §1.5). Exactly one payload pointer is non-nil per event
// Kind; a defensive nil-payload or an out-of-range Kind is skip-and-
// log (never panic — a bad tape event must not kill the ingestor,
// per BODIES §1.6).
//
// Ordering within a single event:
//
//	1. Mutate state (snapshot / store / rollup) — synchronous, so by
//	   the time we Publish, downstream readers (WS, account) see the
//	   new state.
//	2. Publish — non-blocking (FanOut drops on full buffer).
//
// Persist-then-forward (§1.6, canonical candles): store.Insert first;
// on error log + forward anyway (a missing row is the accepted
// "crash gap," not a hard stop).
func routeTapeEvent(
	event emit.TapeEvent,
	ingestCtx context.Context,
	exchangeID ExchangeID,
	market string,
	mode Mode,
	candleStore candleInserter,
	marketSnapshot *MarketSnapshot,
	rollup *streamRollup,
	fanOut *FanOut,
) {
	switch event.Kind {

	case emit.KindTrade:
		if event.Trade == nil {
			slog.Error("broker/marketdata: KindTrade with nil Trade payload — skipped",
				"exch", exchangeID,
				"instrument", event.Instrument,
				"seq", event.Seq,
			)
			return
		}
		marketSnapshot.SetLast(event.Instrument, event.Trade.Price)
		fanOut.PublishSnapshot(event.Instrument, marketSnapshot.Get(event.Instrument), marketSnapshot.Phase())

	case emit.KindQuote:
		if event.Quote == nil {
			slog.Error("broker/marketdata: KindQuote with nil Quote payload — skipped",
				"exch", exchangeID,
				"instrument", event.Instrument,
				"seq", event.Seq,
			)
			return
		}
		marketSnapshot.SetQuote(event.Instrument, *event.Quote)
		fanOut.PublishSnapshot(event.Instrument, marketSnapshot.Get(event.Instrument), marketSnapshot.Phase())

	case emit.KindDepth:
		if event.Depth == nil {
			slog.Error("broker/marketdata: KindDepth with nil Depth payload — skipped",
				"exch", exchangeID,
				"instrument", event.Instrument,
				"seq", event.Seq,
			)
			return
		}
		marketSnapshot.SetDepth(event.Instrument, *event.Depth)
		fanOut.PublishSnapshot(event.Instrument, marketSnapshot.Get(event.Instrument), marketSnapshot.Phase())

	case emit.KindCandle:
		if event.Candle == nil {
			slog.Error("broker/marketdata: KindCandle with nil Candle payload — skipped",
				"exch", exchangeID,
				"instrument", event.Instrument,
				"seq", event.Seq,
			)
			return
		}
		storeCandle := toStoreCandle(market, event.Candle)
		if mode == ModeCanonical {
			if insertErr := candleStore.Insert(ingestCtx, storeCandle); insertErr != nil {
				// Log + forward anyway (BODIES §1.6).
				slog.Error("broker/marketdata: candle Insert failed — forwarding anyway",
					"exch", exchangeID,
					"instrument", storeCandle.Instrument,
					"start", storeCandle.Start,
					"err", insertErr,
				)
			}
		}
		for _, finalizedBar := range rollup.Ingest(storeCandle) {
			finalizedTimeframe := Timeframe(int(finalizedBar.End.Sub(finalizedBar.Start) / time.Minute))
			fanOut.PublishCandle(finalizedBar, finalizedTimeframe)
		}

	case emit.KindSession:
		if event.Session == nil {
			slog.Error("broker/marketdata: KindSession with nil Session payload — skipped",
				"exch", exchangeID,
				"seq", event.Seq,
			)
			return
		}
		// SessionClosed first (BODIES §1.5): flush every forming bar
		// across every instrument before changing phase, so the final
		// bars are stamped with the still-open phase on their outgoing
		// candle packet.
		if event.Session.Kind == emit.SessionClosed {
			for _, finalizedBar := range rollup.FlushAll() {
				finalizedTimeframe := Timeframe(int(finalizedBar.End.Sub(finalizedBar.Start) / time.Minute))
				fanOut.PublishCandle(finalizedBar, finalizedTimeframe)
			}
		}
		marketSnapshot.ApplySession(event.Session.Kind)
		// Session info folds into the snapshot via phase — a snapshot
		// re-emit lets subscribed clients see the new phase without a
		// separate session frame (WS protocol §3.3 decision (a)). The
		// Instrument on a SessionEvent is global across the instance;
		// we publish only against the specific instrument carried on
		// the event. Any subscriber of that instrument snapshot stream
		// will see the new phase on their next packet.
		//
		// Note: Get may return nil if no KindQuote/KindTrade/KindDepth
		// has arrived for this instrument yet (instrument not yet tape-
		// learned). snapshotFrame.marshal handles nil snap gracefully —
		// it emits the phase with zero price data so clients still see
		// the transition without a panic.
		fanOut.PublishSnapshot(event.Instrument, marketSnapshot.Get(event.Instrument), marketSnapshot.Phase())

	default:
		slog.Error("broker/marketdata: unknown TapeEvent Kind — skipped",
			"exch", exchangeID,
			"kind", event.Kind,
			"seq", event.Seq,
		)
	}
}

// toStoreCandle copies an exchange emit.Candle into a store-boundary
// Candle, stamping the market label the ingestor was wired with.
// Fields that cross:
//
//   - Instrument, Start (as-is: time.Time is an absolute instant;
//     Postgres TIMESTAMPTZ normalizes to UTC on insert — BODIES §1.4
//     closed the tz-at-insert question)
//   - Open / High / Low / Close / Volume (verbatim)
//
// Dropped:
//
//   - Seq (meaningless once stored; the DB has no column for it)
//   - End (derived on read as Start + 1min; not persisted)
func toStoreCandle(market string, exchangeCandle *emit.Candle) Candle {
	return Candle{
		Market:     market,
		Instrument: exchangeCandle.Instrument,
		Start:      exchangeCandle.Start,
		Open:       exchangeCandle.Open,
		High:       exchangeCandle.High,
		Low:        exchangeCandle.Low,
		Close:      exchangeCandle.Close,
		Volume:     exchangeCandle.Volume,
	}
}
