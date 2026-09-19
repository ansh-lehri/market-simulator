package emit

import (
	"simulator/contract"
	"simulator/exchange/book"
)

// Emitter is one instrument's outbound voice: owns that instrument's event Seq
// counter and holds the two shared outbound channels (public tape, private
// stream). Single-writer by construction — one Emitter per instrument, called
// only from that instrument's shard loop, so the Seq counter needs no lock
// (EXCHANGE_19 §5, §7). It is the only component that writes the outbound
// channels (invariant 8).
type Emitter struct {
	instrument    contract.InstrumentID
	eventSeq      uint64
	publicTape    chan<- TapeEvent
	privateStream chan<- contract.EngineResult
}

// NewEmitter builds an Emitter for one instrument. Nil channels would deadlock
// on the first send — that is an ownership-model bug, not a runtime condition
// to handle, so it panics.
func NewEmitter(
	instrument contract.InstrumentID,
	publicTape chan<- TapeEvent,
	privateStream chan<- contract.EngineResult,
) *Emitter {
	if publicTape == nil {
		panic("emit.NewEmitter: nil publicTape channel")
	}
	if privateStream == nil {
		panic("emit.NewEmitter: nil privateStream channel")
	}
	return &Emitter{
		instrument:    instrument,
		publicTape:    publicTape,
		privateStream: privateStream,
	}
}

// Next advances the per-instrument event sequence and returns the new value.
// Satisfies the SeqSource interface owned by match (and, when it lands,
// auction): the matcher/auction stamp Trade.Seq from here at trade creation;
// EmitTrades then reuses that stamp rather than re-numbering (invariant 4).
// Also called internally by the Emit* methods that assign a fresh Seq.
func (emitter *Emitter) Next() contract.Seq {
	emitter.eventSeq++
	return contract.Seq(emitter.eventSeq)
}

// EmitTrades pushes one anonymized public print per trade. Trade.Seq was
// stamped by the matcher/auction at creation via Next() and is reused here so
// trade order and tape order agree (invariant 4). Participant identities are
// dropped — the public tape carries price/qty only (invariant 1).
func (emitter *Emitter) EmitTrades(trades []contract.Trade) {
	for _, trade := range trades {
		emitter.publicTape <- TapeEvent{
			Seq:        trade.Seq,
			Instrument: emitter.instrument,
			Kind:       KindTrade,
			Trade:      &PublicTrade{Price: trade.Price, Qty: trade.Qty},
		}
	}
}

// EmitQuote pushes a top-of-book quote snapshot. Callers (the shard) must only
// invoke this when the book's mutation actually changed the quote — invariant
// 6 forbids no-op snapshots (EXCHANGE_19 §5 note).
func (emitter *Emitter) EmitQuote(quote book.QuoteState) {
	emitter.publicTape <- TapeEvent{
		Seq:        emitter.Next(),
		Instrument: emitter.instrument,
		Kind:       KindQuote,
		Quote:      &quote,
	}
}

// EmitDepth pushes a top-N depth snapshot. Same change-only rule as EmitQuote.
func (emitter *Emitter) EmitDepth(depth DepthSnapshot) {
	emitter.publicTape <- TapeEvent{
		Seq:        emitter.Next(),
		Instrument: emitter.instrument,
		Kind:       KindDepth,
		Depth:      &depth,
	}
}

// EmitCandle pushes an OHLCV candle. The aggregator returns the candle with
// Seq zero; the emitter stamps it here so the tape event and the candle share
// one Seq (per-instrument event order).
func (emitter *Emitter) EmitCandle(candle Candle) {
	candle.Seq = emitter.Next()
	emitter.publicTape <- TapeEvent{
		Seq:        candle.Seq,
		Instrument: emitter.instrument,
		Kind:       KindCandle,
		Candle:     &candle,
	}
}

// EmitSession pushes a session-lifecycle event driven by clock transitions
// (EXCHANGE_18 → §6 of this doc).
func (emitter *Emitter) EmitSession(kind SessionEventKind) {
	emitter.publicTape <- TapeEvent{
		Seq:        emitter.Next(),
		Instrument: emitter.instrument,
		Kind:       KindSession,
		Session:    &SessionEvent{Kind: kind},
	}
}

// EmitResult pushes a per-order outcome to the broker via the shared private
// stream. Pass-through: the private stream reuses EngineResult (invariant 7),
// so there is no shaping to do here — matcher, gate, and (via shard wrapping)
// auction all produce EngineResult values that go out unchanged.
func (emitter *Emitter) EmitResult(result contract.EngineResult) {
	emitter.privateStream <- result
}
