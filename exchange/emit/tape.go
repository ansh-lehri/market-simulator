// Package emit is the exchange's outbound voice (EXCHANGE_19). Every other
// component produces neutral records; emit shapes them into tagged wire events
// on the two shared channels (public tape + private stream), owns the
// per-instrument event Seq, and holds the OHLCVAggregator that emits
// continuous candles.
package emit

import (
	"time"

	"simulator/contract"
	"simulator/exchange/book"
)

// TapeKind discriminates the payload carried by a TapeEvent. Exactly one of
// the pointer fields on TapeEvent is non-nil per Kind (EXCHANGE_19 §3).
type TapeKind int

const (
	KindTrade TapeKind = iota
	KindQuote
	KindDepth
	KindCandle
	KindSession
)

// TapeEvent is the single tagged-union type on the shared public channel, so
// the one-out edge rule holds while one channel carries every public kind.
// Seq is per-instrument event order (invariant 3); Instrument is the routing
// key downstream consumers use to filter.
type TapeEvent struct {
	Seq        contract.Seq
	Instrument contract.InstrumentID
	Kind       TapeKind

	Trade   *PublicTrade
	Quote   *book.QuoteState
	Depth   *DepthSnapshot
	Candle  *Candle
	Session *SessionEvent
}

// PublicTrade is the anonymized trade print — no participant identity on the
// public tape (invariant 1). Price/Qty only.
type PublicTrade struct {
	Price contract.Ticks
	Qty   contract.Qty
}

// DepthLevel is one row of the top-N depth ladder emitted on the tape.
type DepthLevel struct {
	Price    contract.Ticks
	TotalQty contract.Qty
}

// DepthSnapshot is the top-N-per-side depth image (decision: N = 10). Lossy
// by design — the tape shows the top of book, not the entire ladder. Bids and
// Asks are ordered best-first.
type DepthSnapshot struct {
	Bids []DepthLevel
	Asks []DepthLevel
}

// Candle is one OHLCV bucket for one instrument. Start is the bucket's start
// instant; the bucket length is the aggregator's configured interval. Seq is
// stamped by the emitter at EmitCandle (invariant 3), not by the aggregator.
type Candle struct {
	Instrument contract.InstrumentID
	Start      time.Time
	Open       contract.Ticks
	High       contract.Ticks
	Low        contract.Ticks
	Close      contract.Ticks
	Volume     contract.Qty
	Seq        contract.Seq
}

// SessionEventKind names the session-lifecycle transitions the emitter
// broadcasts on the public tape (EXCHANGE_19 §3; driven by clock transitions
// per EXCHANGE_18).
type SessionEventKind int

const (
	SessionOpened SessionEventKind = iota
	AuctionWindowOpened
	SessionClosed
)

// SessionEvent is the public-tape payload for a session-lifecycle transition.
type SessionEvent struct {
	Kind SessionEventKind
}
