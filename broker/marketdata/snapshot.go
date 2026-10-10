package marketdata

import (
	"log/slog"
	"sync/atomic"

	"simulator/contract"
	"simulator/exchange/book"
	"simulator/exchange/emit"
)

// ExchangeID identifies one exchange instance within the broker. Type-
// aliased to string so it's interchangeable with funnel.ExchangeID
// (also = string) without a cross-package import in Stage 4. Stamped
// on each MarketSnapshot at wire time by WireInstance (Stage 7).
type ExchangeID = string

// SessionPhase is the instance-wide session state the MarketSnapshot
// tracks. String forms match the WS wire protocol
// (BROKER_MARKETDATA_WS_WIRE_PROTOCOL §3.3: "phase": "open").
//
// Initial state is PhaseUnknown — no session event received yet. The
// phase advances only via ApplySession, which the ingestor calls on
// every KindSession tape event.
type SessionPhase int

const (
	PhaseUnknown SessionPhase = iota
	PhaseOpen
	PhaseAuction
	PhaseClosed
)

// String returns the stable wire form for a SessionPhase.
func (phase SessionPhase) String() string {
	switch phase {
	case PhaseUnknown:
		return "unknown"
	case PhaseOpen:
		return "open"
	case PhaseAuction:
		return "auction"
	case PhaseClosed:
		return "closed"
	}
	return "invalid"
}

// mapSessionKindToPhase translates an exchange emit.SessionEventKind
// (transition) into a SessionPhase (state). The three event kinds map
// 1:1; anything out of range yields PhaseUnknown — defensive, we don't
// panic on tape data (BODIES §1.6 skip-and-log never panic).
func mapSessionKindToPhase(kind emit.SessionEventKind) SessionPhase {
	switch kind {
	case emit.SessionOpened:
		return PhaseOpen
	case emit.AuctionWindowOpened:
		return PhaseAuction
	case emit.SessionClosed:
		return PhaseClosed
	}
	return PhaseUnknown
}

// InstSnap is one instrument's immutable per-instrument snapshot — the
// atomic unit that lives behind each atomic.Pointer slot. Once stored
// by the writer, it is NEVER mutated in place: the writer builds a new
// InstSnap on every change and Stores the fresh pointer, so readers
// Loading the pointer get a consistent view with no torn reads of the
// Depth slice (BODIES §2.0 — this is why we chose atomic CoW over
// RWMutex+map).
//
// Fields are exported so the WS layer (same package today, possibly a
// sibling service later) + tests can read them. The exported shape is
// a contract: callers MUST NOT mutate any field — in particular do
// NOT append to Depth.Bids / Depth.Asks (shared backing array across
// the lifetime of this snap). The next SetDepth call replaces the
// Depth value with a fresh snapshot from the exchange; this snap's
// Depth slice remains a valid read for anyone still holding it.
//
// Has* flags are load-bearing — 0 is a valid tick price so HasLast=false
// is distinct from "LastPrice happens to be 0" (EXCHANGE_11 §7.1).
type InstSnap struct {
	LastPrice   contract.Ticks
	HasLast     bool
	Quote       book.QuoteState
	Depth       emit.DepthSnapshot
	SessionOpen contract.Ticks
	HasOpen     bool
}

// instMap is the immutable-once-published per-instrument slot map.
// atomic.Pointer[instMap] lets the writer swap in a copy-on-insert
// grown map when learning a new instrument (rare — once per instrument
// at session start), without blocking readers.
type instMap map[contract.InstrumentID]*atomic.Pointer[InstSnap]

// MarketSnapshot is the broker-side retained live state for ONE
// exchange instance (BODIES §2). Concurrency = (B) atomic copy-on-write
// (DESIGN §10 Open resolved):
//
//   - Writer: ONE goroutine (the ingestor). Per-field update clones
//     the current InstSnap, mutates the one field, and Stores the new
//     pointer into the instrument's atomic slot. Learning a new
//     instrument copies the instrument map, inserts the new slot, and
//     Stores the fresh map pointer. Single-writer → no CAS loop.
//
//   - Readers: unlimited. Account hot-path (BestAsk at order-place,
//     LastPrice at position mark) + WS join-seed path all Load the
//     atomic.Pointer[instMap] then Load the inner atomic.Pointer[InstSnap]
//     and read the immutable snap by value. No lock, no RWMutex, no
//     deep copy of Depth slices.
//
// Instruments are tape-learned (E3 — L7 provides no instrument list),
// so the map grows at runtime via copy-on-insert.
//
// The phase field is INSTANCE-WIDE (the exchange clock is global). In
// Stage 4 it was a plain writer-only field (BODIES §2.5: "if a cross-
// goroutine reader lands, it gets its own atomic then"). Stage 5's WS
// layer is that cross-goroutine reader (writeLoop + Subscribe's
// seed-build path), so phase was promoted to atomic.Int32 here.
// sessionOpen stays per-instrument (reset across all instruments on
// SessionOpened).
//
// Zero value is NOT usable — construct via NewMarketSnapshot.
type MarketSnapshot struct {
	exch  ExchangeID
	phase atomic.Int32 // holds SessionPhase; promoted to atomic in Stage 5 for cross-goroutine reads (BODIES §2.5)
	inst  atomic.Pointer[instMap]
}

// NewMarketSnapshot builds an empty snapshot stamped with its
// instance's ExchangeID. The instrument map is seeded as empty — the
// first SetX / ApplySession call learns instruments as they appear on
// the tape (E3).
//
// exchangeID is used for boundary slog lines and, via the Stage-6
// cache registry, as the key the account side will look this snapshot
// up under.
func NewMarketSnapshot(exchangeID ExchangeID) *MarketSnapshot {
	marketSnapshot := &MarketSnapshot{exch: exchangeID}
	// phase atomic.Int32 zero-value == 0 == PhaseUnknown; no explicit
	// Store needed, left here as documentation of initial state.
	initialInstrumentMap := make(instMap)
	marketSnapshot.inst.Store(&initialInstrumentMap)
	slog.Info("broker/marketdata: MarketSnapshot built", "exch", exchangeID)
	return marketSnapshot
}

// --- Writer API (ingestor goroutine only) -----------------------------
//
// All writers follow Load → copy → mutate → Store. Single-writer means
// no CAS loop is needed — the Load+Store between is never contended by
// another writer.

// SetLast updates lastPrice for an instrument and, if this is the
// first trade after a SessionOpened in open phase, seeds sessionOpen
// as the day-change base (BODIES §2.3).
func (marketSnapshot *MarketSnapshot) SetLast(instrument contract.InstrumentID, price contract.Ticks) {
	slot := marketSnapshot.getOrCreateSlot(instrument)
	current := slot.Load()
	newSnap := *current // value copy; shares Depth slice ref with current
	newSnap.LastPrice = price
	newSnap.HasLast = true
	if SessionPhase(marketSnapshot.phase.Load()) == PhaseOpen && !newSnap.HasOpen {
		newSnap.SessionOpen = price
		newSnap.HasOpen = true
	}
	slot.Store(&newSnap)
}

// SetQuote replaces the quote (best bid/ask + sizes + Has* flags) for
// an instrument. QuoteState is a value type — the whole thing is
// copied into the new InstSnap.
func (marketSnapshot *MarketSnapshot) SetQuote(instrument contract.InstrumentID, quote book.QuoteState) {
	slot := marketSnapshot.getOrCreateSlot(instrument)
	current := slot.Load()
	newSnap := *current
	newSnap.Quote = quote
	slot.Store(&newSnap)
}

// SetDepth replaces the depth ladder (top-N bids/asks). The exchange
// emits fresh slices per Depth event (EXCHANGE_19); we retain the
// slice refs on the new InstSnap. Since this InstSnap is never mutated
// after Store, and the exchange never mutates the slice after emitting
// it, there is no torn-read window on Depth.
func (marketSnapshot *MarketSnapshot) SetDepth(instrument contract.InstrumentID, depth emit.DepthSnapshot) {
	slot := marketSnapshot.getOrCreateSlot(instrument)
	current := slot.Load()
	newSnap := *current
	newSnap.Depth = depth
	slot.Store(&newSnap)
}

// ApplySession updates the instance-wide phase (writer-only plain
// field) and, on SessionOpened, resets every known instrument's
// HasOpen so the first trade of the new session re-seeds sessionOpen
// as the day-change base (BODIES §2.3).
//
// SessionClosed does NOT clear SessionOpen — the WS payload may still
// want the closed session's open-price (client decides what to show
// post-close).
func (marketSnapshot *MarketSnapshot) ApplySession(kind emit.SessionEventKind) {
	marketSnapshot.phase.Store(int32(mapSessionKindToPhase(kind)))
	if kind != emit.SessionOpened {
		return
	}
	// Reset per-instrument day base so the first trade after open
	// seeds sessionOpen afresh. Iterating existing slots: no
	// copy-on-insert triggers, so the loaded map pointer stays valid.
	m := marketSnapshot.inst.Load()
	for _, slot := range *m {
		current := slot.Load()
		newSnap := *current
		newSnap.HasOpen = false
		newSnap.SessionOpen = 0
		slot.Store(&newSnap)
	}
}

// --- Reader API (any goroutine) ---------------------------------------
//
// Lock-free: Load the map pointer, Load the slot pointer, read fields
// by value. The InstSnap pointer returned by Get is immutable —
// callers MUST NOT mutate any field.

// BestAsk returns the current best-ask tick for an instrument, or
// (0,false) when the quote has no ask side (empty ask book, or the
// instrument hasn't been seen on the tape). Satisfies the "best ask"
// half of funnel.MarketPriceCache via the Stage-6 registry wrapper.
func (marketSnapshot *MarketSnapshot) BestAsk(instrument contract.InstrumentID) (contract.Ticks, bool) {
	m := marketSnapshot.inst.Load()
	slot, ok := (*m)[instrument]
	if !ok {
		return 0, false
	}
	current := slot.Load()
	if !current.Quote.HasAsk {
		return 0, false
	}
	return current.Quote.BestAsk, true
}

// LastPrice returns the most recent trade price for an instrument, or
// (0,false) when no trade has been seen (cold start, or the instrument
// hasn't traded this session). Satisfies the "last price" half of
// funnel.MarketPriceCache.
func (marketSnapshot *MarketSnapshot) LastPrice(instrument contract.InstrumentID) (contract.Ticks, bool) {
	m := marketSnapshot.inst.Load()
	slot, ok := (*m)[instrument]
	if !ok {
		return 0, false
	}
	current := slot.Load()
	if !current.HasLast {
		return 0, false
	}
	return current.LastPrice, true
}

// Get returns the immutable *InstSnap for an instrument, or nil if the
// instrument hasn't been seen on this tape yet. Consumed by the WS
// join-seed path (snapshot subs seed from retained state) and by the
// ingestor's publish path (same InstSnap is forwarded to FanOut).
//
// Returned pointer is immutable — callers MUST NOT mutate any field or
// append to Depth.Bids / Depth.Asks.
func (marketSnapshot *MarketSnapshot) Get(instrument contract.InstrumentID) *InstSnap {
	m := marketSnapshot.inst.Load()
	slot, ok := (*m)[instrument]
	if !ok {
		return nil
	}
	return slot.Load()
}

// Phase returns the current instance-wide session phase. Safe from
// ANY goroutine — reads via atomic.Int32.Load. phase was a plain
// writer-only field in Stage 4 (BODIES §2.5: "if a cross-goroutine
// reader lands, it gets its own atomic then"). Stage 5's WS layer is
// that reader (writeLoop + the Subscribe seed-build path), so phase
// was promoted to atomic.Int32.
func (marketSnapshot *MarketSnapshot) Phase() SessionPhase {
	return SessionPhase(marketSnapshot.phase.Load())
}

// Exch returns this snapshot's instance id. For logs + the Stage-6
// cache registry key.
func (marketSnapshot *MarketSnapshot) Exch() ExchangeID {
	return marketSnapshot.exch
}

// --- internals --------------------------------------------------------

// getOrCreateSlot returns the per-instrument atomic.Pointer slot for an
// instrument, creating it (via copy-on-insert of the instrument map)
// if unseen. Writer-only — a reader must NOT call this (it mutates the
// map pointer). Single-writer invariant → no CAS loop on the map Store.
//
// The fresh slot is pre-seeded with a non-nil *InstSnap (zero-value)
// so a reader that Loads it never nil-derefs.
func (marketSnapshot *MarketSnapshot) getOrCreateSlot(instrument contract.InstrumentID) *atomic.Pointer[InstSnap] {
	m := marketSnapshot.inst.Load()
	if slot, ok := (*m)[instrument]; ok {
		return slot
	}
	// Copy-on-insert — rare, once per instrument per session.
	newMap := make(instMap, len(*m)+1)
	for existingInstrument, existingSlot := range *m {
		newMap[existingInstrument] = existingSlot
	}
	freshSlot := &atomic.Pointer[InstSnap]{}
	freshSlot.Store(&InstSnap{}) // non-nil initial so readers never nil-deref
	newMap[instrument] = freshSlot
	marketSnapshot.inst.Store(&newMap)
	slog.Debug("broker/marketdata: MarketSnapshot learned instrument",
		"exch", marketSnapshot.exch,
		"instrument", instrument,
		"instruments_now", len(newMap),
	)
	return freshSlot
}
