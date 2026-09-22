package reserve

import (
	"simulator/economy/account"
	"simulator/economy/cost"
	"simulator/economy/realize"
)

// AccountLedger is reserve's read handle to accounts — one method,
// returning the live account or nil for unknown ids. Structurally
// identical to realize.AccountLedger; a shared concrete satisfies both
// (Go's duck-typed interfaces). Defined locally because no shared home
// exists yet across economy packages; when one lands, this alias becomes
// a re-export or moves entirely without changing any caller.
//
// A nil return at Reserve / Convert / Release / ReleaseExcess is a
// programming error (Gate 4 would not admit an order for an unloaded
// account); the service layer will panic on it in stage 2.
type AccountLedger interface {
	Get(accountID account.AccountID) *account.Account
}

// Persistence is reserve's slice of the L5 persistence seam — deliberately
// narrower than realize.Persistence, because reserve NEVER enqueues closed
// trades (that is 5.4's line on the realization path). Reserve only marks
// live state dirty so the background snapshotter mirrors {cash, positions,
// reservations} to Redis on its cadence.
//
// Non-blocking: MarkSnapshotDirty MUST NOT do a synchronous Redis or
// Postgres write on the hot path (per economy/CLAUDE.md persistence rule).
// Phase 1 wires an in-memory spy; the real implementation lands behind the
// same interface with zero changes to reserve.
//
// realize.Persistence has this method too (identical signature), so a
// concrete that implements BOTH interfaces trivially works for reserve
// and realize simultaneously. Kept as separate interfaces because
// interface-segregation — reserve should not carry EnqueueClosedTrade in
// its dep list when it never calls it.
type Persistence interface {
	MarkSnapshotDirty(sessionID SessionID, accountID account.AccountID)
}

// EventSink is where reserve emits money events at every reserve /
// convert / release call site. The event *system* (delivery, buffering,
// consumers, whether synchronous or async) is DEFERRED (doc §9); this
// seam is intentionally minimal so nothing depends on the system
// internals yet. Stage 1 wires the interface + concrete event types;
// stages 2–4 wire Emit calls at the right spots.
type EventSink interface {
	Emit(event Event)
}

// Event is the sealed union of every event reserve emits. The isEvent()
// method is unexported so only THIS package can implement it — external
// code cannot invent a new event kind and slip it through an EventSink.
// Consumers type-switch (or errors.As once we grow value-shape events)
// to inspect the concrete payload.
type Event interface {
	isEvent()
}

// ReserveEvent is emitted on a successful ReserveIfAvailable — the
// admission moment where cash (buy) or units (sell) first shift
// Available → Reserved. One of Cash / Units is non-zero, matching the
// side; the other is 0.
type ReserveEvent struct {
	OrderID   OrderID
	AccountID account.AccountID

	// Cash — coins reserved on a buy. 0 on sell records.
	Cash int64

	// Units — position units reserved on a sell. 0 on buy records.
	Units int64
}

func (ReserveEvent) isEvent() {}

// ConvertEvent is emitted per-fill inside Convert (§3.5), and also on the
// terminal per-order brokerage debit when RemainingQty reaches 0 (§3.4).
// The buy variant sets CashDebited + UnitsCredited (Reserved→Settled cash,
// new lot delivered); the sell variant sets UnitsDelivered + CashCredited
// (Reserved units out, proceeds into Available). Cost carries whichever
// CostBreakdown hit Available on this event — the per-fill breakdown for
// a fill event, or the per-order breakdown for the terminal brokerage.
type ConvertEvent struct {
	OrderID   OrderID
	AccountID account.AccountID

	// Buy-side (0 on sell events / terminal-brokerage events).
	CashDebited   int64
	UnitsCredited int64

	// Sell-side (0 on buy events / terminal-brokerage events).
	UnitsDelivered int64
	CashCredited   int64

	// Cost — the breakdown that was debited to Available on this event.
	// Empty (Total == 0, nil Lines) for a fill whose per-fill cost was
	// zero, or when the emitter chose to omit it (documented at call
	// site in stage 3).
	Cost cost.CostBreakdown
}

func (ConvertEvent) isEvent() {}

// ReleaseEvent is emitted by BOTH Release (any non-fill terminal —
// cancel, expire, IOC/market remainder, FOK reject, auction lapse,
// qty-decrease modify) and ReleaseExcess (market-buy buffer true-up on
// completion). One of Cash / Units is non-zero, matching the side; the
// other is 0.
type ReleaseEvent struct {
	OrderID   OrderID
	AccountID account.AccountID

	// Cash — coins returned to Available (buy release, or market-buy
	// residue from ReleaseExcess). 0 on sell releases.
	Cash int64

	// Units — position units returned to PosAvailable (sell release).
	// 0 on buy releases and on ReleaseExcess.
	Units int64
}

func (ReleaseEvent) isEvent() {}

// RealizerHandoff is the narrow subset of *realize.Realizer that reserve
// uses. Convert's sell branch calls OnSellFill; nothing else in reserve
// reaches into realize. Defined as an interface (not a concrete
// *realize.Realizer field) so stage 3 tests can substitute a spy that
// records what RealizationInput reserve hands over, without wiring the
// full Realizer machinery (accumulator book, ledger, persistence spy).
type RealizerHandoff interface {
	OnSellFill(input realize.RealizationInput)
}
