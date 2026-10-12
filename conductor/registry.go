package conductor

import (
	"sync"
	"time"

	"simulator/broker/marketdata"
	"simulator/contract"
	"simulator/exchange/wire"
)

// Mode picks between the two session/market flavors. Distinct from
// marketdata.Mode (same semantics, different package type) — the
// canonical and on-demand spin-up phases translate at the WireInstance
// call site.
type Mode int

const (
	Canonical Mode = iota
	OnDemand
)

func (mode Mode) String() string {
	switch mode {
	case Canonical:
		return "canonical"
	case OnDemand:
		return "ondemand"
	}
	return "invalid"
}

// MarketState is the lifecycle of a Market record.
//   - Provisioning: reserved in the registry, nothing wired/started.
//   - Live: constructed, seeded, wired, started — admittable.
//   - TearingDown: a close-shape teardown is in flight.
type MarketState int

const (
	Provisioning MarketState = iota
	Live
	TearingDown
)

func (state MarketState) String() string {
	switch state {
	case Provisioning:
		return "provisioning"
	case Live:
		return "live"
	case TearingDown:
		return "tearing_down"
	}
	return "invalid"
}

// SessionState is the lifecycle of a Session record.
//   - Starting: reserved; the broker install may be in flight.
//   - Active: installed on its owning shard; the user may trade.
//   - Stopping: teardown in flight.
//   - Ended: vestigial — sessions are deleted on reap, never tombstoned.
//     Reserved so late phases that explicitly want a final state can set
//     it without redefining the enum.
//   - Failed: on-demand setup-failure terminal state. Set when the
//     held-setup path (populate / construct / start / install) fails
//     after the market record has been reserved.
type SessionState int

const (
	Starting SessionState = iota
	Active
	Stopping
	Ended
	Failed
)

func (state SessionState) String() string {
	switch state {
	case Starting:
		return "starting"
	case Active:
		return "active"
	case Stopping:
		return "stopping"
	case Ended:
		return "ended"
	case Failed:
		return "failed"
	}
	return "invalid"
}

// AccountType marks whether the account survives the session.
//   - Durable: canonical sessions — the account persists.
//   - Ephemeral: on-demand sessions — PersistOnClose=false; the account
//     is discarded at teardown.
type AccountType int

const (
	Durable AccountType = iota
	Ephemeral
)

func (accountType AccountType) String() string {
	switch accountType {
	case Durable:
		return "durable"
	case Ephemeral:
		return "ephemeral"
	}
	return "invalid"
}

// Market is the registry's per-instance record. Unexported fields are
// written only by Conductor methods under Registry.mu.
type Market struct {
	ID    MarketID
	Mode  Mode
	Tag   MarketTag
	State MarketState

	// exch is the wire.Exchange handle; set at finalize-Live.
	exch *wire.Exchange

	// inbound is exch.Inbound retained for the join flow's StartSession
	// binding (the per-session half of the exchange seam the conductor
	// injects).
	inbound chan<- contract.InboundMessage

	// fanout is the MarketDataPort.WireInstance return — retained for
	// the WebSocket dispatcher, which resolves sessionID → Session.Market
	// → Market.fanout and hands the socket to the shipped market-data
	// handler.
	fanout *marketdata.FanOut

	// closeAt is the terminal PhaseClosed instant (canonical markets) or
	// the session-end instant (on-demand) — arms teardown/reap.
	closeAt time.Time

	// warmupEndsAt is the construct instant plus the warm-up window for
	// on-demand sessions (agents trade during this window; the user is
	// not yet admitted). Zero for canonical markets.
	warmupEndsAt time.Time

	// members tracks SessionIDs currently attached to this Market —
	// populated by join finalize, pruned by leave/teardown.
	members map[SessionID]struct{}
}

// Session is the registry's per-user record.
type Session struct {
	ID          SessionID
	User        UserID
	Market      MarketID
	Account     AccountID
	AccountType AccountType
	Participant ParticipantID
	State       SessionState
	FailReason  string // non-empty only when State == Failed
}

// Registry holds all conductor state behind one mutex (the one-mutex
// rule: lock only for the microsecond reserve / finalize / state-flip
// steps, never across a call-out to the broker, market-data, or the
// exchange).
//
// All three maps are written only under mu. Reads also take mu — the
// lock is the key race surface; broad RLock is deliberately NOT used
// because the longest critical sections are µs reserve/finalize flips.
//
// Zero value is NOT usable — construct via newRegistry.
type Registry struct {
	mu       sync.Mutex
	markets  map[MarketID]*Market
	sessions map[SessionID]*Session
	byUser   map[UserID]*Session
}

func newRegistry() *Registry {
	return &Registry{
		markets:  make(map[MarketID]*Market),
		sessions: make(map[SessionID]*Session),
		byUser:   make(map[UserID]*Session),
	}
}
