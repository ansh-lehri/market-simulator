package funnel

import (
	"log/slog"

	"simulator/economy/account"
	"simulator/economy/capstone"
	"simulator/economy/realize"
	"simulator/economy/reserve"
)

// shard is one owner goroutine's home — the sole caller into L5 for
// every account that hashes to it. Every field below is touched ONLY by
// the shard's own goroutine (or by NewBroker at construction time,
// before Start spawns that goroutine), so there is no mutex anywhere on
// this type.
//
// Field origin (which point / stage each field comes from):
//   inbox        Point 1 — the one channel per shard (BROKER_TIER0 §Q4).
//   brokerID     Point 3 — set at NewBroker time; stamped on every outbound
//                InboundMessage the shard eventually punches (Tier 1).
//   sessions     Point 5 — live map keyed by AccountID; populated at session
//                load (a lifecycle concern, out of scope for Phase 0);
//                the shard is the sole reader/writer.
//   tracks       Point 5 — thin exchange-seam records keyed by OrderID;
//                populated at order punch (Tier 1); the shard is the
//                sole reader/writer.
//   bundle       Point 6 — the shard's per-bundle handles ({Pool, Redis,
//                Persist, Cost, Lifecycle}); {Ledger, Reserve, Realizer}
//                on the bundle are the capstone stubs and are IGNORED
//                (BROKER_FUNNEL/CLAUDE.md decision P1-D1). Held for
//                Phase-1 teardown ordering.
//   resv         Point 6 — the shard's OWN ReservationService, built with
//                the shard itself as the AccountLedger and this bundle's
//                Persist + Cost.
//   realiz       Point 6 — the shard's OWN Realizer, same ledger and
//                bundle wiring. Built BEFORE resv (reserve takes realizer
//                as its RealizerHandoff, per capstone.Wire's own order).
//
// Handler bodies (order intake, cancel, settlement, bank, reads, stops)
// live in shard_handlers.go as Tier-1+/-3+/-4+ empty seams — Phase 0
// builds the machine, not the bodies.
type shard struct {
	inbox    chan Command
	brokerID BrokerID

	sessions map[AccountID]*ownedSession
	tracks   map[OrderID]*OrderTrack

	bundle *capstone.Bundle
	resv   *reserve.ReservationService
	realiz *realize.Realizer
}

// newShard constructs one shard: fresh inbox of the given depth, empty
// sessions / tracks maps, and (when bundle != nil) the per-shard
// realizer + reservation service wired with the shard as the
// AccountLedger and the bundle's Persist + Cost as the collaborators.
//
// Wiring order matches capstone.Wire (bundle.go): realizer FIRST (reserve
// takes it as RealizerHandoff), then reserve. Both NewX constructors
// only store their args — no I/O, no goroutine — so building them at
// NewBroker time is safe even when Persist / Cost are typed-nil.
//
// A nil bundle is legal in Phase 0: tests that only exercise the machine
// (dispatch, routing, reply, Stop-drains) pass nil bundles and the shard
// leaves resv / realiz as nil. A production BringUp (Phase 1) always
// supplies real bundles.
func newShard(brokerID BrokerID, inboxDepth int, bundle *capstone.Bundle) *shard {
	builtShard := &shard{
		inbox:    make(chan Command, inboxDepth),
		brokerID: brokerID,
		sessions: make(map[AccountID]*ownedSession),
		tracks:   make(map[OrderID]*OrderTrack),
		bundle:   bundle,
	}
	if bundle != nil {
		builtShard.realiz = realize.NewRealizer(builtShard, bundle.Persist, bundle.Cost)
		builtShard.resv = reserve.NewReservationService(
			builtShard,
			bundle.Cost,
			builtShard.realiz,
			nopEvents{},
			bundle.Persist,
		)
	}
	return builtShard
}

// Get satisfies BOTH reserve.AccountLedger and realize.AccountLedger
// (BROKER_TIER0 §Point-6). The shard IS the ledger — it resolves an
// AccountID against its own sessions map, returning the inner *Account
// held on the LoadedAccount (economy/lifecycle types).
//
// A missing account returns nil; per reserve/realize contracts a nil at
// Reserve / Convert / Release / ReleaseExcess is a programming error —
// Gate 4 would not admit an order for an unloaded account. Phase 0's
// handler bodies never call Reserve / Convert / ..., so this method is
// exercised only by tests and (Phase 1+) by the real handlers.
//
// The read is lazy: because sessions is a pointer held by both the shard
// and (via the AccountLedger interface) by the compute stack, any
// account loaded after NewBroker is visible on the very next Get. This
// is only safe because of single-writer — the shard is the sole writer
// of the sessions map.
func (ownerShard *shard) Get(accountID account.AccountID) *account.Account {
	if loadedSession, ok := ownerShard.sessions[accountID]; ok {
		return loadedSession.account.Account
	}
	return nil
}

// run is the shard's serial loop — the body of the go s.run() spawned
// in Broker.Start. Ranges the inbox: one command at a time, fully
// dispatched before the next is pulled. Ranging blocks when empty (no
// busy-wait) and exits cleanly when Broker.Stop closes the inbox.
//
// This "one command at a time, fully finished before the next" is what
// makes single-writer true (BROKER_TIER0 §Point-4). No locks, no
// per-command goroutines — the serialness is the feature.
func (ownerShard *shard) run() {
	slog.Debug("shard: run loop entered", "broker_id", ownerShard.brokerID)
	for cmd := range ownerShard.inbox {
		ownerShard.dispatch(cmd)
	}
	slog.Debug("shard: run loop exited", "broker_id", ownerShard.brokerID)
}
