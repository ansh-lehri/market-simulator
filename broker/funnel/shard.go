package funnel

import (
	"context"
	"log/slog"
	"strconv"

	"simulator/economy/account"
	"simulator/economy/capstone"
	"simulator/economy/lifecycle"
	"simulator/economy/realize"
	"simulator/economy/reserve"
)

// sessionSaver is the narrow slice of lifecycle.Service that
// handleFinalizeMarketOnShard needs — one method, the same signature
// shipped by lifecycle.Service.SessionSave. Introduced so in-package
// tests can swap a fake without touching the shard's bundle wiring (the
// bundle holds a concrete *lifecycle.Service everywhere else).
//
// Production: the real *lifecycle.Service satisfies this interface
// trivially.
// Tests:      a hand-written fake records calls and controls outcomes.
type sessionSaver interface {
	SessionSave(ctx context.Context, sessionID string, loaded lifecycle.LoadedAccount) error
}

// sessionDiscarder is the parallel narrow slice of lifecycle.Service
// that handleDiscardSession needs — one method, matching
// lifecycle.Service.SessionDiscard. Same pattern as sessionSaver: the
// bundle's concrete *lifecycle.Service satisfies both interfaces, and
// tests inject a hand-written fake.
type sessionDiscarder interface {
	SessionDiscard(ctx context.Context, sessionID string, loaded lifecycle.LoadedAccount) error
}

// detachedAccount is the parked state of a canonical session that was
// uninstalled via EndSession(Mode=Canonical) — the detach-keep teardown
// path. The LoadedAccount survives on the shard so a later rejoin can
// re-install over its carried positions and so FinalizeMarket at EOD can
// persist it + unload it. Keyed by AccountID in shard.detached.
//
// Three fields:
//
//   - loaded         the whole lifecycle.LoadedAccount envelope exactly
//                    as it sat on the live session when uninstall landed.
//                    Positions, cash buckets, reservation map all intact.
//   - lastSessionID  the SessionID the account was bound to at uninstall
//                    time. FinalizeMarket calls lifecycle.SessionSave with
//                    this value, and a re-install can detect "same session
//                    returning" if it ever needs to.
//   - exchangeID     the ExchangeID the account was bound to at uninstall
//                    time. Redundant with loadedByExchange but keeps the
//                    sweep self-contained — the handler reads it off the
//                    detached record rather than walking the whole index.
type detachedAccount struct {
	loaded        lifecycle.LoadedAccount
	lastSessionID SessionID
	exchangeID    ExchangeID
}

// shard is one owner goroutine's home — the sole caller into the
// reserve + realize engine for every account that hashes to it. Every field below is touched ONLY by
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

	// detached parks LoadedAccounts whose session was uninstalled via a
	// canonical EndSession (detach-keep). The account survives on the
	// shard until FinalizeMarket persists + unloads it at EOD. Written
	// by handleUninstallSession and handleFinalizeMarketOnShard; read by
	// handleFinalizeMarketOnShard. Owner-only, no mutex (single-writer).
	detached map[AccountID]detachedAccount

	// loadedByExchange indexes every LoadedAccount on this shard — both
	// the live set in `sessions` and the parked set in `detached` — by
	// the ExchangeID it was bound to at install time. FinalizeMarket's
	// per-shard sweep walks loadedByExchange[exchangeID] to find the
	// accounts belonging to one closing market. Written by
	// handleInstallSession (add on install), handleUninstallSession
	// (entry stays — account is still loaded on the shard, just parked),
	// and handleFinalizeMarketOnShard (delete on unload). Owner-only.
	loadedByExchange map[ExchangeID]map[AccountID]struct{}

	// saver is the narrow lifecycle.Service surface FinalizeMarket's
	// sweep uses to persist each account. Set at newShard time from
	// bundle.Lifecycle when the bundle is present; nil otherwise
	// (Phase-0 tests that never exercise finalize). handleFinalizeMarket
	// OnShard surfaces nil as a per-account save error on the aggregate
	// reply — so a shard with no saver reports accurately rather than
	// panicking deep in the sweep.
	saver sessionSaver

	// discarder is the narrow lifecycle.Service surface the on-demand
	// discard handler uses to drop the ephemeral account's Redis
	// snapshot. Set alongside saver at newShard time from
	// bundle.Lifecycle; nil otherwise (bundle-less test wirings).
	// handleDiscardSession surfaces nil as an ErrDiscardFailed on the
	// reply — same reporting contract as a save-side missing saver.
	discarder sessionDiscarder

	// seq — Phase-5 Point-1 OrderID mint source (BROKER_PHASE5 §1.4 P5-D4).
	// Shard-local monotonic uint64; the shard goroutine is the sole writer,
	// so a plain `s.seq++` is correct — no atomic, no lock. Mint shape is
	// "<AccountID>:<seq>" so settlement's accountOf parses the owner back.
	// Crash-resume of this counter is B9, carried.
	seq uint64

	// mktCache — Phase-5 Point-1 (market-buy BestAsk) + Phase-6 Point-2
	// (position mark LastPrice) read seam. Threaded in by NewBroker; nil
	// is substituted with nopMarketCache{} at newShard so the place
	// handler can dereference it unconditionally. Both methods are
	// keyed by (ExchangeID, InstrumentID) — callers pass
	// sess.binding.ExchangeID. Real impl is B10 (Tier 5), not built.
	// Tests swap the field directly for sealed scenarios.
	mktCache MarketPriceCache

	// slippageBufferBps — Phase-5 market-buy buffer dial (P5-D8), copied
	// in from MoneyConfig.SlippageBufferBps at NewBroker time. Shard-local
	// so slippageBuffer() reads a single field on the owner goroutine.
	// Zero by default; a positive bps sizes an extra buffer on top of
	// bestAsk*qty via `notional * bps / 10000`.
	slippageBufferBps int64

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
//
// mktCache is Phase-5's BestAsk + Phase-6's LastPrice seam, threaded in
// from NewBroker. nil is substituted with nopMarketCache{} so the place
// handler's unconditional mktCache.BestAsk(...) stays safe for callers
// (bringup, funnel-internal tests) that have no real cache to inject.
// Real wiring is Phase 7 (B10 ingestor + WireInstance).
func newShard(brokerID BrokerID, inboxDepth int, bundle *capstone.Bundle, mktCache MarketPriceCache) *shard {
	if mktCache == nil {
		mktCache = nopMarketCache{}
	}
	builtShard := &shard{
		inbox:            make(chan Command, inboxDepth),
		brokerID:         brokerID,
		sessions:         make(map[AccountID]*ownedSession),
		tracks:           make(map[OrderID]*OrderTrack),
		detached:         make(map[AccountID]detachedAccount),
		loadedByExchange: make(map[ExchangeID]map[AccountID]struct{}),
		mktCache:         mktCache,
		bundle:           bundle,
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
		if bundle.Lifecycle != nil {
			builtShard.saver = bundle.Lifecycle
			builtShard.discarder = bundle.Lifecycle
		}
	}
	return builtShard
}

// nextOrderID mints a fresh OrderID for a Phase-5 PlaceOrder (BROKER_PHASE5
// §1.4 P5-D4). Shape is "<AccountID>:<seq>" where seq is this shard's local
// monotonic uint64 — bumped on every call, so two orders on the same shard
// (whether same or different accounts) never collide.
//
// Owner-goroutine only: `s.seq++` is a plain increment, no atomic, no lock
// — the single-writer rule is the lock. A race-detector pass over the
// Phase-5 settle/apply flows is the standing gate.
//
// Encoding rationale: the OrderID crosses the exchange seam and comes back
// echoed on EngineResult.OrderID; the settlement consumer parses the
// AccountID out via accountOf (LastIndex-based) so there is no cross-shard
// OrderID→AccountID map. Keep the shape ":"-delimited.
//
// Resharding caveat / crash-resume (B9, carried): on recovery, the shard
// must resume `seq` past the highest minted value for its accounts, or the
// first mint after restart would collide with a surviving reservation.
// Phase-5 implementation treats this as a flagged follow-up — the field is
// in place, the snapshot shape is Phase-2 session_save's call.
func (ownerShard *shard) nextOrderID(accountID AccountID) OrderID {
	ownerShard.seq++
	return accountID + ":" + strconv.FormatUint(ownerShard.seq, 10)
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
