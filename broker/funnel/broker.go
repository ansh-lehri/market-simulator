package funnel

import (
	"context"
	"log/slog"
	"sync"

	"simulator/contract"
	"simulator/economy/capstone"
	"simulator/economy/reserve"
)

// Broker is the ONE externally-visible entity of the funnel — shards and
// the router are internal (BROKER_TIER0 §Point-3). Callers hit the
// public methods below; the router hashes AccountID and hands to the
// right shard. shardCount, the hash, and the shard slice are invisible
// and retunable without touching any caller.
//
// Fields:
//   brokerID    identity of this broker instance; stamped on outbound
//               punches (Tier 1 consumes it).
//   shards      the fixed pool of N owner goroutines. N = len(shards) —
//               the shard count is derived from the bundles slice at
//               construction, not a separate arg (BROKER_PHASE1 §1.4).
//   shardCount  cached len(shards) so route() reads no slice length.
//   store       the broker-owned durable layer (BROKER_PHASE1 §5); nil in
//               Phase-0 tests, real in Phase-1 BringUp. Held on Broker so
//               shutdown ordering (owners exit → bundle Close → store
//               pool close) has a single owner.
//   shardWG     tracks the N run-goroutines so Stop can block until they
//               have drained and returned. Load-bearing at Phase-1
//               shutdown: a shard's final MarkSnapshotDirty must not land
//               on a closed bundle handle (BROKER_PHASE1 §1.5).
type Broker struct {
	brokerID   BrokerID
	shards     []*shard
	shardCount int
	store      *Store
	shardWG    sync.WaitGroup
}

// NewBroker builds N = len(bundles) shards, one per bundle, each carrying
// its own per-shard L5 wiring (Point 6). The construction happens BEFORE
// Start spawns any goroutine, so touching the shard slice here is
// single-threaded and safe without a lock.
//
// Signature per BROKER_PHASE1 §1.4 (supersedes the Tier-0 doc's earlier
// NewBroker(brokerID, n, inboxDepth, cost, persist, lifecycle) shape).
// A nil-slot in bundles yields a shard with nil resv / realiz — legal
// for Phase-0 machine tests that never invoke a handler body that would
// call reserve / realize.
//
// A nil store is legal in Phase 0; Phase-1 BringUp always supplies a
// real *Store.
//
// Panics if bundles is empty — a zero-shard broker cannot route (hash
// modulo zero is undefined). This is a construction-time invariant, not
// a runtime condition; a caller passing an empty slice is a programming
// error.
func NewBroker(brokerID BrokerID, inboxDepth int, bundles []*capstone.Bundle, store *Store) *Broker {
	if len(bundles) == 0 {
		panic("funnel: NewBroker requires at least one bundle (shardCount = len(bundles) must be > 0)")
	}
	builtBroker := &Broker{
		brokerID:   brokerID,
		shards:     make([]*shard, len(bundles)),
		shardCount: len(bundles),
		store:      store,
	}
	for shardIndex, bundle := range bundles {
		builtBroker.shards[shardIndex] = newShard(brokerID, inboxDepth, bundle)
	}
	slog.Info("funnel: NewBroker built",
		"broker_id", brokerID,
		"shard_count", builtBroker.shardCount,
		"inbox_depth", inboxDepth,
		"store_present", store != nil,
	)
	return builtBroker
}

// Start spawns one owner goroutine per shard; each enters run() and
// parks on its inbox. Ingress (route from any of the four traffic
// sources) must not begin until Start has returned.
func (broker *Broker) Start() {
	slog.Info("funnel: Broker.Start spawning shards",
		"broker_id", broker.brokerID,
		"shard_count", broker.shardCount,
	)
	for _, shardOwner := range broker.shards {
		broker.shardWG.Add(1)
		go func(spawnedShard *shard) {
			defer broker.shardWG.Done()
			spawnedShard.run()
		}(shardOwner)
	}
}

// Stop closes each shard's inbox and waits for every owner goroutine to
// drain and return. Ingress must ALREADY be stopped before Stop — a
// route() after close panics on send-to-closed-channel (mirrors the
// exchange's "stop punching before Stop()" rule, BROKER_TIER0 §Point-1).
//
// The WaitGroup wait is load-bearing: Phase-1 shutdown tears bundles
// down in strict order (owners exit → bundle.Close → store pool close),
// and a shard's final MarkSnapshotDirty must not race a closed bundle
// handle. Stop returning IS the signal that owners have exited.
func (broker *Broker) Stop() {
	slog.Info("funnel: Broker.Stop closing inboxes",
		"broker_id", broker.brokerID,
		"shard_count", broker.shardCount,
	)
	for _, shardOwner := range broker.shards {
		close(shardOwner.inbox)
	}
	broker.shardWG.Wait()
	slog.Info("funnel: Broker.Stop shards drained", "broker_id", broker.brokerID)
}

// route is the dumb ingress: hash the account, send to that shard's
// inbox. No branching, no lock, no shared state — the exact contention
// the funnel exists to kill would otherwise sit here (BROKER_TIER0
// §Point-3). Every command arrives with its routing key already filled
// in; obtaining the key is the CALLER's job, not route's.
//
// Unexported: external callers use the public request/response methods
// (PlaceOrder / CancelOrder / ...) or the push helpers (PushApplyResult
// / PushFireStop) below. In-package tests call route directly to probe
// the router without spinning up handlers.
func (broker *Broker) route(cmd Command) {
	shardIndex := hashAccount(cmd.Account(), broker.shardCount)
	broker.shards[shardIndex].inbox <- cmd
}

// ---- request/response entry points -----------------------------------
//
// Each fresh reply channel is created HERE, buffered size 1, and stapled
// to the command by the caller. Self-addressing (BROKER_TIER0 §8.2): the
// broker never decides which reply channel to use — the caller brings
// its own, on its own goroutine's stack; the shard sends the result to
// whatever ReplyTo arrived. A thousand concurrent requests → a thousand
// distinct channels, no registry, no lookup.
//
// The select is what makes "no hang on a wedged shard" true: the caller
// waits for the reply OR its ctx cancel, whichever fires first.

// PlaceOrder submits an order for the given account/session and blocks
// until the shard replies or ctx cancels. See Tier-1 for the handler
// body; in Phase 0 the reply is an empty PlaceResult.
func (broker *Broker) PlaceOrder(ctx context.Context, accountID AccountID, sessionID SessionID, order OrderSpec) (PlaceResult, error) {
	replyChannel := make(chan PlaceResult, 1)
	broker.route(PlaceOrder{AccountID: accountID, SessionID: sessionID, Order: order, ReplyTo: replyChannel})
	select {
	case reply := <-replyChannel:
		return reply, nil
	case <-ctx.Done():
		return PlaceResult{}, ErrBrokerTimeout
	}
}

// CancelOrder pulls an open order and blocks until the shard replies or
// ctx cancels.
func (broker *Broker) CancelOrder(ctx context.Context, accountID AccountID, orderID OrderID) (CancelResult, error) {
	replyChannel := make(chan CancelResult, 1)
	broker.route(CancelOrder{AccountID: accountID, OrderID: orderID, ReplyTo: replyChannel})
	select {
	case reply := <-replyChannel:
		return reply, nil
	case <-ctx.Done():
		return CancelResult{}, ErrBrokerTimeout
	}
}

// BankMove requests a bank ↔ trading transfer for the given account.
// Amount is strictly positive; direction is carried by dir (root value
// discipline). Body owned by Tier 3.
func (broker *Broker) BankMove(ctx context.Context, accountID AccountID, dir BankDir, amount int64) (BankResult, error) {
	replyChannel := make(chan BankResult, 1)
	broker.route(BankMove{AccountID: accountID, Dir: dir, Amount: amount, ReplyTo: replyChannel})
	select {
	case reply := <-replyChannel:
		return reply, nil
	case <-ctx.Done():
		return BankResult{}, ErrBrokerTimeout
	}
}

// ReadBalance requests a live balance snapshot for the given account.
// Copy-out semantics live in the handler (BROKER_TIER0 §Point-5 read
// rule); the returned value is a by-value copy, never a pointer into
// the owned Account.
func (broker *Broker) ReadBalance(ctx context.Context, accountID AccountID) (BalanceSnapshot, error) {
	replyChannel := make(chan BalanceSnapshot, 1)
	broker.route(ReadBalance{AccountID: accountID, ReplyTo: replyChannel})
	select {
	case reply := <-replyChannel:
		return reply, nil
	case <-ctx.Done():
		return BalanceSnapshot{}, ErrBrokerTimeout
	}
}

// ReadPositions requests a live positions snapshot for the given account.
// Same copy-out rule as ReadBalance.
func (broker *Broker) ReadPositions(ctx context.Context, accountID AccountID) (PositionsSnapshot, error) {
	replyChannel := make(chan PositionsSnapshot, 1)
	broker.route(ReadPositions{AccountID: accountID, ReplyTo: replyChannel})
	select {
	case reply := <-replyChannel:
		return reply, nil
	case <-ctx.Done():
		return PositionsSnapshot{}, ErrBrokerTimeout
	}
}

// ---- push entry points -----------------------------------------------
//
// Push callers do not block on a reply — the shard fully processes the
// command and returns nothing. The caller is responsible for having
// resolved the AccountID before calling; for ApplyResult this means the
// result-ingress goroutine parses the account out of the echoed
// OrderID ("<AccountID>:<seq>" shape, BROKER_TIER0 §Point-3) before
// building the ApplyResult.
//
// Backpressure: route may block on a full shard inbox — intentional
// backpressure per BROKER_TIER0 §Point-1 Decisions. Push callers accept
// this.

// PushApplyResult ingests a settlement outcome from the exchange for the
// given account. AccountID must already be resolved (from Result.OrderID)
// upstream of this call — the router does not parse it.
func (broker *Broker) PushApplyResult(accountID AccountID, result contract.EngineResult) {
	broker.route(ApplyResult{AccountID: accountID, Result: result})
}

// PushFireStop notifies the shard that a watched stop has fired.
func (broker *Broker) PushFireStop(accountID AccountID, stop StopRecord) {
	broker.route(FireStop{AccountID: accountID, Stop: stop})
}

// ---- deferred EventSink stand-in -------------------------------------

// nopEvents is the funnel's stand-in for reserve.EventSink — the event
// system is deferred (economy/reserve/CLAUDE.md §9); reserve wires the
// interface + concrete event types but nothing consumes them. capstone's
// own wiring uses an equivalent nopEvents{} in its bundle; the funnel
// mirrors it so the compute stack has a well-typed sink.
//
// Kept unexported (broker-internal). When a real event consumer lands,
// it replaces this at NewBroker wiring time.
type nopEvents struct{}

func (nopEvents) Emit(event reserve.Event) { _ = event }
