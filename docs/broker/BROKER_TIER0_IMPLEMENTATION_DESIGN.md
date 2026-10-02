Broker — Tier 0 Implementation Design (The Concurrency Funnel)

What this doc is. The complete implementation design of the broker's Tier‑0 layer: the sharded single‑writer funnel every money‑touching flow runs inside, the command model, the router, the serial loop, the owned state, the per‑shard L5 wiring, the reply mechanism, the read rule, and the co‑located session binding. Tier 0 is the machine; every later tier (order intake, settlement, stops, session lifecycle, reads) is a body that plugs into the seams defined here.

Grounding. Every fact here traces to one of three sources, and nothing is written from recollection:

BROKER_CONCURRENCY_AND_ROUTING.md — the locked Tier‑0 design doc (the funnel machine, command model, router, reply mechanism, read rule, binding‑carrying model).
The economy package source (account, reserve, cost, realize, persistence, capstone) — read from source for the per‑shard L5 wiring (Point 6).
Decisions locked in the design conversation that produced this doc (marked [decided here] where they extend the source).

Scope boundary (strictly Tier 0). In scope: the eight points below + the read rule + the guarantees. Out of scope and deliberately not referenced here: all handler bodies (order intake / punch construction / settlement outcomes / stops / bank moves / reads — Tier 1+), the session‑lifecycle mechanics that inject the binding (the session‑start/stop command flow — the lifecycle tier), and the exchange‑facing consumer goroutines that feed the router (a later tier). Where Tier 0 stops, this doc stops.

Status. Funnel machine, command model, router, serial loop, owned state, per‑shard L5 wiring, reply mechanism, read rule, and binding‑carrying model — LOCKED. Handler bodies are deliberately empty seams.

0. Why Tier 0 exists, and why it comes first

The L5 money hands (MoveCash, MovePosition, CreditPosition, RemovePosition, CreditCash, DebitCash) are pure and do check‑then‑mutate with no internal locking. That is only safe if exactly one goroutine ever touches a given Account at a time. The account package states this in its own contract: it assumes single‑writer and does not enforce it.

But the broker has four concurrent sources of work all landing on the same account:

Consumer actions — HTTP (human) and agent API calls: place, cancel, bank moves, reads.
Exchange results — EngineResult pushed in asynchronously: fills, rejections, cancel‑acks.
Stop fires — internally generated when a watched stop triggers.
Reads — must see a consistent snapshot, never a half‑applied mutation.

If any two ran in parallel against the same Account, their check‑then‑mutate sequences would interleave and corrupt the ledger. The funnel is the broker‑side mirror of the exchange's single‑writer‑per‑instrument rule — except the broker shards on account, because money is per‑account. Every later responsibility touches money, so every later responsibility depends on this machine existing first. Hence Tier 0.

1. The load‑bearing spine

Settled before design; recorded so nothing downstream re‑litigates them (BROKER_CONCURRENCY_AND_ROUTING.md §1).

One broker, one process. A single broker goroutine‑cluster in the same process as the exchanges, serving the whole participant population, fanning out to the N exchange handles L7 provides. Not one broker per exchange; not one per session.
The funnel = sharded single‑writer. A fixed pool of owner (shard) goroutines; each account is routed by hash(AccountID) % N to exactly one shard, which is the sole caller into the L5 hands for its accounts. Routing key is AccountID.
Broker is one entity; shards are internal. The exchange, UIs, and agents connect to the broker. The router hashes AccountID and hands to the right shard. N is a broker‑internal tuning dial with no external contract.
Owner tied to a live Account instance, not a human identity. The registry is keyed so a session dimension (SessionID+AccountID) can be added later without collision.
One live session per user at a time (canonical XOR on‑demand; no mid‑session switching). So per‑account and per‑account‑per‑session collapse into one today.
On‑demand seeding = check‑and‑snapshot. On‑demand start is gated by a real balance check against the live canonical account; the qualifying figure seeds an isolated on‑demand account (PersistOnClose=false); canonical is otherwise untouched.
2. The shape in one picture
        exchange EngineResult ─┐
        HTTP handler (human) ──┤
        agent API handler ─────┼──► Broker.route ──hash(AccountID)%N──► shard[k].inbox
        stop-fire ─────────────┘        (dumb: hash + channel send)          │
                                                                             ▼
                                                    shard[k] goroutine: serial loop
                                                    owns: map[AccountID]*ownedSession
                                                          per-shard reservation book + realizer
                                                          exchange-seam OrderTracks
                                                    sole caller into the L5 hands
                                                                   │
                                          reply on cmd.ReplyTo (if present) ──► caller unblocks
                                          punch InboundMessage ──► the session's bound exchange handle

Four traffic sources, one router, N owner goroutines, each the sole writer of its accounts. Request/response callers block on a per‑request reply channel; push callers don't.

Point 1 — The shard set

What it is. The fixed pool of N owner goroutines. Each account belongs to exactly one shard by hash(AccountID) % N; that shard is the sole goroutine that touches that account's state. Point 1 is the pool, the hashing, and the goroutine lifecycle — nothing more. (The inbox's element type Command is Point 2; the loop body is Point 4; the owned state is Points 5–6.)

What's expected / why. One guarantee: single‑writer‑per‑account by construction — one shard = one goroutine = one serial loop, so no two mutations on one account can interleave. Two properties the assignment must have: deterministic within a run (an account always hashes to the same shard, or its state would split) and well‑distributed (accounts spread evenly).

Cross‑layer interaction. Near‑pure broker plumbing — no calls into L5, the exchange, or L7. The only touch is the key type: the routing key is account.AccountID (a string alias in the economy source). [decided here] The broker aliases its AccountID to account.AccountID so the funnel key is exactly the type the L5 hands take — no conversion at the L5 seam.

Ingress / egress. Ingress: config values only — N and inbox depth. Egress: a running set of N idle owner goroutines, each parked on its inbox. The pool depends on nothing from the exchange or L7 to come up — an empty shard needs no exchange channel.

The implementation.

The hash — [decided here] FNV‑1a computed inline over the string, allocation‑free (the router runs on every command, so the hot path must not allocate). The source doc leaves hash() abstract; this is the implementation choice:

go
type AccountID = account.AccountID // = string, the economy type

// hashAccount maps an AccountID to a shard index in [0, n).
// FNV-1a inline over the string bytes: deterministic within a run, well-distributed, zero-alloc.
func hashAccount(aid AccountID, n int) int {
    const offset32, prime32 = uint32(2166136261), uint32(16777619)
    h := offset32
    for i := 0; i < len(aid); i++ {
        h ^= uint32(aid[i])
        h *= prime32
    }
    return int(h % uint32(n))
}

The pool and its lifecycle (shard fields shown here are Point‑1 only; the rest fill in Points 5–6):

go
// Start spawns one owner goroutine per shard. run()'s body is Point 4.
func (b *Broker) Start() {
    for _, s := range b.shards {
        go s.run()
    }
}

// Stop closes each inbox; the serial loop's `for range inbox` then exits and the goroutine
// returns. CONTRACT: ingress must already be stopped before Stop — a route() after close panics
// on send-to-closed-channel (mirrors the exchange's "stop punching before Stop()" rule).
func (b *Broker) Stop() {
    for _, s := range b.shards {
        close(s.inbox)
    }
}

Pseudo‑flow (bring‑up). NewBroker(n, inboxDepth, …) builds N shards with empty maps → Start() spawns N owner goroutines → each enters for cmd := range inbox and idles. The shards come up empty and long‑lived; accounts are loaded into them later (at session start, a lifecycle concern — out of scope here). Goroutines alive after bring‑up: N shard owners (+ the persistence drainer from Point 6).

Decisions & considerations.

N is a construction parameter / config dial, sizing deferred (spine item 3 / §10). No external contract; retunable without touching any caller.
Inbox depth is a construction parameter — value OPEN. Unbuffered (route blocks on a busy shard → natural backpressure) vs small buffer (absorbs bursts but hides backlog). Raised, not locked. This remains an open tuning decision.
Point 2 — The command model

What it is. How one unit of work is represented as it enters the funnel. Every account‑ touching job is a typed struct carrying its routing key and, if the caller needs an answer, a reply channel.

What's expected / why. Two locked decisions (BROKER_CONCURRENCY_AND_ROUTING.md §3):

Q1 — typed commands, not closures. Work is a PlaceOrder{…} struct, not a func(*Account). Keeps all account‑mutating logic inside the owner where it is auditable and consistent with L5's "the hands live in one place" spirit; a closure would scatter account logic across submitters.
Q4 — one inbox per shard, keyed on AccountID. A shard owns many accounts; all their commands share one inbox, tagged by account. Accepted tradeoff: two different accounts on the same shard are serialized against each other though they share no state.

The load‑bearing split (§3.2): request/response vs push isn't "channel or not" — it's whether the caller blocks for a reply. HTTP/agent callers block; the exchange settlement path pushes. That is what lets one funnel serve every interface uniformly.

The interface and the type aliases (§3):

go
type AccountID    = string
type SessionID    = string
type OrderID      = string
type ExchangeID   = string

// Command is the funnel's unit of work. Account() returns the routing key; the router reads
// ONLY this. Every command implements it.
type Command interface {
    Account() AccountID
}

"Every command implements it" — Go interfaces are implicit and methods attach to a specific type, so each concrete command carries its own trivial Account() returning its stored AccountID. That single shared method is what lets route() treat all command types uniformly.

Request/response commands (carry a ReplyTo channel, buffered size 1 — the mechanism is Point 7):

go
type PlaceOrder struct {
    AccountID AccountID
    SessionID SessionID
    Order     OrderSpec        // side, instrument, qty, type, limit price — body owned by Tier 1
    ReplyTo   chan PlaceResult // buffered(1)
}
type CancelOrder struct {
    AccountID AccountID
    OrderID   OrderID
    ReplyTo   chan CancelResult // buffered(1)
}
type BankMove struct {          // bank <-> available, Tier 3
    AccountID AccountID
    Dir       BankDir           // PullToTrading | PushToBank
    Amount    int64
    ReplyTo   chan BankResult   // buffered(1)
}
type ReadBalance struct {       // live cash read, Tier 4
    AccountID AccountID
    ReplyTo   chan BalanceSnapshot // buffered(1)
}
type ReadPositions struct {     // live positions + marks, Tier 4
    AccountID AccountID
    ReplyTo   chan PositionsSnapshot // buffered(1)
}

Push commands (no reply — nobody waits):

go
type ApplyResult struct {       // settle an exchange EngineResult, Tier 1
    AccountID AccountID
    Result    EngineResult      // the whole result, wrapped with the resolved account
}
type FireStop struct {          // a stop fired, Tier 2
    AccountID AccountID
    Stop      StopRecord
}

Each implements the interface trivially (func (c PlaceOrder) Account() AccountID { return c.AccountID }, one per command).

Cross‑layer touch. Most command payloads are broker‑owned. Two seam facts: AccountID is account.AccountID; and ApplyResult.Result is contract.EngineResult, the exchange→broker wire type — the one command whose payload is a foreign‑layer type. The EngineResult echoes OrderID + BrokerID but no AccountID — which is why the result path must resolve the account before it can build an ApplyResult (see Point 3).

Decisions & considerations.

Naming reconciliation (noted, not resolved here): §3 names the money command BankMove; a later doc's category list calls it Transfer. Same command; pick one when Tier 3 is built.
Point 3 — The router

What it is. The broker's ingress: the single dumb step that takes a command and gets it to the shard that owns its account. It is also the encapsulation boundary — Broker is the one externally visible entity; shards and the router are internal (BROKER_CONCURRENCY_AND_ROUTING.md §8/§9).

What's expected / why.

route() is deliberately dumb (B1): hash the key, send to that inbox. No logic, no lock. Any branching or shared state here would sit on the single hottest path read by every goroutine — the exact contention the funnel exists to kill.
Broker is one entity. The external contract is a set of Broker methods; N, the hash, the shard slice are invisible and retunable.
Every command arrives with its routing key already filled in. Obtaining the key is the caller's job, not the router's — which is what keeps route() uniform across all four sources.

The entity and the ingress (§8):

go
// Broker — the ONE externally-visible entity. Shards + router are internal.
type Broker struct {
    brokerID BrokerID
    shards   []*shard
    n        int
    // + the shared L5 infra handles wired at construction (Point 6)
}

// route — the dumb ingress: hash the account, send to that shard's inbox.
func (b *Broker) route(cmd Command) {
    k := hashAccount(cmd.Account(), b.n)
    b.shards[k].inbox <- cmd
}

The four traffic sources — how each obtains its key before routing. Three are trivial; one is not:

HTTP handler (human) — already knows the AccountID (the acting session's account).
Agent API handler — same.
Stop‑watcher — the StopRecord carries its AccountID.
Exchange result ingress — the only hard one. An inbound EngineResult echoes OrderID + BrokerID but no AccountID (§ appendix 4). So the account must be resolved from OrderID before the ApplyResult can be built and routed. route() stays dumb; the resolution happens upstream, in the result‑ingress caller.

How results resolve the account — [decided here]. The Tier‑0 source (appendix 4) records that result ingress resolves the owning shard via an OrderID → AccountID map (from the OrderTrack or the L5 ReservationRecord, both keyed by OrderID). That map is a cross‑shard lookup — the OrderTrack lives inside a shard, but you need the account to know which shard — which puts shared mutable state on the results hot path. Decision: mint OrderID with the account encoded in it — OrderID = "<AccountID>:<seq>" — so the result‑ingress caller parses the account straight out of the echoed OrderID with no shared index and no lookup. The exchange treats OrderID as an opaque key and echoes it verbatim, so encoding does not touch the exchange contract. This is the OrderID syntax decision; the actual minting of OrderID happens at order intake (a later tier), which must honor this "<AccountID>:<seq>" shape.

Cross‑layer touch. route() itself touches no other layer (hash + channel send). The Broker public methods are the layer‑above's entry point (HTTP routes, agent API call these, never route() directly). The result path's key‑resolution reads the account out of the broker‑minted OrderID (per the decision above).

Pseudo‑flow.

Request/response source (HTTP / agent): knows AccountID → Broker.<Method>(...) → route() → shard.inbox
Stop source:                            StopRecord.AccountID → route(FireStop{...}) → shard.inbox
Result ingress:  EngineResult{OrderID,BrokerID}  ──parse AccountID from OrderID──►
                 route(ApplyResult{AccountID, Result}) → shard.inbox

By the time anything reaches route(), the AccountID is always already present — three sources have it for free; the result source parses it from OrderID. The router never branches on source.

Point 4 — The serial loop & dispatch

What it is. The body of the owner goroutine each shard runs — the go s.run() from Point 1 made concrete. It pulls one command at a time, fully handles it, and only then pulls the next. That "one at a time, fully finished before the next" is the mechanism that makes single‑writer true (BROKER_CONCURRENCY_AND_ROUTING.md §4).

What's expected / why. Two tiny parts:

The loop (run) — for cmd := range inbox { dispatch(cmd) }. Ranging blocks when empty (no busy‑wait) and exits cleanly when the inbox is closed at shutdown. No locks, no per‑command goroutines. The serialness is the feature.
The dispatch (dispatch) — a type‑switch to the matching handler. Request/response cases send the result on the command's reply channel; push cases run and return nothing.

The two methods (§4):

go
// run — the serial loop. One command at a time, fully applied before the next is pulled.
func (s *shard) run() {
    for cmd := range s.inbox {
        s.dispatch(cmd)
    }
}

// dispatch — type-switch to the matching handler. Request/response cases send on c.ReplyTo;
// push cases return nothing. Handler BODIES are later tiers.
func (s *shard) dispatch(cmd Command) {
    switch c := cmd.(type) {
    case PlaceOrder:    c.ReplyTo <- s.handlePlaceOrder(c)     // Tier 1
    case CancelOrder:   c.ReplyTo <- s.handleCancel(c)         // Tier 1
    case BankMove:      c.ReplyTo <- s.handleBankMove(c)       // Tier 3
    case ReadBalance:   c.ReplyTo <- s.handleReadBalance(c)    // Tier 4
    case ReadPositions: c.ReplyTo <- s.handleReadPositions(c)  // Tier 4
    case ApplyResult:   s.handleApplyResult(c)                 // Tier 1 (push)
    case FireStop:      s.handleFireStop(c)                    // Tier 2 (push)
    default:
        panic(fmt.Sprintf("shard: unknown command %T", cmd))
    }
}

The handle* bodies are the seams later tiers fill; out of scope here.

Cross‑layer touch. None in Point 4 itself. The loop and switch are pure broker plumbing; all layer interactions live inside the handler bodies.

Pseudo‑flow (the guarantee this buys). Two commands for the same account can sit in the inbox together, but they are applied strictly one after the other on one goroutine — so the check‑then‑mutate sequences inside the L5 hands can never interleave. That is the whole point of Tier 0, made real by this loop.

Decisions & considerations.

The panic on an unknown command is deliberate — an unroutable command type is a programming error (a command added without a dispatch case), not a runtime condition to recover from.
Point 5 — The shard's owned state (ownedSession, OrderTrack)

What it is. The two things a shard owns and is the sole toucher of (besides its L5 services, Point 6): ownedSession (everything about one live session) and OrderTrack (the broker's thin per‑order record for the exchange seam only).

Why this shape. The governing discipline (BROKER_CONCURRENCY_AND_ROUTING.md appendix 1, correcting an earlier wrong WorkingOrder idea): L5 is part of the broker, so all reservation money state stays in L5's ReservationBook and is never duplicated. The broker adds only the state L5 has no reason to know — the exchange‑seam facts. Reserved cash/units, remaining qty, original qty, ever‑filled, brokerage‑charged all live in L5's ReservationRecord and are read from L5; they are never mirrored on OrderTrack.

ownedSession — the meeting point of three layers. Source §4 defines {account, binding}; appendix 3 adds participant; and the economy source (lifecycle.LoadedAccount) refines the account field:

go
// ownedSession — one live session. The shard owns these outright; nobody else reads them.
type ownedSession struct {
    account     LoadedAccount    // [refined here] holds the L5 LoadedAccount (Account *Account +
                                 // OwnerType + OwnerID), not a bare *Account — save/discard need it
    binding     SessionBinding   // L7's per-session exchange wiring (Point 8)
    participant ParticipantID    // self-match id, distinct from BrokerID; stamped on every punch
}

The account field is the live in‑memory L5 account — the thing single‑writer exists to protect. [refined here] The Tier‑0 source shows account *Account; the economy lifecycle source shows SessionSave/SessionDiscard take a LoadedAccount{Account *account.Account; OwnerType; OwnerID}, so ownedSession holds LoadedAccount and code that needs the raw account uses .account.Account.

OrderTrack — the thin exchange‑seam record (§4). Tier‑0 shape:

go
// OrderTrack — ONLY exchange-seam facts L5 doesn't carry. NO reserved amounts, NO remaining/orig
// qty, NO ever-filled/brokerage flags — all of that is L5's ReservationRecord, read from L5.
type OrderTrack struct {
    OrderID        OrderID
    AccountID      AccountID     // which of this shard's accounts owns the order
    Instrument     InstrumentID  // for routing a cancel to the right instrument
    Status         OrderStatus   // Working | PartiallyFilled | Filled | Cancelled | Rejected
    CancelInFlight bool          // a cancel was punched, awaiting the exchange's ack
}

Note: a later tier (order intake) extends OrderTrack with an IsMarketBuy flag (for the market‑buy buffer true‑up at settlement). That field is not Tier‑0; the five fields above are the Tier‑0 shape.

What each is for. ownedSession = what the shard needs to act for a live user: the account (every money handler mutates it), the binding (where to punch — Point 8), the participant (stamped on the outgoing order). OrderTrack = what the shard needs to interpret the exchange's replies about an order it already sent: match a returning result by OrderID, route a cancel (needs the instrument), know a cancel is in flight, and answer "list my open orders" without cracking open L5's reservation book.

Note on the result path. OrderTrack.AccountID is for the shard's own bookkeeping (a shard owns many accounts). It is a per‑shard map, which is exactly why the cross‑shard result ingress cannot use it to resolve the account — and why the OrderID = "<AccountID>:<seq>" encoding (Point 3) is used instead.

Ingress / egress (lifetime). ownedSession: created at session load, read/mutated throughout, removed at session end. OrderTrack: created at order punch, mutated at settlement (status transitions; CancelInFlight set on cancel‑punch, cleared on ack), removed at a terminal state. Both maps are touched only by the owning shard's goroutine — no locks.

Read rule (live vs durable)

Part of Tier 0 (BROKER_CONCURRENCY_AND_ROUTING.md §5). Two kinds of read, resolved differently.

The rule.

Live session reads (balance, positions, open orders, live marks) → go through the owner, which returns a by‑value copy of the in‑memory truth. Never Redis/Postgres.
Cross‑session durable reads (closed‑trade history, realized‑P&L records, cost breakdowns) → read Postgres directly, not the owner — that data is not in the live account at all.

Why live reads must be in‑memory, not Redis.

In‑memory is the freshest copy, not the stalest. Redis is mirrored on a cadence (async, via MarkSnapshotDirty), so it lags the in‑memory account. A read from Redis could return an old balance (a reservation not yet mirrored) — showing money already committed.
Serving reads from the owner kills the race by construction. A read serialized against every write can never catch a half‑applied mutation.
Writes are async to keep the hot path unblocked; coupling read correctness to mirror latency would defeat that.

Same principle both ways: read data from where it actually lives.

The copy‑out contract. A read handler copies fields out and returns a value, never a pointer into the owned Account. Example (handleReadBalance, §5.3): reads sess.account's cash split, returns a BalanceSnapshot value; Available is signed and returned raw, never clamped.

Point 6 — Per‑shard L5 wiring

What it is. The handlers (Point 4) don't do money work themselves — they drive L5. Point 6 gives each shard the L5 services to drive. Verified against the economy package source.

The core decision — which services are per‑shard, which shared. Getting this wrong breaks single‑writer. The rule: anything holding per‑account mutable state must be per‑shard; anything stateless or I/O‑funnel‑shaped is shared.

Service	Per‑shard / shared	Source evidence
Reservation service (reserve, L5.2)	Per‑shard	Owns book *ReservationBook, created internally per NewReservationService call. No mutex.
Realizer (realize, L5.4)	Per‑shard	Owns book map[accKey]*RealizationAccumulator, with the in‑code contract "Single‑writer assumed (the broker funnel enforces it later); no mutex, no goroutine."
Cost module (cost, L5.3)	Shared	Pure arithmetic; holds no per‑order state (the brokerage fire‑once guard is on the reservation record). Safe read‑only across shards.
Persistence (persistence)	Shared	One Postgres pool + one Redis mirror + one write‑behind drainer goroutine (Start launches it). Shards call MarkSnapshotDirty/EnqueueClosedTrade into the shared buffer.

Forced, not chosen. Grepping reserve and realize from source shows zero mutexes and zero goroutines in either package; both hold plain Go maps and explicitly assume one writer. Sharing either across shards → concurrent map writes → data race. One reserve + one realizer per shard is mandatory, and the shard's single goroutine is exactly the "single‑writer the broker funnel enforces" their comments count on.

The AccountLedger seam. Reserve and realizer reach the account through an AccountLedger interface — a single method, from source:

go
// reserve.AccountLedger (structurally identical to realize.AccountLedger):
type AccountLedger interface { Get(accountID account.AccountID) *account.Account }

The shard is the ledger — it implements Get over its own sessions map, returning the account it owns. This is why a per‑shard reservation service works: its ledger only ever resolves accounts this shard owns.

go
// the shard IS the AccountLedger. Reads sessions lazily (empty at construction, filled at
// session load), returns the inner *Account out of the held LoadedAccount.
func (s *shard) Get(aid AccountID) *account.Account {
    return s.sessions[aid].account.Account
}

Because the ledger is a live view over the shard's sessions map (a pointer, not a copy), any account added to the map later is visible to the next Get immediately — which is why it is safe to build reserve/realizer at bring‑up when sessions is still empty. This lazy‑read is only safe because of single‑writer: only the shard's own goroutine ever writes sessions and calls Get.

The wiring — shared once, per‑shard N times.

Built once (shared): the cost module and the persistence component (+ its Start, which launches the drainer). Plus the lifecycle service (used by later tiers).
Built N times (per‑shard), inside NewBroker: for each shard — its ledger (the shard itself), then its realizer, then its reservation service (which takes the realizer + shared cost + shared persistence). Realizer is built before reserve, because reserve takes the realizer as its RealizerHandoff.

Constructor signatures (from source): NewReservationService(ledger, costs, realiz, events, persist) and NewRealizer(ledger, persist, costs). The same shared *persistence.Component goes to both (reserve uses the narrow MarkSnapshotDirty‑only seam; realize uses the full seam incl. EnqueueClosedTrade — structural typing). The deferred EventSink is passed a no‑op (nopEvents{} in the source's own wiring).

Reuse of capstone — [decided here]. capstone.Wire returns a Bundle{Pool, Redis, Persist, Cost, Ledger, Realizer, Reserve, Lifecycle}. The broker calls capstone.Wire once and uses only {Pool, Redis, Persist, Cost, Lifecycle} (the shared infra), and ignores {Ledger, Reserve, Realizer}. Reason, confirmed from source: capstone's ledger is a documented stub — the in‑code comment states memoryLedger is "a stand‑in for the broker funnel's owner‑side registry, which does not exist yet… NOT a package. NOT reusable. Lives ONLY under economy/capstone/ until the broker funnel lands and owns the real registry." Its Reserve / Realizer are bound to that stub, and there is exactly one of each (the broker needs N, per‑shard). So the broker reuses the real shared infra and builds the per‑shard reserve/realizer itself, each bound to its shard as the ledger. capstone remains the reference for the shared wiring order (config.Load → pool → redis → persistence.NewComponent(+Start) → cost).

What the handlers then call (bodies are later tiers): s.resv.ReserveIfAvailable (Gate‑4 check‑and‑reserve), s.resv.Convert (per‑fill; its sell branch calls s.realiz.OnSellFill internally), s.resv.Release / s.resv.ReleaseExcess, s.realiz.SettleSessionEnd (teardown auto‑square). All on the shard's one goroutine, on its own reservation book — single‑writer intact.

Point 7 — The reply mechanism

What it is. How a synchronous caller hands work to a shard and gets the answer back without ever blocking the shard, and without hanging forever on failure (BROKER_CONCURRENCY_AND_ROUTING.md §3.1, §8.1, §8.2).

What's expected / why. The asymmetry (§3.2): the caller blocks; the shard never does. Three locked pieces:

Reply channel is per‑request, buffered size 1. The caller makes a fresh chan Result per command. Buffered(1) is critical: the shard's send never blocks even if the caller already walked away (timed out) — the value drops into the buffer and the shard moves on.
The caller selects on reply‑vs‑timeout — select { case r := <-reply … case <-ctx.Done(): timeout }. It waits for its answer or gives up on its own deadline; it never hangs.
A timeout is the caller's problem, not the shard's. If the caller times out, the shard still processes the command (already in the inbox) and still fires the result into the buffered channel, which nobody reads and which is garbage‑collected. No leak, no block, no corruption — the work still happened correctly; the caller just stopped waiting.

The entry point (with the timeout) (§8.1):

go
var ErrBrokerTimeout = errors.New("broker: request timed out") // maps to HTTP 503

func (b *Broker) PlaceOrder(ctx context.Context, aid AccountID, sid SessionID, o OrderSpec) (PlaceResult, error) {
    reply := make(chan PlaceResult, 1)          // fresh, buffered(1) — shard never blocks sending
    b.route(PlaceOrder{AccountID: aid, SessionID: sid, Order: o, ReplyTo: reply})
    select {
    case r := <-reply:
        return r, nil
    case <-ctx.Done():
        return PlaceResult{}, ErrBrokerTimeout   // shard still runs later; result GC'd
    }
}

The same shape wraps CancelOrder, BankMove, ReadBalance, ReadPositions. Push callers skip the helper and route a no‑reply command directly.

Self‑addressing (§8.2). The broker never decides which reply channel to use. The caller creates a fresh channel per request, staples it to the command, and blocks on it. The channel lives on that handler goroutine's stack and travels inside the command to the shard; the shard sends the result to cmd.ReplyTo, whatever arrived. A thousand concurrent requests → a thousand distinct channels, each on its own handler's stack. No registry, no lookup — the return path is a value the caller brought.

How the HTTP leg connects. In Go's standard net/http server, each request runs on its own goroutine. The HTTP handler goroutine calls Broker.PlaceOrder(r.Context(), …) and blocks that same goroutine on the select. When the reply lands, the method returns, and the handler — still on the same goroutine, still holding the live connection — writes the HTTP response. The channel never "reaches HTTP"; the round‑trip happens inside the handler goroutine's blocking call. r.Context() is the caller's deadline: client disconnect or request timeout cancels it → the select hits ctx.Done() → 503. (Agents call the same HTTP surface — separate process, one request goroutine per call — identical mechanism.)

Pseudo‑flow.

HAPPY:   handler: reply=chan(1); route(cmd{ReplyTo:reply}); select…
         shard:   pulls cmd → handle → reply <- result   (send returns instantly)
         handler: select gets result → writes HTTP 200
TIMEOUT: handler: ctx.Done() fires first → return 503, walks away
         shard:   (later) pulls cmd → handle → reply <- result → dropped in buffer, unread → GC'd

Decisions & considerations.

A command runs to completion even after the caller times out. So a PlaceOrder can be reserved and punched after the caller got a 503 — correct and intentional (the truth is whatever the shard did). The caller reconciles via the pull‑based reads; a 503 is not "nothing happened."
Point 8 — The session binding

What it is. When a shard punches an order it must know which exchange goroutine's inbound channel to send to. That is per‑session (canonical vs on‑demand). SessionBinding is that punch handle, held next to the account on ownedSession — the outbound half of the exchange seam (BROKER_CONCURRENCY_AND_ROUTING.md §7).

What's expected / why. The locked decision (§7, "Option A"): the handle is co‑located session state, read as a plain field at punch time — no lookup, no lock. Reasons: the punch hot path must be contention‑free (a field read on already‑owned state beats a shared map every shard hits); its lifetime equals the session's (loads and unloads with the account); single‑writer stays intact (only the owner reads it). The payoff: punching is mode‑blind — the shard sends to sess.binding.ExchangeInbound, whatever channel was bound; canonical vs on‑demand lives entirely in what was injected, never in punch logic.

The struct (§7, with appendix 2 confirming the channel element type):

go
// SessionBinding — the per-session exchange wiring, held next to the Account. Read ONLY by the
// owner, ONLY at punch time — a field access, no lookup, no lock. Replaced each session.
type SessionBinding struct {
    SessionID       SessionID
    ExchangeID      ExchangeID              // which exchange instance — identity / logging
    ExchangeInbound chan<- InboundMessage   // the ONE inbound channel this session punches to
}

ExchangeInbound is the load‑bearing field — write‑only (chan<-), the exchange's inbound edge. InboundMessage is the confirmed wrapper (Kind + BrokerID + Instrument + Submit *FundedOrder | Cancel *CancelRequest), not a bare FundedOrder (appendix 2).

Cross‑layer touch. It is the broker→exchange punch seam, per session, write‑only — the shard can only send, never receive (results come back a different way, not on this channel). It completes the punch trio: the channel (this binding), the participant self‑match id (on ownedSession), and the broker id (on the shard).

Pseudo‑flow (punch, mode‑blind) (§7):

after Gate-4 reserve succeeds and OrderID/BrokerID are assigned:
    sess := s.sessions[c.AccountID]
    sess.binding.ExchangeInbound <- InboundMessage{ BrokerID: s.brokerID, OrderID: oid,
                                                     Instrument: c.Order.Instrument, /* Submit: &FundedOrder{...} */ }
    // canonical vs on-demand? invisible here — whichever channel was bound is used.

Boundary (Tier‑0 stops here). Point 8 locks the struct and the co‑located carrying model only. How the binding is injected — who hands the channel to the broker and how a session is loaded onto the owning shard — is a later‑tier (lifecycle) concern and is out of scope for this doc (mirrors §10's own boundary statement). The full punch construction (building the complete InboundMessage/FundedOrder) is the order‑intake tier.

What Tier 0 guarantees

(BROKER_CONCURRENCY_AND_ROUTING.md §9.)

Single‑writer per account — one shard goroutine, serial loop, sole caller into the L5 hands.
Broker is one entity — outside sees Broker methods; shards + router are internal.
Uniform across interfaces — HTTP handler, agent handler, result ingress, stop‑watcher are all in‑process callers building commands; request/response ones block on a reply, push ones don't. No channel crosses the network.
Reads correct by construction — a live read is a command on the owner returning a by‑value copy of the freshest (in‑memory) truth; durable cross‑session reads hit Postgres directly.
Owner tied to the live account instance — the shard owns map[AccountID]*ownedSession; a session dimension keys in later without touching this structure.
Mode‑blind punching — canonical vs on‑demand is absorbed entirely into the injected SessionBinding.
No hang on a wedged shard — the reply block is context‑bounded.
Decisions & considerations captured (this design pass)
AccountID aliases account.AccountID — the funnel key is the L5 type; no conversion at the L5 seam.
Hash = FNV‑1a inline, zero‑alloc — the router runs on every command; the hot path must not allocate. (Source leaves hash() abstract; this is the implementation choice.)
N and inbox depth are construction parameters. N: a tuning dial, sizing deferred. Inbox depth: OPEN — unbuffered (backpressure) vs small buffer (burst absorption) not yet locked.
Reply channel: per‑request, buffered(1) — the shard never blocks on the send, even post‑timeout.
Per‑shard vs shared L5 (forced by source): reserve + realizer per‑shard (lock‑free, single‑writer‑assumed); cost + persistence shared.
capstone.Wire reused for shared infra only (Pool/Redis/Persist/Cost/Lifecycle); its stub Ledger and stub‑bound Reserve/Realizer are ignored; per‑shard reserve/realizer are broker‑built.
OrderID syntax = "<AccountID>:<seq>" — encodes the account so result ingress parses it out of the echoed OrderID, avoiding a cross‑shard OrderID→AccountID map. (Resolves the Tier‑0 result‑ingress key‑resolution; the actual minting is a later tier and must honor this shape.)
ownedSession.account holds LoadedAccount (not bare *Account) — save/discard need it. (Refines the Tier‑0 source's *Account, from the economy lifecycle source.)
OrderTrack is thin — exchange‑seam facts only; all reservation money state is read from L5, never duplicated.
Constraints handed to later tiers (emerged during Tier 0)
OrderID minting must produce "<AccountID>:<seq>". Result ingress depends on parsing the account from the OrderID; the tier that mints OrderID must honor this shape.
L5 reservation service is write‑only — needs read accessors. From source, the ReservationService exposes only the four write methods (ReserveIfAvailable/Convert/ Release/ReleaseExcess) + SetLogger; the broker needs to read reservation facts (RemainingQty/IsComplete for settlement completion; a record view for the open‑orders read). This is an L5‑side change, used by later tiers.
Inbox depth value — still to be set (see Decisions).
Deferred / out of scope (the Tier‑0 boundary, made explicit)

(BROKER_CONCURRENCY_AND_ROUTING.md §10.)

Handler bodies (handlePlaceOrder, handleCancel, handleApplyResult, handleBankMove, handleReadBalance, handleReadPositions, handleFireStop) — later tiers. Tier 0 specifies only the machine they run inside.
Missing‑account / not‑yet‑loaded handling — a read or action for an account not in s.sessions is a lifecycle concern (accounts load in at session start).
Injection mechanics of SessionBinding — how the binding reaches the broker and how a session loads onto the owning shard — a lifecycle concern. The carrying model (co‑located) is locked here.
The exchange‑facing consumer goroutines that drain the exchange's outbound streams and feed the router — a later tier. Tier 0 defines only the router's side (a result arrives with the account already resolvable from OrderID, and is routed like any command).
Async fill‑notification surface to UIs/agents — a separate outbound mechanism from the request/response reply channel; designed with the interfaces.
Shard count N tuning — an infra/perf question, not a contract.
Concurrent live sessions per user — forbidden today (spine item 5).