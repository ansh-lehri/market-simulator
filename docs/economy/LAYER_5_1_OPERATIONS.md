# Layer 5.1 — Account State Operations (Reads & Writes)

**Status:** Write primitives locked. Read primitives locked (14, 15, 16). `drip_tap` and
crash-recovery load are OPEN (flagged at the end). This document is the complete, grounded
specification of every state operation on 5.1 discussed and agreed — nothing cut.

**Scope reminder — the two tiers.** Everything in this document is **Tier 1: the hands.** These are
dumb, single-responsibility, in-process functions that either mutate or read one account's state.
They hold no policy and make no decisions. The **Tier 2 brain** (`reserve_if_available`, `convert`,
`release`, `release_excess`, cost-application, realization, debt-priority) — the multi-step flows that
*decide what to do* and then *call these hands in sequence* — is specified elsewhere. A hand is never
an HTTP endpoint; HTTP-ness attaches to Tier-2 flows that face remote callers.

---

## 0. Foundations

### 0.1 The sole-writer principle

5.1 is the **only** code path in the system allowed to write money truth (cash balances, positions,
costs, realized P&L). The engine (Layer 1) is the only writer of market truth (the order book, fills).
They meet at exactly one seam: the engine's Admission Gate 4 calls the Tier-2 `reserve_if_available`,
which in turn drives these 5.1 hands. Within 5.1, these mutation primitives are the *only* functions
that touch the bucket fields — everything else reads.

### 0.2 The hands-vs-brain split

- **Hands (this doc):** move coins/units between named buckets, mint them in, burn them out, or read
  a field. Each enforces exactly one local guard. No policy, no cross-layer knowledge, no I/O on the
  hot path.
- **Brain (Tier 2):** reads state, decides, then calls the hands in the right order, emits events,
  and hands off to other layers (e.g. 5.4 realization).

The reason this split is load-bearing: keeping the hands pure and I/O-free is what makes the whole
economy **reproducible** — replaying the same ordered sequence of hand calls from the same starting
state reproduces the exact ledger, every time, because no hand reads a clock, a random source, or an
external store.

### 0.3 Value discipline

- **All money is integer coins (`int64`). All prices are integer ticks (`int64`).** Never float —
  float money is a bug (matches the engine's integer-tick discipline).
- **All primitive `amount`/`units` arguments are strictly positive.** Direction is carried by *which
  primitive* and *which buckets* (`from`/`to`), never by the sign of the argument. A non-positive
  argument is always a caller bug and is rejected.
- **Signedness lives in balances, not arguments.** `trading_cash` (durable) and the live `Available`
  bucket may be **negative** (Option 4 / Option B — see §1.6). P&L numbers may be negative. But those
  negatives are *results* the brain computes and reports; they never enter a hand as a negative
  argument.

### 0.4 The account state model

One account's state, in memory during a live session:

```go
// InstrumentID and the id types are plain strings in the prototype
// (FKs wire up when instrument config gets a durable store).
type AccountID    = string
type InstrumentID = string

// Account is the in-memory working state for ONE participant during ONE live session.
// It is loaded once at session start (from Postgres) and is the authoritative,
// current-to-the-tick copy for the whole session. It is mirrored asynchronously to the
// Redis snapshot for crash recovery, and (canonical only) its durable figure is written
// back to Postgres at session end. No hand in this document performs any of those writes.
type Account struct {
    ID       AccountID
    Cash     Cash
    Positions map[InstrumentID]*Position

    // write-back policy, set at load time by session type (see load_account, §2.3):
    //   canonical  -> true  (durable figure written to Postgres at session end)
    //   on-demand  -> false (isolated copy, discarded at close, never written back)
    PersistOnClose bool
}

// Cash is the live trading-account cash split PLUS the live bank copy.
// This split (available/reserved/settled) exists ONLY in memory during a session and in
// its Redis mirror. Postgres has NO split — it holds one signed `trading_cash` figure that
// this split reconciles to (persistence §3.1, line 128).
type Cash struct {
    Available int64 // free trading-account cash, reservable against orders. MAY be negative (Option B).
    Reserved  int64 // cash locked behind resting buy orders. >= 0.
    Settled   int64 // realized/spent trading-account cash this session. >= 0.
    Bank      int64 // live copy of the bank/buffer pool. NOT trading-visible; >= 0.
}

// Position is the live per-instrument holding. Session-scoped: born empty, must end flat
// (auto-squared at close). NEVER persisted to Postgres — Redis snapshot only.
type Position struct {
    Available int64 // units held, free to sell. >= 0 (no primitive drives it negative).
    Reserved  int64 // units locked behind a resting sell. >= 0.
    OpenLots  []Lot // FIFO queue in ARRIVAL ORDER: append at tail, consume from head.
}

// Lot is one priced batch of currently-held units — the cost-basis record.
// One structure serving three needs (persistence B8/B10/§4.1): the position itself,
// `position_age` (from EntryAt), and 5.4's FIFO lot-matching. No duplication.
type Lot struct {
    EntryPrice   int64     // integer ticks — the MAKER price this fill executed at (engine invariant §6.4)
    EntryAt      time.Time // when this fill happened — sole source of position_age (L5 field, not on agent record)
    RemainingQty int64     // units of THIS lot not yet sold; starts == fill qty; decremented by RemovePosition
}
```

### 0.5 The bucket enum

```go
type Bucket int

const (
    Available Bucket = iota // free trading cash
    Reserved                // locked behind resting orders
    Settled                 // realized/spent this session
    Bank                    // buffer pool (not trading-visible)
)

type PosBucket int

const (
    PosAvailable PosBucket = iota
    PosReserved
    // NO PosSettled — positions are ephemeral (auto-squared at close); a "settled position"
    // is meaningless (economy §3.1).
)
```

### 0.6 Coupling invariants (asserted, not assumed)

Two structural invariants hold across all operations and are asserted in the primitives that could
violate them. A violation is a **programming error**, surfaced by a dev-fatal panic — never swallowed.

1. **Cash conservation (transfers only):** a pure bucket-to-bucket `MoveCash` leaves the sum of all
   cash buckets unchanged. Asserted in `MoveCash`. (Mint/burn — `CreditCash`/`DebitCash` — break this
   on purpose and do not assert it.)

2. **Position–lot coupling:** for every instrument,
   `Position.Available + Position.Reserved == sum(OpenLots[i].RemainingQty)`.
   Every held unit is accounted for in exactly one lot. Preserved by `CreditPosition` (adds to both
   sides equally) and `RemovePosition` (removes from both sides equally); untouched by `MovePosition`
   (shuffles between buckets, total unchanged). Asserted in `RemovePosition`.

---

## 1. WRITE PRIMITIVES (the mutations)

There are six locked write primitives plus one held (`drip_tap`). They fall into two shapes:

- **Transfers** (conserving, between two buckets of the same account): `MoveCash`, `MovePosition`.
- **Mint / burn** (break conservation on purpose — value enters from or leaves to outside the
  account): `CreditCash`, `DebitCash`, `CreditPosition`, `RemovePosition`.

The transfer-vs-mint/burn split is deliberate: folding mint/burn into the transfer primitives would
destroy the conservation invariant that makes the transfers verifiable. The economy doc names these as
distinct commands (§3.3); this honors that.

---

### 1.1 `MoveCash` — cash transfer between buckets

**What it is.** The single cash-transfer primitive. Moves a positive integer amount of coins between
two buckets of one already-loaded account. Holds no policy; the caller decides what to move and why.
The only cash primitive that conserves total cash. In-memory only, zero I/O.

**The moves it serves (all conserving):**

| Caller (Tier 2)             | from → to             | Trigger                                   |
|-----------------------------|-----------------------|-------------------------------------------|
| `reserve_if_available` buy  | Available → Reserved  | buy order admitted at Gate 4              |
| `convert` buy fill          | Reserved → Settled    | each buy fill's principal debit           |
| `release` buy terminal      | Reserved → Available  | cancel / expire / IOC-FOK remainder       |
| `release_excess` market buy | Reserved → Available  | unused slippage buffer returned           |
| bank-refill (user action)   | Available → Bank      | user banks profit                         |
| bank-pull (user action)     | Bank → Available      | user pulls buffer into trading account    |

Sell proceeds are **not** here (money enters from outside → `CreditCash`). Cost debits are **not**
here (money leaves the sim → `DebitCash`). Drip is **not** here (generated, no source bucket →
`drip_tap`).

**The legal move-set is the guardrail.** Not every `(from,to)` pair is meaningful. `Settled → Reserved`
(un-debiting a filled trade) is nonsense; `Bank → Reserved` (reserving directly against the buffer)
violates the rule that only trading-account `Available` is reservable — the bank is not trading-visible,
you must pull it to `Available` first. Encoding the legal set here is where "bank is not trading-visible"
and "can't un-settle a fill" become **structural**, not merely documented.

```go
// legalCashMove reports whether a (from,to) transfer is a permitted move.
// This is where structural guarantees live: bank-not-reservable, no un-settling.
func legalCashMove(from, to Bucket) bool {
    switch {
    case from == Available && to == Reserved:  return true // reserve (buy)
    case from == Reserved && to == Settled:     return true // buy convert
    case from == Reserved && to == Available:   return true // release / release_excess
    case from == Available && to == Bank:       return true // bank-refill
    case from == Bank && to == Available:       return true // bank-pull
    default:                                     return false
    }
}

// bucketPtr returns a pointer to the named cash bucket field, so writes through it
// hit the real struct.
func (c *Cash) bucketPtr(b Bucket) *int64 {
    switch b {
    case Available: return &c.Available
    case Reserved:  return &c.Reserved
    case Settled:   return &c.Settled
    case Bank:      return &c.Bank
    }
    return nil
}

// MoveCash moves `amount` coins from bucket `from` to bucket `to` within one account's cash.
// Pure, conserving, in-memory, zero I/O. Direction is entirely in from/to; amount is always > 0.
// On ANY error, state is untouched (no partial mutation).
func (c *Cash) MoveCash(from, to Bucket, amount int64) error {
    // 1. VALIDATE amount — non-positive is always a caller bug (direction is in the buckets).
    if amount <= 0 {
        return ErrNonPositiveAmount
    }

    // 2. VALIDATE legal edge — refuse any move not in the permitted set.
    //    (This subsumes the from==to no-op check: same-bucket moves are not in the set.)
    if !legalCashMove(from, to) {
        return ErrIllegalMove{From: from, To: to}
    }

    src := c.bucketPtr(from)
    dst := c.bucketPtr(to)

    // 3. CHECK sufficiency — "no negative balance by construction" for transfers.
    //    You can only move what is actually in `from`. A reserve can't exceed available;
    //    a release can't exceed reserved. NOTE: this is bucket-underflow protection, a
    //    DIFFERENT thing from the durable account going net-negative from fees (that is
    //    DebitCash's doing, §1.6). A transfer can never create a negative bucket.
    sumBefore := c.Available + c.Reserved + c.Settled + c.Bank
    if *src < amount {
        return ErrInsufficient{Bucket: from, Have: *src, Want: amount}
    }

    // 4. MUTATE.
    *src -= amount
    *dst += amount

    // 5. ASSERT conservation — total cash unchanged. A failure here is a programming error.
    sumAfter := c.Available + c.Reserved + c.Settled + c.Bank
    if sumBefore != sumAfter {
        panic("MoveCash violated cash conservation")
    }

    return nil
}
```

**Atomicity.** No locks. `MoveCash` runs inside the session's single goroutine (one active session per
user per market); steps 3→4 cannot be interleaved by another order from the same participant, so
check-then-mutate is atomic for free — inherited from the engine's serialization, not added here.

**Failure semantics.** Every failure path returns *before* the mutation. A failed `MoveCash` mutates
nothing; the caller gets a typed error and decides (e.g. `reserve_if_available` turns an
`ErrInsufficient` on the reserve into its `insufficient funds` rejection to the engine). `MoveCash`
never emits events, never rejects orders, never talks to the engine.

**Storage.** In-memory only. Zero I/O — no Redis, no Postgres, no disk, no network. It is on the
hottest path in the system and does nothing but integer arithmetic. Persistence of the resulting state
happens elsewhere (async Redis mirror; session-end Postgres write), driven by other components reading
this state — never from inside `MoveCash`.

---

### 1.2 `MovePosition` — position transfer between buckets

**What it is.** The position-side twin of `MoveCash`. Moves a positive integer number of units of one
instrument between the two position buckets (`Available ↔ Reserved`). Conserving; does not touch
`OpenLots`. In-memory only, zero I/O.

**The moves it serves (both conserving) — only two:**

| Caller (Tier 2)             | from → to                     | Trigger                                    |
|-----------------------------|-------------------------------|--------------------------------------------|
| `reserve_if_available` sell | PosAvailable → PosReserved    | sell order admitted at Gate 4 (lock units) |
| `release` sell terminal     | PosReserved → PosAvailable    | resting sell cancelled / expired           |

Buy fills (units in) → `CreditPosition`. Sell fills (units out) → `RemovePosition`. Neither is
`MovePosition`. Positions have a far smaller move-set than cash: no `Settled` bucket, no cross-pool
(bank/drip) concept — positions live entirely inside the trading-account context.

**`MovePosition` does NOT touch `OpenLots`.** A reserve/release moves unit *counts* between Available
and Reserved; it does not consume or create lots. Lots are born on a buy fill (`CreditPosition` pushes
a lot) and consumed on a sell fill (`RemovePosition` pops/decrements FIFO). Locking units for a resting
sell reserves the *quantity* but does not yet decide *which lots* — lot matching happens at **fill
time** (in `RemovePosition`), never at reserve time. This is deliberate and load-bearing: FIFO-matching
at reserve time would be wrong.

```go
func legalPosMove(from, to PosBucket) bool {
    switch {
    case from == PosAvailable && to == PosReserved: return true // sell reserve (lock)
    case from == PosReserved && to == PosAvailable: return true // sell release (unlock)
    default:                                         return false
    }
}

func (p *Position) posBucketPtr(b PosBucket) *int64 {
    switch b {
    case PosAvailable: return &p.Available
    case PosReserved:  return &p.Reserved
    }
    return nil
}

// MovePosition moves `units` of one instrument between the two position buckets.
// Pure, conserving, in-memory, zero I/O. OpenLots untouched.
func (p *Position) MovePosition(from, to PosBucket, units int64) error {
    // 1. VALIDATE units.
    if units <= 0 {
        return ErrNonPositiveUnits
    }

    // 2. VALIDATE legal edge (subsumes the same-bucket no-op check).
    if !legalPosMove(from, to) {
        return ErrIllegalMove{}
    }

    src := p.posBucketPtr(from)
    dst := p.posBucketPtr(to)

    // 3. CHECK sufficiency — the sell-side DOUBLE-COMMIT guard.
    //    Available -> Reserved can only lock units actually held and not already locked.
    //    A second resting sell can't re-lock units a first sell already reserved, because
    //    after the first reserve those units are in Reserved, not Available. This is where
    //    "you can't double-sell the same units" becomes structural (economy §4.3).
    sumBefore := p.Available + p.Reserved
    if *src < units {
        return ErrInsufficientPosition{Have: *src, Want: units}
    }

    // 4. MUTATE.
    *src -= units
    *dst += units

    // 5. ASSERT unit conservation (bucket total unchanged; OpenLots untouched so coupling holds).
    sumAfter := p.Available + p.Reserved
    if sumBefore != sumAfter {
        panic("MovePosition violated unit conservation")
    }

    return nil
}
```

**Atomicity / failure / storage.** Identical to `MoveCash`: single session goroutine (no locks);
every failure returns before mutation (a failed sell-reserve → `reserve_if_available` returns
`insufficient position` to the engine); in-memory only, zero I/O. Positions are session-scoped and
never persist to Postgres (die at close, auto-squared); they are included in the Redis crash snapshot
by a separate snapshotter reading this state, never written here.

---

### 1.3 `CreditPosition` — mint units in (buy fill)

**What it is.** The position-minting primitive. On a buy fill, units the account did not previously
hold are delivered in. `CreditPosition` adds them to `Available` and records the **lot** (price, time,
qty) so later sells can FIFO-match against it. Breaks unit-conservation on purpose (units enter from
outside — the market delivered them), which is why it is separate from `MovePosition`.

**Why `entryPrice`/`entryAt` are passed in, not read.** The primitive is pure: it does not read the
clock or the price. The caller (`convert`, buy-fill path) already has the authoritative `maker_price`
from the engine's fill event and the fill timestamp. Passing them in keeps the primitive deterministic
and reproducible — if it read `time.Now()` itself, replay would produce different `EntryAt` values and
break reproducibility.

**Newly-bought units land in `Available`, not `Reserved`.** You own them outright; nothing has locked
them. They are immediately free to sell. This is *the* place a buy increases `Available`.

**Append at the TAIL.** FIFO means earliest-bought matched first. New lots are the newest → appended
to the end of the queue; sells consume from the head (oldest first) in `RemovePosition`. Order is
load-bearing: reversing it would be LIFO — wrong cost basis, wrong realized P&L.

**Partial fills = multiple lots.** A market buy for 100 filling 50 @ 500 then 50 @ 502 calls
`CreditPosition` twice (once per fill, driven by `convert`), producing two lots at different prices.

```go
// CreditPosition adds `units` newly-bought units of one instrument to Available and records
// a FIFO open lot capturing the maker price and fill time. Increases total held units on
// purpose (no conservation assert). In-memory only, zero I/O.
func (p *Position) CreditPosition(units int64, entryPrice int64, entryAt time.Time) error {
    // 1. VALIDATE.
    if units <= 0 {
        return ErrNonPositiveUnits
    }
    if entryPrice <= 0 {
        // a zero/negative cost basis would corrupt every future P&L that matches this lot;
        // a bad fill event signals an engine bug — reject loudly.
        return ErrBadPrice
    }

    // 2. MUTATE — newly-bought units are immediately free to sell.
    p.Available += units

    // 3. RECORD — push a new lot at the TAIL (arrival order → FIFO).
    p.OpenLots = append(p.OpenLots, Lot{
        EntryPrice:   entryPrice,
        EntryAt:      entryAt,
        RemainingQty: units,
    })

    // Coupling invariant preserved: Available += units AND a lot with RemainingQty == units
    // added, so (Available+Reserved) and sum(RemainingQty) both rose by `units`.
    return nil
}
```

**No conservation assertion — deliberate.** This increases total units held on purpose; that is the
job. The invariant it instead preserves is the **position–lot coupling** (§0.6): it adds the same
`units` to `Available` and to a new lot's `RemainingQty`, so both sides rise equally.

**Atomicity / failure / storage.** Single session goroutine, no locks. Validation fails before any
mutation; nothing changes on error (a bad fill event failing validation signals an engine bug — reject
loudly rather than silently accept). In-memory only, zero I/O. Lots are session-scoped, never
persisted to Postgres; included in the Redis snapshot by the separate snapshotter reading `OpenLots`.

---

### 1.4 `RemovePosition` — burn units out (sell fill), FIFO-match cost basis

**What it is.** The position-burning primitive. On a sell fill, units the account held are delivered
*out* (gone — the buyer got them). `RemovePosition` removes them from `Reserved` (they were locked
there when the sell was placed) and **consumes the matching open lots FIFO**, returning which cost
bases were used — the raw material 5.4 turns into realized P&L. Mirror of `CreditPosition`.

**Units leave from `Reserved`, not `Available`.** When the sell was *placed*, `MovePosition` locked
the units `Available → Reserved`. They sat in `Reserved` while the order rested. So when the sell
*fills*, they leave from `Reserved`. This is why sells reserve position up front — so the units are
guaranteed present and earmarked at fill time.

**The return value is the point.** Unlike the other primitives (which return only `error`),
`RemovePosition` returns the **list of lot-slices it consumed** — because that is the cost-basis
breakdown 5.4 needs. The primitive does the FIFO matching (it owns the lots); **5.4 does the money
math** from the result. This keeps 5.1 free of P&L semantics — it moves units, it does not judge
profit. `RemovePosition` deliberately does **not** take an exit/sell price: it reports only which cost
bases were consumed; the caller/5.4 already has the exit price from the fill event and computes gross
P&L. (Locked design choice: hands, not brain.)

```go
// LotMatch is one consumed slice of an open lot — the cost-basis breakdown handed to 5.4.
type LotMatch struct {
    EntryPrice int64     // cost basis of this consumed slice
    Qty        int64     // units matched at this entry price
    EntryAt    time.Time // fill time of the consumed lot (for downstream reporting)
}

// RemovePosition removes `units` of one instrument out of Reserved (delivered by a sell fill)
// and consumes matching open lots FIFO from the HEAD, returning the cost-basis breakdown.
// Decreases total held units on purpose. In-memory only, zero I/O. Does NO money math.
func (p *Position) RemovePosition(units int64) (matched []LotMatch, err error) {
    // 1. VALIDATE.
    if units <= 0 {
        return nil, ErrNonPositiveUnits
    }
    // You can never deliver out more than was locked. Because the sell reserved exactly its
    // quantity up front (MovePosition), a fill can only ever be <= Reserved. Checks Reserved,
    // not Available — the units in flight for this sell live in Reserved.
    if units > p.Reserved {
        return nil, ErrInsufficientReserved{Have: p.Reserved, Want: units}
    }

    // 2. REMOVE units from Reserved.
    p.Reserved -= units

    // 3. CONSUME lots FIFO from the HEAD (oldest first) and build the match list.
    remaining := units
    for remaining > 0 {
        lot := &p.OpenLots[0] // oldest lot (head)
        take := lot.RemainingQty
        if remaining < take {
            take = remaining
        }
        matched = append(matched, LotMatch{
            EntryPrice: lot.EntryPrice,
            Qty:        take,
            EntryAt:    lot.EntryAt,
        })
        lot.RemainingQty -= take
        remaining -= take
        if lot.RemainingQty == 0 {
            p.OpenLots = p.OpenLots[1:] // drop fully-consumed lot from the head
        }
    }

    // 4. ASSERT position–lot coupling still holds: we removed `units` from Reserved AND
    //    consumed exactly `units` worth of lot quantity, so both sides dropped equally.
    if p.Available+p.Reserved != sumRemaining(p.OpenLots) {
        panic("RemovePosition desynced buckets from lots")
    }

    return matched, nil
}

func sumRemaining(lots []Lot) int64 {
    var s int64
    for i := range lots {
        s += lots[i].RemainingQty
    }
    return s
}
```

**Worked example.** Held `Reserved = 100`, `OpenLots = [{500, 50}, {502, 50}]`. Sell fills 60:
`Reserved → 40`; FIFO consumes all 50 @ 500 (lot dropped) + 10 @ 502 (lot now `{502, 40}`); returns
`[{500,50}, {502,10}]`. 5.4 then takes that + exit price 520 →
`gross = (520-500)*50 + (520-502)*10 = 1000 + 180 = 1180`. The primitive produced the *what-was-consumed*;
5.4 did the *profit*.

**The 5.4 handoff.** This primitive triggers realization: `convert` (sell fill) → `RemovePosition` →
gets `matched` → hands `matched` + exit price to 5.4 → 5.4 computes gross/costs/net and produces the
closed-trade record. `RemovePosition` is step one of realization; it computes no money itself.

**Atomicity / failure / storage.** Single session goroutine, no locks; the FIFO loop is not
interruptible by another order from the same participant. Validation before any mutation; on error
nothing changes and `matched` is nil. Partial consumption within a *successful* call is normal FIFO,
not partial failure. In-memory only, zero I/O — lots die at session close. The *realized trade* 5.4
subsequently produces from `matched` **is** persisted (the `participant_trades` row), but that write
is 5.4's/persistence's job, off the hot path, not `RemovePosition`'s.

---

### 1.5 `CreditCash` — mint cash in (sell proceeds)

**What it is.** The cash-minting primitive. On a sell fill, proceeds arrive from outside the account
(the buyer's money). `CreditCash` adds those coins to a cash bucket. Breaks cash-conservation on
purpose (money enters from outside), mirror of `CreditPosition`. In-memory only, zero I/O.

**Proceeds land in `Available`, immediately tradeable.** Per economy STL-2, sell proceeds are instantly
available to trade — no settlement delay, no T+1 (deliberate: intraday prototype, frictionless capital
recycling). So the credit targets `Available`; the money is live the instant the sell fills.

**`amount` is strictly positive — always.** Proceeds are `fill_qty × maker_price`, which is always
positive. Even a catastrophic loss produces *positive* proceeds (you sold 50 @ 300 for +15,000 coins);
the loss lives in the *comparison* against cost basis (a 5.4 computation), never as a negative deposit.
A negative `CreditCash` would double-count the loss and corrupt the balance. The loss the user sees is
5.4's computed number in P&L / trade-history views, not a primitive amount.

**`CreditCash` does not compute proceeds.** The caller (`convert`, sell-fill path) already has
`fill_qty × maker_price` and passes the finished number in. The hand deposits what it is handed
(consistent with "5.1 does no money math").

**Debt-priority is NOT this primitive's decision.** Under MON-7 / Option 4, if the durable trading
account is net-negative, incoming funds should settle the debt first. That *decision* is 5.5's, made
**before** calling this primitive — the caller consults 5.5, which either routes to debt settlement or
calls `CreditCash(Available, proceeds)`. `CreditCash` never branches on debt state — that would put
policy in the hand.

```go
// legalCreditTarget constrains where minted cash may land.
func legalCreditTarget(to Bucket) bool {
    // proceeds are always tradeable-on-arrival; no path mints into Reserved/Settled/Bank.
    return to == Available
}

// CreditCash mints `amount` coins (arriving from outside the account, e.g. sell proceeds)
// into a cash bucket. Increases total cash on purpose (no conservation assert). Strictly
// positive amount. In-memory only, zero I/O.
func (c *Cash) CreditCash(to Bucket, amount int64) error {
    // 1. VALIDATE.
    if amount <= 0 {
        return ErrNonPositiveAmount // a "negative credit" is a bug; losses are 5.4's numbers, not amounts
    }
    if !legalCreditTarget(to) {
        // minting into Reserved (money behind a non-existent order), Settled (pretending cash
        // was already spent-and-realized), or Bank (proceeds silently vanishing) is nonsense.
        return ErrIllegalCreditTarget{To: to}
    }

    // 2. MUTATE — deposit into the target bucket.
    *c.bucketPtr(to) += amount

    return nil
}
```

**No conservation assertion — deliberate.** This increases total cash on purpose (money genuinely
entered the sim from the counterparty). The invariant it respects: it never touches `Reserved` or
`Settled`, so it cannot corrupt in-flight reservations or the realized figure — it only grows
spendable `Available`.

**Atomicity / failure / storage.** Single session goroutine, no locks; validation before mutation;
in-memory only, zero I/O. The resulting balance persists at session end (separate action); crash
snapshot via the separate snapshotter — not here.

---

### 1.6 `DebitCash` — burn cash out (costs) — **Option B**

**What it is.** The cash-burning primitive. Removes coins from a cash bucket — the money leaves the
account entirely (exits the sim). This is how **costs** are applied: brokerage, STT, exchange, SEBI,
stamp, GST, and the CST-5 auto-square fee. Breaks cash-conservation on purpose, twin of
`RemovePosition`. In-memory only, zero I/O.

**Burns from `Available`.** `Available` is the account's live spendable cash — the single place "what
you can actually use" is tracked; fees reduce what you can use, so debiting `Available` directly is
correct. Debiting `Settled` would conflate *cost* with *realized principal* and muddy the figure 5.4
reconciles against. On a sell, proceeds just landed in `Available` via `CreditCash`, so the fee comes
right back out of the same bucket — "you received proceeds minus fees as usable cash," exactly correct.
Fees were never reserved (reservation only ever locked principal, `best_ask × qty + buffer`), so they
are paid out of remaining `Available` at fill time.

**Option B — the debit may drive `Available` negative, with no sufficiency gate.** This layer behaves
like a real broker (Zerodha/Groww): the broker debits statutory charges regardless and lets the ledger
reflect reality — it does not block a fill because brokerage would tip the balance negative. So
`DebitCash` subtracts whether or not the bucket covers it; `Available` can go below zero. There is **no
`ErrInsufficient` on the cost path** — a cost always applies.

**Consequences of Option B (conscious, not accidental):**

1. **"Available ≥ 0" is no longer an invariant** — but only `DebitCash` can violate it. `MoveCash` and
   `CreditCash` cannot produce a negative (bounded: you can only move what's there; credits only add).
   So the negative enters the system through exactly one door — costs — which maps precisely to the
   broker reality (fees, not principal, overdraw you).

2. **The negative naturally suppresses new buying.** `reserve_if_available`'s fundability check
   (`ReadAvailable() >= required`) fails for any positive requirement when `Available` is negative — no
   special-casing needed. A negative `Available` means "no buying power until you're back above water,"
   exactly how a broker margin-blocks you after charges eat your balance.

3. **5.5's role narrows and is correct.** Going negative happens automatically here — no brain needed,
   just arithmetic. Coming back from negative is 5.5's job: MON-7 debt-priority routes later-arriving
   funds (drip, bank pull, winning proceeds) to clear the negative first. 5.5 is not consulted at debit
   time at all.

**`amount` is strictly positive.** A cost is always positive; the negative is in the *balance*, never
the amount. `DebitCash` burns one number — the **total** from 5.3's itemized stack. The itemization
(what the user sees in the cost-breakdown view) lives in 5.3's record, not here; the hand does not know
or care what the fee is *for*.

```go
// legalBurnSource constrains where burned cash leaves from.
func legalBurnSource(from Bucket) bool {
    return from == Available // costs come out of spendable cash
}

// DebitCash burns `amount` coins out of a cash bucket (costs leaving the sim). Under Option B
// it may drive Available negative — NO sufficiency gate, broker-style. Decreases total cash on
// purpose (no conservation assert). Strictly positive amount. In-memory only, zero I/O.
func (c *Cash) DebitCash(from Bucket, amount int64) error {
    // 1. VALIDATE — only the two caller-bug guards remain; there is NO "insufficient" condition.
    if amount <= 0 {
        return ErrNonPositiveAmount
    }
    if !legalBurnSource(from) {
        return ErrIllegalBurnSource{From: from}
    }

    // 2. MUTATE — burn coins. MAY drive the bucket negative; allowed by Option B (broker parity).
    *c.bucketPtr(from) -= amount

    return nil
}
```

**No conservation assertion — deliberate.** Money left the sim on purpose. The invariant respected: it
only burns from the legal source, never touches `Reserved` (can't fee-charge locked order money) or
`Settled` (keeps realized principal clean).

**Atomicity / failure / storage.** Single session goroutine, no locks; validation before mutation
(only the two caller-bug guards); in-memory only, zero I/O. The *cost record* 5.3 produces (itemized,
for the user's cost-breakdown view) is what eventually persists — the `trade_costs` write, off the hot
path, not here. `DebitCash` just removes coins from memory.

---

## 3. Cross-cutting properties (all operations)

- **Reproducibility.** Every write primitive and the two hot-path reads are pure functions of
  (current in-memory state, arguments) — no clock, no randomness, no external read. Replaying the same
  ordered sequence from the same starting state reproduces the exact ledger. `load_account` is the one
  operation that reads a store, and it does so once, at the boundary, before any order can route in.

- **Atomicity without locks.** Everything runs inside the session's single goroutine (one active
  session per participant per market). Check-then-mutate sequences cannot be interleaved by another
  order from the same participant — inherited from the engine's serialization model, not added here.

- **Failure never partially mutates.** Every write validates fully before the first field write; on any
  error, state is untouched and the caller decides. Programming-error conditions (broken invariants,
  missing working state on the hot path) panic in dev rather than returning a swallow-able error.

- **I/O boundaries.** The mutation primitives and hot-path reads do **zero I/O**. Persistence happens
  at the boundaries by separate components reading this state: async Redis mirror (crash recovery),
  session-end Postgres write-back (canonical only), write-behind buffer → `participant_trades` for
  closed trades. None of those writes are performed from inside any primitive in this document.

---

## 4. OPEN items (flagged, not specified here)

These were explicitly deferred in this thread and are **not** locked. They are listed so the surface is
complete and nothing is silently missing.

### 4.1 `drip_tap` — auto-refill tap (OPEN, HELD)

The last-resort tap that generates coins into the trading account when both the trading pool and the
bank are empty. It is **neither** `MoveCash` (no source bucket — the drip *generates* coins) **nor**
`CreditCash` (it has its own trigger logic and session-denominated accrual). It is a distinct primitive
with genuinely open design: the economy doc left the drip's accrual **provisional** — coins-per-session
and the sessions-to-recovery budget `N` are unset tuning dials. The durable fields exist already
(`accounts.drip_active`, `accounts.drip_progress`; and the Redis snapshot mirrors `drip_active`,
`drip_progress`). **Held deliberately** — nobody hits zero on day one, so it is not demo-critical.
To be specified: trigger condition, accrual mechanics (session-denominated), the mutation itself, and
the interaction with `drip_progress` persistence.

### 4.2 Crash-recovery load (OPEN)

The fourth read: rebuild the full in-memory working state from the **Redis** snapshot after a crash
(item 52). It parallels `load_account` but sources Redis and restores the **full working split** — not
just durable capital. Exact keys/fields to read (persistence §4.1):

- `l5:snap:{session_id}:acct:{account_id}:cash` → `available, reserved, settled, bank_cash,
  drip_active, drip_progress`
- `l5:snap:{session_id}:acct:{account_id}:pos:{instrument_id}` → `available_units, reserved_units,
  open_lots:[{entry_price, entry_at, remaining_qty}]`
- `l5:snap:{session_id}:acct:{account_id}:resv:{order_id}` → reservation records (5.2 state)

Deferred with the joint L5+L2+engine snapshot-capture coordination pass (persistence §2 note). Not
written here; flagged so the read surface is known to be incomplete by exactly this one operation.

---

## 5. Operation index

| # | Operation | Kind | Direction | Store touch | Status |
|---|-----------|------|-----------|-------------|--------|
| 1 | `MoveCash` | write | transfer (cash) | none (in-mem) | LOCKED |
| 2 | `MovePosition` | write | transfer (units) | none (in-mem) | LOCKED |
| 3 | `CreditPosition` | write | mint (units in) | none (in-mem) | LOCKED |
| 4 | `RemovePosition` | write | burn (units out) + FIFO match | none (in-mem) | LOCKED |
| 5 | `CreditCash` | write | mint (cash in) | none (in-mem) | LOCKED |
| 6 | `DebitCash` | write | burn (cash out), Option B | none (in-mem) | LOCKED |
| 14 | `ReadAvailableCash` | read | — | in-mem (loaded once) | LOCKED |
| 15 | `ReadAvailablePosition` | read | — | in-mem (loaded once) | LOCKED |
| 16 | `load_account` | read | — | **Postgres** `accounts` | LOCKED |
| — | `drip_tap` | write | mint (generated) | `accounts` drip_* | OPEN / HELD |
| — | crash-recovery load | read | — | **Redis** snapshot | OPEN |