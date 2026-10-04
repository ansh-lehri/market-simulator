# broker/order — Order Core & Order Endpoints (Tier 1)

<!-- Provisional path. The handlers are shard methods, the entry-points + endpoints are Broker/HTTP;
     whether this is one package or split is orientation's call. Content is load-bearing, path is not.
     READ ORDER (all apply together):
       1) /CLAUDE.md                                            — root constitution
       2) broker/CLAUDE.md                                      — the broker (funnel) constitution
       3) THIS file
       4) docs/broker/BROKER_PHASE5_IMPLEMENTATION_DESIGN.md    — the spec (Points 1-4 LOCKED)
          docs/broker/BROKER_ORDER_LIFECYCLE_intake_punch_settle.md — authority (intake §3, cancel §4, settle §6)
          docs/broker/BROKER_READ_SURFACES_tier4.md             — §0 HTTP house style, §9 modify composer
          docs/exchange/Exchange_Broker_Contract.md             — the wire seam (§4 result shapes, §6-8 the three "doubts") -->

## Spec & authority
`BROKER_PHASE5_IMPLEMENTATION_DESIGN.md` is the spec; the order-lifecycle, read-surfaces, and
Exchange–Broker contract docs are the authorities it cites. **Code wins over doc** — every type is
source-bound to `contract` / `economy/reserve` / `economy/realize` / `exchange`; the Phase-5 decision
log already lists the places the authority's pseudo-code was corrected against source (follow the
corrections, not the pseudo-code).

## What it IS (one line)
The order core: **place** (`handlePlaceOrder`), **cancel** (`handleCancel`), **settle**
(`handleApplyResult`, the six-outcome adapter), and the three order **HTTP endpoints**. It fills the
handler bodies at **both ends of the Phase-4 exchange loop** — the side that builds+punches orders, and
the side that applies what comes back.

## THE two disciplines the whole phase rests on
- **Two-record division (P5-D1).** L5's `ReservationRecord` (keyed by `OrderID`, per-shard) is the
  **money truth** — reserved cash/units, qty progress, brokerage state — and the broker **never
  duplicates** it. The broker's **`OrderTrack` is thin exchange-seam state only**: `OrderID`,
  `AccountID`, `Instrument`, `Status`, `CancelInFlight`, `IsMarketBuy` — **no amounts, no qty
  progress**. "How much is left / is it complete" is **read from L5** (`RemainingQty`), never mirrored.
- **Accepted, not executed.** Every order action is *accepted*, not *executed*: the punch is
  fire-and-forget, the real outcome returns asynchronously via settlement, and no endpoint ever claims
  "done." Accept is HTTP **202**.

## Owns / public surface
```go
func (s *shard) handlePlaceOrder(c PlaceOrder) PlaceResult     // Point 1 — owner goroutine
func (s *shard) handleCancel(c CancelOrder) CancelResult       // Point 2 — owner goroutine
func (s *shard) handleApplyResult(c ApplyResult)               // Point 3 — push, no reply
func (b *Broker) PlaceOrder(ctx, aid, OrderSpec) (PlaceResult, error)   // Point 4 entry (mirrors Broker.Transfer)
func (b *Broker) Cancel(ctx, aid, oid) (CancelResult, error)
// POST /v1/orders · POST /v1/orders/cancel · POST /v1/orders/modify
type OrderSpec struct { Instrument; Side; Type; TIF; Qty int64; LimitPrice int64 }
type OrderTrack struct { OrderID; AccountID; Instrument; Status; CancelInFlight bool; IsMarketBuy bool }  // 6 fields, thin
// helpers: nextOrderID, validateSpec, convertTrades, tradeID, maxSpend, slippageBuffer, finalizeTrack
```
New shard state: `s.tracks map[OrderID]*OrderTrack`, `s.seq uint64`, `s.mktCache` (the BestAsk
interface — concrete impl deferred). `s.resv` is the per-shard reservation service from Phase 1.

## Load-bearing rules — PLACE (Point 1)
Order of operations is the design: **session → validateSpec → (market-buy price) → nextOrderID →
reserve → track → punch → reply.** Any pre-punch failure **punches nothing** (P5-D6).
- `validateSpec` is **structural, pre-pricing, BEFORE reserve** (a bad spec never touches L5):
  `knownInstrument`, `Qty>0`, valid `Side`, `Limit→LimitPrice>0`, valid `Type`.
- **Market-buy is the one priced branch (P5-D3).** Only a market **buy** needs a price (no limit, cash
  held before fill price is known): read `mktCache.BestAsk` (miss → `ReasonNoMarketPrice`), size
  `slippageBuffer(bestAsk, qty)`, pass **both** into `reserve.Order` (L5 computes
  `requiredCash = BestAsk*Qty + SlippageBuffer`), and compute the **equal** `maxSpend` for
  `FundedOrder.MaxSpend`. **`reserved == MaxSpend` by construction** → exchange staleness is harmless,
  `ReleaseExcess` refunds the unspent buffer at settlement. For limits/sells `BestAsk`/buffer are `0`
  and L5 ignores them.
- `nextOrderID` (P5-D4): a shard-local `uint64`, plain `s.seq++` (owner goroutine — **no atomic, no
  lock**), formatted `"<AccountID>:<seq>"` so settlement parses the account back.
- **Gate 4 = `ReserveIfAvailable`, which returns `error` only** (P5-D2, corrects the doc's `res,_:=`):
  map a reject via `errors.Is` on L5's typed reserve sentinels. Reserve **before** punch, create the
  `OrderTrack` **before** punch.
- Punch via **`s.punch`** (Phase 4 Point 1) with `Kind: contract.SubmitOrder`, `STP: CancelIncoming`
  on every submit. `BrokerID` is stamped inside `s.punch`.

## Load-bearing rules — CANCEL (Point 2), async (P5-D10)
`handleCancel` is **validate → mark in-flight → punch; NO release.** Release waits for the exchange's
**echoed** ack in settlement — the broker's `Status` is a **best-effort pre-filter**, the exchange is
authoritative, and releasing now could double-spend against a fill that already won the race.
- Ownership guard: `tr.AccountID == c.AccountID` (**no cross-account cancel**) → else `ReasonUnknownOrder`.
- Only `Working`/`PartiallyFilled` are cancellable → else `ReasonNotCancellable`.
- **Idempotent** (P5-D11): `CancelInFlight` already set → return `CancelPending`, **punch nothing again**.
- Set `CancelInFlight = true` + `MarkSnapshotDirty(accountID)` (P5-D13 — survives crash, no re-punch on
  recovery), then `s.punch` with `Kind: contract.CancelOrder` (qualify it — collides with the broker
  `CancelOrder` command type) + `CancelRequest{OrderID}` (whole-order only). `CancelPending` means
  "requested," never "cancelled."

## Load-bearing rules — SETTLE (Point 3), the six-outcome adapter (P5-D15)
Route each `EngineResult` → L5 op(s) → `OrderTrack.Status` → cleanup. **No money logic (L5 owns it).**
`convertTrades` calls `Convert` **5-arg** (P5-D16): `makerPrice = t.Price` (always the maker's resting
price), `exitAt = time.Now()` (contract carries no timestamp — carried, §flags), `tradeID =
"<OrderID>:<Trade.Seq>"`.
- **Matched** (`settleMatch`) — taker, **one-shot** (all prints batched in `Match.Trades`): convert,
  then status — `len(Trades)==0 && Rested → Working` (**pure-rest guard, P5-D17**); `Rested →
  PartiallyFilled`; `CancelledQty>0 → Release(CancelledQty)` + `Filled`; else `Filled`. If
  `IsMarketBuy && Filled → ReleaseExcess` (buffer true-up). `finalizeTrack`.
- **MakerFill** (`settleMakerFill`) — a **resting** order got hit, **incremental** (many over its
  life): convert, then **completion is an L5 fact** — `RemainingQty==0 → Filled + finalize`, else
  `PartiallyFilled`.
- **Diverted** (`markDiverted`) — auction ACK, **two-phase** (P5-D19): status `Working`, **no L5 call**
  (hold the reservation across the gap); the auction-clear result lands on the **same `OrderID`** and
  runs `settleMatch`.
- **Rejected** (`settleRejected`) — `Release(OrderID, 0)` (**0 = release all**) + `Rejected` + finalize.
- **Cancelled** (`settleCancelled`) — **race discipline (P5-D18): `Release(OrderID, r.CancelledQty)` —
  exactly the echoed qty, never an assumed remainder** — clear `CancelInFlight`, `Cancelled`, finalize.
- **CancelRejected** (`settleCancelRejected`) — order already gone: **clear the flag only, no
  finalize/mark** (channel order means the terminating result already finalized; defensive no-op).
- `finalizeTrack(aid, id)` (P5-D20): if terminal `delete(s.tracks, id)`; `MarkSnapshotDirty(aid)` —
  `aid` **threaded from `c.AccountID`** (fixes the doc's placeholder; no re-parse). Unknown outcome →
  **panic**.

## Load-bearing rules — ENDPOINTS (Point 4)
Three `POST`, explicit `account_id` in body, no auth, **handler owns the timeout → 504**. `PlaceOrder`/
`Cancel` entry-points mirror `Broker.Transfer` (buffered-1 reply, `route`, `select` on reply vs ctx).
- **`/v1/orders`** = place (§9.2 STEP B standalone); **`/v1/orders/cancel`** = cancel (§9.2 STEP A,
  `POST`-with-body); **`/v1/orders/modify`** = **cancel + resubmit composed** (P5-D14 — **no
  `handleModify` primitive**). Modify is **Option-1 gate-on-local-acceptance**: punch cancel → if
  `CancelFailed` return 409, don't place, original untouched; if `CancelPending` → punch place; compose
  from **both local results**. Partial failure (cancel pending + place rejected at Gate 4) → **reported,
  never rolled back** → `200 {modify_status:"partial", warning}`. Not idempotent (fresh `OrderID`); no
  pre-cancel funds check.
- **Status table (P5-D26/27/28):** `202` accept `{order_id}` · `400` malformed body (pre-funnel) ·
  `409` `ReasonNoSession` **and** all cancel-fails · `422` place business rejects (validation /
  `ReasonInsufficientFunds` / `ReasonNoMarketPrice`) · `504` timeout · modify `200`
  `{modify_status: ok|partial|rejected}`. **No per-reason codes** — the exact `reason` string is in the
  body. `ReasonInsufficientFunds` is **not** split funds-vs-units.

## Deps & seam
Phase 0 (`route`, reply, `s.punch` seam, `ownedSession`, `shard.brokerID`); Phase 4 (`s.punch`,
`ApplyResult`); L5 `economy/reserve` (`ReserveIfAvailable`, `Convert`, `Release`, `ReleaseExcess`,
**`RemainingQty`** — see prerequisite flag) + its typed sentinels; `economy/realize` (`TradeID`);
`contract` (`InboundMessage`, `FundedOrder`, `CancelRequest`, `EngineResult`, `MatchResult`, `Trade`,
the `Outcome` enum). `s.mktCache.BestAsk` is an **interface** here (concrete impl is the deferred
market-data component).

## Do NOT
- mirror any money/qty state onto `OrderTrack` (read `RemainingQty` from L5);
- release on cancel (release only on the exchange's echoed `CancelledQty` at settlement);
- release the assumed remainder in `settleCancelled` (exactly `r.CancelledQty`);
- make an L5 call in `markDiverted`, or finalize/mark in `settleCancelRejected`;
- build a `handleModify` primitive (modify is composed cancel+place);
- lock/atomic `nextOrderID` (owner goroutine);
- leave `contract.SubmitOrder`/`contract.CancelOrder` unqualified (name collision);
- emit per-reason HTTP codes (buckets + `reason` string in body).

## Flags (surface in orientation, don't silently resolve)
- **PREREQUISITE — L5 read accessor.** Verify `ReservationService.RemainingQty` (and `IsComplete`)
  exist in the built `economy/reserve`. The service was built **write-only**; if these are missing, a
  small **L5 addition is a prerequisite** before Phase 5 compiles. **Code wins over doc.**
- **Market-buy runtime is blocked** on the deferred market-data component (`mktCache.BestAsk`). Build
  the branch against the **interface** + a test stub; real market-buy can't run until that lands.
- **`knownInstrument` instrument-set (P5-D9)** must be injected on the session at **L7 session-install**
  — not defined in any doc yet. Add the field; decide how it's stubbed for tests (empty/permissive)
  until L7 fills it. **Cross-point.**
- **`slippageBuffer` sizing (P5-D8)** — a config dial (bps of notional), value open, pinned when
  market-buy goes live.
- **`OrderTrack` durability (B9)** + crash-resume of the shard `OrderID` counter — carried, ties into
  Phase-2 `session_save`.
- **`exitAt` = `time.Now()`** won't match the exchange's session-local candle time under a simulated
  clock — needs an exchange fill-timestamp on the contract (an exchange change). Carried.

## Tests — SPLIT by auditor boundary
**AUDITOR-OWNED (a separate blind session, per `AUDIT.md` — tests authored from the docs, NOT the
implementation; the implementer runs them and MUST NOT edit them):**
- **The six-outcome adapter (Point 3)** — one golden scenario per outcome (Matched incl. pure-rest /
  partial / IOC-leftover / full; MakerFill incremental→complete; Diverted→clear; Rejected; Cancelled
  with echoed qty; CancelRejected no-op), each pinning the **exact L5 call sequence + resulting
  `Status` + finalize**, derived from the order-lifecycle doc + the Exchange–Broker contract (§6-8). A
  **property**: release completeness — every non-filled unit is freed **exactly once** across the
  branches.
- **Market-buy reservation (Point 1)** — `maxSpend == L5 requiredCash == BestAsk*Qty + SlippageBuffer`
  (the `reserved == MaxSpend` invariant), and the `ReleaseExcess` buffer true-up on a filled market-buy.

**IMPLEMENTER-OWNED (objective, normal integration/table tests; assertions trace to the doc):**
`validateSpec` table; `nextOrderID` (format, monotonic, uniqueness); `handleCancel` (ownership guard,
not-cancellable, idempotency, mark-dirty, no-release); reserve-reason `errors.Is` mapping; the HTTP
status table; modify composition (gate-on-local-acceptance, partial-failure reporting, no rollback);
`-race` standing gate (all handlers on the owner, settlement routed in).