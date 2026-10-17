# broker/reads — Read Surfaces (Tier 4)

<!-- The layout is now concrete (the broker is built through Phase 4). This package is broker/reads;
     the shard-side bodies it rides live in broker/funnel; the durable reads in broker/store.
     READ ORDER (all apply together):
       1) /CLAUDE.md                                            — root constitution
       2) broker/CLAUDE.md                                      — the broker (funnel) constitution
       3) THIS file
       4) docs/broker/BROKER_PHASE6_IMPLEMENTATION_DESIGN.md    — the spec (all 6 points LOCKED; §11 file-list, §12 order)
          docs/broker/BROKER_READ_SURFACES_tier4.md             — the Tier-4 design authority -->

## Spec & authority
`BROKER_PHASE6_IMPLEMENTATION_DESIGN.md` is the spec (it was written against the **real built** code and
lists its source authorities in its header). **Code wins over doc** — `broker/funnel`, `broker/store`,
`broker/transfer`, `broker/bringup`, `economy/account`, `economy/realize`, `economy/reserve` all exist;
verify every signature against them. The doc's decision log (P6-D1–D36) is all **LOCKED**; its §11
file-touch-list and §12 order are the build map.

## What it IS (one line)
The nine account `GET` read endpoints — **balance**, **positions**, **open orders**, **observation**,
**trades + pnl**, **costs + costs/summary**, and the **pre-session account_cash** read (`/v1/account/cash`).
A **`broker/reads` HTTP package** for all nine (they share one GET skeleton), **plus** the shard-
handler bodies/commands the live-family rides in `broker/funnel`, the `economy/reserve` read
accessor they need, and the `MarketDataCache` interface.

## THE BRIDGE (what's already built vs what Phase 6 adds)
- **Built, do not re-create:** `ReadBalance`/`ReadPositions` commands + facades + dispatch (as stubs
  returning empty snapshots); **all** `Store` durable reads (`RecentTrades`, `TradePage`, `PnLSummary`,
  `CostsPerTradeIN`, `CostsAggregateIN` + their shapes); the `transfer` HTTP-handler precedent; the
  `bringup/country.go` allowlist pattern.
- **Phase 6 adds:** the handler bodies + snapshot fields; two new commands (`ReadOpenOrders`,
  `ReadObservation`) + facades + dispatch + handlers; shared shard read helpers; the `broker/reads`
  package; and the cross-layer edits below.
- **Dependency, not a Phase-6 edit:** Phase 5's `handlePlaceOrder` is still a stub, so `tracks` +
  reservation records are empty — **A11/A14 return empty orders until Phase 5 lands.** Not a blocker.

## The two read families (this is the spine)
- **Live** (balance, positions, open orders, observation) → through the **funnel** to the owning
  shard; in-memory `economy` state. `!Present` (no live session) → **409**; handler-owned ctx timeout →
  **504** (`ErrBrokerTimeout`).
- **Durable** (trades, pnl, costs, costs/summary, account_cash) → straight to **`Store`** (Postgres),
  off-shard, no funnel, no live session. `ErrLoadFailed` (incl. ctx timeout) → **503** (**no 504**).
  - **List-shaped** (trades, costs, costs/summary, pnl): empty is a valid outcome →
    **200** with `[]` or zero-filled totals. **Never 404/409.**
  - **Singular-object** (account_cash): a missing account is genuinely not-found →
    **404** on `store.ErrNoAccount`. Deliberate carve-out from the list-family's no-404 rule, because
    `{trading_cash:0, bank_cash:0}` for a non-existent account would be a lie, not an empty list.
- **observation straddles:** one atomic live shard read **plus** one durable `Store.RecentTrades`,
  concurrent.

## Load-bearing rules
- **Copy-out (single-writer safety).** A shard read handler runs on the owner goroutine and returns
  **by-value** data only — never a pointer into the live `*Account` / `*ReservationRecord` / lot slice.
  `Cash` is a value struct; `View` returns `*record` dereferenced; position views carry scalars.
- **Money = bare `int64` units, NO currency/`unit` field anywhere** (P6-D33 — supersedes the Tier-4
  doc's `"unit":"paise"`). Costs are still *computed* per country, but *presented* as plain integers.
- **Shared helpers, one impl** (`funnel/reads.go`): `readBalance`, `readPositions`, `readOpenOrders` —
  reused by the single-read handlers **and** the A14 composite. Handlers are thin wrappers that add the
  liveness/`Present` check.
- **A8 balance:** direct 4-bucket `Cash` copy on the owner (no accessor returns all four;
  `ReadAvailableCash` is `Available` only). Path is `sess.account.Account.Cash`.
- **A9 positions:** weighted-avg via `realize.ComputeWeightedAvgEntry(lots)`; **gross** mark via
  `realize.ComputeUnrealizedMark(lots, ltp, 0)` (0 incurred-cost ⇒ net==gross ⇒ read gross). **Mark
  degrades**: `mktCache == nil` (or cold) → `MarkAvailable:false`, position + avg still returned. Flat
  ≠ no-session: empty positions → **200 `{positions:[]}`**, only no-session → 409. Optional
  `instrument` filter, applied **shard-side**. **No `NetUnrealized` field** (absent, not 0 — deferred).
- **A11 orders:** index by `tracks` (the open set — terminal orders already removed), join
  money/qty/side/**limit price** via `resv.View(orderID)`. **Live-only** (409 on no session; history is
  A10). Needs the L5 reserve edits (below).
- **A14 observation:** **one atomic shard command** (`ObservationLive` = balance + positions + orders
  via the shared helpers — **not three round-trips**, which could interleave with a fill) **plus**
  concurrent durable `RecentTrades(…, 20)` (the account's **own** closed trades, `WHERE account_id=$1`
  — not market-wide), merged in the HTTP handler on one ctx. **Degrade-don't-fail:** only the **live**
  half drives 504/409/500; a durable failure → `recent_trades:[]`, `trades_degraded:true`, **still 200**.
- **A10 trades/pnl:** pure durable — `Store.TradePage`/`PnLSummary` are built; this is **HTTP + shaping
  only**. Keyset pagination on `exit_at` (`next_before` = last row's `exit_at`; a short/empty page is
  the last page). `/pnl` is a **separate** endpoint (the account-wide aggregate isn't recomputed per
  page). No 409/404.
- **A16 costs:** read-side **country dispatch** — `country` is **required**; `IN` → `CostsPerTradeIN`/
  `CostsAggregateIN`, `US` → **501** (honest, slot defined), else → **400**. The handler is
  **country-agnostic** — it sees only generic `[]Line`/`Total`; column/tax-name knowledge stays
  quarantined in the store + `country/india`. `Total` is the **stored** column, never recomputed; only
  non-zero lines surface. Same keyset cursor as A10.
- **account_cash (pre-session cash view):** pure durable read via `Store.AccountCash`; returns the
  signed `trading_cash` + non-negative `bank_cash` pair as **bare int64 fields** (no unit, no
  envelope). Answers the signup/login flow — a user with an `account_id` but no live session yet
  must still see their money, which the live-family `/v1/account/balance` cannot provide (409
  without a session). Status map: 200 ok · 400 missing `account_id` · **404** on `store.ErrNoAccount`
  (singular-object carve-out above) · 503 on `store.ErrLoadFailed` incl. ctx timeout · 500 unknown.
  Store returns the raw signed trading_cash — no shaping, no comparison; the handler stays a
  pass-through.
- **L5 reserve edits (X1/X2/X3) — the one real cross-layer change:** add `LimitPrice int64` to
  `ReservationRecord` (`book.go`); set `LimitPrice: order.LimitPrice` in **both** `reserveBuy` and
  `reserveSell` (`reserve.go`); add `func (s *ReservationService) View(orderID) (ReservationRecord,
  bool)` returning `*rec` dereferenced or `(zero,false)` (`service.go`). This is the single read path
  for A11 **and** Phase-5 completion (**supersedes** P5-D22's non-existent `RemainingQty`/`IsComplete`).
- **`MarketDataCache` (X4):** an interface in `funnel/marketdata.go` (`BestAsk`, `LastPrice`); a
  **nil-able** `mktCache` field on `shard`, threaded through `NewBroker`/`newShard` (**param last**,
  bringup is the only caller, passes `nil` for now). Every mark branch guards `if mktCache != nil`.
  Real impl + wiring = **Phase 7**.

## Deps & seam
`broker/funnel` (`route`, buffered-1 reply, `ErrBrokerTimeout`, `ownedSession`, `tracks`, `resv`,
`realiz`); `broker/store` (the five durable reads + shapes — **built**); `economy/account`
(`Cash`/`Position`/`Lot`), `economy/realize` (`ComputeWeightedAvgEntry`/`ComputeUnrealizedMark` —
**built**), `economy/reserve` (`View` — **new, X3**); `broker/transfer` (HTTP precedent); `broker/
bringup` (route registration + `reads.Config{ReadTimeout}` + `nil` cache). `*funnel.Broker` satisfies
`brokerPort`, `*store.Store` satisfies `storePort`; tests inject stubs.

## Do NOT
- recompute any P&L, cost, or mark figure — project stored columns / call the **built** `realize` funcs;
- leak a pointer into live `*Account`/`*ReservationRecord`/lots (copy-out only);
- add a `unit`/currency field anywhere;
- make three round-trips for A14 (one atomic command);
- emit 409 on any durable read, or 504 on any durable read (`ErrLoadFailed` → 503);
- emit 404 on a **list-shaped** durable read (empty is valid → 200 with `[]` / zero-filled totals).
  The **singular-object** durable read (`account_cash`) deliberately does emit 404 on
  `ErrNoAccount` — see the account_cash rule above;
- block A9/A14 on the cache (degrade — `MarkAvailable:false`);
- teach the A16 handler column/tax names (dispatch to store methods; keep it country-agnostic);
- back-calculate `LimitPrice` from `ReservedCash` (store it — partial fills/sells/market-buffer make it
  unreliable).

## Flags (surface in orientation, don't silently resolve)
- **L5 reserve change (X1/X2/X3)** is a prerequisite for A11/A14 and **also resolves Phase-5 P5-D22** —
  whoever builds first does it. The accessor is `View(orderID) (ReservationRecord, bool)` (supersedes
  the interim `PREREQ_L5_ReservationView.md`, which had the wrong name/shape and missed `LimitPrice`).
- **`NewBroker` signature changes** (add `mktCache MarketDataCache`, last arg). Bringup is the only
  caller; funnel-internal tests pass `nil`.
- **Marks degrade** until Phase 7 wires a real `MarketDataCache` (A9/A14 `MarkAvailable:false`).
- **Net-unrealized P&L deferred** — needs an L5 write-path change (`Position.IncurredEntryCosts`
  accumulator in `reserve.Convert` + snapshot). Gross only for now.
- **A11/A14 orders are empty until Phase 5's `handlePlaceOrder` body lands** (X8 dependency).

## Tests — objective, normal (no goldens-first, no auditor)
Every read is a projection or a call into already-built L5 compute, so there's no self-consistent-wrong
risk. Integration + table, assertions trace to the doc's status maps:
- **live:** balance 4 buckets + 409 no-session; positions weighted-avg + gross mark (stub `mktCache`) +
  degrade (nil cache → `MarkAvailable:false`) + flat → 200 empty + instrument filter + 409; orders join
  `tracks`+`View` (seed a record) + 409; observation atomic trio + concurrent trades + degrade (durable
  fail → `trades_degraded`, still 200) + 409/504.
- **durable:** trades keyset pagination (`next_before`, short=last) + empty → 200 + `ErrLoadFailed` →
  503; pnl summary incl. breakeven counting; costs dispatch (IN→200, US→501, bad/missing→400) +
  non-zero-lines-only + empty → 200; account_cash status map (200 ok / 400 missing param /
  **404 `ErrNoAccount`** / 503 wrapped `ErrLoadFailed` / 500 unknown / 405 non-GET) + signed
  trading_cash passthrough + two-field bare-int64 body shape + store-call witness.
- **L5 reserve:** `View` returns a copy + `(zero,false)` on unknown id; `LimitPrice` set in
  `reserveBuy`/`reserveSell` (0 for market).
- `go test ./... -race` standing gate (read commands + A14 concurrent fan-out).