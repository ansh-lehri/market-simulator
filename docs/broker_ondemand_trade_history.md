# Broker — On-Demand In-Session Trade Visibility (A14 extension)

**Scope:** make an on-demand session's **closed trades viewable during the session**, through the
A14 observation read. Covers the retention of the assembled trade rows, the shard read, and the
A14 handler merge. Does **not** add any durability — on-demand still persists nothing.

**Status:** DESIGN — grounded against `economy` source + the locked broker docs; implementation
open.

**Where it sits:** an extension to A14 (`BROKER_READ_SURFACES_tier4.md` — the Tier-4 read
surfaces) and the on-demand realization path (`LAYER_5_4_REALIZATION.md`). Touches one line of
Phase-2 teardown (`BROKER_PHASE2_IMPLEMENTATION_DESIGN.md` Point 4).

**Authority / grounding:** `LAYER_5_4_REALIZATION.md` (where closed trades are assembled + the
`PersistOnClose` gate), `BROKER_TIER0_IMPLEMENTATION_DESIGN.md` Point 6 (the shard *is* the
`AccountLedger`), `BROKER_READ_SURFACES_tier4.md` §5 (A14), read against the `economy/realize`,
`economy/account`, and `economy/capstone` source.

---

## 0. Why this exists — the gap

On-demand (practice) sessions set `PersistOnClose = false`. The realization path
(`LAYER_5_4_REALIZATION.md`) reads that flag and, on a position-flattening fill, **assembles the
closed-trade row and then drops it** — no enqueue to the write-behind buffer, nothing in
`participant_trades` (the durable Postgres closed-trade log). That is the correct, defining
on-demand property: *nothing persists, gone after close.*

But both trade-read surfaces read Postgres: A10 (`/v1/account/trades`, `/pnl`) is purely durable,
and A14's (`/v1/account/observation`) `recent_trades` field also comes from Postgres
(`store.RecentTrades`). So for an on-demand session, closed trades were **invisible even while the
session was live** — the trade *effects* showed up in balance/positions (live shard read), but the
trade *list* came back empty.

Original intent was ephemeral-and-invisible. As the system evolved, the requirement changed:
**show the in-session trades for the session's duration.** After close they still vanish — that part
is unchanged.

## 1. Grounded facts this design rests on

- **Where trades are born** (`economy/realize`, `Realizer.OnSellFill`): per sell fill, on
  `PositionNowFlat`, `acc.assemble(exit_reason)` produces a fully-formed **`ParticipantTradeRow`**
  (`TradeID, AccountID, SessionID, InstrumentID, Direction, Quantity, EntryPrice, ExitPrice,
  EntryAt, ExitAt, GrossPnl, TotalCosts, NetPnl, PnlPct, ExitReason`) + a cost child. The gate:
  `if acc.PersistOnClose { persist.EnqueueClosedTrade(row, costRow) }` — else the row is discarded.
- **One shared `*Account` per live session** (`BROKER_TIER0_IMPLEMENTATION_DESIGN.md` Point 6):
  the shard **is** the `AccountLedger` — `func (s *shard) Get(aid) *account.Account { return
  s.sessions[aid].account.Account }`, a live pointer view over `s.sessions`, not a copy. The
  per-shard realizer, the reserver, and the observation read all resolve to the **same** `*Account`
  the `ownedSession` holds. (The `capstone.memoryLedger` is a documented test stand-in; the shard
  is the real registry.)
- **Import-cycle constraint:** `ParticipantTradeRow` lives in `realize`, and `realize` imports
  `account`. A `[]realize.ParticipantTradeRow` field on `account.Account` would be an import cycle.
  → the in-session log cannot live on `account.Account`; it lives on the **realizer**.
- **A14 today** (`BROKER_READ_SURFACES_tier4.md` §5): `handleReadObservation` on the shard returns
  `LiveObservation{Present, Cash, Positions, Orders}`; the HTTP handler fans out the live shard read
  **plus** a durable `store.RecentTrades`, and merges — degrading `recent_trades` to `[]` if the
  durable read fails.

## 2. The design — three seams

### 2.1 Retention — `economy/realize`

The in-session trade log lives on the realizer (same package as the row type; no import cycle),
keyed by account. The flush gate retains instead of dropping, for on-demand only:

```go
type Realizer struct {
    ...
    sessionTrades map[AccountID][]ParticipantTradeRow   // on-demand only; in-session; never persisted
}

// flush gate (position flat):
if acc.PersistOnClose {
    s.persist.EnqueueClosedTrade(row, costRow)                      // canonical → Postgres (unchanged)
} else {
    r.sessionTrades[acc.AccountID] = append(r.sessionTrades[acc.AccountID], row)   // on-demand → in-session
}

// read accessor (called on the shard goroutine):
func (r *Realizer) SessionTrades(aid AccountID) []ParticipantTradeRow { return r.sessionTrades[aid] }

// teardown eviction (see §3):
func (r *Realizer) ClearSession(aid AccountID) { delete(r.sessionTrades, aid) }
```

`ParticipantTradeRow` already carries every field the external trade view needs, so it shapes 1:1
to the `recent_trades` shape — a live row is indistinguishable from a durable one to the client.

### 2.2 Shard read — `handleReadObservation`

The live read returns the in-session trades plus an **ephemeral** flag (so the handler knows the
durable read is pointless):

```go
cmd.Reply <- LiveObservation{
    Present:       true,
    Cash:          copyCash(sess.loaded.Account.Cash),
    Positions:     buildPositionViews(...),
    Orders:        buildOrderViews(...),
    Ephemeral:     !sess.loaded.Account.PersistOnClose,                    // on-demand
    SessionTrades: buildTradeViews(s.realizer.SessionTrades(cmd.AccountID)),
}
```

`LiveObservation` gains `Ephemeral bool` and `SessionTrades []TradeView`. `buildTradeViews` is the
same 1:1 shaping already used for durable rows.

> **Field-path note:** uses `sess.loaded.Account` per the Phase-2 correction (`ownedSession.loaded`
> is a `LoadedAccount`). The Tier-0 / Tier-4 docs still show the older `sess.Account` /
> `s.sessions[aid].account.Account` — same structure, stale field name (same drift logged as P3-D6
> in the Phase-3 doc).

### 2.3 A14 handler — the merge becomes mode-aware

```
live = <shard observation read>
if live.Ephemeral:                                   // on-demand
    recent_trades = live.SessionTrades               // from the live in-session log
    // SKIP the durable store.RecentTrades read entirely — known-empty for on-demand
else:                                                // canonical
    recent_trades = store.RecentTrades(...)          // unchanged, buffer-lag caveat and all
```

Same endpoint, same response shape; the source of `recent_trades` is the live log for on-demand,
the durable log for canonical. The on-demand path also drops one pointless Postgres query + the
fan-out.

## 3. Lifecycle & correctness

- **Single-writer:** both the append (realization, on fill-apply) and the read (observation
  handler) run on the **owning shard's goroutine** — no lock, no race. Same discipline as `s.tracks`.
- **Discarded at close:** on-demand teardown routes to `session_discard`; the in-session log is
  never persisted and dies with the session — the defining property is preserved.
- **Teardown eviction (required).** `sessionTrades` is keyed by account on a long-lived per-shard
  realizer, so a **reused** on-demand account on the same shard would otherwise see a prior
  session's trades. Phase-2 teardown (`BROKER_PHASE2_IMPLEMENTATION_DESIGN.md` Point 4,
  `TeardownSession` / `SessionDiscard`) must call `realizer.ClearSession(aid)`. Correctness, not
  optional.

## 4. What this deliberately leaves alone

- **Canonical path** — untouched; `sessionTrades` is populated only for `!PersistOnClose`.
  Canonical still flows to `participant_trades` and reads durably.
- **A10** (`/v1/account/trades`, `/pnl`) — stays purely durable ("never the shard"). On-demand has
  no persistent history by design; A10 returning empty for it is correct.
- **In-session P&L summary** — out of scope (the ask was the trade list). The realizer already
  accumulates the pieces; an in-session aggregate is an easy follow-on if wanted.
- **Ring bound** — the read already clamps to ~20 recent; practice sessions are short. Left
  uncapped-per-session unless a cap is later wanted.

## 5. Decision log

- **OD-1** — in-session trade log lives on the **realizer** (`realize` pkg), keyed by `AccountID`,
  **not** on `account.Account` (import cycle). Read via `SessionTrades(aid)`; shard exposes it to
  the observation handler.
- **OD-2** — retained only for `!PersistOnClose`; served **only** via A14; A10 stays durable-only.
- **OD-3** — A14 handler is mode-aware: `Ephemeral` → serve `SessionTrades`, skip the durable
  read; else durable as before. `LiveObservation` gains `Ephemeral` + `SessionTrades`.
- **OD-4** — Phase-2 teardown must `realizer.ClearSession(aid)` — stale-trade correctness for
  reused on-demand accounts.

## 6. Cross-doc annotations needed

- `LAYER_5_4_REALIZATION.md` §8 — the on-demand branch currently "computes and displays … enqueues
  nothing / drops." Annotate: for on-demand it now **retains** the row in
  `Realizer.sessionTrades` (served live via A14), still no persistence.
- `LAYER_5_2_LIFECYCLE_FLOWS.md` and `L2_ONDEMAND_DESIGN.md` — both claim on-demand P&L is
  "visible in-session"; that was aspirational (no read path existed). Point them here; it is now
  true for the **trade list** via A14 (and remains invisible *after* close).
- `BROKER_READ_SURFACES_tier4.md` §5 (A14) — note the mode-aware `recent_trades` source and the
  new `LiveObservation` fields.

## 7. Seams touched (implementation checklist)

- `economy/realize` — `Realizer.sessionTrades` map, the flush-gate `else` branch,
  `SessionTrades(aid)`, `ClearSession(aid)`.
- Shard observation handler + `LiveObservation` — `Ephemeral`, `SessionTrades`, `buildTradeViews`.
- A14 HTTP handler — mode-aware merge (skip durable read when `Ephemeral`).
- Phase-2 teardown — one `realizer.ClearSession(aid)` call.

*Verify the exact `realize` flush-gate location and the shard's realizer field name against source
at implementation time; this design is read-grounded but the edits land in `economy`.*