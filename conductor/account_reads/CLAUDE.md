# layer7/reads — Read & Account Endpoints (L7-3)

<!-- Provisional path; orientation's call. Note: this is L7's reads package — NOT broker/reads
     (which ships the account money reads). READ ORDER:
     1) /CLAUDE.md  2) layer7/CLAUDE.md  3) THIS file  4) docs/LAYER7_READ_AND_ACCOUNT_ENDPOINTS_IMPL.md -->

## Spec & authority
`LAYER7_READ_AND_ACCOUNT_ENDPOINTS_IMPL.md` is the spec (MC.*/IN.*/AO.* decisions). **No broker prereq**
— `EnsureAccount` is shipped. Three simple, mostly-independent doors the frontend hits around a session;
none mutate L7 state (catalog only *reads* the registry). **Code wins over doc.**

## What it IS (one line)
Three read-shaped doors: **`GET /v1/markets`** (catalog + liveness + session window), **`GET
/v1/instruments?market=`** (static instrument list), **`POST /v1/account/open`** (get-or-create account
→ handle). All obey the two walls: **money-blind** (open returns only `account_id`; balance/trades/pnl
are the broker's shipped `/v1/account/*` the frontend calls directly) and **H.8** (no prices — those are
market-data over the WS).

## Load-bearing rules
- **`GET /v1/markets` (P9+P10):** for each `listConfiguredMarkets()` tag → `loadConfig` (catalog +
  window) + `isCanonicalLive(tag)` (registry: `findLiveCanonical != nil`, lock only for the scan — no
  call-out inside) → emit `{tag, active, session:{opens_at, closes_at, duration, wall_clock}}`. Array
  membership == "supported" (no `supported` field). On-demand `od:` instances excluded (Mode==Canonical
  filter). **One bad market config → skip + log, not a whole-endpoint error**; empty catalog → `200
  {markets:[]}`. Window = **Option A current-or-next** via `nextSessionWindow` (config-only, anchors the
  **open boundary** not `open−lead`, weekday roll, independent of liveness).
- **`GET /v1/instruments?market=` (P8):** list-only from `MarketConfig.Instruments`; core fields
  `{id, tick_size}` (band_bps/seed_price optional-from-config, static, **never** LTP/quote/depth). Config
  read, **no registry, works even when the market is closed**. Missing `market` → 400; unknown tag → 404.
- **`POST /v1/account/open` (P7):** sole call `broker.EnsureAccount(ctx, lifecycle.OwnerHuman, user_id)`
  → `{account_id}`. **Idempotent — signup and login both hit it, both `200`** (not 201; no create-vs-fetch
  surfaced). **No registry, no lock, no XOR, no market param, money-blind.** Pass-through, nothing to roll
  back. Status: 400 bad/empty user_id or `ErrBadOwnerID` · 503 `store.ErrNoAccount`(retry-exhausted)/
  `ErrCreateFailed` · 504 ctx deadline · 500 `ErrBadOwnerType`/`ErrNegativeStarting*` (L7 bug/misconfig).
- All three mount on `Runtime.Mux`, reuse the shared `recoverPanic`/method-guard/`writeJSON`/`writeError`
  plumbing; handler owns its ctx timeout where it calls the broker.

## Deps & seam
L7-0 scaffold (`loadConfig`/`listConfiguredMarkets`, `Registry`, `findLiveCanonical` from L7-2 or add it
here if L7-2 not yet in), the spin-up config types + `atWallClock`/`isWeekend`/`LoadLocation` helpers,
shipped `funnel.EnsureAccount`. No market-data, no store reads.

## Do NOT
- serve any price/LTP/quote/depth (H.8 — WS only); read money tables (money-blind — open returns only
  the handle);
- take the registry mutex across a call-out (markets scan is lock-only-for-the-scan);
- touch the registry/XOR in account-open (it is session-less);
- return 201 or surface create-vs-fetch for account-open; 404 "no markets" (empty = 200).

## Flags (orientation)
- `findLiveCanonical` is defined in L7-2 (join). If L7-3 is built before/independently, lift it into the
  scaffold or define it here — one copy.
- `nextSessionWindow` reuses the spin-up timer's weekday/tz helpers but anchors the **open boundary**
  (not `open − preopen_lead`). Weekdays only (no holiday calendar) — same caveat as spin-up.
- Strict-login / `created` flag parked (P7-adjacent) — `EnsureAccount` always creates; fine for the
  trusted-identity prototype.

## Tests (objective; no goldens/auditor)
- `/v1/markets`: multi-tag catalog; `active` reflects a Live registry entry vs not; on-demand excluded;
  `nextSessionWindow` today-vs-next-weekday (injected `now`); one bad config skipped (others still 200);
  empty catalog → 200 `{markets:[]}`.
- `/v1/instruments`: list shape; missing param → 400; unknown tag → 404; works with no Live market.
- `/v1/account/open` (fake broker port): create & re-open both → 200 same id; the error→status map
  (503/504/500); missing user_id → 400.
- `-race` on the markets registry scan.