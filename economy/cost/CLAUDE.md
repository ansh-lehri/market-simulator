# economy/cost (Layer 5.3) — Cost & Tax Engine
<!-- Per-package CLAUDE.md. READ ORDER for any session working in this package, all four
     apply together:
       1) /CLAUDE.md            — root project constitution
       2) /economy/CLAUDE.md    — money-truth (L5) constitution
       3) THIS file             — what is specific to the cost package
       4) the spec doc below    — the authority for behavior
     This file does not repeat the two constitutions above; it only pins them to this
     package and states what is cost-specific. -->

## Spec (the authority — read in orientation, implement exactly, invent nothing)
- `docs/economy/LAYER_5_3_COST_ENGINE.md` — the whole doc (self-contained). It defines the
  interface, the India rulebook, the two compute methods, the GST split, and the CST-5 rule.

## What 5.3 IS (one line)
Pure arithmetic — turnover × injected rates — that **computes and returns** the friction (fees +
taxes) on a trade. It is not *price* (engine) and not *P&L* (5.4). It **never mutates, never
persists, never touches an account** — the caller (5.2 `Convert` / terminal step) debits what 5.3
returns, via `DebitCash`.

## Owns / public surface
- **Interface:** `CostModule{ ComputeFillCost(FillCostInput) CostBreakdown;
  ComputeOrderCost(OrderCostInput) CostBreakdown }` — the ONE contract every country implements;
  callers bind only to this.
- **India impl:** `IndiaCostModule{ rates IndiaRates }`; `IndiaRates` = the **injected** rulebook
  (STT/Exchange/SEBI/Stamp/GST as `Rate`, Brokerage/AutosquareFee as `int64` coins).
- **Types:** `FillCostInput`, `OrderCostInput`, `CostBreakdown{ Lines map[string]int64; Total int64 }`,
  `Side{Buy,Sell}`, `Rate{Num,Den int64}`.
- **Helpers:** `applyRate` (integer, single rounding convention) and `sum`.
- **US slot: empty.** Interface/inputs/output are already US-ready; do **not** write `USCostModule`
  this session.

## The rules that a wrong guess corrupts money (verbatim from the doc — hold exactly)
- **Brokerage = ₹20 per *executed* order, ONCE, never per fill; ZERO if the order never filled.**
  Lives ONLY in `ComputeOrderCost` (returns `Total: 0` when `!EverFilled`). It must **never** appear
  in `ComputeFillCost`. This is the single most bug-prone rule — keep it isolated. (5.3 only *computes*
  it from `EverFilled`; the fire-exactly-once guard is 5.2's job via the reservation flags.)
- **Side-awareness:** STT **sell-side only**; stamp **buy-side only**; exchange + SEBI **both sides**.
  The STT-on-buy bug is avoided structurally by the `if Side == Sell` / `if Side == Buy` guards —
  keep them.
- **GST split (Option A, locked):** `ComputeFillCost` charges GST on the per-fill **(exchange + SEBI)**;
  `ComputeOrderCost` charges the ₹20 **plus GST on the ₹20**. GST spans two granularities — do not
  fold it into one place.
- **CST-5 auto-square:** `IsAutosquare == true` adds the `autosquare_fee` line on top of the normal
  sell-side stack. No periodic/time-based fee exists — 5.3 fires **only** on fills and auto-squares,
  **never on a clock**.
- **Market-agnostic boundary:** the output `Lines` map is generic **data** (India keys:
  `stt/stamp/exchange/sebi/gst/autosquare_fee/brokerage`). **`Total` is the only universal field**
  callers depend on — never expose India-named fields as fixed struct fields.
- **Rates are injected** (`IndiaRates`), never hardcoded in logic. **Integer coins/ticks only**; every
  line routes through `applyRate` so rounding is uniform and replay is bit-identical.

## Deps & seam
- **Leaf — no economy deps.** Must NOT import `account` / `realize` / `reserve` / `store` /
  `lifecycle`.
- **Holds NO `Persistence`, does ZERO I/O** — no store, no clock, no randomness. Pure function of
  `(input, injected rules)`.
- The rulebook (`IndiaRates`) is injected config supplied by the caller/wiring — this package defines
  its shape, not its values-at-runtime.

## Do NOT (in this package)
- No I/O of any kind; **no log inside the compute path** (it must stay pure/replayable).
- **Never mutate or debit an account** — 5.3 computes, the caller debits. No `DebitCash`, no `Account`
  reference.
- Never put brokerage in `ComputeFillCost`; never charge brokerage per fill.
- Never apply STT to a buy or stamp to a sell.
- Never hardcode a rate in logic; never leak an India-named line key as a fixed output field.
- No `Persistence`, no goroutine, no mutex — single-threaded (single-writer assumed).
- Do not build the US module.

## Confirm BEFORE pinning any golden values (doc §8.2 — open items)
- **Rounding convention:** `applyRate` uses half-up as a *placeholder*. Lock the convention explicitly
  (it decides paise-level figures and bit-identical replay) before hand-computing goldens.
- **Rate re-verification:** the ₹20-flat-vs-"₹20-or-%-whichever-lower" cap and the STT rate
  (0.025% vs a reported 0.05%) were not re-verified in design — confirm the exact rate values first.
- Surface both in orientation; do not guess and bake them into tests.

## Tests (this package — pure, no DB, no Persistence spy)
- **Table-driven units** for `ComputeFillCost`: buy vs sell (STT only on sell, stamp only on buy,
  exchange/SEBI both, GST on exchange+SEBI), and the `IsAutosquare` branch adding `autosquare_fee`.
- **Table-driven** for `ComputeOrderCost`: `EverFilled == false` → `Total == 0`; `true` → brokerage +
  GST-on-brokerage.
- **Golden hand-computed India cases** (once rates + rounding are locked): a specific turnover → the
  exact per-line coin amounts and `Total`, showing the `applyRate` arithmetic.
- **`rapid` property tests:** `Total == sum(Lines)` always; buy breakdowns never contain `stt`, sell
  breakdowns never contain `stamp`.
- `-race` on; **single-threaded**; determinism is trivial here (pure) but assert same input → identical
  output.