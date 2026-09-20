Layer 5.3 — Cost & Tax Engine (Market-Agnostic Cost Computation)

Scope: the cost/tax subcomponent of Layer 5 — items 33–36 on the Layer-5 action list. Covers the per-fill turnover/statutory stack, the per-executed-order charge (brokerage), the CST-5 auto-square fee line, and the itemized breakdown output — all behind a market-agnostic interface so a new country is a module swap, not a code change. Design flow and the Go structs / method shapes for each. Includes the 5.2 wiring correction that the brokerage rule forces.

What this document is NOT: it is not the 5.1 Tier-1 hands (LAYER_5_1_OPERATIONS.md), not the 5.2 reservation methods (referenced by their locked names — Convert, Release, ReleaseExcess), and not the 5.4 realizer (LAYER_5_4_REALIZATION.md, which consumes 5.3's output). It does not design the Tier-2 persistence write path — 5.3 does zero I/O.

Status: items 33–36 LOCKED at design-and-flow depth, in the market-agnostic shape decided during design. The India cost stack is the filled rulebook slot; the US slot is explicitly empty. Two items carried at implementation: the rounding convention (lock once, §2.3) and the ₹20-cap nuance (§6.4). The GST-split decision (Option A) is recorded in §5. Open/deferred in §8.

Naming. Doc labels use snake_case for readability; Go identifiers are PascalCase for all exported names; unexported helpers are camelCase.

0. Orientation — what 5.3 is, and the one architectural rule

5.3 owns the friction: the fees + taxes skimmed on every trade, beyond the share price itself. It is distinct from price (the engine's job) and P&L (5.4's job). It is pure arithmetic — turnover × rates — that computes and returns; it never mutates, never persists, never talks to a store. The debit of what it computes is the caller's job (5.2's Convert/terminal step, via the Tier-1 DebitCash hand). This purity is what makes 5.3 trivially reproducible and testable: same input + same injected rulebook → same output, always.

0.1 The market-agnostic rule (the architectural decision)

The single most important decision in 5.3: the whole cost stack (statutory charges and tax) lives inside a per-country module, but every country module implements the same interface, so inputs and outputs are identical no matter which country is plugged in. Nothing outside 5.3 (Convert, 5.4) ever knows or cares which country it is — callers bind only to the interface.

Why per-country modules and not a shared "list of rate lines": the mechanism differs between countries, not just the numbers. India's GST is 18% on a subset of lines (brokerage + exchange + SEBI, not STT/stamp); the US has no GST at all but has its own logic (SEC fee, TAF per-share not per-value, FINRA). You cannot capture "how tax is computed" as a rate on a line — so the whole computation is the country's, encapsulated. Adding the USA = write a USCostModule + a US rulebook and inject it; zero change to the generic engine or to any caller.

Convert / terminal step  ──talks only to──►  CostModule (interface)
                                                  │  implemented by
                                    ┌─────────────┴─────────────┐
                              IndiaCostModule              USCostModule (future, empty slot)
                              (STT/stamp/exch/SEBI/GST,     (SEC/TAF/FINRA, no GST)
                               ₹20 brokerage, CST-5)
0.2 Locked value discipline
All money is integer coins (int64); all prices integer ticks (int64). Never float — float money is a bug. Rates are applied through one integer helper (§2.3).
5.3 does zero I/O — no Redis, no Postgres, no snapshot marks. Pure function of (input, injected rules).
1. The India cost stack (the filled rulebook slot)

Verified real India intraday rates (≈ Sept 2026), locked as the India rulebook slot:

Brokerage = flat ₹20 per executed order (+GST). Per executed order — once — not per fill.
STT = 0.025%, sell side only (no buy-side STT).
Exchange transaction charge ≈ 0.00297% of turnover, both sides.
SEBI turnover fee = ₹10 per crore (~0.0001%), both sides.
Stamp duty = 0.003%, buy side only.
GST = 18%, on (brokerage + exchange + SEBI) only — NOT on STT or stamp.
CST-5 auto-square fee — an extra line applied only on a forced session-end close (is_autosquare). Amount provisional (a dial); the mechanism is a distinct cost line.

Dropped / deferred for the intraday prototype (recorded so their absence is deliberate):

Income tax — deferred (intraday = speculative-business-income at slab; no STCG/LTCG, no holding-period logic). Per-trade costs already deliver the "costs eat profit" lesson.
DP charges — dropped (intraday = no demat movement; charging one would be unrealistic).
AMC — dropped this session. 5.3 has no periodic/time-based fee; it fires only on fills and auto-squares, never on a clock.
1.1 Locked mechanics (the design decisions behind the rates)

(1) Granularity — per-fill vs per-order:

Turnover-based charges (STT, exchange, SEBI, stamp) are linear (rate × traded_value) → accrue per fill, summed (mathematically identical to computing once on the total; per-fill fits 5.2's incremental Convert).
Brokerage (₹20) is per executed order — once, NOT per fill. An order that fills in 5 pieces still pays one ₹20. Attached at order termination; an order fully cancelled before any fill pays zero brokerage. (This is the one non-obvious rule and the most likely implementation bug — isolated in its own method, §4.)

(2) Timing — real-time debit: costs debit the trading account in real-time as they accrue (per-fill turnover; brokerage at completion), matching instant settlement and reinforcing "you watch costs eat the position."

(3) Side-awareness (required): the per-fill input carries side. STT sell-only; stamp buy-only; exchange/SEBI/brokerage both sides; GST on (brokerage + exchange + SEBI). (A common real-world bug is applying STT to the buy leg — explicitly avoided.)

(4) Auto-square fee (CST-5): an is_autosquare flag → apply the normal sell-side stack plus the CST-5 line.

2. The universal contract
2.1 The interface every country implements

CostModule is the one interface Convert and 5.4 talk to. Same input in, same output out, for every country. Swapping countries cannot break a caller because the caller only sees CostModule. (Go interface satisfaction is the "inheritance" intended here: one contract, many implementations, callers bound to the contract.)

go
// CostModule — the ONE interface every country implements. Convert and 5.4 talk ONLY to this.
// Same input in, same output out, for every country. Swapping countries cannot break a caller.
type CostModule interface {
    ComputeFillCost(in FillCostInput) CostBreakdown   // per-fill turnover/statutory charges
    ComputeOrderCost(in OrderCostInput) CostBreakdown // per-executed-order (brokerage-like)
}

type Side int
const (
    Buy Side = iota
    Sell
)
2.2 Identical inputs (same struct for every country)

A country ignores fields it does not use; no country gets a different signature.

go
// FillCostInput — identical for every country. India uses Side/FillQty/MakerPrice/IsAutosquare;
// other countries use what they need and ignore the rest.
type FillCostInput struct {
    Side         Side
    FillQty      int64
    MakerPrice   int64 // integer ticks
    IsAutosquare bool  // CST-5 trigger (India); other countries may ignore
}

// OrderCostInput — identical for every country. Drives the per-executed-order charge (brokerage).
type OrderCostInput struct {
    EverFilled bool  // false => per-executed-order charge is zero (per-executed-order rule)
    TotalQty   int64 // for markets that tier brokerage by size (India ignores)
    Side       Side  // some markets' per-order fees are side-aware
}
2.3 Identical output (country-agnostic breakdown)

The output cannot have India-named fields (STT, GST) or the US breaks it. Lines are data, keyed by name; Total is the only fixed field everyone downstream depends on.

go
// CostBreakdown — country-agnostic output. Lines keyed by name (India: stt/exchange/sebi/stamp/gst/
// autosquare_fee; USA would emit sec_fee/taf/finra). Total is the universal field callers read.
type CostBreakdown struct {
    Lines map[string]int64 // line name -> amount (coins)
    Total int64            // sum of Lines — the universal field
}

So Convert calls costModule.ComputeFillCost(in), gets back {Lines, Total}, debits Total via DebitCash, and hands Lines to 5.4 for the itemized view — never mentioning STT or GST. Swap in USCostModule and Convert is byte-for-byte unchanged; it just sees different keys in Lines.

2.4 Integer-rate helper (shared utility, not country logic)
go
type Rate struct{ Num, Den int64 } // e.g. 0.025% = {25, 100000}

// applyRate — integer turnover charge with ONE rounding convention (half-up), applied everywhere
// so results are reproducible and consistent across every line.
func applyRate(value int64, r Rate) int64 {
    return (value*r.Num + r.Den/2) / r.Den // half-up
}

func sum(m map[string]int64) int64 {
    var t int64
    for _, v := range m {
        t += v
    }
    return t
}

Rounding convention — lock once. Half-up is the placeholder used here. It affects whether paise-level figures match a real broker and whether replays are bit-identical. Every cost line routes through applyRate, so the convention must be locked once, at implementation.

3. Item 33 (+ 35, + 36) — ComputeFillCost (per-fill turnover stack)
3.1 What it is

The per-fill call Convert invokes once per fill. It computes the turnover-based lines (STT, exchange, SEBI, stamp, and the per-fill GST) plus the CST-5 line when the close is a forced auto-square. Brokerage is NOT here — it is per-order (item 34, §4). It emits the itemized CostBreakdown (item 36).

Side-awareness: STT sell-only, stamp buy-only, exchange/SEBI both sides — the classic STT-on-buy bug is structurally avoided by the if Side == Sell guard.

3.2 India implementation
go
// IndiaRates — injected config (the rulebook slot). NOT hardcoded in logic.
type IndiaRates struct {
    STT           Rate  // 0.025% sell-side only
    Exchange      Rate  // ~0.00297% both sides
    SEBI          Rate  // ~0.0001% both sides
    Stamp         Rate  // 0.003% buy-side only
    GST           Rate  // 18% on (brokerage + exchange + sebi)
    Brokerage     int64 // ₹20 flat per executed order (coins)
    AutosquareFee int64 // CST-5 (provisional dial)
}

type IndiaCostModule struct{ rates IndiaRates }

// ComputeFillCost — per-fill turnover charges + the GST that applies to per-fill lines.
// India's mechanism, fully encapsulated. Side-aware: STT sell-only, stamp buy-only,
// exchange/SEBI both. Brokerage is NOT here (per-executed-order — see ComputeOrderCost).
func (m IndiaCostModule) ComputeFillCost(in FillCostInput) CostBreakdown {
    lines := map[string]int64{}
    tv := in.MakerPrice * in.FillQty // turnover for this fill (integer ticks × qty)

    if in.Side == Sell {
        lines["stt"] = applyRate(tv, m.rates.STT) // sell-side ONLY (never on buy — the classic bug)
    }
    if in.Side == Buy {
        lines["stamp"] = applyRate(tv, m.rates.Stamp) // buy-side ONLY
    }
    lines["exchange"] = applyRate(tv, m.rates.Exchange) // both sides
    lines["sebi"]     = applyRate(tv, m.rates.SEBI)     // both sides

    // GST 18% on (exchange + sebi) at the per-fill level. Brokerage's GST is charged with
    // brokerage at order-cost time (the per-order line isn't in this per-fill object) — see §5.
    lines["gst"] = applyRate(lines["exchange"]+lines["sebi"], m.rates.GST)

    if in.IsAutosquare {
        lines["autosquare_fee"] = m.rates.AutosquareFee // CST-5 extra line on forced close
    }

    return CostBreakdown{Lines: lines, Total: sum(lines)}
}
3.3 Items covered here
33 — the turnover stack (STT/exchange/SEBI/stamp), side-aware, integer rates.
35 — the CST-5 auto-square line (IsAutosquare branch).
36 — emits the itemized CostBreakdown{Lines, Total} — the universal output consumed by Convert (debit) and 5.4 (three-line view).
4. Item 34 — ComputeOrderCost (brokerage; the ₹20-once rule)
4.1 What it is

The single most bug-prone rule in the component. Brokerage is ₹20 per executed order — once, charged when an order that filled reaches its terminal state; ZERO if it never filled (fully cancelled before any fill). Isolated in its own method so the "per executed order" logic lives in one place.

4.2 India implementation
go
// ComputeOrderCost — per-executed-order charge (brokerage + its GST). Charged ONCE when an order
// that filled reaches its terminal state. Zero if the order never filled ("per executed order").
func (m IndiaCostModule) ComputeOrderCost(in OrderCostInput) CostBreakdown {
    if !in.EverFilled {
        return CostBreakdown{Lines: map[string]int64{}, Total: 0} // no fills => no brokerage
    }
    lines := map[string]int64{"brokerage": m.rates.Brokerage}
    lines["gst"] = applyRate(m.rates.Brokerage, m.rates.GST) // GST on brokerage (India's rule)
    return CostBreakdown{Lines: lines, Total: sum(lines)}
}
4.3 When it fires — the RemainingQty == 0 transition (and the cancel case)

Brokerage is not charged inside the per-fill path. It fires when an order that filled reaches its terminal state, exactly once, guarded by a flag on the reservation record. The clean signal is the moment RemainingQty hits 0 inside Convert — that is "order fully executed," and it is only reachable via Convert, which only runs on a fill, so EverFilled is trivially true there. Zero brokerage for never-filled orders falls out for free: a cancelled-unfilled order goes through Release, never Convert, so it never hits this block.

The one case where brokerage lives outside Convert: a partial-fill-then-cancel order. It filled (so brokerage is owed — "per executed order" = ≥1 fill), but it reaches its terminal state in Release, not at RemainingQty == 0 in Convert. So Release also checks and charges brokerage. A BrokerageCharged flag on the reservation record ensures it fires exactly once regardless of which method sees the terminal state.

ReleaseExcess needs no brokerage block — it runs only after a market buy that fully filled, so Convert already charged brokerage when RemainingQty hit 0. (A BrokerageCharged == true assertion there is a reasonable invariant check.)

4.4 Brokerage is country-dependent and order-level

Brokerage is not a special hardcoded ₹20 — it is the per-executed-order line of the India module. Another country's brokerage (or $0 zero-commission) is just its own ComputeOrderCost. And per the locked cost decision, brokerage is order-level: it hits the account at the order's terminal step, but is not attributed into any single trade's trade_costs row or net_pnl (5.4 excludes it) — it is presented at order level, the way a real broker's contract note shows it.

5. The GST split (Option A — recorded)

GST is 18% on (brokerage + exchange + SEBI). But exchange + SEBI are per-fill (computed in ComputeFillCost) and brokerage is per-order (computed in ComputeOrderCost), so GST spans two granularities and cannot be fully computed in either place alone.

Option A (locked): per-fill ComputeFillCost charges GST on the per-fill (exchange + SEBI); ComputeOrderCost at termination charges the ₹20 plus GST on the ₹20. GST is split across the two events, each covering its own base. Chosen because it matches the locked real-time-debit timing — each fill's cost is complete when charged, so the running number bleeds as you watch. (The rejected Option B deferred all GST to termination, which would leave per-fill debits slightly incomplete until a termination true-up — fighting the real-time-bleed design.)

6. The 5.2 wiring correction (what the brokerage rule forces)

The brokerage rule (§4) is the one real change to the earlier 5.2 flows: Convert/Release must call ComputeOrderCost at the order's terminal step, guarded by a once-only flag. This section records the exact wiring so it is not lost.

6.1 Reservation-record additions
go
// The reservation record (5.2's book, whole-economy scope) gains:
//   EverFilled       bool  // set true on the first Convert; drives ComputeOrderCost
//   BrokerageCharged bool  // ensures ComputeOrderCost fires exactly once per order
//   OrigQty          int64 // original order qty, for OrderCostInput.TotalQty
6.2 In Convert — after shrinking the record (both sides)

Replaces the old "delete if fully filled" block:

go
    rec.EverFilled = true // any Convert means at least one fill happened

    // Order fully executed → charge the per-executed-order cost (brokerage) ONCE, here.
    if rec.RemainingQty == 0 && !rec.BrokerageCharged {
        oc := s.costs.ComputeOrderCost(OrderCostInput{
            EverFilled: rec.EverFilled, TotalQty: rec.OrigQty, Side: rec.Side,
        })
        if oc.Total > 0 {
            a.DebitCash(Available, oc.Total)               // Option B debit (may drive Available < 0)
            s.events.Emit(ConvertEvent{ /* ...brokerage line... */ Cost: oc})
        }
        rec.BrokerageCharged = true
    }

    // existing cleanup (fully-filled record removal)
    if rec.RemainingQty == 0 && rec.ReservedCash == 0 && rec.ReservedUnits == 0 {
        delete(s.book.records, orderID)
    }
6.3 In Release — the partial-fill-then-cancel case
go
    // If this order ever filled but brokerage wasn't charged (it terminated via cancel, not via
    // RemainingQty==0 in Convert), charge the per-executed-order cost now — once.
    if rec.EverFilled && !rec.BrokerageCharged {
        oc := s.costs.ComputeOrderCost(OrderCostInput{
            EverFilled: true, TotalQty: rec.OrigQty, Side: rec.Side,
        })
        if oc.Total > 0 {
            a.DebitCash(Available, oc.Total)
            s.events.Emit(ReleaseEvent{ /* ...brokerage line... */ })
        }
        rec.BrokerageCharged = true
    }
    // then the existing release logic
6.4 ReleaseExcess

No brokerage block (a fully-filled market buy already charged brokerage in Convert). Optional: assert rec.BrokerageCharged == true as an invariant check.

7. Auto-square (how 5.4 uses 5.3)

At session-end auto-square (5.4 §5), 5.3 is called exactly as for a voluntary close, with two differences flowing through the same inputs:

ComputeFillCost(FillCostInput{Side: Sell, FillQty: openQty, MakerPrice: closingPrice, IsAutosquare: true}) — the IsAutosquare flag adds the CST-5 autosquare_fee line (the only economic difference from a voluntary close).
ComputeOrderCost(OrderCostInput{EverFilled: true, ...}) — the forced order is an executed order, so order-level brokerage applies, once.

5.3 itself has no auto-square-specific branch beyond the IsAutosquare flag it already handles; the trigger, the direct Tier-1 book-out, and the roster are 5.4's concern.

8. Cross-cutting properties, open & deferred
8.1 Cross-cutting
5.3 does zero I/O — pure function of (input, injected rules); fully reproducible (no clock, no randomness, no store read).
5.3 computes; the caller debits — Convert (per fill) and the terminal step (per order) call the Tier-1 DebitCash hand with the returned Total. 5.3 never touches an account.
Market-agnostic at the boundary — every caller binds to CostModule; the Lines map is the portable interchange format. Per-country storage tables are a separate concern (5.4's assembly maps the India Lines to the India trade_costs columns; the US would get its own table).
Integer discipline — all money integer coins, all prices integer ticks, one rounding helper.
8.2 Open / deferred (carried, not resolved here)
Rounding convention — half-up is the placeholder in applyRate; lock once at implementation (affects paise-level matching and bit-identical replay).
Rate re-verification + ₹20-cap nuance — the ₹20 flat slightly overcharges tiny trades vs the real "₹20-or-%-whichever-lower" cap; the actual current broker figures and the official STT sell-side rate (0.025% vs a reported 0.05%) were not independently re-verified this session — confirm at implementation.
US rulebook — the US module (SEC/TAF/FINRA, no STT/stamp/GST), its cost-basis convention, session-end mark, and tax treatment are researched independently when built; never inferred from India. The interface, inputs, and output shape are already US-ready — only a USCostModule + US rulebook need writing, with no change to callers.
Income tax / period reckoning — deferred (India prototype = "transaction costs only"; the slot exists for capital-gains/holding-period/slab logic if ever un-deferred).
Order↔trade keying for order-level brokerage — how the order-level brokerage charge is keyed and persisted relative to the trade record is a Tier-2 write-path detail, deferred.