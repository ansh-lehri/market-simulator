Exchange 1.5 Circuit Systems — Implementation

Prerequisites: EXCHANGE_00_ARCHITECTURE.md (esp. §6a per-instrument sharding), EXCHANGE_12_MATCHING_CORE.md (the CircuitClamp interface the matcher calls), EXCHANGE_14_ADMISSION_GATE.md (checkCircuitBand, which reads this component's bounds). Uses those types directly.

What 1.5 owns: the circuit band for one instrument — the price range trading is allowed in for the session — and the authority on "may a trade print at this price?" It is the single source of truth for both halves of the circuit rule:

the reject-half — the gate's checkCircuitBand rejects a limit order priced beyond the band (reads 1.5's bounds), and
the clamp-half — the matcher's walk calls PriceAllowed(p) before printing at each level, halting when a trade would breach the band (implemented by 1.5).

Because both halves read the same band object, the admit-side and the clamp-side can never disagree.

0. Scope decisions (locked)
Clamp only, no market-wide halt. Hitting the band caps the individual order's matching walk (fills up to the band, cancels/rests the remainder — 1.2). It does not freeze the session or halt other participants. Market-wide / index halts were deferred in the original design and are out of scope for the prototype.
Per-instrument. There is one CircuitBand per instrument, owned inside that instrument's InstrumentState on its shard. ACME's band and WIDGET's band are independent, computed from each instrument's own reference. (No index exists in the prototype; banding is inherently per-stock.)
Frozen for the whole session. The band is computed once at session start from a static daily reference and never recomputed. Floating / recentering bands are deferred. A price that runs to a band edge simply stops there for the rest of the session.
Reference = prior session close, or a seed price for the first-ever session, supplied by Layer 7 at session setup. The band width is a rulebook config value (basis points).
1. The component
go
type CircuitBand struct {
    reference Ticks // frozen daily reference price for this instrument; set once at session start
    lower     Ticks // reference adjusted down by the band width
    upper     Ticks // reference adjusted up by the band width
}

// NewCircuitBand computes the frozen bounds once, at session start.
// reference: this instrument's prior close (or seed price for the first session), from Layer 7.
// bandBps:   band half-width in basis points, from the rulebook (e.g. 1000 = ±10%).
func NewCircuitBand(reference Ticks, bandBps int) *CircuitBand {
    delta := reference * Ticks(bandBps) / 10000
    return &CircuitBand{
        reference: reference,
        lower:     reference - delta,
        upper:     reference + delta,
    }
}

The bounds are plain integer ticks, computed once. There is no per-order work and no recomputation — the struct is effectively immutable for the session's life.

Rounding note. reference * bandBps / 10000 is integer division and floors. The resulting bounds are therefore exact integer ticks (as required — see 1.1's integer discipline). If a specific rounding of the band edge to the tick grid is later wanted, it is applied here, once, at construction — never on the hot path.

2. The two read surfaces

1.5 exposes exactly two reads. Both are consulted by other components; 1.5 itself initiates nothing.

go
// PriceAllowed implements the CircuitClamp interface (1.2). The matcher calls it before
// printing a trade at price p, at each level of the walk. A trade may print iff p is
// within [lower, upper] inclusive.
func (b *CircuitBand) PriceAllowed(p Ticks) bool {
    return p >= b.lower && p <= b.upper
}

// Bounds is read by the gate's checkCircuitBand (1.4) — the reject-half of the rule.
// Returning the same lower/upper that PriceAllowed enforces is what guarantees the
// admit-side and clamp-side agree.
func (b *CircuitBand) Bounds() (lower, upper Ticks) {
    return b.lower, b.upper
}

PriceAllowed satisfies the matcher's CircuitClamp interface with no adapter — the matcher's stubbed collaborator is now this concrete type, injected from the instrument's state.

3. How the two halves use it (the whole circuit rule)

Reject-half — at admission (1.4 checkCircuitBand). For a limit order, the gate reads Bounds() and rejects if the order's limit price is outside [lower, upper]. Market orders have no price to reject on and pass this stage — they are governed by the clamp instead.

Clamp-half — during matching (1.2 walk). The matcher, before printing at a level's price, calls PriceAllowed(level.Price). If it returns false, the walk halts: nothing more fills, and the remainder is cancelled with ReasonCircuitClamp (market/IOC) or, for a limit, rests at its price. This catches the case a single resting order can't: a large taker eating up through levels whose individual prices are fine until the next level would breach the band — the walk stops at the band even though no single order was beyond it.

Together: the gate stops orders priced beyond the band from entering; the clamp stops matching from driving price beyond the band. Same bounds, two moments.

4. A natural consequence worth naming (not a bug)

When the clamp halts a walk and a limit remainder rests at the band edge, the book then holds resting interest sitting exactly at the circuit. The quote will show it, and it can never be crossed further (any trade beyond it is disallowed by the clamp) for the rest of the frozen session. This is correct behaviour — price has hit the circuit and stalls there — not a defect. It is the visible manifestation of "a price that runs to the band edge stops there."

5. Ownership & concurrency
One CircuitBand per instrument, held in that instrument's InstrumentState, on its shard (architecture §6a). It is read by that instrument's gate (Bounds) and that instrument's matcher (PriceAllowed) — both of which run on the same shard worker, serially. So there is no concurrent access to a band and no lock is needed.
The band is effectively immutable after construction (frozen session), so even the single-writer concern is moot — nothing writes it after NewCircuitBand.
Cross-instrument: ACME's band and WIDGET's band are separate objects on their respective shards; they never interact.
6. Invariants 1.5 must hold
The bounds the gate reads (Bounds) are exactly the bounds the clamp enforces (PriceAllowed). One object, one source of truth — admit-side and clamp-side cannot diverge.
The band is frozen for the session — computed once at NewCircuitBand, never recomputed (floating bands deferred).
Bounds are exact integer ticks — no float ever enters the band.
Hitting the band clamps the order, never the market — no session-wide halt (deferred).
The band is per-instrument — one per stock, from that stock's own reference; no shared or index-level band exists in the prototype.
7. Deferred / handed elsewhere
Market-wide / index halts — not in the prototype (no index exists). If added, a halt would live above the per-instrument bands as a session-level control that the gate and matcher also consult; the clamp-only design here does not preclude it.
Floating / recentering bands — deferred; would replace the frozen NewCircuitBand with a band that recomputes lower/upper as price moves, updating on the shard that owns the instrument. The two read surfaces (PriceAllowed, Bounds) would stay identical, so neither the gate nor the matcher would change.
Reference-price sourcing — Layer 7 supplies the frozen reference (prior close / seed) at session setup; 1.5 only consumes it.
The clamp's halt behaviour (cancel vs rest of the remainder) — owned by the matcher (1.2); 1.5 only answers the boolean.