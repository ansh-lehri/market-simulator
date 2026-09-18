Exchange — Architecture & Boundary Decisions (Layer 1, Implementation)

Status: Locked. This document is the shared preamble for every exchange component implementation doc (1.1 through 1.9). Component docs assume everything here and do not repeat it.

Scope of this doc: what the exchange is, what it is not, what it is split from, and how it connects to the broker. No component internals here — those live in their own docs. Read this first; it is the frame the rest hang on.

0. One-paragraph orientation

The matching engine is being built as a standalone exchange: a pure order-matching machine that owns market truth (the order book, price, trades, depth, session phase) and knows nothing about money, balances, or identity. It receives already-funded orders from a broker, matches them, and emits two output streams: private fill/terminal events back to the broker, and a public market-data tape. Everything to do with money — accounts, funding checks, reservations, costs, P&L — lives in the broker (Layer 5), which sits beside the exchange. For the prototype the two run as two goroutines in one process, separated later into two services without a rewrite. The seam between them is treated as a message boundary from day one so that "later" is a transport swap, not an untangling.

1. The exchange / broker split — what moved and why
1.1 The real-world mapping

In real markets the responsibilities are cleanly divided:

A broker holds your money, knows who you are, checks that you can afford an order, reserves your funds, and only then submits a clean order to the exchange on your behalf. The broker is where identity and money live.
An exchange matches anonymous order flow. It never checks anyone's bank balance. It trusts that every order reaching it has already been funded and vouched for by a broker. It knows a participant only as an opaque id, used solely to stop an order matching against itself.

The order of operations in the real world is: user → broker (reserve funds) → exchange (match) → fill flows back to broker (settle) → broker updates user.

1.2 What the original design had, and the problem it caused

The original Layer 1 design folded the broker's funding check into the exchange's admission gate as Gate 4 (reserve_if_available). That single gate reached out of the exchange, into the money domain (Layer 5), synchronously, on every order. This is what created every awkwardness we hit:

The exchange could no longer be reasoned about as a pure matching machine — it called into money-truth mid-admission.
Some Layer 5 methods had to be in-process (the hot-path reserve/convert calls the matching goroutine drove), while others (reads, transfers) were called by other layers or users — a split-personality seam that was hard to place.
The "lock-free because one goroutine owns everything" story got tangled up with which money methods were and weren't in that goroutine.
1.3 The diagnosis: exactly one gate was in the wrong layer

Running all nine subcomponents through a single test — would a real exchange own this, or does it presuppose broker-only knowledge (who you are, what you own, what you can afford)? — gives a clean verdict:

Subcomponent	Nature	L5 dependency?
1.1 Order Book	Exchange	none
1.2 Continuous Matching Core	Exchange	none
1.3 Auction Matcher	Exchange	none
1.5 Circuit Systems	Exchange	none (reads rulebook config)
1.6 Stop / Trigger Layer	Broker	relocated — see note
1.7 Price / Quote State	Exchange	none
1.8 Session-Phase / Clock	Exchange	none
1.9 Event Emitter	Exchange	none
1.4 Admission Gate — Gates 0–3 + liquidity pre-check	Exchange	none
1.4 Admission Gate — Gate 4 (reserve-if-available)	Broker	the sole seam

The admission gate, verified against the design doc gate-by-gate:

Gate 0 (structural / well-formed) — order's own fields only. No external state.
Gate 1 (session phase) — reads 1.8, in-engine.
Liquidity pre-check (market order vs empty opposite book) — reads 1.1; the doc places it explicitly before fund reservation.
Gate 2 (tick-grid) — reads injected rulebook tick.
Gate 3 (circuit-band reject) — reads injected rulebook band + the frozen daily reference held in-engine.
Gate 4 (buying-power / position) — the design doc's own words: "the seam to the economy domain, Layer 5 … the one external consult."

The doc's use of "one" is the confirmation: Gate 4 is the only thing in the entire exchange that reaches into Layer 5. Everything else reads in-engine state or injected rulebook config. The rulebook is injected configuration, not a runtime call to another layer — it is present at session start and never phoned.

Note on 1.6 (stops) — relocated to the broker. An earlier version kept stops in the engine (watching last price, firing on a cross). That was reconsidered against how real venues actually work: brokers overwhelmingly hold the dormant stop and monitor it themselves, sending the exchange only the resulting live order once the trigger fires (Fidelity, Schwab, Vanguard, SEC/FINRA guidance all describe stops as held off-market until triggered, then routed as an ordinary market/limit order). To the exchange, a triggered stop is indistinguishable from any other order. Relocating stops to the broker also cleanly resolves the funding-timing seam (the broker holds the stop and the money, so it reserves at fire time with no cross-seam round-trip) and makes fire-time re-admission automatic (the fired order simply arrives through the normal inbound channel and runs the full pipeline). The exchange therefore has no stop types, no trigger field, and no dormant-order storage — stop-market and stop-limit are dropped from the order menu entirely, exactly like Gate 4's funding check.

1.4 The move

Gate 4 is removed from the exchange and relocated into the broker, alongside Layer 5. With it gone, the exchange makes zero Layer 5 calls. It becomes genuinely standalone: it receives funded orders, matches them, and returns fills/data. The entire L1↔L5 entanglement was this one gate, and relocating it un-entangles the two layers completely.

2. Process topology
Prototype: exchange and broker are two goroutines within one OS process. The exchange goroutine owns the book and market truth; the broker session goroutine owns the account and reservation truth.
Long-run (deferred): if the platform proves out, split broker and exchange into two separate services/processes. The seam discipline below (§4) is what makes that a transport swap rather than a rewrite.

The exchange is, in the prototype, physically one deployable that contains both the exchange and broker goroutines. This is an explicit, accepted prototype compromise; the message boundary marks exactly where the future process split falls.

3. The channel topology (the seam)

Three surfaces. Exactly one inbound to the exchange, two outbound from it.

                    ┌─────────────────────────────────────────────┐
   funded orders    │                                             │
  (submit / cancel  │              EXCHANGE goroutine             │
   / modify)        │        (owns Book, Quote, market truth)     │
 ───────────────────▶                                             │
   [inbound chan]   │                                             │
                    │                                             │
                    └──────┬──────────────────────────┬───────────┘
                           │                          │
       private fills &     │                          │  public market-data tape
       terminal events     │                          │  (trades, quotes, depth, OHLCV)
       (carry opaque       │                          │
        order_id)          ▼                          ▼
                    [private-out chan]          [public-out broadcast]
                           │                          │
                           ▼                          ▼
                    ┌─────────────┐         ┌──────────────────────────┐
                    │   BROKER    │         │ tape consumers: broker    │
                    │ (L5 + Gate4)│         │ relay to users, charts,   │
                    │ owns account│         │ persistence, Layer 6      │
                    └─────────────┘         └──────────────────────────┘
Inbound — funded orders (broker → exchange). New funded orders, cancels, modifies. The exchange trusts every order here is already funded.
Private-outbound — fills & terminal events (exchange → broker). Every fill, cancel-ack, rejection, expiry — each carrying the opaque order_id so the broker can match the event to the reservation it made and run Convert/Release.
Public-outbound — the tape (exchange → broadcast). Trades, quote changes, depth, OHLCV. Not broker-specific: the broker relays its users' relevant slice, but charts, persistence, and Layer 6 consume the same broadcast.

Both outbound streams share one monotonic sequence space (see 1.9 when written).

3.1 Why the fill stream is "ordered, exactly-once" for free

Because both sides are goroutines in one process, the private-outbound stream is a Go channel, which is ordered and exactly-once by construction. The reservation-bookkeeping invariant on the broker side (reserved == exactly the unfilled qty) depends on fills arriving in order and not being lost or duplicated — and a channel gives that with no sequencing/ack/replay machinery. That machinery only becomes necessary if/when the split goes cross-process; the message boundary (§4) is what lets it be added then without touching the exchange.

4. Seam discipline — messages, never shared memory

This is the single rule that keeps the future process split cheap, and it is enforced from day one even though both sides are in-process:

The exchange never reads the account, balance, or any broker state.
The broker never reads the book, a price level, or any exchange state.
Everything crosses the seam as a message over a channel — a value, never a pointer into the other side's live state.

Concretely: the broker submits a FundedOrder value; it does not hand the exchange a pointer into an account. The exchange emits fill event values; it does not hand the broker a pointer into the book. If this discipline holds, then "separate into two processes later" = "swap the channel for a network transport," not "untangle two structures that grew into each other."

5. The three load-bearing invariants (inherited by all components)
Reserve stays synchronous and upstream of submission. The broker reserves first, and submits to the exchange only on success; on failure it rejects and never submits. The exchange therefore only ever sees funded orders. This is the broker's control flow; the exchange's side of it is simply the contract that every inbound order is trusted-funded.
Fills drive Convert across the seam, in order. Matching happens in the exchange; the resulting fills must drive the broker's Convert/Release. Fills originate in the exchange, cross the private-outbound channel in order, and the broker applies them. Order and exactly-once delivery are guaranteed by the channel (§3.1).
One goroutine per session owns each truth. The exchange session goroutine is the sole writer of the book/quote/market truth; the broker session goroutine is the sole writer of the account/reservation truth. Single-writer on each side is what makes both lock-free and deterministic. On the exchange side specifically: no other goroutine holds a pointer to the book, the order index, or the quote — readers receive emitted event values, never a pointer in.
6. What the exchange carries for the broker (message-field obligations)

Two fields the exchange must carry, so that behaviours which used to depend on a live L5 call become zero-dependency:

Opaque order_id. Broker-assigned, carried on the funded order, and echoed on every fill/terminal event for that order. The exchange never interprets it; it is the broker's handle to match events back to reservations.
Reservation envelope (MaxSpend) on market buys. The funded-cap behaviour (a market buy fills only the quantity its reservation covers, cancelling the rest — original §7.4) used to be Gate-4-adjacent. With Gate 4 at the broker, the affordability number travels with the funded order: the broker says "this market buy is good for at most this much cash," and the exchange's walk caps against that pre-attached number. No live L5 call, even for the cap. The funded-cap stays entirely inside the exchange, gated by a value the broker attached.
6a. Per-instrument sharding & the connection edge (foundational)

The exchange serves many instruments — the prototype may add a handful of stocks, but the design target is thousands, and there is no index in the prototype. Two rules govern how that scales; both are locked and non-negotiable.

The connection edge is one-in / one-out, always. Anyone connecting to the exchange — the broker, or any future consumer — speaks over exactly one inbound channel and one outbound channel. Orders for all instruments arrive multiplexed on the one inbound; events for all instruments leave multiplexed on the one outbound. Every message is self-describing (carries its Instrument), so multiplexing thousands of instruments over one channel is just tagging. There are never per-instrument connections or channels exposed to a connected party. The broker holds one send channel and one receive channel, and has no knowledge of the exchange's internal instrument structure. This is standing procedure.

Internally the exchange shards by instrument over a fixed worker pool. Instruments are not one-goroutine-each (that does not scale to thousands). Instead:

A fixed pool of P shard workers (P sized to cores, configurable — not tied to instrument count).
Each instrument is assigned to exactly one shard by a stable hash (shard = hash(InstrumentID) % P). All of that instrument's state — its Book, QuoteState, CircuitBand, Matcher, and per-instrument Seq counter, bundled as an InstrumentState — lives on that shard and is touched only by that shard's worker.
A worker owns many instruments and processes their orders serially, so within any one instrument the single-writer / lock-free / deterministic guarantee holds. Different shards run concurrently (free parallelism); same-shard instruments don't, but they share no state, so serial is fine.
An ingress goroutine reads the one inbound channel and routes each order to its shard by the hash. Shards write events to the one outbound channel (egress fan-in). The connected party sees only the two channels.

Adding an instrument is not a nightmare and not a goroutine spawn: create its InstrumentState and register it in the map on its assigned shard. Goroutine count stays P + ingress + monitor whether there are 5 instruments or 5,000, and the connection edge never changes.

The only cross-shard shared state is the global session phase (1.8), held as an atomic.Int32 — one writer (the clock monitor), many shard readers. Everything else is single-writer within its shard.

The full ingress → shard → components → egress wiring — structs, pseudo-wiring, and the call-flow logic that ties every component together — is written as its own dedicated document after the component internals (1.1–1.9) are complete, so it wires finished pieces rather than moving targets. This section records the foundational decision; that doc realizes it.

7. Build order

Components are implemented in dependency order; each has its own doc:

1.1 Order Book + 1.7 Quote State — the state everything operates on.
1.2 Continuous Matching Core — exercises the book; last-price and slippage emerge.
1.4 Admission Gate (reduced: Gates 0–3 + liquidity pre-check) / 1.5 Circuit Systems / 1.8 Session-Phase.
1.3 Auction Matcher.
1.9 Event Emitter — vocabulary sketched as we go, locked last.
Dedicated sharding/wiring doc — ingress → shard → components → egress.

(1.6 Stop / Trigger is not an exchange component — relocated to the broker, §1.)

8. Open items explicitly deferred (not this layer)
Live-read concurrency on the broker side: how an off-path HTTP handler (an agent or user reading the live account, or a mid-session bank↔trading transfer) reads/mutates the account the broker session goroutine owns, without racing it. Resolves by routing such calls through the broker goroutine when a session is live, or restricting them to the durable copy. Broker/L5 concern, deferred there — does not affect the exchange.
Exact wire protocol for order-ingress and data-egress (WebSocket / gRPC stream / etc.) — pinned when Layer 6 (streaming) is designed against the finished exchange boundary. This doc fixes the shape (one inbound stream, one private-outbound stream, one public broadcast); Layer 6 picks the transport.