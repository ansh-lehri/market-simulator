# book — order book (1.1) + derived quote (1.7)

## Spec
`docs/exchange/EXCHANGE_00` (split, seam, sharding) + `EXCHANGE_11` (this component).

## What this package owns
- The order book: a `SideBook` per side, `PriceLevel` FIFO queues (front = oldest =
  fills first), and the O(1) order index for cancel-by-id.
- The derived `QuoteState` (best bid/ask + sizes + last price, each with a `Has*`
  presence flag), kept consistent as a side effect of every mutation.
- The whole write surface = four mutators: `Insert`, `Cancel`, `ReduceFront`, `Quote`.

## Invariants (EXCHANGE_11 §9 — hold after every mutation)
1. Price is always integer ticks. No float ever enters the book.
2. `level.TotalQty == Σ order.Remaining` for every level.
3. A resting order has `Remaining > 0`; removed the instant it hits 0.
4. `index` is exactly the set of live resting orders.
5. Time priority is `Seq` order within a level; `Seq` never changes once stamped.
6. The quote reflects the current top; `Has*` cleared when a side empties.
7. Last price changes only on a trade, written by the matcher — never by book mutators.

## Deps
- `contract` (value aliases, `FundedOrder`, `RestingOrder`).`PriceLevel`,
  `QuoteState` are book-internal.
- `SideBook` is an **interface** — default a red-black tree; benchmark a sorted slice
  against it (tight circuit bands keep level counts low). Build behind the interface.

## Do NOT
- Touch money, positions, or any account state.
- Emit wire events, or decide admission/matching — only provide the ops those call.
- Use a mutex — single-writer by construction.
- Write last price — that is the matcher's output, not a book mutation.