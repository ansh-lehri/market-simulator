Exchange 1.1 Order Book + 1.7 Quote State — Implementation

Prerequisite: EXCHANGE_00_ARCHITECTURE.md (the split, the seam, the channel topology, the invariants). This doc assumes all of it.

Why 1.1 and 1.7 together: the quote (best bid/ask, last price) is derived from the book — it changes at the exact same moments the book's top changes. They share mutation points, so designing them apart would mean describing the same events twice. The book is the source of truth; the quote is a maintained projection of its top.

0. What this component is

The order book is the exchange's core state: every resting order, organized so that matching can find the best price and the oldest order at that price instantly, and so that a cancel can find and remove any order instantly. The quote state is the small derived summary the rest of the system reads and the tape publishes: best bid, best ask, their aggregate sizes, and the last traded price — each with an explicit "does this exist yet?" flag.

Neither touches money. The book knows a participant only as an opaque ParticipantID, used solely for self-match prevention. This is pure market truth.

Both live as private fields of the exchange session goroutine. No other goroutine holds a pointer to them. Every mutation happens inside that goroutine, draining the inbound channel one message at a time. That single fact is why none of the structures below carry a mutex: there is never concurrent access.

1. Value types — the integer discipline

Everything the book holds is an integer. This is not an optimization; it is a correctness requirement.

go
type Ticks         = int64   // price as an integer count of ticks
type Qty           = int64   // quantity in integer units; a live order's qty is always > 0
type OrderID       = string  // broker-assigned, opaque to the exchange, echoed on every event
type ParticipantID = string  // opaque; used ONLY for self-match prevention, never for money
type InstrumentID  = string  // opaque instrument key
type Seq           = uint64  // monotonic admission sequence — the engine's time-priority clock

Why price is Ticks (integer), never a float. The book is keyed and sorted by price. Floating-point prices break a price-keyed structure in two ways: (1) 0.05 * 3 is not exactly 0.15 in IEEE-754, so two orders "at the same price" can compare unequal and land in different levels; (2) sorting and equality become fuzzy. Representing price as an integer count of ticks — ₹501.05 at a ₹0.05 tick becomes 10021 — makes every price an exact integer. Equality, ordering, and level-keying are all exact integer operations. The rupee value is reconstructed only for display (ticks × tick_size), never used inside the engine.

Why Seq is the clock. Time priority ("first order at a price fills first") needs a definition of "first" that is unambiguous even when two orders arrive in the same instant. Seq is a monotonic counter stamped on each order at admission, inside the single goroutine. Because the goroutine processes messages strictly one at a time off the inbound channel, no two orders ever get the same Seq, and the channel order is the arrival order. The counter, not wall-clock time, is the authority on priority — this is what makes ordering deterministic.

2. The funded-order inbound message

This is what the broker submits after it has already reserved funds (see architecture §5, invariant 1). The exchange trusts it is funded. Note what it carries and — deliberately — what it does not: no balance, no account reference, no money field of any kind except the pre-attached reservation cap.

go
type Side    int
const ( Buy  Side = iota; Sell )

type OrdType int
const ( Limit OrdType = iota; Market )   // stop types are broker-side, never seen here

type TIF     int
const ( GTC TIF = iota; IOC; FOK )   // GTC-limit rests; Market/IOC/FOK never rest

type FundedOrder struct {
    OrderID      OrderID        // broker-assigned, opaque, echoed back on all events
    Participant  ParticipantID  // opaque id, self-match prevention only
    Instrument   InstrumentID
    Side         Side
    Type         OrdType
    TIF          TIF
    Qty          Qty            // requested quantity, integer units, > 0
    LimitPrice   Ticks          // set for Limit; ignored for Market

    // Reservation envelope, pre-attached by the broker (architecture §6):
    // for a market BUY, the maximum cash the reservation covers
    // (best_ask×qty + slippage buffer). The funded-cap walk caps against THIS.
    // 0 when not applicable (sells, limits).
    MaxSpend     Ticks
    STP         STPMode // zero value = CancelIncoming
}

No stop order types. Stop-market and stop-limit orders are held, monitored, and triggered entirely on the broker side (like the funding check — see architecture §1). The broker watches the exchange's public last-price tape and, when a stop's trigger crosses, submits the resulting order as an ordinary funded Market or Limit order through the normal inbound channel. The exchange therefore never sees a stop, has no trigger field, and has no dormant-order storage. To the exchange a triggered stop is indistinguishable from any other market/limit order.

MaxSpend is how the funded-cap stays in the exchange with zero L5 dependency: the broker reserved a specific amount of cash for a market buy and tells the exchange "this order is good for at most this much." When the matching walk (1.2) would spend more than MaxSpend, it caps the fill and cancels the remainder — reading a value already on the order, never calling back to the broker.

3. The resting order record

Only orders that rest live in the book. Market, IOC, and FOK orders either fill immediately or don't; they never become resting records. So a RestingOrder is always a GTC-limit that entered the book with quantity left.

go
type RestingOrder struct {
    OrderID     OrderID
    BrokerID     BrokerID
    Participant ParticipantID
    Side        Side
    Price       Ticks   // the resting (maker) price
    Remaining   Qty     // decremented as it fills; the order is removed at 0
    Seq         Seq     // time priority, stamped at admission, fixed for the order's life
}

There is no TIF field here on purpose: by the time an order rests, we already know it is GTC — the non-resting TIFs never reach this struct.

4. The price level — a FIFO queue of orders at one price
4.1 What a level is and why it is a queue

At a single price, many participants can have resting orders. They must fill in time order — the order that rested first fills first (the time half of price-time priority). So a price level is not one order; it is a time-ordered queue of the orders at that price:

Ask level 10021:   [A: 50] → [B: 30] → [C: 20]
                    oldest                newest
                    (fills first)         (fills last)

A matching buy consumes A first, then B, then C. A new sell resting at 10021 appends to the back, behind C.

4.2 The struct
go
import "container/list"

type PriceLevel struct {
    Price    Ticks
    Orders   *list.List  // doubly-linked list of *RestingOrder; front = oldest = fills first
    TotalQty Qty         // cached sum of Remaining across all orders at this level
}
4.3 Why a doubly-linked list for Orders

The queue at a level faces exactly three operations. The structure is chosen by the cost of each:

Fill from the front — matching always consumes the oldest order first.
Append at the back — a new resting order joins at the end.
Cancel an arbitrary order — a participant cancels an order that may be anywhere in the middle of the queue.

Operations 1 and 2 are front/back and any queue handles them. Operation 3 is what forces the structure, and cancels are frequent — place-and-cancel churn is most of a real book's message volume. Cost of "remove B from the middle":

Structure	Remove-from-middle	Why
Slice / array	O(n)	must shift every element after B
Singly-linked list	O(n)	to unlink B you need its predecessor A; from B alone you must scan the front to find A
Doubly-linked list	O(1)	B knows both neighbours; A.next=C; C.prev=A — no scan, no shift

But O(1) cancel needs two things together: finding B, and removing B once found. The doubly-linked list gives the removal. The order index (§5.2) gives the find — it points straight at B's node. With a singly-linked list, even holding a pointer to B you'd still be stuck, because you'd need A to unlink B and B doesn't know A. The doubly-linked list is exactly what turns "I have the node" into "I can remove it in O(1)."

Implementation uses Go's standard container/list (a doubly-linked list): list.Element is the node, list.Remove(e) is the O(1) unlink — no hand-rolled pointer plumbing.

If cancels were rare this would be over-engineering and a slice would be simpler. They are not, so the structure pays for itself.

4.4 Why TotalQty is cached separately from Orders

Orders and TotalQty answer two different questions at two different frequencies:

Orders answers "who fills next, in what order" — needed only while actually matching against this level.
TotalQty answers "how much total sits at this price" — needed on every quote and depth publish, which happens constantly (on every top-of-book change).

In the example above TotalQty = 100 (50+30+20). If it weren't cached, every depth/quote publish would have to walk the queue and sum — O(n) in the number of orders at the level, on a hot and frequent path. Caching it makes that read O(1): maintain it incrementally (+50 when A rests, −50 when A fully fills), read it for free. One int64 field and two arithmetic ops per mutation buy an O(1) publish. This is the classic space-for-time trade, justified by how often depth publishes.

The invariant to preserve on every mutation: TotalQty equals the sum of Remaining over the orders in Orders. Every code path that adds, reduces, or removes an order at this level must adjust TotalQty in the same step.

5. The book — two sorted sides plus the order index
go
type Book struct {
    Instrument InstrumentID

    asks  SideBook   // ascending:  best ask = lowest price
    bids  SideBook   // descending: best bid = highest price

    index map[OrderID]*orderLocation  // order_id -> exact location, for O(1) cancel

    quote QuoteState  // the derived projection (§7), maintained on every top change
}

type orderLocation struct {
    side  Side
    level *PriceLevel
    node  *list.Element  // the node inside level.Orders
}
5.1 The two sides and their sort direction

A "side" of the book is many price levels kept sorted by price, so the best price is instantly available and the matching walk can proceed best→worst:

Bid side (sorted high→low):   10020 → 10019 → 10018 → ...   best bid = top = highest
Ask side (sorted low→high):   10021 → 10022 → 10023 → ...   best ask = top = lowest

"Best" differs by side (highest bid vs lowest ask), which is why the two sides sort in opposite directions but expose the identical interface.

5.2 The order index — why cancel needs it

Without an index, cancelling order B means searching for it: which side, which price level, where in the queue. That is a walk of the book — unacceptable for a frequent operation. The index maps OrderID → orderLocation, so cancel is:

index[id] → the exact side, level, and node. O(1).
Unlink the node from the level's doubly-linked list. O(1) (§4.3).
Decrement level.TotalQty; drop the level if it is now empty; refresh the quote if the top changed.
Delete the index entry; return the removed RestingOrder (the broker uses it to release the leftover reservation).

The index and the doubly-linked list are a pair: the map finds the node, the list removes it. Neither alone gives O(1) cancel.

5.3 SideBook — the sorted-side interface (concrete structure deferred)

A side must keep price levels sorted and support exactly the operations matching and admission need. Those operations are stable; the structure under them is a performance choice we defer to benchmarking. So the side is an interface:

go
type SideBook interface {
    Best() (*PriceLevel, bool)            // top of book; false if the side is empty
    LevelAt(p Ticks) (*PriceLevel, bool)  // find an existing level at price p
    Insert(p Ticks) *PriceLevel           // get-or-create the level at price p
    RemoveLevel(p Ticks)                   // drop an emptied level
    Iterate(func(*PriceLevel) bool)        // walk best→worst; stop when fn returns false
}

Matching calls asks.Best(), bids.Iterate(...), etc., and does not care what sits underneath. We pick an implementation, benchmark it, and swap it if wrong without touching a line of matching.

Default: a balanced ordered-map / red-black tree (one per side).

Best() = the extreme node (leftmost for ascending asks, rightmost for descending bids): O(log n), trivially cacheable to O(1).
LevelAt / Insert / RemoveLevel: O(log n).
Iterate best→worst: in-order traversal from the extreme.

Chosen as the default for predictable O(log n) on every operation and correctness-first simplicity. The tree is keyed by Ticks; the two sides are the same tree with opposite comparison direction.

Alternatives, to benchmark against the default:

Skip list — same asymptotic profile, often good cache behaviour and simpler code; the usual concurrent-variant advantage is irrelevant here (single goroutine).
Sorted slice — Best() is O(1) (index 0), but inserting a new price level shifts elements, O(n). Wins when the number of distinct price levels stays small and best-of-book reads dominate. A tight circuit band (which caps how far prices can travel in a session) keeps the level count low, so for the prototype a sorted slice may well beat the tree — worth measuring, hence the interface.

The choice is a benchmark away and costs nothing to change later; the interface is the promise of that.

6. Book operations — the surface matching and admission call

These four mutators are the book's whole write surface. Every one of them keeps the quote (§7) consistent as a side effect, so no caller ever has to remember to refresh it.

go
// Insert rests a maker order (only reached for GTC-limit with qty left after matching).
// - asks/bids.Insert(price) to get-or-create the level
// - append the *RestingOrder at the back of level.Orders (newest time priority)
// - level.TotalQty += order.Remaining
// - index[order.OrderID] = {side, level, node}
// - if this changed the top of its side, recompute that side of the quote
func (b *Book) Insert(o *RestingOrder)

// Cancel removes an order by id. O(1) via the index (§5.2).
// Returns the removed order (broker releases its leftover reservation) and
// whether it was found. Not-found is normal (already filled/cancelled) — not an error.
// - locate via index; unlink node; level.TotalQty -= o.Remaining
// - RemoveLevel if the level is now empty
// - delete index entry
// - if the top changed, recompute that side of the quote
func (b *Book) Cancel(id OrderID) (*RestingOrder, bool)

// ReduceFront applies a fill that consumed `by` units from the front order of a level.
// Called by the matching core as it walks a level.
// - front.Remaining -= by ; level.TotalQty -= by
// - if front.Remaining == 0: remove the front node, delete its index entry,
//   RemoveLevel if the level emptied
// - if the top changed, recompute that side of the quote
// (last price is written by the matching core on the fill, not here — §7.2)
func (b *Book) ReduceFront(level *PriceLevel, by Qty)

// Quote returns the current derived state. O(1) — reads the maintained projection.
func (b *Book) Quote() QuoteState

A modify is handled by the admission/matching layer as cancel-then-reinsert (a price or size-up change loses time priority — standard exchange behaviour); the book itself needs no separate modify primitive, only Cancel + Insert.

7. Quote / Price state (1.7)
7.1 The struct — explicit absence
go
type QuoteState struct {
    BestBid      Ticks
    BidQty       Qty
    HasBid       bool

    BestAsk      Ticks
    AskQty       Qty
    HasAsk       bool

    LastPrice    Ticks
    HasLastPrice bool
}

Why the Has* booleans exist. Absence is a real, distinct state that a zero value cannot represent. "No bid exists" (empty bid side) is not the same as "the best bid is price 0" — and with integer ticks, 0 is a representable price. "No trade has happened yet" (session just opened) is not the same as "last price is 0." Collapsing these onto a zero value would make matching and the tape unable to tell an empty book from a book priced at zero. So each quantity carries an explicit presence flag, and every reader must check it before trusting the value.

This directly serves the liquidity pre-check (architecture §1.3): a market order against an empty opposite side is rejected, and "empty opposite side" is exactly !HasAsk (for a buy) or !HasBid (for a sell) — a clean boolean, not a guess-from-zero.

7.2 Two triggers, two writers

The quote's two halves update at different times and from different code:

Bid/ask (top of book) updates on any top-of-book change — an order placed, cancelled, or filled at the current best. This is frequent. It is recomputed by the book mutators in §6 (Insert, Cancel, ReduceFront) whenever they change the top of a side: read side.Best(), copy its Price and TotalQty into the quote (or clear Has* if the side emptied).
Last price updates only on a fill — it is the price of the most recent trade. It is written by the matching core (1.2) at the moment a trade prints, not by the book mutators. HasLastPrice becomes true on the first-ever trade and stays true. Price is emergent here in the strict sense: last price is a by-product of matching, never computed or set from outside.

Keeping these two triggers separate matters: recomputing bid/ask does not touch last price, and a fill updates both (the trade prints last price; consuming the maker changes the top, refreshing bid/ask).

7.3 Publication

Every quote change is published to the tape (public-outbound) and the relevant private events go to the broker — but how events are shaped, sequenced, and batched is the Event Emitter's job (1.9), not the book's. The book's responsibility ends at keeping QuoteState correct and signalling that it changed; 1.9 turns that into tape messages. This doc defines the state and its maintenance; the emitter doc defines the wire events.

8. Concurrency and ownership (restating the frame for this component)
Book, its two SideBooks, every PriceLevel, the index, and the QuoteState are private to the exchange session goroutine. No pointer to any of them crosses the seam.
Therefore no mutexes anywhere in this component. Single-writer by construction (architecture §5, invariant 3).
Readers on the other side of the seam (broker, tape consumers) never receive a pointer into these structures — they receive emitted event values (1.9). Book.Quote() returns a QuoteState by value, a copy, so even an in-process caller cannot hold a live pointer into book state.
9. Invariants this component must always hold
Price is always integer ticks. No float ever enters the book.
level.TotalQty == Σ order.Remaining for every level, after every mutation.
A resting order has Remaining > 0. It is removed the instant it hits 0.
index is exactly the set of live resting orders — every resting order has an index entry pointing at its actual node; nothing else does.
Time priority is Seq order within a level — front of the queue is the lowest Seq, and Seq never changes for an order once stamped.
The quote reflects the current top — after any mutation that changed a side's top, that side of the quote is recomputed in the same step; Has* flags are cleared when a side empties.
Last price changes only on a trade, written by the matching core, never by book mutators.
10. What this component does NOT do (handed to later components)
Deciding whether an order may enter — that is the Admission Gate (1.4).
Matching an incoming order against the book — that is the Matching Core (1.2); this doc only provides the operations it calls (Best, Iterate, ReduceFront, Insert).
Shaping/sequencing wire events from state changes — that is the Event Emitter (1.9).
Anything about money — that is the broker (Layer 5), across the seam.