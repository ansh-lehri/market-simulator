Layer 5.4 — Realized-P&L / Settlement (Computation & Settlement)

Scope: the realization/settlement subcomponent of Layer 5 — items 37–44 on the Layer-5 action list. Covers realized-P&L computation (FIFO), the per-round-trip close accumulator, the unrealized mark, the weighted-average display price, session-end auto-square settlement, the session-type persistence gate, and the closed-trade record assembly. Design flow and the Go structs / method shapes for each.

What this document is NOT: it is not the 5.1 Tier-1 hands (see LAYER_5_1_OPERATIONS.md and LAYER_5_1_READ_OPERATIONS.md), not the 5.2 reservation methods (see the 5.2 flows), not the 5.3 cost engine, and not the Tier-2 persistence write path internals (buffer drainer, Postgres inserts) — those are referenced by their locked names but designed elsewhere. It does not design the C12 external read/query surface (item 17) — that is deferred.

Status: items 37–44 LOCKED at design-and-flow depth, against the closed Tier-1 hands and the closed 5.2/5.3 flows. The realized-P&L behaviour is pre-decided in LAYER_5_ECONOMY.md §6 (FIFO, three-line view, LTP mark, auto-square-as-settlement-mark, session-type gate); this document turns that behaviour into concrete flow + Go shapes. Open/deferred items are listed in §10.

Naming. Doc labels use snake_case for readability; Go identifiers are PascalCase for all exported names (methods, types, exported fields); unexported helpers are camelCase.

0. Orientation — what 5.4 is, and its two hard boundaries

5.4 answers one question: "how much did a trade actually make or lose, after costs, and what gets displayed while a position is still open?" It is the narrative layer — it explains money movement; it does not move money (with one settlement-side exception, §5). The trading account's cash balance is not profit: cash moves on every fill, but realized P&L is specifically "proceeds of a closed position − FIFO cost basis − costs." 5.4 owns that computation.

Two boundaries are load-bearing and were locked during design:

Boundary 1 — 5.4 owns NO lot queue. It is a pure consumer. The FIFO open-lot queue lives in Tier-1's Position.OpenLots (maintained by CreditPosition at buy fills and RemovePosition at sell fills). RemovePosition already performs the FIFO match and returns the matched lots. 5.4 receives those matched lots (handed through Convert → OnSellFill) and computes arithmetic on them. It never holds, mutates, or re-derives lot state. (Item 38 on the action list — "maintain the FIFO open-lot queue" — is therefore discharged entirely by Tier-1; 5.4 adds nothing to it.)

Boundary 2 — 5.4 computes; 5.2 moves money. On a voluntary close, the cash has already moved: Convert's sell branch already did CreditCash(Available, proceeds) before 5.4 runs. 5.4 computes the P&L narrative on top. The one exception is session-end auto-square (§5), where there is no order and no Convert, so 5.4 books the settlement directly via Tier-1 hands.

0.1 The one piece of state 5.4 owns

5.4 holds exactly one piece of state: the close-accumulator, one entry per open (account, instrument) round-trip in progress. It exists because a participant_trades row is one full round-trip (locked), not one row per fill — so partial sells must accumulate until the position goes flat, then flush as a single row. This is the only state 5.4 owns; it still owns no lot queue.

TIER 2 / settlement driver (Layer 7-core) ── triggers ──► auto-square (§5)
        │
5.2 Convert (sell fill) ── OnSellFill ──► 5.4:
        │  ComputeRealizedPnl (per fill, FIFO) ──► fold into close-accumulator
        │  on position flat: assemble round-trip row ──► (session-type gate) ──► Redis buffer
        ▼
TIER 1 hands (5.1): RemovePosition (returns matched lots), CreditCash, ReadAvailablePosition
1. Shared types
go
type AccountID    = string
type InstrumentID = string
type SessionID    = string
type TradeID      = string

type Side int
const (
    Buy Side = iota
    Sell
)

// Lot — the Tier-1 open-lot shape (defined in LAYER_5_1_OPERATIONS.md §0.4), reproduced
// here for a self-contained read. 5.4 CONSUMES these; it does not own or mutate them.
type Lot struct {
    EntryPrice   int64     // integer ticks — maker price of the buy fill
    EntryAt      time.Time // fill time — source of position_age; the round-trip's entry timestamp
    RemainingQty int64     // units of this lot not yet sold
}

// MatchedLot — a slice of an open lot that a sell fill consumed, as returned by
// RemovePosition. Carries how many units of THIS lot the fill took, plus that lot's entry
// price/time, so 5.4 can price the realization per lot (FIFO-faithful).
type MatchedLot struct {
    EntryPrice int64     // the consumed lot's entry price (integer ticks)
    EntryAt    time.Time // the consumed lot's entry time
    QtyFromLot int64     // units of this fill drawn from this lot
}

// CostBreakdown — the market-agnostic 5.3 output (from LAYER_5_3). Lines keyed by name;
// Total is the universal field. 5.4 receives this per fill and sums it into the accumulator.
// (India emits stt/exchange/sebi/stamp/gst/autosquare_fee; brokerage is NOT here — it is
// order-level, per the locked cost decision.)
type CostBreakdown struct {
    Lines map[string]int64
    Total int64
}
2. Item 37 — ComputeRealizedPnl (per-fill FIFO realization)
2.1 What it is

The core realization computation. Runs per sell fill (invoked from OnSellFill, which Convert's sell branch already calls). Pure computation — no mutation, no I/O, no lot-queue ownership. It consumes the FIFO-matched lots Tier-1 returned and produces this fill's realization triple (gross / costs / net).

2.2 Inputs — all already produced upstream

Carried on the RealizationInput that Convert passes to OnSellFill:

MatchedLots []MatchedLot — the FIFO-matched buy lots RemovePosition consumed and returned. Already FIFO-ordered by Tier-1; 5.4 does not choose them.
FillQty int64 — units sold in this fill.
MakerPrice int64 — the exit price (the fill-at-maker-price invariant).
Cost CostBreakdown — the 5.3 per-fill stack for this fill (turnover charges; brokerage excluded — it is order-level).
AccountID, SessionID, InstrumentID — identity / scope.
2.3 The computation — per-lot FIFO loop

Gross is summed per matched lot, not as a single subtraction — because one sell fill can span several buy lots at different entry prices, and pricing it per lot is exactly what makes it FIFO rather than weighted-average. (Weighted-average and FIFO give identical total P&L over a fully-closed session; they differ only on partial closes — the live running number — which is why FIFO-per-close is the faithful choice.)

gross = 0
for each ml in MatchedLots:
    gross += (MakerPrice − ml.EntryPrice) × ml.QtyFromLot
costs = Cost.Total
net   = gross − costs        // signed; a loss is negative; no clamp
2.4 Method shape
go
// RealizationInput is handed from Convert's sell branch (via OnSellFill) per sell fill.
type RealizationInput struct {
    AccountID       AccountID
    SessionID       SessionID
    InstrumentID    InstrumentID
    FillQty         int64
    MakerPrice      int64
    MatchedLots     []MatchedLot
    Cost            CostBreakdown
    PositionNowFlat bool // set by Convert from post-RemovePosition state (see §6.2)
}

// RealizationResult is the per-fill triple. Signed throughout.
type RealizationResult struct {
    Gross int64          // Σ per-lot (MakerPrice − entry) × qtyFromLot
    Costs int64          // Cost.Total (turnover; brokerage excluded)
    Net   int64          // Gross − Costs
    Lines map[string]int64 // itemized cost lines carried through for the three-line view
}

// ComputeRealizedPnl prices ONE sell fill against its FIFO-matched lots. Pure computation:
// no mutation, no I/O, owns no lot queue, moves no money (Convert already credited proceeds).
func ComputeRealizedPnl(in RealizationInput) RealizationResult {
    var gross int64
    for _, ml := range in.MatchedLots {
        gross += (in.MakerPrice - ml.EntryPrice) * ml.QtyFromLot
    }
    costs := in.Cost.Total
    return RealizationResult{
        Gross: gross,
        Costs: costs,
        Net:   gross - costs,
        Lines: in.Cost.Lines,
    }
}
2.5 Boundaries
No lot-queue touch — consumes matched lots, owns nothing.
No I/O — pure arithmetic, reproducible (same inputs → same result; no clock, no randomness).
Signed throughout — gross/net may be negative; never clamped (consistent with Option B and the signed durable account).
Moves no money — Convert already did CreditCash(Available, proceeds). This computes the narrative only.
2.6 Note — per-fill vs per-close

ComputeRealizedPnl runs per fill. A position "close" the user perceives as one trade may span several fills. The aggregation into one row per round-trip is the accumulator's job (§3); item 37 itself computes per fill.

3. Item 44 (accumulation half) — the close-accumulator
3.1 Why it exists

A participant_trades row is one full round-trip (locked): accumulate partial sells, emit one row when the position goes flat. So ComputeRealizedPnl's per-fill results must fold into a per-(account, instrument) accumulator that flushes once, on flat. This is 5.4's only state.

3.2 Shape
go
// RealizationAccumulator — 5.4's ONLY state. One entry per open (account, instrument)
// round-trip in progress. Folds per-fill realizations until the position goes flat, then
// assembles one participant_trades row (+ one trade_costs child) and is deleted.
type RealizationAccumulator struct {
    AccountID    AccountID
    SessionID    SessionID
    InstrumentID InstrumentID

    QtyClosed  int64            // units realized across the partial sells so far
    GrossAccum int64            // Σ per-lot FIFO gross across those sells (signed)
    CostsAccum map[string]int64 // Σ the 5.3 per-fill cost lines (turnover; brokerage excluded)
    CostsTotal int64            // Σ Cost.Total across the sells

    // entry basis across the matched (closed) lots — for the row's single weighted-avg entry_price
    entryValueSum int64 // Σ (ml.EntryPrice × ml.QtyFromLot) across matched lots
    entryQtySum   int64 // Σ ml.QtyFromLot                       across matched lots

    // exit basis across the partial sells — qty-weighted exit price for the row
    exitValueSum int64 // Σ (MakerPrice × FillQty) across sells
    exitQtySum   int64 // Σ FillQty                across sells

    FirstEntryAt time.Time // earliest matched lot's EntryAt (round-trip start)
    ExitAt       time.Time // latest fill time (round-trip close; the ordering key)

    PersistOnClose bool // carried from the account — the session-type gate (§7)
}

// The accumulator store — keyed by (account, instrument). 5.4-held.
type RealizationBook struct {
    accums map[accKey]*RealizationAccumulator
}
type accKey struct {
    Account    AccountID
    Instrument InstrumentID
}
3.3 Lifecycle
Born on the first sell fill that realizes against an open position (no accumulator exists until a position starts closing).
Updated on each subsequent sell fill of that instrument — ComputeRealizedPnl runs per fill and feeds the accumulator (adds gross, sums cost lines, extends the weighted exit and matched entry sums, advances QtyClosed, moves ExitAt forward).
Flushed when the position goes flat (that instrument's available + reserved units hit 0): assemble one participant_trades row + one trade_costs row, enqueue to the buffer (gated by PersistOnClose, §7), then delete the accumulator entry.
3.4 The fold
go
// fold merges one per-fill realization into the round-trip accumulator.
func (acc *RealizationAccumulator) fold(in RealizationInput, r RealizationResult) {
    acc.QtyClosed  += in.FillQty
    acc.GrossAccum += r.Gross
    acc.CostsTotal += r.Costs
    for name, amt := range r.Lines {
        if acc.CostsAccum == nil {
            acc.CostsAccum = map[string]int64{}
        }
        acc.CostsAccum[name] += amt
    }
    // matched-entry basis (for weighted-avg entry_price on the row)
    for _, ml := range in.MatchedLots {
        acc.entryValueSum += ml.EntryPrice * ml.QtyFromLot
        acc.entryQtySum   += ml.QtyFromLot
        if acc.FirstEntryAt.IsZero() || ml.EntryAt.Before(acc.FirstEntryAt) {
            acc.FirstEntryAt = ml.EntryAt
        }
    }
    // exit basis (qty-weighted exit price)
    acc.exitValueSum += in.MakerPrice * in.FillQty
    acc.exitQtySum   += in.FillQty
    acc.ExitAt        = now() // latest fill time; the round-trip close/ordering key
}
3.5 OnSellFill — the handler that ties it together
go
// OnSellFill is the realization handoff Convert's sell branch calls per sell fill.
// It runs the per-fill FIFO computation, folds it into the round-trip accumulator, and —
// when Convert signals the position is now flat — assembles + enqueues one round-trip row,
// gated by the session-type flag, then clears the accumulator.
func (s *Realizer) OnSellFill(in RealizationInput) {
    key := accKey{in.AccountID, in.InstrumentID}

    // 1. Per-fill FIFO realization.
    r := ComputeRealizedPnl(in)

    // 2. Born-or-fetch the round-trip accumulator.
    acc := s.book.accums[key]
    if acc == nil {
        acc = &RealizationAccumulator{
            AccountID: in.AccountID, SessionID: in.SessionID, InstrumentID: in.InstrumentID,
            PersistOnClose: s.ledger.MustGet(in.AccountID).PersistOnClose, // session-type gate
        }
        s.book.accums[key] = acc
    }

    // 3. Fold this fill in.
    acc.fold(in, r)

    // 4. On flat: assemble the round-trip row(s), gate on session type, enqueue, delete.
    if in.PositionNowFlat {
        row, costRow := acc.assemble('voluntary')      // §4 assembly; exit_reason = voluntary here
        if acc.PersistOnClose {                          // §7 session-type gate
            s.persist.EnqueueClosedTrade(row, costRow)   // item 49 → Redis l5:buf:trades
        }
        delete(s.book.accums, key)
    }
}

(The 'voluntary' exit-reason literal is illustrative; the real type is an enum — see §4.2. Auto-square uses the same assembly with exit_reason = 'auto_square', §5.)

4. Items 42 + 44 (assembly half) — the three-line view & closed-trade record
4.1 The three-line view (item 42)

The three-line view is not a separate computation — it is the output shape of the realization: gross → itemized costs → net. At the per-fill level it is RealizationResult{Gross, Costs, Net, Lines}; at the round-trip level it is the accumulator's GrossAccum → CostsAccum → (Gross − CostsTotal). All three figures are shown because the itemized costs between gross and net is the teaching mechanic (made ₹X → kept ₹Y). Structure is market-agnostic; only which cost lines exist is country-specific (they arrive already keyed in Lines).

4.2 The closed-trade record assembly (item 44)

On flush, the accumulator assembles two rows. Assembly is in-memory; the rows go to the buffer (item 49), never directly to Postgres.

go
type ExitReason string
const (
    ExitVoluntary  ExitReason = "voluntary"
    ExitAutoSquare ExitReason = "auto_square"
)

// ParticipantTradeRow — one round-trip summary row (schema per LAYER_5_PERSISTENCE §3.2).
type ParticipantTradeRow struct {
    TradeID      TradeID
    AccountID    AccountID
    SessionID    SessionID
    InstrumentID InstrumentID
    Direction    string     // 'long' (prototype long-only; stored for when short exists)
    Quantity     int64      // total units of the round-trip (QtyClosed)
    EntryPrice   int64      // weighted-average of the matched entries (see §4.3)
    ExitPrice    int64      // qty-weighted exit across the partial sells
    EntryAt      time.Time  // earliest matched lot (round-trip start)
    ExitAt       time.Time  // latest fill (round-trip close; ordering key)
    GrossPnl     int64      // signed
    TotalCosts   int64      // >= 0; turnover stack; brokerage EXCLUDED (order-level)
    NetPnl       int64      // signed; Gross − TotalCosts
    PnlPct       float64    // signed, on net
    ExitReason   ExitReason
}

// TradeCostRow — the itemized India cost child (1:1, keyed by trade_id). This is the ONE place
// that maps the market-agnostic Lines map to the India-named columns.
type TradeCostRow struct {
    TradeID       TradeID
    Brokerage     int64 // 0 here — brokerage is order-level and persisted on its own path (decision 2)
    STT           int64
    Exchange      int64
    SEBI          int64
    Stamp         int64
    GST           int64
    AutosquareFee int64 // populated only when ExitReason = auto_square
    Total         int64 // == ParticipantTradeRow.TotalCosts (denormalized for audit)
}

// assemble turns the flushed accumulator into the two rows. exitReason distinguishes a
// voluntary close from an auto-square (§5).
func (acc *RealizationAccumulator) assemble(exitReason ExitReason) (ParticipantTradeRow, TradeCostRow) {
    entryPrice := int64(0)
    if acc.entryQtySum > 0 {
        entryPrice = acc.entryValueSum / acc.entryQtySum // weighted-avg of matched entries
    }
    exitPrice := int64(0)
    if acc.exitQtySum > 0 {
        exitPrice = acc.exitValueSum / acc.exitQtySum    // qty-weighted exit
    }
    net := acc.GrossAccum - acc.CostsTotal

    row := ParticipantTradeRow{
        TradeID:      newTradeID(),
        AccountID:    acc.AccountID,
        SessionID:    acc.SessionID,
        InstrumentID: acc.InstrumentID,
        Direction:    "long",
        Quantity:     acc.QtyClosed,
        EntryPrice:   entryPrice,
        ExitPrice:    exitPrice,
        EntryAt:      acc.FirstEntryAt,
        ExitAt:       acc.ExitAt,
        GrossPnl:     acc.GrossAccum,
        TotalCosts:   acc.CostsTotal,
        NetPnl:       net,
        PnlPct:       pnlPct(net, acc.entryValueSum), // signed, on net
        ExitReason:   exitReason,
    }
    cost := TradeCostRow{
        TradeID:       row.TradeID,
        STT:           acc.CostsAccum["stt"],
        Exchange:      acc.CostsAccum["exchange"],
        SEBI:          acc.CostsAccum["sebi"],
        Stamp:         acc.CostsAccum["stamp"],
        GST:           acc.CostsAccum["gst"],
        AutosquareFee: acc.CostsAccum["autosquare_fee"],
        Total:         acc.CostsTotal,
    }
    return row, cost
}
4.3 The single entry_price on a multi-lot round-trip

participant_trades has one entry_price column, but a FIFO round-trip can close lots bought at several prices. The row's entry price is the weighted-average of the matched entry lots, so the summary reconciles (gross ≈ (exit − entry_price) × qty at the row level). The per-lot FIFO detail already did its job computing GrossAccum correctly; the row just needs one honest summary entry price — and weighted-avg-of-matched-entries is that (the same display-avg / FIFO-gross split used throughout the layer).

4.4 Where brokerage is (and isn't)

Brokerage is order-level, charged once per executed order at RemainingQty == 0 (or at partial-fill-then-cancel), by the 5.3 ComputeOrderCost path wired into 5.2. It is not part of a trade's trade_costs row and does not enter the round-trip NetPnl. It hits the account (via DebitCash) at the order's terminal step, and is persisted on its own order-keyed path (a Tier-2 write-path detail, deferred). This keeps each trade row showing transaction-level economics the way a real broker presents them; brokerage shows at order level.

5. Item 41 — session-end auto-square settlement
5.1 What it is

At session end every still-open position is force-closed ("auto-squared"), because the sim is intraday-only: every session must end flat. Mechanically it is a forced close, priced at the official closing price, with an extra CST-5 fee. It is categorically a session-end event/trigger (a forced close), not a new P&L computation — it feeds the same realization framework a voluntary close does.

5.2 The pre-resolved decision (settlement-side mark — do not reopen)

Auto-square is a settlement-side mark, not order injection (locked, inherited from the engine's design):

The closing auction clears first, on genuine resting/auction interest — auto-squared positions are NOT in that auction set.
The auction produces the official closing price.
Then 5.4 books out every open position at that closing price — no orders injected, no feedback loop.

Clean split: the engine produces the price (market truth); 5.4 books the money (money truth). 5.4 never reaches into the auction or pushes orders.

5.3 Trigger and roster are EXTERNAL inputs

5.4 does not own the trigger or the session roster. A dedicated end-of-session/settlement driver (Layer 7-core / after-market settlement service) owns when the session ends and calls 5.4's settlement entry point, supplying:

the accounts to settle (the driver knows the session's participant set — it obtains this via the read flows), and
the closing price per instrument (from the engine).

5.4 reads the open lot detail for those positions from Tier-1 (Position.OpenLots) — it is supplied which accounts/positions, but reads the FIFO lot data from live Tier-1 state, keeping it a lot consumer (never an owner). So: input = roster + closing prices; lot data = read from Tier-1.

5.4 The forced book-out goes through Tier-1 hands directly

An auto-square has no engine fill, no order, no reservation, no Gate 4 — so it does not route through Convert (which expects a reservation record). This is the one place 5.4 commands Tier-1 rather than purely computing: it books the settlement directly via the hands. Justified because it is a settlement event 5.4 owns, not an order.

5.5 The flow
settlement driver (Layer 7-core) calls SettleSessionEnd(accounts, closingPrice[])
  for each account in accounts:                       (roster supplied by driver)
    for each open position (instrument with open_qty > 0):   (open_qty read from Tier-1)

      1. exit_price = closingPrice[instrument]         (engine's official close — settlement mark)

      2. Book out open_qty directly via Tier-1 hands (NO Convert):
           matched := RemovePosition(instrument, open_qty)   // returns FIFO-matched lots
           CreditCash(Available, exit_price × open_qty)       // settlement proceeds

      3. cost := 5.3 ComputeFillCost(Sell, open_qty, exit_price, IsAutosquare=true)
           // adds the CST-5 autosquare_fee line — the ONLY economic difference from a
           // voluntary close.
           DebitCash(Available, cost.Total)

      4. Run the SAME realization path: ComputeRealizedPnl on `matched`, fold into the
         accumulator (which now force-flushes, position is flat).

      5. Order-level brokerage applies (forced order is an executed order → ComputeOrderCost,
         once, order-level) — DebitCash, per the locked cost decision.

      6. assemble(exit_reason = auto_square) → row + cost child; if PersistOnClose,
         EnqueueClosedTrade → Redis buffer; delete accumulator.
5.6 What is reused vs new
Reused: the entire realization + cost + accumulator + persistence path (items 37, 3, 5.3, and the write path) — auto-square is "an involuntary close plus a fee," invoked with exit = closing price and IsAutosquare = true.
New (only these): (a) the session-end trigger loop over open positions, driven externally; (b) sourcing exit_price from the engine's closing print rather than a fill; (c) the IsAutosquare flag flowing to 5.3 for CST-5; (d) exit_reason = 'auto_square' on the row; (e) the direct-Tier-1 book-out (no Convert).
5.7 Method shape
go
// SettleSessionEnd is 5.4's settlement entry point, invoked by the external end-of-session
// driver (Layer 7-core). The driver supplies the roster and the engine's closing prices; 5.4
// reads lot detail from Tier-1 and books each open position out directly via the hands.
func (s *Realizer) SettleSessionEnd(accounts []AccountID, closingPrice map[InstrumentID]int64) {
    for _, accountID := range accounts {
        a := s.ledger.MustGet(accountID)
        for instrumentID, pos := range a.Positions {
            openQty := pos.Available + pos.Reserved
            if openQty == 0 {
                continue
            }
            exitPrice := closingPrice[instrumentID]

            // Direct book-out via Tier-1 hands (no Convert; no reservation exists).
            matched := a.RemovePosition(instrumentID, openQty)
            a.CreditCash(Available, exitPrice*openQty)

            // Sell-side cost stack + CST-5 (IsAutosquare = true).
            cost := s.costs.ComputeFillCost(FillCostInput{
                Side: Sell, FillQty: openQty, MakerPrice: exitPrice, IsAutosquare: true,
            })
            if cost.Total > 0 {
                a.DebitCash(Available, cost.Total)
            }

            // Same realization path; fold with PositionNowFlat = true to force the flush.
            in := RealizationInput{
                AccountID: accountID, SessionID: a.SessionID, InstrumentID: instrumentID,
                FillQty: openQty, MakerPrice: exitPrice, MatchedLots: matched,
                Cost: cost, PositionNowFlat: true,
            }
            r := ComputeRealizedPnl(in)
            key := accKey{accountID, instrumentID}
            acc := s.book.accums[key]
            if acc == nil {
                acc = &RealizationAccumulator{
                    AccountID: accountID, SessionID: a.SessionID, InstrumentID: instrumentID,
                    PersistOnClose: a.PersistOnClose,
                }
                s.book.accums[key] = acc
            }
            acc.fold(in, r)

            // Order-level brokerage on the forced (executed) order.
            oc := s.costs.ComputeOrderCost(OrderCostInput{EverFilled: true, TotalQty: openQty, Side: Sell})
            if oc.Total > 0 {
                a.DebitCash(Available, oc.Total)
            }

            row, costRow := acc.assemble(ExitAutoSquare)
            if acc.PersistOnClose {
                s.persist.EnqueueClosedTrade(row, costRow)
            }
            delete(s.book.accums, key)
        }
    }
}
6. Item 39 — ComputeUnrealizedMark (live paper P&L on an open position)
6.1 What it is

The live "how am I doing right now" number on an open position — paper P&L before any sell. Purely a read-time computation; never stored, never snapshotted (it is derived).

6.2 The locked rules it obeys
Mark against LTP (last-traded price) — not mid, not bid/ask. Chosen because every real retail platform shows LTP; matching reality wins.
Net of costs already incurred — subtract entry-side costs actually charged so far; exit costs are NOT applied (nothing has been sold), so the running number behaves like a real broker's.
Brokerage excluded from the per-position mark (order-level, per decision 2) — the live number and the eventual realized number then tell the same story.
Derived at read-time — computed fresh on every read from live inputs; stored nowhere.
6.3 Inputs — all live elsewhere; 5.4 reads, owns nothing
Open lots for the instrument — Tier-1 Position.OpenLots.
LTP — the engine's last-traded-price quote state (Layer 1 market truth), per instrument.
Incurred entry-side turnover costs already charged on the buys that built this position.
6.4 The computation
open_qty         = Σ lot.RemainingQty over OpenLots
gross_unrealized = Σ over lots: (LTP − lot.EntryPrice) × lot.RemainingQty   // per-lot; signed
net_unrealized   = gross_unrealized − entry_turnover_costs_already_incurred  // exit costs NOT applied
6.5 Method shape
go
// ComputeUnrealizedMark returns the live paper P&L for one open position, marked to LTP,
// net of entry-side turnover costs already incurred (exit costs NOT applied; brokerage
// excluded). Pure read-time computation: zero I/O, never stored, owns nothing.
func ComputeUnrealizedMark(lots []Lot, ltp int64, incurredEntryCosts int64) (gross, net int64) {
    for _, lot := range lots {
        gross += (ltp - lot.EntryPrice) * lot.RemainingQty
    }
    net = gross - incurredEntryCosts
    return gross, net
}
6.6 Boundaries
Pure read-time computation. Zero I/O. Never stored, never in the snapshot (derived, B11).
Owns no state (unlike the realized-close accumulator, which is state; the mark is not).
Consumers: the human live-P&L UI and agent REACT reads — i.e. this feeds the C12 read surface (item 17, deferred). ComputeUnrealizedMark is the computation; how it is served is the deferred read-surface question.
7. Item 40 — weighted-average display price (open position)
7.1 What it is

The "buy average" shown next to an open position — the single price a broker app displays for what is held. Display only — it is not the realization basis (that is FIFO, item 37).

7.2 The computation
open_qty           = Σ lot.RemainingQty
weighted_avg_entry = (Σ lot.EntryPrice × lot.RemainingQty) / open_qty
// open_qty == 0 (flat) → no open position → no display price (nil/empty, not divide-by-zero)
7.3 Method shape
go
// ComputeWeightedAvgEntry returns the qty-weighted mean entry price of the still-open lots —
// the "buy average" shown for the open position. Display only (FIFO is the realization basis).
// Returns (0, false) when flat. Pure read-time computation; owns nothing.
func ComputeWeightedAvgEntry(lots []Lot) (avg int64, hasOpen bool) {
    var valueSum, qtySum int64
    for _, lot := range lots {
        valueSum += lot.EntryPrice * lot.RemainingQty
        qtySum   += lot.RemainingQty
    }
    if qtySum == 0 {
        return 0, false
    }
    return valueSum / qtySum, true
}
7.4 Why it coexists with FIFO

Two numbers, two purposes, both locked: weighted-average is what you see for the open position (display); FIFO is how each close is priced for realized P&L. Identical total P&L over a fully-closed session; they differ only on partial closes — exactly what real Indian brokers do (weighted-avg shown for the open position, FIFO-matched on realization). No conflict, by design.

7.5 Boundaries

Pure read-time computation; zero I/O; not stored; owns nothing. Same consumer/read-surface note as §6.6 — the open-position view shows both the weighted-avg entry (this) and the LTP-based paper P&L (§6) together.

8. Item 43 — session-type tag (the persistence gate)
8.1 What it is

The control that makes canonical-vs-on-demand persistence work. Every realization the accumulator flushes is gated by a session-type tag deciding whether it is written back.

8.2 The mechanism (already half-built)

It is the PersistOnClose boolean that LoadAccount set at session start (the single canonical/on-demand difference, locked in the Tier-2 lifecycle):

Canonical → PersistOnClose = true → the flush enqueues to l5:buf:trades.
On-demand → PersistOnClose = false → the flush computes and displays the realized P&L but enqueues nothing (isolated, discarded at close).
on accumulator flush (position flat):
    row, costRow = accumulator.assemble(exit_reason)
    if account.PersistOnClose:              ← the session-type gate
        EnqueueClosedTrade(row, costRow)     ← canonical only
    // on-demand: skip enqueue; P&L already computed + displayed
    delete accumulator
8.3 Why it is read at each flush

A single session is entirely one type, but the persistence layer filters per-event ("tag every realized-P&L event; the persistence layer filters on it"). Carrying it per-flush keeps the gate local to the write decision rather than a global lookup. The realized-P&L math is identical for both session types — only the write-back differs. That is the whole point of the gate.

9. The persistence path (how records land) — reference

5.4 never writes Postgres directly. The full chain for a realized/auto-square close:

OnSellFill / SettleSessionEnd
  → ComputeRealizedPnl (item 37)                 [per-fill FIFO]
  → accumulator.fold                              [item 44 accumulation]
  → on flat: assemble round-trip row + cost child [item 44 assembly]
  → if PersistOnClose: EnqueueClosedTrade         [item 43 gate + item 49 → Redis l5:buf:trades]
  // async, elsewhere (NOT 5.4):
  drainer: l5:buf:trades → Postgres participant_trades + trade_costs   [items 50, 47/48]
Enqueue, never direct Postgres — the hot path stays clean.
On-demand suppresses the enqueue — the single boolean (PersistOnClose) is the whole canonical/on-demand difference; computation and display still happen, nothing persists.
Brokerage not in trade_costs — order-level; persisted on its own order-keyed path (write-path detail, deferred).
India-key mapping (the market-agnostic Lines map → named trade_costs columns) happens once, at assembly (§4.2).
10. Cross-cutting properties, open & deferred
10.1 Cross-cutting
5.4 owns no lot queue — pure consumer of Tier-1's matched lots (Boundary 1). Its only state is the close-accumulator (§3).
5.4 computes; 5.2 moves money (Boundary 2) — the sole exception is auto-square (§5), which books settlement directly via Tier-1 hands because there is no order/Convert.
Signed throughout — gross/net may be negative; never clamped.
Reproducibility — §2/§6/§7 are pure functions of their inputs (no clock except the fold's ExitAt stamp, no randomness, no external read); replaying the same fill sequence reproduces the same rows.
Determinism of the flush — one round-trip → exactly one participant_trades row + one trade_costs child, emitted on flat, gated by session type.
10.2 Open / deferred (carried, not resolved here)
The close-accumulator must survive crash recovery. It is live session state (a partially-closed round-trip), so for a canonical session it belongs in the Redis snapshot alongside cash, positions, and reservation records — else a crash mid-round-trip loses the partial. This adds the accumulator as a fourth thing the snapshotter mirrors. Deferred to the joint L5+L2+engine snapshot-capture coordination pass (the same deferred cross-layer pass noted in the persistence and lifecycle docs); not built here.
Convert passes PositionNowFlat into OnSellFill (it holds the post-RemovePosition state). This is a small wiring addition to the 5.2 Convert sell branch and its RealizationInput; noted as the seam with Tier-1.
C12 external read/query surface (item 17) — get balance / live P&L / trade history / cost breakdown / recent_streak, served to the human UI, agent REACT, and internal callers (e.g. the auto-square/settlement driver reading the roster). The unrealized mark (§6) and weighted-avg display (§7) are computations this surface will serve; the surface itself is deferred to the final read-surface pass.
US rulebook values — 5.4's structure is market-agnostic; the India cost-line names in the trade_costs mapping (§4.2) are the India slot. US (SEC/TAF/FINRA, no GST) gets its own cost table and mapping when built. Not inferred from India.
Order↔trade keying for order-level brokerage — how the order-level brokerage charge is keyed and persisted relative to the trade record is a Tier-2 write-path detail, deferred (the common prototype case is one closing order = one trade row).