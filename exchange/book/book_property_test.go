package book

import (
	"fmt"
	"sort"
	"testing"

	"pgregory.net/rapid"

	"simulator/contract"
)

// --- shadow model --------------------------------------------------------

// modelOrder is the shadow model's view of one resting order.
type modelOrder struct {
	OrderID   contract.OrderID
	Side      contract.Side
	Price     contract.Ticks
	Remaining contract.Qty
	Seq       contract.Seq
}

// bookModel tracks the state the Book is expected to be in. Every op updates
// the model in lockstep with the real Book; the invariant checker compares the
// two. Last price is intentionally NOT tracked here — invariant 7 says the
// book mutators must never touch it, and that half of the quote is checked by
// a separate assertion driven off the op history.
type bookModel struct {
	orders map[contract.OrderID]*modelOrder
	seqCtr contract.Seq
}

func newBookModel() *bookModel {
	return &bookModel{orders: map[contract.OrderID]*modelOrder{}}
}

func (model *bookModel) nextSeq() contract.Seq {
	model.seqCtr++
	return model.seqCtr
}

func (model *bookModel) liveIDsSorted() []contract.OrderID {
	ids := make([]contract.OrderID, 0, len(model.orders))
	for id := range model.orders {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
	return ids
}

func (model *bookModel) frontOrderAt(side contract.Side, price contract.Ticks) *modelOrder {
	var candidates []*modelOrder
	for _, order := range model.orders {
		if order.Side == side && order.Price == price {
			candidates = append(candidates, order)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(a, b int) bool { return candidates[a].Seq < candidates[b].Seq })
	return candidates[0]
}

type sidePrice struct {
	side  contract.Side
	price contract.Ticks
}

func (model *bookModel) nonEmptyLevels() []sidePrice {
	seen := map[sidePrice]bool{}
	for _, order := range model.orders {
		seen[sidePrice{order.Side, order.Price}] = true
	}
	out := make([]sidePrice, 0, len(seen))
	for pair := range seen {
		out = append(out, pair)
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].side != out[b].side {
			return out[a].side < out[b].side
		}
		return out[a].price < out[b].price
	})
	return out
}

// --- op ADT --------------------------------------------------------------

type bookOp interface {
	applyToBook(*Book)
	applyToModel(*bookModel)
	describe() string
}

// opInsert stores field values rather than a *contract.RestingOrder because
// the determinism test applies the same op sequence to two independent books.
// If both books stored the same pointer, a partial ReduceFront on book A
// would mutate the order that book B is still holding intact.
type opInsert struct {
	orderID     contract.OrderID
	participant contract.ParticipantID
	brokerID    contract.BrokerID
	side        contract.Side
	price       contract.Ticks
	remaining   contract.Qty
	seq         contract.Seq
}

func (op *opInsert) applyToBook(book *Book) {
	book.Insert(&contract.RestingOrder{
		OrderID:     op.orderID,
		Participant: op.participant,
		BrokerID:    op.brokerID,
		Side:        op.side,
		Price:       op.price,
		Remaining:   op.remaining,
		Seq:         op.seq,
	})
}
func (op *opInsert) applyToModel(model *bookModel) {
	model.orders[op.orderID] = &modelOrder{
		OrderID:   op.orderID,
		Side:      op.side,
		Price:     op.price,
		Remaining: op.remaining,
		Seq:       op.seq,
	}
}
func (op *opInsert) describe() string { return fmt.Sprintf("Insert(%s)", op.orderID) }

type opCancel struct {
	id             contract.OrderID
	expectedResult bool // model prediction — was this id live at op-generation time?
}

func (op *opCancel) applyToBook(book *Book) {
	_, hit := book.Cancel(op.id)
	if hit != op.expectedResult {
		panic(fmt.Sprintf("Cancel(%s): got hit=%v want %v", op.id, hit, op.expectedResult))
	}
}
func (op *opCancel) applyToModel(model *bookModel) { delete(model.orders, op.id) }
func (op *opCancel) describe() string              { return fmt.Sprintf("Cancel(%s)", op.id) }

type opReduce struct {
	side  contract.Side
	price contract.Ticks
	by    contract.Qty
}

func (op *opReduce) applyToBook(book *Book) {
	level, ok := book.sideBook(op.side).LevelAt(op.price)
	if !ok {
		panic(fmt.Sprintf("Reduce: no level at side=%v price=%d", op.side, op.price))
	}
	book.ReduceFront(level, op.by)
}
func (op *opReduce) applyToModel(model *bookModel) {
	front := model.frontOrderAt(op.side, op.price)
	if front == nil {
		panic(fmt.Sprintf("Reduce model: no front at side=%v price=%d", op.side, op.price))
	}
	front.Remaining -= op.by
	if front.Remaining == 0 {
		delete(model.orders, front.OrderID)
	}
}
func (op *opReduce) describe() string {
	return fmt.Sprintf("Reduce(side=%v,price=%d,by=%d)", op.side, op.price, op.by)
}

type opSetLastPrice struct {
	price contract.Ticks
}

func (op *opSetLastPrice) applyToBook(book *Book)     { book.SetLastPrice(op.price) }
func (op *opSetLastPrice) applyToModel(*bookModel)    {}
func (op *opSetLastPrice) describe() string           { return fmt.Sprintf("SetLastPrice(%d)", op.price) }

// --- op generator (coherent with a shadow model) ------------------------

var opChoiceLabels = []string{"insert", "cancel", "cancel_ghost", "reduce", "trade"}

// drawOneOp emits a legal op given the current shadow model — Cancel targets a
// currently-live id (or a ghost id for the miss path), Reduce targets a
// non-empty level with `by ∈ [1, front.Remaining]` (the min-with-front
// contract the matcher would honour).
func drawOneOp(rt *rapid.T, model *bookModel, orderIDCounter, ghostCounter *int) bookOp {
	choice := rapid.SampledFrom(opChoiceLabels).Draw(rt, "op")
	switch choice {
	case "insert":
		*orderIDCounter++
		id := contract.OrderID(fmt.Sprintf("O%d", *orderIDCounter))
		side := contract.Side(rapid.IntRange(0, 1).Draw(rt, "side"))
		price := contract.Ticks(rapid.IntRange(10, 30).Draw(rt, "price"))
		qty := contract.Qty(rapid.IntRange(1, 100).Draw(rt, "qty"))
		return &opInsert{
			orderID:     id,
			participant: "P",
			brokerID:    "B",
			side:        side,
			price:       price,
			remaining:   qty,
			seq:         model.nextSeq(),
		}

	case "cancel":
		live := model.liveIDsSorted()
		if len(live) == 0 {
			// Nothing live — degrade to a ghost so the op is still meaningful.
			*ghostCounter++
			return &opCancel{
				id:             contract.OrderID(fmt.Sprintf("ghost%d", *ghostCounter)),
				expectedResult: false,
			}
		}
		idx := rapid.IntRange(0, len(live)-1).Draw(rt, "cancelPick")
		return &opCancel{id: live[idx], expectedResult: true}

	case "cancel_ghost":
		*ghostCounter++
		return &opCancel{
			id:             contract.OrderID(fmt.Sprintf("ghost%d", *ghostCounter)),
			expectedResult: false,
		}

	case "reduce":
		levels := model.nonEmptyLevels()
		if len(levels) == 0 {
			// Fall back to a ghost cancel.
			*ghostCounter++
			return &opCancel{
				id:             contract.OrderID(fmt.Sprintf("ghost%d", *ghostCounter)),
				expectedResult: false,
			}
		}
		idx := rapid.IntRange(0, len(levels)-1).Draw(rt, "reducePick")
		target := levels[idx]
		front := model.frontOrderAt(target.side, target.price)
		by := contract.Qty(rapid.IntRange(1, int(front.Remaining)).Draw(rt, "by"))
		return &opReduce{side: target.side, price: target.price, by: by}

	case "trade":
		price := contract.Ticks(rapid.IntRange(1, 1_000_000).Draw(rt, "tradePrice"))
		return &opSetLastPrice{price: price}
	}
	panic("unreachable")
}

// generateOps produces a coherent sequence via a private shadow model. The
// returned ops can be applied to any fresh Book without needing the model.
func generateOps(rt *rapid.T) []bookOp {
	count := rapid.IntRange(0, 200).Draw(rt, "opCount")
	shadow := newBookModel()
	orderIDCounter := 0
	ghostCounter := 0
	ops := make([]bookOp, 0, count)
	for step := 0; step < count; step++ {
		op := drawOneOp(rt, shadow, &orderIDCounter, &ghostCounter)
		op.applyToModel(shadow)
		ops = append(ops, op)
	}
	return ops
}

// --- assertions ---------------------------------------------------------

// assertBookMatchesModel checks invariants 2, 3, 4, 5, 6 of EXCHANGE_11 §9.
// Invariant 1 (integer ticks) is enforced by the type system.
// Invariant 7 (last price only via SetLastPrice) is checked by
// assertLastPriceInvariant using the op history, not the model.
func assertBookMatchesModel(rt *rapid.T, book *Book, model *bookModel) {
	// Invariant 4 — index bijection with model orders.
	if len(book.index) != len(model.orders) {
		rt.Fatalf("invariant 4 (index bijection): book.index len=%d, model orders=%d",
			len(book.index), len(model.orders))
	}
	for id := range model.orders {
		if _, present := book.index[id]; !present {
			rt.Fatalf("invariant 4: model has %q, book.index does not", id)
		}
	}

	// Bucket model orders by (side, price), sorted within each bucket by Seq.
	byBucket := map[contract.Side]map[contract.Ticks][]*modelOrder{
		contract.Buy:  {},
		contract.Sell: {},
	}
	for _, order := range model.orders {
		byBucket[order.Side][order.Price] = append(byBucket[order.Side][order.Price], order)
	}
	for _, byPrice := range byBucket {
		for _, group := range byPrice {
			sort.Slice(group, func(a, b int) bool { return group[a].Seq < group[b].Seq })
		}
	}

	// Walk every level in the book, on each side.
	for _, side := range []contract.Side{contract.Buy, contract.Sell} {
		expected := byBucket[side]
		bookLevelCount := 0
		book.sideBook(side).Iterate(func(level *PriceLevel) bool {
			bookLevelCount++
			expectedGroup, ok := expected[level.Price]
			if !ok || len(expectedGroup) == 0 {
				rt.Fatalf("side %v: book has level at price %d, model has none", side, level.Price)
			}

			// Walk the queue, verifying invariants 2, 3, 5.
			var walkedQty contract.Qty
			var walkedIDs []contract.OrderID
			var prevSeq contract.Seq
			position := 0
			for node := level.Orders.Front(); node != nil; node = node.Next() {
				order := node.Value.(*contract.RestingOrder)

				// Invariant 3
				if order.Remaining <= 0 {
					rt.Fatalf("invariant 3: live order %s has Remaining=%d", order.OrderID, order.Remaining)
				}
				walkedQty += order.Remaining
				walkedIDs = append(walkedIDs, order.OrderID)

				// Invariant 5 — Seq strictly increasing front→back.
				if position > 0 && order.Seq <= prevSeq {
					rt.Fatalf("invariant 5: Seq not strictly increasing at side %v price %d pos %d (prev=%d cur=%d)",
						side, level.Price, position, prevSeq, order.Seq)
				}
				prevSeq = order.Seq
				position++
			}

			// Invariant 2 — cached TotalQty matches walked sum.
			if walkedQty != level.TotalQty {
				rt.Fatalf("invariant 2: side %v price %d TotalQty=%d walked=%d",
					side, level.Price, level.TotalQty, walkedQty)
			}

			// Order-by-order comparison against the model (also cross-checks 5).
			if len(walkedIDs) != len(expectedGroup) {
				rt.Fatalf("side %v price %d: book has %d orders, model has %d",
					side, level.Price, len(walkedIDs), len(expectedGroup))
			}
			for pos, expectedOrder := range expectedGroup {
				if walkedIDs[pos] != expectedOrder.OrderID {
					rt.Fatalf("side %v price %d pos %d: book has %s, model has %s",
						side, level.Price, pos, walkedIDs[pos], expectedOrder.OrderID)
				}
				indexed := book.index[expectedOrder.OrderID].node.Value.(*contract.RestingOrder)
				if indexed.Remaining != expectedOrder.Remaining {
					rt.Fatalf("order %s Remaining: book=%d model=%d",
						expectedOrder.OrderID, indexed.Remaining, expectedOrder.Remaining)
				}
			}
			return true
		})
		if bookLevelCount != len(expected) {
			rt.Fatalf("side %v: book has %d levels, model has %d", side, bookLevelCount, len(expected))
		}
	}

	// Invariant 6 — quote reflects the top of each side.
	quote := book.Quote()

	// Bid: max buy price.
	bidExists := false
	var bidBestPrice contract.Ticks
	var bidBestQty contract.Qty
	for price, orders := range byBucket[contract.Buy] {
		if len(orders) == 0 {
			continue
		}
		if !bidExists || price > bidBestPrice {
			bidExists = true
			bidBestPrice = price
			var sum contract.Qty
			for _, order := range orders {
				sum += order.Remaining
			}
			bidBestQty = sum
		}
	}
	if quote.HasBid != bidExists {
		rt.Fatalf("invariant 6 (bid presence): got HasBid=%v want %v", quote.HasBid, bidExists)
	}
	if bidExists {
		if quote.BestBid != bidBestPrice || quote.BidQty != bidBestQty {
			rt.Fatalf("invariant 6 (bid): got (price=%d qty=%d) want (price=%d qty=%d)",
				quote.BestBid, quote.BidQty, bidBestPrice, bidBestQty)
		}
	} else if quote.BestBid != 0 || quote.BidQty != 0 {
		rt.Fatalf("invariant 6 (bid clearing): HasBid=false but BestBid=%d BidQty=%d",
			quote.BestBid, quote.BidQty)
	}

	// Ask: min sell price.
	askExists := false
	var askBestPrice contract.Ticks
	var askBestQty contract.Qty
	for price, orders := range byBucket[contract.Sell] {
		if len(orders) == 0 {
			continue
		}
		if !askExists || price < askBestPrice {
			askExists = true
			askBestPrice = price
			var sum contract.Qty
			for _, order := range orders {
				sum += order.Remaining
			}
			askBestQty = sum
		}
	}
	if quote.HasAsk != askExists {
		rt.Fatalf("invariant 6 (ask presence): got HasAsk=%v want %v", quote.HasAsk, askExists)
	}
	if askExists {
		if quote.BestAsk != askBestPrice || quote.AskQty != askBestQty {
			rt.Fatalf("invariant 6 (ask): got (price=%d qty=%d) want (price=%d qty=%d)",
				quote.BestAsk, quote.AskQty, askBestPrice, askBestQty)
		}
	} else if quote.BestAsk != 0 || quote.AskQty != 0 {
		rt.Fatalf("invariant 6 (ask clearing): HasAsk=false but BestAsk=%d AskQty=%d",
			quote.BestAsk, quote.AskQty)
	}
}

// assertLastPriceInvariant enforces invariant 7 using the op history: no book
// mutator writes LastPrice / HasLastPrice; only SetLastPrice does.
func assertLastPriceInvariant(rt *rapid.T, book *Book, everSet bool, lastValue contract.Ticks) {
	quote := book.Quote()
	if everSet {
		if !quote.HasLastPrice || quote.LastPrice != lastValue {
			rt.Fatalf("invariant 7: after SetLastPrice(%d) got has=%v price=%d",
				lastValue, quote.HasLastPrice, quote.LastPrice)
		}
		return
	}
	if quote.HasLastPrice || quote.LastPrice != 0 {
		rt.Fatalf("invariant 7: book mutator leaked into last price: has=%v price=%d",
			quote.HasLastPrice, quote.LastPrice)
	}
}

// --- snapshot for determinism check -------------------------------------

type orderSnapshot struct {
	OrderID   contract.OrderID
	Remaining contract.Qty
	Seq       contract.Seq
}

type levelSnapshot struct {
	price    contract.Ticks
	totalQty contract.Qty
	orders   []orderSnapshot
}

func snapshotSide(side SideBook) []levelSnapshot {
	var snapshot []levelSnapshot
	side.Iterate(func(level *PriceLevel) bool {
		snap := levelSnapshot{price: level.Price, totalQty: level.TotalQty}
		for node := level.Orders.Front(); node != nil; node = node.Next() {
			order := node.Value.(*contract.RestingOrder)
			snap.orders = append(snap.orders, orderSnapshot{
				OrderID: order.OrderID, Remaining: order.Remaining, Seq: order.Seq,
			})
		}
		snapshot = append(snapshot, snap)
		return true
	})
	return snapshot
}

func assertBooksIdentical(rt *rapid.T, a, b *Book) {
	if a.Quote() != b.Quote() {
		rt.Fatalf("determinism: quotes differ\n  a=%+v\n  b=%+v", a.Quote(), b.Quote())
	}
	if len(a.index) != len(b.index) {
		rt.Fatalf("determinism: index size a=%d b=%d", len(a.index), len(b.index))
	}
	for id := range a.index {
		if _, ok := b.index[id]; !ok {
			rt.Fatalf("determinism: id %s in a, not in b", id)
		}
	}
	for _, side := range []contract.Side{contract.Buy, contract.Sell} {
		aSide := snapshotSide(a.sideBook(side))
		bSide := snapshotSide(b.sideBook(side))
		if len(aSide) != len(bSide) {
			rt.Fatalf("determinism: side %v level count a=%d b=%d", side, len(aSide), len(bSide))
		}
		for pos, aLevel := range aSide {
			bLevel := bSide[pos]
			if aLevel.price != bLevel.price || aLevel.totalQty != bLevel.totalQty {
				rt.Fatalf("determinism: side %v pos %d a=%+v b=%+v", side, pos, aLevel, bLevel)
			}
			if len(aLevel.orders) != len(bLevel.orders) {
				rt.Fatalf("determinism: side %v price %d order count a=%d b=%d",
					side, aLevel.price, len(aLevel.orders), len(bLevel.orders))
			}
			for j := range aLevel.orders {
				if aLevel.orders[j] != bLevel.orders[j] {
					rt.Fatalf("determinism: side %v price %d pos %d a=%+v b=%+v",
						side, aLevel.price, j, aLevel.orders[j], bLevel.orders[j])
				}
			}
		}
	}
}

// --- tests --------------------------------------------------------------

// TestBook_Property_InvariantsHoldAcrossRandomOps runs a random legal op
// sequence through a single book and asserts EXCHANGE_11 §9 invariants 2–7
// after every op. Invariant 1 (integer ticks) is enforced by the type system.
func TestBook_Property_InvariantsHoldAcrossRandomOps(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		book := NewBook("ACME")
		model := newBookModel()
		everSetLastPrice := false
		var lastValue contract.Ticks

		opCount := rapid.IntRange(0, 200).Draw(rt, "opCount")
		orderIDCounter := 0
		ghostCounter := 0
		for step := 0; step < opCount; step++ {
			op := drawOneOp(rt, model, &orderIDCounter, &ghostCounter)
			op.applyToBook(book)
			op.applyToModel(model)
			if sp, isTrade := op.(*opSetLastPrice); isTrade {
				everSetLastPrice = true
				lastValue = sp.price
			}
			assertBookMatchesModel(rt, book, model)
			assertLastPriceInvariant(rt, book, everSetLastPrice, lastValue)
		}
	})
}

// TestBook_Determinism_SameOpsProduceSameState pre-generates a coherent op
// sequence, applies it to two fresh books independently, and asserts the
// resulting state is byte-identical (per constitution §Testing). Any hidden
// non-determinism — a map-iteration leak, an unstable Iterate order, a
// timing-dependent code path — surfaces here.
func TestBook_Determinism_SameOpsProduceSameState(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		ops := generateOps(rt)

		bookA := NewBook("ACME")
		bookB := NewBook("ACME")
		for _, op := range ops {
			op.applyToBook(bookA)
		}
		for _, op := range ops {
			op.applyToBook(bookB)
		}
		assertBooksIdentical(rt, bookA, bookB)
	})
}
