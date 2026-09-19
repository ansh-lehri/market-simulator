package match

import (
	"testing"

	"simulator/contract"
	"simulator/exchange/book"
)

// newTestMatcher wires a Matcher against a fresh ACME book with the always-
// allow clamp and a monotonic sequence stub.
func newTestMatcher(t *testing.T) (*Matcher, *book.Book) {
	t.Helper()
	orderBook := book.NewBook("ACME")
	matcher := NewMatcher(orderBook, allowAllClamp{}, &monotonicSeq{})
	return matcher, orderBook
}

// makeResting builds a resting maker for seeding the book in golden tests.
func makeResting(
	id contract.OrderID,
	participant contract.ParticipantID,
	side contract.Side,
	price contract.Ticks,
	remaining contract.Qty,
	seq contract.Seq,
) *contract.RestingOrder {
	return &contract.RestingOrder{
		OrderID:     id,
		Participant: participant,
		BrokerID:    "B_MAKER",
		Side:        side,
		Price:       price,
		Remaining:   remaining,
		Seq:         seq,
	}
}

// assertTrade fails the test if the observed trade does not match every
// field of the expected trade (Seq excluded — the sequence source is a stub).
func assertTrade(t *testing.T, index int, got, want contract.Trade) {
	t.Helper()
	if got.Instrument != want.Instrument {
		t.Errorf("trade[%d].Instrument: got %q, want %q", index, got.Instrument, want.Instrument)
	}
	if got.Price != want.Price {
		t.Errorf("trade[%d].Price: got %d, want %d", index, got.Price, want.Price)
	}
	if got.Qty != want.Qty {
		t.Errorf("trade[%d].Qty: got %d, want %d", index, got.Qty, want.Qty)
	}
	if got.TakerOrder != want.TakerOrder {
		t.Errorf("trade[%d].TakerOrder: got %q, want %q", index, got.TakerOrder, want.TakerOrder)
	}
	if got.MakerOrder != want.MakerOrder {
		t.Errorf("trade[%d].MakerOrder: got %q, want %q", index, got.MakerOrder, want.MakerOrder)
	}
	if got.TakerSide != want.TakerSide {
		t.Errorf("trade[%d].TakerSide: got %v, want %v", index, got.TakerSide, want.TakerSide)
	}
	if got.TakerParticipant != want.TakerParticipant {
		t.Errorf("trade[%d].TakerParticipant: got %q, want %q", index, got.TakerParticipant, want.TakerParticipant)
	}
	if got.MakerParticipant != want.MakerParticipant {
		t.Errorf("trade[%d].MakerParticipant: got %q, want %q", index, got.MakerParticipant, want.MakerParticipant)
	}
}

// EXCHANGE_12 §7 remainder table (GTC-limit row): with no opposite liquidity,
// the whole order rests at its own LimitPrice; last price does not move; the
// book's quote reflects the new resting bid.
func TestMatch_EmptyBook_LimitGTC_RestsAtLimitPrice(t *testing.T) {
	matcher, orderBook := newTestMatcher(t)

	order := &contract.FundedOrder{
		OrderID:     "O1",
		Participant: "P1",
		Instrument:  "ACME",
		Side:        contract.Buy,
		Type:        contract.Limit,
		TIF:         contract.GTC,
		Qty:         10,
		LimitPrice:  10000,
	}
	const brokerID contract.BrokerID = "B1"
	const admissionSeq contract.Seq = 42

	result := matcher.Match(order, brokerID, admissionSeq)

	if got := len(result.Trades); got != 0 {
		t.Errorf("Trades: got %d, want 0", got)
	}
	if result.CancelledQty != 0 {
		t.Errorf("CancelledQty: got %d, want 0", result.CancelledQty)
	}
	if result.CancelReason != contract.ReasonNone {
		t.Errorf("CancelReason: got %v, want ReasonNone", result.CancelReason)
	}
	if result.LastPriceMoved {
		t.Errorf("LastPriceMoved: got true, want false")
	}
	if result.LastPrice != 0 {
		t.Errorf("LastPrice: got %d, want 0", result.LastPrice)
	}

	if result.Rested == nil {
		t.Fatal("Rested: got nil, want non-nil")
	}
	if result.Rested.OrderID != "O1" {
		t.Errorf("Rested.OrderID: got %q, want %q", result.Rested.OrderID, "O1")
	}
	if result.Rested.Participant != "P1" {
		t.Errorf("Rested.Participant: got %q, want %q", result.Rested.Participant, "P1")
	}
	if result.Rested.BrokerID != brokerID {
		t.Errorf("Rested.BrokerID: got %q, want %q", result.Rested.BrokerID, brokerID)
	}
	if result.Rested.Side != contract.Buy {
		t.Errorf("Rested.Side: got %v, want Buy", result.Rested.Side)
	}
	if result.Rested.Price != 10000 {
		t.Errorf("Rested.Price: got %d, want 10000", result.Rested.Price)
	}
	if result.Rested.Remaining != 10 {
		t.Errorf("Rested.Remaining: got %d, want 10", result.Rested.Remaining)
	}
	if result.Rested.Seq != admissionSeq {
		t.Errorf("Rested.Seq: got %d, want %d", result.Rested.Seq, admissionSeq)
	}

	quote := orderBook.Quote()
	if !quote.HasBid || quote.BestBid != 10000 || quote.BidQty != 10 {
		t.Errorf("book quote: got %+v, want bid 10000 qty 10", quote)
	}
	if quote.HasLastPrice {
		t.Errorf("HasLastPrice: got true, want false")
	}
}

// EXCHANGE_12 §7 remainder table (Market row): a market order against an
// empty opposite book prints nothing, never rests, and reports the full qty
// cancelled with ReasonNoLiquidity.
func TestMatch_EmptyBook_Market_CancelsWithNoLiquidity(t *testing.T) {
	matcher, orderBook := newTestMatcher(t)

	order := &contract.FundedOrder{
		OrderID:     "O1",
		Participant: "P1",
		Instrument:  "ACME",
		Side:        contract.Buy,
		Type:        contract.Market,
		TIF:         contract.IOC,
		Qty:         5,
		MaxSpend:    1_000_000,
	}

	result := matcher.Match(order, "B1", 7)

	if got := len(result.Trades); got != 0 {
		t.Errorf("Trades: got %d, want 0", got)
	}
	if result.Rested != nil {
		t.Errorf("Rested: got %+v, want nil", result.Rested)
	}
	if result.CancelledQty != 5 {
		t.Errorf("CancelledQty: got %d, want 5", result.CancelledQty)
	}
	if result.CancelReason != contract.ReasonNoLiquidity {
		t.Errorf("CancelReason: got %v, want ReasonNoLiquidity", result.CancelReason)
	}
	if result.LastPriceMoved {
		t.Errorf("LastPriceMoved: got true, want false")
	}

	quote := orderBook.Quote()
	if quote.HasBid || quote.HasAsk || quote.HasLastPrice {
		t.Errorf("book quote should be untouched, got %+v", quote)
	}
}

// EXCHANGE_12 §7 remainder table (IOC row): an IOC limit against an empty
// opposite book prints nothing, never rests, and reports the full qty
// cancelled with ReasonIOCLeftover.
func TestMatch_EmptyBook_IOCLimit_CancelsWithIOCLeftover(t *testing.T) {
	matcher, orderBook := newTestMatcher(t)

	order := &contract.FundedOrder{
		OrderID:     "O1",
		Participant: "P1",
		Instrument:  "ACME",
		Side:        contract.Buy,
		Type:        contract.Limit,
		TIF:         contract.IOC,
		Qty:         5,
		LimitPrice:  10000,
	}

	result := matcher.Match(order, "B1", 3)

	if got := len(result.Trades); got != 0 {
		t.Errorf("Trades: got %d, want 0", got)
	}
	if result.Rested != nil {
		t.Errorf("Rested: got %+v, want nil", result.Rested)
	}
	if result.CancelledQty != 5 {
		t.Errorf("CancelledQty: got %d, want 5", result.CancelledQty)
	}
	if result.CancelReason != contract.ReasonIOCLeftover {
		t.Errorf("CancelReason: got %v, want ReasonIOCLeftover", result.CancelReason)
	}
	if result.LastPriceMoved {
		t.Errorf("LastPriceMoved: got true, want false")
	}

	quote := orderBook.Quote()
	if quote.HasBid || quote.HasAsk || quote.HasLastPrice {
		t.Errorf("book quote should be untouched, got %+v", quote)
	}
}

// G1 — price improvement: a Buy Limit @10025 that hits a resting Ask @10023
// prints at the MAKER's price (10023), never the taker's (EXCHANGE_12 §2 +
// §12 inv 1). Last price = final trade price = 10023.
func TestMatch_G1_PriceImprovement(t *testing.T) {
	matcher, orderBook := newTestMatcher(t)
	orderBook.Insert(makeResting("M1", "P_M", contract.Sell, 10023, 40, 1))

	order := &contract.FundedOrder{
		OrderID:     "T",
		Participant: "P_T",
		Instrument:  "ACME",
		Side:        contract.Buy,
		Type:        contract.Limit,
		TIF:         contract.GTC,
		Qty:         40,
		LimitPrice:  10025,
	}

	result := matcher.Match(order, "B_T", 100)

	if len(result.Trades) != 1 {
		t.Fatalf("Trades: got %d, want 1", len(result.Trades))
	}
	assertTrade(t, 0, result.Trades[0], contract.Trade{
		Instrument:       "ACME",
		Price:            10023,
		Qty:              40,
		TakerOrder:       "T",
		MakerOrder:       "M1",
		TakerSide:        contract.Buy,
		TakerParticipant: "P_T",
		MakerParticipant: "P_M",
	})
	if result.Rested != nil {
		t.Errorf("Rested: got %+v, want nil", result.Rested)
	}
	if result.CancelledQty != 0 {
		t.Errorf("CancelledQty: got %d, want 0", result.CancelledQty)
	}
	if !result.LastPriceMoved || result.LastPrice != 10023 {
		t.Errorf("LastPrice: got (%d, moved=%v), want (10023, true)", result.LastPrice, result.LastPriceMoved)
	}

	quote := orderBook.Quote()
	if quote.HasAsk {
		t.Errorf("ask side should be empty after M1 fully consumed, quote=%+v", quote)
	}
	if !quote.HasLastPrice || quote.LastPrice != 10023 {
		t.Errorf("book LastPrice: got (%d, has=%v), want (10023, true)", quote.LastPrice, quote.HasLastPrice)
	}
}

// G2 — slippage across two levels + rest remainder: Buy Limit @10025 qty 60
// against Ask 10023×30, 10024×20, 10026×50. Prints 30@10023 then 20@10024;
// the third level's price (10026) fails priceAcceptable and the walk stops.
// GTC-limit remainder of 10 rests at the taker's LimitPrice (10025) as a
// new bid. Last price = final trade's maker price = 10024.
// EXCHANGE_12 §7 (remainder table) + §8.1 (slippage).
func TestMatch_G2_SlippageThenRest(t *testing.T) {
	matcher, orderBook := newTestMatcher(t)
	orderBook.Insert(makeResting("M1", "P_M1", contract.Sell, 10023, 30, 1))
	orderBook.Insert(makeResting("M2", "P_M2", contract.Sell, 10024, 20, 2))
	orderBook.Insert(makeResting("M3", "P_M3", contract.Sell, 10026, 50, 3))

	order := &contract.FundedOrder{
		OrderID:     "T",
		Participant: "P_T",
		Instrument:  "ACME",
		Side:        contract.Buy,
		Type:        contract.Limit,
		TIF:         contract.GTC,
		Qty:         60,
		LimitPrice:  10025,
	}

	result := matcher.Match(order, "B_T", 200)

	if len(result.Trades) != 2 {
		t.Fatalf("Trades: got %d, want 2", len(result.Trades))
	}
	assertTrade(t, 0, result.Trades[0], contract.Trade{
		Instrument: "ACME", Price: 10023, Qty: 30,
		TakerOrder: "T", MakerOrder: "M1", TakerSide: contract.Buy,
		TakerParticipant: "P_T", MakerParticipant: "P_M1",
	})
	assertTrade(t, 1, result.Trades[1], contract.Trade{
		Instrument: "ACME", Price: 10024, Qty: 20,
		TakerOrder: "T", MakerOrder: "M2", TakerSide: contract.Buy,
		TakerParticipant: "P_T", MakerParticipant: "P_M2",
	})

	if result.Rested == nil {
		t.Fatal("Rested: got nil, want non-nil (GTC-limit remainder should rest)")
	}
	if result.Rested.Price != 10025 || result.Rested.Remaining != 10 || result.Rested.Side != contract.Buy {
		t.Errorf("Rested: got price=%d rem=%d side=%v, want price=10025 rem=10 side=Buy",
			result.Rested.Price, result.Rested.Remaining, result.Rested.Side)
	}
	if result.Rested.BrokerID != "B_T" || result.Rested.Seq != 200 {
		t.Errorf("Rested envelope: got broker=%q seq=%d, want broker=%q seq=200",
			result.Rested.BrokerID, result.Rested.Seq, "B_T")
	}
	if result.CancelledQty != 0 {
		t.Errorf("CancelledQty: got %d, want 0", result.CancelledQty)
	}
	if !result.LastPriceMoved || result.LastPrice != 10024 {
		t.Errorf("LastPrice: got (%d, moved=%v), want (10024, true)", result.LastPrice, result.LastPriceMoved)
	}

	quote := orderBook.Quote()
	if !quote.HasAsk || quote.BestAsk != 10026 || quote.AskQty != 50 {
		t.Errorf("best ask: got (has=%v price=%d qty=%d), want (true, 10026, 50)",
			quote.HasAsk, quote.BestAsk, quote.AskQty)
	}
	if !quote.HasBid || quote.BestBid != 10025 || quote.BidQty != 10 {
		t.Errorf("best bid: got (has=%v price=%d qty=%d), want (true, 10025, 10)",
			quote.HasBid, quote.BestBid, quote.BidQty)
	}
	if !quote.HasLastPrice || quote.LastPrice != 10024 {
		t.Errorf("book LastPrice: got (%d, has=%v), want (10024, true)", quote.LastPrice, quote.HasLastPrice)
	}
}

// G9 — partial then rest (GTC-limit): Buy Limit @10000 qty 10 against a
// single resting Ask 10000×3. Consumes the level entirely, remainder 7 rests
// at 10000 as a bid. EXCHANGE_12 §7 remainder table.
func TestMatch_G9_PartialThenRest(t *testing.T) {
	matcher, orderBook := newTestMatcher(t)
	orderBook.Insert(makeResting("M1", "P_M", contract.Sell, 10000, 3, 1))

	order := &contract.FundedOrder{
		OrderID:     "T",
		Participant: "P_T",
		Instrument:  "ACME",
		Side:        contract.Buy,
		Type:        contract.Limit,
		TIF:         contract.GTC,
		Qty:         10,
		LimitPrice:  10000,
	}

	result := matcher.Match(order, "B_T", 300)

	if len(result.Trades) != 1 {
		t.Fatalf("Trades: got %d, want 1", len(result.Trades))
	}
	assertTrade(t, 0, result.Trades[0], contract.Trade{
		Instrument: "ACME", Price: 10000, Qty: 3,
		TakerOrder: "T", MakerOrder: "M1", TakerSide: contract.Buy,
		TakerParticipant: "P_T", MakerParticipant: "P_M",
	})
	if result.Rested == nil {
		t.Fatal("Rested: got nil, want non-nil")
	}
	if result.Rested.Price != 10000 || result.Rested.Remaining != 7 {
		t.Errorf("Rested: got price=%d rem=%d, want price=10000 rem=7",
			result.Rested.Price, result.Rested.Remaining)
	}
	if result.CancelledQty != 0 {
		t.Errorf("CancelledQty: got %d, want 0", result.CancelledQty)
	}
	if !result.LastPriceMoved || result.LastPrice != 10000 {
		t.Errorf("LastPrice: got (%d, moved=%v), want (10000, true)", result.LastPrice, result.LastPriceMoved)
	}

	quote := orderBook.Quote()
	if quote.HasAsk {
		t.Errorf("ask side should be empty, got %+v", quote)
	}
	if !quote.HasBid || quote.BestBid != 10000 || quote.BidQty != 7 {
		t.Errorf("bid: got (has=%v price=%d qty=%d), want (true, 10000, 7)",
			quote.HasBid, quote.BestBid, quote.BidQty)
	}
}

// G10 — time priority within a level: two Asks at 10000, M1 seq=1 (oldest),
// M2 seq=2. Buy Limit @10000 qty 7 must fill M1 fully (5) first, then 2
// from M2 — never the other order. EXCHANGE_12 §12 inv 2.
func TestMatch_G10_TimePriorityWithinLevel(t *testing.T) {
	matcher, orderBook := newTestMatcher(t)
	orderBook.Insert(makeResting("M1", "P_M1", contract.Sell, 10000, 5, 1))
	orderBook.Insert(makeResting("M2", "P_M2", contract.Sell, 10000, 5, 2))

	order := &contract.FundedOrder{
		OrderID:     "T",
		Participant: "P_T",
		Instrument:  "ACME",
		Side:        contract.Buy,
		Type:        contract.Limit,
		TIF:         contract.GTC,
		Qty:         7,
		LimitPrice:  10000,
	}

	result := matcher.Match(order, "B_T", 400)

	if len(result.Trades) != 2 {
		t.Fatalf("Trades: got %d, want 2", len(result.Trades))
	}
	assertTrade(t, 0, result.Trades[0], contract.Trade{
		Instrument: "ACME", Price: 10000, Qty: 5,
		TakerOrder: "T", MakerOrder: "M1", TakerSide: contract.Buy,
		TakerParticipant: "P_T", MakerParticipant: "P_M1",
	})
	assertTrade(t, 1, result.Trades[1], contract.Trade{
		Instrument: "ACME", Price: 10000, Qty: 2,
		TakerOrder: "T", MakerOrder: "M2", TakerSide: contract.Buy,
		TakerParticipant: "P_T", MakerParticipant: "P_M2",
	})
	if result.Rested != nil {
		t.Errorf("Rested: got %+v, want nil", result.Rested)
	}
	if result.CancelledQty != 0 {
		t.Errorf("CancelledQty: got %d, want 0", result.CancelledQty)
	}
	if !result.LastPriceMoved || result.LastPrice != 10000 {
		t.Errorf("LastPrice: got (%d, moved=%v), want (10000, true)", result.LastPrice, result.LastPriceMoved)
	}

	quote := orderBook.Quote()
	if !quote.HasAsk || quote.BestAsk != 10000 || quote.AskQty != 3 {
		t.Errorf("ask leftover (M2 with 3): got (has=%v price=%d qty=%d), want (true, 10000, 3)",
			quote.HasAsk, quote.BestAsk, quote.AskQty)
	}
}

// G3 — Market BUY funded-cap floor: MaxSpend=25000 against Ask 10000×3 and
// Ask 10025×2. Loop 1: fillQty=min(5,3)=3, affordable=25000/10000=2 (floor)
// → capped to 2, print 2@10000, spent=20000, remaining=3. Loop 2: same
// level has 1 left; fillQty=min(3,1)=1, affordable=(25000-20000)/10000=0
// → fillQty=0, break FundedCap. CancelledQty=3, residual 5000 cash is
// unspendable at 10000/unit (expected — EXCHANGE_12 §8.3). Never rests.
func TestMatch_G3_MarketBuy_FundedCapFloor(t *testing.T) {
	matcher, orderBook := newTestMatcher(t)
	orderBook.Insert(makeResting("M1", "P_M1", contract.Sell, 10000, 3, 1))
	orderBook.Insert(makeResting("M2", "P_M2", contract.Sell, 10025, 2, 2))

	order := &contract.FundedOrder{
		OrderID:     "T",
		Participant: "P_T",
		Instrument:  "ACME",
		Side:        contract.Buy,
		Type:        contract.Market,
		TIF:         contract.IOC,
		Qty:         5,
		MaxSpend:    25000,
	}

	result := matcher.Match(order, "B_T", 500)

	if len(result.Trades) != 1 {
		t.Fatalf("Trades: got %d, want 1", len(result.Trades))
	}
	assertTrade(t, 0, result.Trades[0], contract.Trade{
		Instrument: "ACME", Price: 10000, Qty: 2,
		TakerOrder: "T", MakerOrder: "M1", TakerSide: contract.Buy,
		TakerParticipant: "P_T", MakerParticipant: "P_M1",
	})
	if result.Rested != nil {
		t.Errorf("Rested: got %+v, want nil (market never rests)", result.Rested)
	}
	if result.CancelledQty != 3 {
		t.Errorf("CancelledQty: got %d, want 3", result.CancelledQty)
	}
	if result.CancelReason != contract.ReasonFundedCap {
		t.Errorf("CancelReason: got %v, want ReasonFundedCap", result.CancelReason)
	}
	if !result.LastPriceMoved || result.LastPrice != 10000 {
		t.Errorf("LastPrice: got (%d, moved=%v), want (10000, true)", result.LastPrice, result.LastPriceMoved)
	}

	// Ask side still has M1 (1 left) and M2 (2 left) — the walk stopped
	// before consuming either fully.
	quote := orderBook.Quote()
	if !quote.HasAsk || quote.BestAsk != 10000 || quote.AskQty != 1 {
		t.Errorf("best ask: got (has=%v price=%d qty=%d), want (true, 10000, 1)",
			quote.HasAsk, quote.BestAsk, quote.AskQty)
	}
}

// G3b — Market BUY without cap-bite: MaxSpend far exceeds cost. Order
// fully fills at the resting price with no CancelledQty, no FundedCap.
// Regression: funded-cap logic must not distort the normal-fill path.
func TestMatch_G3b_MarketBuy_NoCapBite(t *testing.T) {
	matcher, orderBook := newTestMatcher(t)
	orderBook.Insert(makeResting("M1", "P_M", contract.Sell, 10000, 5, 1))

	order := &contract.FundedOrder{
		OrderID:     "T",
		Participant: "P_T",
		Instrument:  "ACME",
		Side:        contract.Buy,
		Type:        contract.Market,
		TIF:         contract.IOC,
		Qty:         5,
		MaxSpend:    1_000_000,
	}

	result := matcher.Match(order, "B_T", 501)

	if len(result.Trades) != 1 {
		t.Fatalf("Trades: got %d, want 1", len(result.Trades))
	}
	assertTrade(t, 0, result.Trades[0], contract.Trade{
		Instrument: "ACME", Price: 10000, Qty: 5,
		TakerOrder: "T", MakerOrder: "M1", TakerSide: contract.Buy,
		TakerParticipant: "P_T", MakerParticipant: "P_M",
	})
	if result.Rested != nil {
		t.Errorf("Rested: got %+v, want nil", result.Rested)
	}
	if result.CancelledQty != 0 {
		t.Errorf("CancelledQty: got %d, want 0", result.CancelledQty)
	}
	if result.CancelReason != contract.ReasonNone {
		t.Errorf("CancelReason: got %v, want ReasonNone", result.CancelReason)
	}
	if !result.LastPriceMoved || result.LastPrice != 10000 {
		t.Errorf("LastPrice: got (%d, moved=%v), want (10000, true)", result.LastPrice, result.LastPriceMoved)
	}
}

// G3c — Market BUY exact-fit at MaxSpend: MaxSpend=20000 buys exactly 2
// units of Ask 10000×2 in the first level (spent hits MaxSpend after the
// fill), leaving remainder 3 that step (h) cancels with FundedCap.
// Distinguishes the step-(h) post-fill halt from the step-(e) affordability
// zero-out (which is G3).
func TestMatch_G3c_MarketBuy_ExactFitStopsAtStepH(t *testing.T) {
	matcher, orderBook := newTestMatcher(t)
	orderBook.Insert(makeResting("M1", "P_M1", contract.Sell, 10000, 2, 1))
	orderBook.Insert(makeResting("M2", "P_M2", contract.Sell, 10005, 5, 2))

	order := &contract.FundedOrder{
		OrderID:     "T",
		Participant: "P_T",
		Instrument:  "ACME",
		Side:        contract.Buy,
		Type:        contract.Market,
		TIF:         contract.IOC,
		Qty:         5,
		MaxSpend:    20000, // exactly covers 2 units at 10000
	}

	result := matcher.Match(order, "B_T", 502)

	if len(result.Trades) != 1 {
		t.Fatalf("Trades: got %d, want 1", len(result.Trades))
	}
	assertTrade(t, 0, result.Trades[0], contract.Trade{
		Instrument: "ACME", Price: 10000, Qty: 2,
		TakerOrder: "T", MakerOrder: "M1", TakerSide: contract.Buy,
		TakerParticipant: "P_T", MakerParticipant: "P_M1",
	})
	if result.Rested != nil {
		t.Errorf("Rested: got %+v, want nil", result.Rested)
	}
	if result.CancelledQty != 3 {
		t.Errorf("CancelledQty: got %d, want 3", result.CancelledQty)
	}
	if result.CancelReason != contract.ReasonFundedCap {
		t.Errorf("CancelReason: got %v, want ReasonFundedCap", result.CancelReason)
	}
	if !result.LastPriceMoved || result.LastPrice != 10000 {
		t.Errorf("LastPrice: got (%d, moved=%v), want (10000, true)", result.LastPrice, result.LastPriceMoved)
	}

	// M1 fully consumed, M2 still resting untouched.
	quote := orderBook.Quote()
	if !quote.HasAsk || quote.BestAsk != 10005 || quote.AskQty != 5 {
		t.Errorf("best ask: got (has=%v price=%d qty=%d), want (true, 10005, 5)",
			quote.HasAsk, quote.BestAsk, quote.AskQty)
	}
}

// G3d — Market SELL regression: funded-cap logic is Market-BUY-only. A Sell
// market walks bids without any MaxSpend check and fully fills against a
// resting bid. Guards against a hypothetical bug where the affordability
// branch (or step (h)) is accidentally reached for Sell markets.
func TestMatch_G3d_MarketSell_FullFill(t *testing.T) {
	matcher, orderBook := newTestMatcher(t)
	orderBook.Insert(makeResting("M1", "P_M", contract.Buy, 10005, 5, 1))

	order := &contract.FundedOrder{
		OrderID:     "T",
		Participant: "P_T",
		Instrument:  "ACME",
		Side:        contract.Sell,
		Type:        contract.Market,
		TIF:         contract.IOC,
		Qty:         5,
		MaxSpend:    0, // not used on a sell; must not influence the walk
	}

	result := matcher.Match(order, "B_T", 503)

	if len(result.Trades) != 1 {
		t.Fatalf("Trades: got %d, want 1", len(result.Trades))
	}
	assertTrade(t, 0, result.Trades[0], contract.Trade{
		Instrument: "ACME", Price: 10005, Qty: 5,
		TakerOrder: "T", MakerOrder: "M1", TakerSide: contract.Sell,
		TakerParticipant: "P_T", MakerParticipant: "P_M",
	})
	if result.Rested != nil {
		t.Errorf("Rested: got %+v, want nil", result.Rested)
	}
	if result.CancelledQty != 0 {
		t.Errorf("CancelledQty: got %d, want 0", result.CancelledQty)
	}
	if result.CancelReason != contract.ReasonNone {
		t.Errorf("CancelReason: got %v, want ReasonNone", result.CancelReason)
	}
	if !result.LastPriceMoved || result.LastPrice != 10005 {
		t.Errorf("LastPrice: got (%d, moved=%v), want (10005, true)", result.LastPrice, result.LastPriceMoved)
	}
}
