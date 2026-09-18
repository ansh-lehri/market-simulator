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
