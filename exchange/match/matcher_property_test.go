package match

import (
	"fmt"
	"testing"

	"pgregory.net/rapid"

	"simulator/contract"
	"simulator/exchange/book"
)

// drawTakerSide draws Buy or Sell uniformly for a rapid test case.
func drawTakerSide(t *rapid.T) contract.Side {
	if rapid.Bool().Draw(t, "takerBuy") {
		return contract.Buy
	}
	return contract.Sell
}

// seedRandomMakers fills a fresh book with 1..6 resting orders on
// `makerSide`, prices in [10000, 10050], qty in [1, 20], strictly
// increasing admission Seq. Returns matcher, book, and the set of maker
// prices that were resting before the walk began.
func seedRandomMakers(t *rapid.T, makerSide contract.Side) (*Matcher, *book.Book, map[contract.Ticks]bool) {
	orderBook := book.NewBook("ACME")
	matcher := NewMatcher(orderBook, allowAllClamp{}, &monotonicSeq{})

	count := rapid.IntRange(1, 6).Draw(t, "makerCount")
	makerPrices := make(map[contract.Ticks]bool, count)
	for makerIndex := 0; makerIndex < count; makerIndex++ {
		price := contract.Ticks(rapid.Int64Range(10000, 10050).Draw(t, fmt.Sprintf("price[%d]", makerIndex)))
		qty := contract.Qty(rapid.Int64Range(1, 20).Draw(t, fmt.Sprintf("qty[%d]", makerIndex)))
		orderBook.Insert(&contract.RestingOrder{
			OrderID:     contract.OrderID(fmt.Sprintf("M%d", makerIndex)),
			Participant: contract.ParticipantID(fmt.Sprintf("P_M%d", makerIndex)),
			BrokerID:    "B_M",
			Side:        makerSide,
			Price:       price,
			Remaining:   qty,
			Seq:         contract.Seq(makerIndex + 1),
		})
		makerPrices[price] = true
	}
	return matcher, orderBook, makerPrices
}

// crossesTakerLimit reports whether `tradePrice` is on the cross-side of
// the taker's LimitPrice — the direction depends on takerSide.
func crossesTakerLimit(tradePrice contract.Ticks, order *contract.FundedOrder) bool {
	if order.Side == contract.Buy {
		return tradePrice <= order.LimitPrice
	}
	return tradePrice >= order.LimitPrice
}

// Invariant 1 (EXCHANGE_12 §12): every trade prints at a maker price that
// was in the pre-walk book, on the cross-side of the taker's LimitPrice.
// Exercised for BOTH taker sides — rapid draws the side each iteration.
func TestProperty_Inv1_TradePriceIsAMakerPrice(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		takerSide := drawTakerSide(t)
		matcher, _, makerPrices := seedRandomMakers(t, oppositeSide(takerSide))

		takerLimit := contract.Ticks(rapid.Int64Range(9900, 10100).Draw(t, "takerLimit"))
		takerQty := contract.Qty(rapid.Int64Range(1, 200).Draw(t, "takerQty"))

		order := &contract.FundedOrder{
			OrderID:     "T",
			Participant: "P_T",
			Instrument:  "ACME",
			Side:        takerSide,
			Type:        contract.Limit,
			TIF:         contract.GTC,
			Qty:         takerQty,
			LimitPrice:  takerLimit,
		}
		result := matcher.Match(order, "B_T", 999)

		for tradeIndex, trade := range result.Trades {
			if !makerPrices[trade.Price] {
				t.Fatalf("takerSide=%v trade[%d].Price=%d not among pre-walk maker prices %v",
					takerSide, tradeIndex, trade.Price, makerPrices)
			}
			if !crossesTakerLimit(trade.Price, order) {
				t.Fatalf("takerSide=%v trade[%d].Price=%d does not cross taker LimitPrice=%d",
					takerSide, tradeIndex, trade.Price, order.LimitPrice)
			}
			if trade.Qty <= 0 {
				t.Fatalf("takerSide=%v trade[%d].Qty=%d, want > 0",
					takerSide, tradeIndex, trade.Qty)
			}
			if trade.TakerSide != takerSide {
				t.Fatalf("trade[%d].TakerSide=%v, want %v", tradeIndex, trade.TakerSide, takerSide)
			}
			if trade.Instrument != "ACME" {
				t.Fatalf("trade[%d].Instrument=%q, want %q", tradeIndex, trade.Instrument, "ACME")
			}
		}
	})
}

// Invariant 2 (EXCHANGE_12 §12): time priority within a level — the walk
// always consumes the level's front (lowest Seq) first. Test with all
// makers on ONE price level, exercised for BOTH taker sides.
func TestProperty_Inv2_TimePriorityWithinLevel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		takerSide := drawTakerSide(t)
		makerSide := oppositeSide(takerSide)

		orderBook := book.NewBook("ACME")
		matcher := NewMatcher(orderBook, allowAllClamp{}, &monotonicSeq{})

		makerCount := rapid.IntRange(2, 6).Draw(t, "makerCount")
		const commonPrice contract.Ticks = 10000
		for makerIndex := 0; makerIndex < makerCount; makerIndex++ {
			qty := contract.Qty(rapid.Int64Range(1, 10).Draw(t, fmt.Sprintf("qty[%d]", makerIndex)))
			orderBook.Insert(&contract.RestingOrder{
				OrderID:     contract.OrderID(fmt.Sprintf("M%d", makerIndex)),
				Participant: contract.ParticipantID(fmt.Sprintf("P_M%d", makerIndex)),
				BrokerID:    "B_M",
				Side:        makerSide,
				Price:       commonPrice,
				Remaining:   qty,
				Seq:         contract.Seq(makerIndex + 1),
			})
		}

		takerQty := contract.Qty(rapid.Int64Range(1, int64(makerCount*10)).Draw(t, "takerQty"))
		order := &contract.FundedOrder{
			OrderID:     "T",
			Participant: "P_T",
			Instrument:  "ACME",
			Side:        takerSide,
			Type:        contract.Limit,
			TIF:         contract.GTC,
			Qty:         takerQty,
			LimitPrice:  commonPrice, // any Buy@10000 crosses Ask@10000; any Sell@10000 crosses Bid@10000
		}
		result := matcher.Match(order, "B_T", 999)

		// Maker ids are "M0", "M1", ...; parse the numeric suffix and
		// assert it never decreases across the trade sequence.
		previousMakerIndex := -1
		for tradeIndex, trade := range result.Trades {
			var makerIndex int
			if _, err := fmt.Sscanf(string(trade.MakerOrder), "M%d", &makerIndex); err != nil {
				t.Fatalf("takerSide=%v trade[%d] unparseable MakerOrder %q: %v",
					takerSide, tradeIndex, trade.MakerOrder, err)
			}
			if makerIndex <= previousMakerIndex {
				t.Fatalf("takerSide=%v trade[%d] maker %q broke time priority (index %d, previous %d)",
					takerSide, tradeIndex, trade.MakerOrder, makerIndex, previousMakerIndex)
			}
			previousMakerIndex = makerIndex
		}
	})
}

// Invariant 6 (EXCHANGE_12 §12): LastPriceMoved iff any trade printed; when
// moved, both the result and the book's quote reflect the FINAL trade's
// (maker) price. Exercised for BOTH taker sides.
func TestProperty_Inv6_LastPriceMatchesFinalTrade(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		takerSide := drawTakerSide(t)
		matcher, orderBook, _ := seedRandomMakers(t, oppositeSide(takerSide))

		takerLimit := contract.Ticks(rapid.Int64Range(9900, 10100).Draw(t, "takerLimit"))
		takerQty := contract.Qty(rapid.Int64Range(1, 200).Draw(t, "takerQty"))

		order := &contract.FundedOrder{
			OrderID:     "T",
			Participant: "P_T",
			Instrument:  "ACME",
			Side:        takerSide,
			Type:        contract.Limit,
			TIF:         contract.GTC,
			Qty:         takerQty,
			LimitPrice:  takerLimit,
		}
		result := matcher.Match(order, "B_T", 999)

		if result.LastPriceMoved != (len(result.Trades) > 0) {
			t.Fatalf("takerSide=%v LastPriceMoved=%v, len(Trades)=%d — must agree",
				takerSide, result.LastPriceMoved, len(result.Trades))
		}
		quote := orderBook.Quote()
		if !result.LastPriceMoved {
			if quote.HasLastPrice {
				t.Fatalf("takerSide=%v book.HasLastPrice=true after a walk that printed nothing",
					takerSide)
			}
			return
		}
		wantPrice := result.Trades[len(result.Trades)-1].Price
		if result.LastPrice != wantPrice {
			t.Fatalf("takerSide=%v result.LastPrice=%d, want %d (final trade)",
				takerSide, result.LastPrice, wantPrice)
		}
		if !quote.HasLastPrice || quote.LastPrice != wantPrice {
			t.Fatalf("takerSide=%v book LastPrice: got (%d, has=%v), want (%d, true)",
				takerSide, quote.LastPrice, quote.HasLastPrice, wantPrice)
		}
	})
}
