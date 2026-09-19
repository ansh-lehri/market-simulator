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

// Invariant 3 (EXCHANGE_12 §12): no negative or zero fills; no over-fill of
// the taker's Qty; for a Market BUY, total spend never exceeds MaxSpend.
// Exercised for BOTH taker sides — funded-cap only applies to Buy, but the
// over-fill and positive-qty portions hold for Sell too.
func TestProperty_Inv3_MarketBuyRespectsMaxSpend(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		takerSide := drawTakerSide(t)
		matcher, _, _ := seedRandomMakers(t, oppositeSide(takerSide))

		// Range spans MaxSpend values that always bite, sometimes bite, and
		// never bite for the seed sizes (up to 6 makers × 20 qty × 10050).
		maxSpend := contract.Ticks(rapid.Int64Range(0, 2_000_000).Draw(t, "maxSpend"))
		takerQty := contract.Qty(rapid.Int64Range(1, 200).Draw(t, "takerQty"))

		order := &contract.FundedOrder{
			OrderID:     "T",
			Participant: "P_T",
			Instrument:  "ACME",
			Side:        takerSide,
			Type:        contract.Market,
			TIF:         contract.IOC,
			Qty:         takerQty,
			MaxSpend:    maxSpend,
		}
		result := matcher.Match(order, "B_T", 999)

		var totalQty contract.Qty
		var totalSpend contract.Ticks
		for tradeIndex, trade := range result.Trades {
			if trade.Qty <= 0 {
				t.Fatalf("takerSide=%v trade[%d].Qty=%d, want > 0",
					takerSide, tradeIndex, trade.Qty)
			}
			totalQty += trade.Qty
			totalSpend += trade.Price * trade.Qty
		}

		if totalQty > order.Qty {
			t.Fatalf("takerSide=%v over-fill: totalQty=%d, order.Qty=%d",
				takerSide, totalQty, order.Qty)
		}
		if totalQty+result.CancelledQty != order.Qty {
			t.Fatalf("takerSide=%v qty accounting: totalQty=%d + CancelledQty=%d != order.Qty=%d",
				takerSide, totalQty, result.CancelledQty, order.Qty)
		}
		if result.Rested != nil {
			t.Fatalf("takerSide=%v Market must never rest, got Rested=%+v", takerSide, result.Rested)
		}

		if takerSide == contract.Buy {
			if totalSpend > order.MaxSpend {
				t.Fatalf("Buy market over-spent: totalSpend=%d, MaxSpend=%d, trades=%+v",
					totalSpend, order.MaxSpend, result.Trades)
			}
			// If any qty was cancelled with FundedCap, spending must be at
			// or above (MaxSpend - one unit at the halted level's price) —
			// i.e., the walk didn't stop early. We cannot know the halted
			// price without replaying, so bound with the simpler statement:
			// spent ≤ MaxSpend, tested above.
			if result.CancelledQty > 0 && result.CancelReason != contract.ReasonFundedCap &&
				result.CancelReason != contract.ReasonNoLiquidity {
				t.Fatalf("Buy market cancelled with unexpected reason %v (trades=%+v cancelled=%d)",
					result.CancelReason, result.Trades, result.CancelledQty)
			}
		} else {
			// Sell market cannot trigger funded-cap; if anything cancels,
			// the reason must be NoLiquidity (opposite bid side emptied).
			if result.CancelledQty > 0 && result.CancelReason != contract.ReasonNoLiquidity {
				t.Fatalf("Sell market cancelled with unexpected reason %v (trades=%+v cancelled=%d)",
					result.CancelReason, result.Trades, result.CancelledQty)
			}
		}
	})
}

// drawSTPMode picks one of the three STP modes uniformly.
func drawSTPMode(t *rapid.T) contract.STPMode {
	choice := rapid.IntRange(0, 2).Draw(t, "stpMode")
	switch choice {
	case 0:
		return contract.CancelIncoming
	case 1:
		return contract.CancelResting
	default:
		return contract.CancelBoth
	}
}

// Invariant 4 (EXCHANGE_12 §12): a self-match never prints a trade under
// any STP mode. Exercised across BOTH taker sides, BOTH order types
// (Limit/Market), and ALL THREE STP modes. Half the seeded makers share
// the taker's ParticipantID so the self-match path is actually hit — a
// property test that doesn't reach the branch it claims to test is a
// worthless test.
func TestProperty_Inv4_NoSelfMatchTradesUnderAnySTP(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		takerSide := drawTakerSide(t)
		makerSide := oppositeSide(takerSide)
		stpMode := drawSTPMode(t)
		takerIsMarket := rapid.Bool().Draw(t, "takerIsMarket")

		const takerParticipant contract.ParticipantID = "P_T"
		orderBook := book.NewBook("ACME")
		matcher := NewMatcher(orderBook, allowAllClamp{}, &monotonicSeq{})

		makerCount := rapid.IntRange(1, 6).Draw(t, "makerCount")
		for makerIndex := 0; makerIndex < makerCount; makerIndex++ {
			shareParticipant := rapid.Bool().Draw(t, fmt.Sprintf("shareParticipant[%d]", makerIndex))
			participant := contract.ParticipantID(fmt.Sprintf("P_M%d", makerIndex))
			if shareParticipant {
				participant = takerParticipant
			}
			price := contract.Ticks(rapid.Int64Range(10000, 10050).Draw(t, fmt.Sprintf("price[%d]", makerIndex)))
			qty := contract.Qty(rapid.Int64Range(1, 20).Draw(t, fmt.Sprintf("qty[%d]", makerIndex)))
			orderBook.Insert(&contract.RestingOrder{
				OrderID:     contract.OrderID(fmt.Sprintf("M%d", makerIndex)),
				Participant: participant,
				BrokerID:    "B_M",
				Side:        makerSide,
				Price:       price,
				Remaining:   qty,
				Seq:         contract.Seq(makerIndex + 1),
			})
		}

		takerQty := contract.Qty(rapid.Int64Range(1, 200).Draw(t, "takerQty"))
		order := &contract.FundedOrder{
			OrderID:     "T",
			Participant: takerParticipant,
			Instrument:  "ACME",
			Side:        takerSide,
			TIF:         contract.IOC,
			Qty:         takerQty,
			STP:         stpMode,
		}
		if takerIsMarket {
			order.Type = contract.Market
			// Only Market BUY reads MaxSpend; use a large cap so funded-
			// cap doesn't hide the self-match code path.
			order.MaxSpend = 5_000_000
		} else {
			order.Type = contract.Limit
			order.TIF = contract.GTC
			order.LimitPrice = contract.Ticks(rapid.Int64Range(9900, 10100).Draw(t, "takerLimit"))
		}

		result := matcher.Match(order, "B_T", 999)

		for tradeIndex, trade := range result.Trades {
			if trade.TakerParticipant == trade.MakerParticipant {
				t.Fatalf("takerSide=%v STP=%v market=%v trade[%d] is a self-match: taker=%q maker=%q (all trades=%+v)",
					takerSide, stpMode, takerIsMarket, tradeIndex,
					trade.TakerParticipant, trade.MakerParticipant, result.Trades)
			}
		}

		// Extra checks for the SelfMatch-cancellation modes: when the walk
		// halted on a self-cross, the incoming must be cancelled and never
		// rested — even for a GTC-limit. EXCHANGE_12 §7.
		if result.CancelReason == contract.ReasonSelfMatch {
			if result.Rested != nil {
				t.Fatalf("STP=%v cancelled with SelfMatch but Rested is set: %+v",
					stpMode, result.Rested)
			}
			if stpMode == contract.CancelResting {
				t.Fatalf("STP=CancelResting must never end with ReasonSelfMatch (it pulls makers and continues); got trades=%+v cancelled=%d",
					result.Trades, result.CancelledQty)
			}
		}
	})
}

// Invariant 5 (EXCHANGE_12 §12): Market and IOC never rest — only GTC-limit
// remainders do. Exercised across BOTH taker sides and ALL THREE
// non-restable order shapes ({Limit-IOC, Market-IOC, Market-GTC}). Also
// asserts qty accounting closes for those shapes.
func TestProperty_Inv5_MarketAndIOCNeverRest(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		takerSide := drawTakerSide(t)
		matcher, _, _ := seedRandomMakers(t, oppositeSide(takerSide))

		kind := rapid.IntRange(0, 2).Draw(t, "orderKind")
		order := &contract.FundedOrder{
			OrderID:     "T",
			Participant: "P_T",
			Instrument:  "ACME",
			Side:        takerSide,
			Qty:         contract.Qty(rapid.Int64Range(1, 200).Draw(t, "takerQty")),
		}
		switch kind {
		case 0: // Limit + IOC
			order.Type = contract.Limit
			order.TIF = contract.IOC
			order.LimitPrice = contract.Ticks(rapid.Int64Range(9900, 10100).Draw(t, "limit"))
		case 1: // Market + IOC
			order.Type = contract.Market
			order.TIF = contract.IOC
			if takerSide == contract.Buy {
				order.MaxSpend = contract.Ticks(rapid.Int64Range(0, 2_000_000).Draw(t, "maxSpend"))
			}
		default: // Market + GTC (unusual but valid — market ignores TIF for resting)
			order.Type = contract.Market
			order.TIF = contract.GTC
			if takerSide == contract.Buy {
				order.MaxSpend = contract.Ticks(rapid.Int64Range(0, 2_000_000).Draw(t, "maxSpend"))
			}
		}

		result := matcher.Match(order, "B_T", 999)

		if result.Rested != nil {
			t.Fatalf("takerSide=%v kind=%d (type=%v tif=%v) must never rest, got Rested=%+v",
				takerSide, kind, order.Type, order.TIF, result.Rested)
		}
		var totalQty contract.Qty
		for _, trade := range result.Trades {
			totalQty += trade.Qty
		}
		if totalQty+result.CancelledQty != order.Qty {
			t.Fatalf("takerSide=%v kind=%d qty accounting: filled=%d + cancelled=%d != order.Qty=%d",
				takerSide, kind, totalQty, result.CancelledQty, order.Qty)
		}
	})
}

// Clamp property (EXCHANGE_12 §6 (b), §15 §3): under any circuit band, no
// trade ever prints at a clamp-disallowed price, AND if the result carries
// CancelReason == ReasonCircuitClamp, no trade printed at a price above the
// clamp's upper bound (Buy) / below the lower bound (Sell) — the walk
// really did halt at the band. Exercised across BOTH taker sides and BOTH
// order types (Limit, Market).
func TestProperty_ClampDisallowedPriceNeverPrints(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		lower := contract.Ticks(rapid.Int64Range(9990, 10025).Draw(t, "clampLower"))
		upper := contract.Ticks(rapid.Int64Range(10025, 10060).Draw(t, "clampUpper"))
		if lower > upper {
			lower, upper = upper, lower
		}
		clamp := bandedClamp{lower: lower, upper: upper}

		takerSide := drawTakerSide(t)
		makerSide := oppositeSide(takerSide)

		orderBook := book.NewBook("ACME")
		matcher := NewMatcher(orderBook, clamp, &monotonicSeq{})

		// Seed makers across a range that partially overlaps the clamp so
		// clamp-halts are actually reached.
		makerCount := rapid.IntRange(1, 6).Draw(t, "makerCount")
		for makerIndex := 0; makerIndex < makerCount; makerIndex++ {
			price := contract.Ticks(rapid.Int64Range(9950, 10100).Draw(t, fmt.Sprintf("price[%d]", makerIndex)))
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
		}

		takerIsMarket := rapid.Bool().Draw(t, "takerIsMarket")
		takerQty := contract.Qty(rapid.Int64Range(1, 200).Draw(t, "takerQty"))
		order := &contract.FundedOrder{
			OrderID:     "T",
			Participant: "P_T",
			Instrument:  "ACME",
			Side:        takerSide,
			Qty:         takerQty,
		}
		if takerIsMarket {
			order.Type = contract.Market
			order.TIF = contract.IOC
			if takerSide == contract.Buy {
				order.MaxSpend = 100_000_000
			}
		} else {
			order.Type = contract.Limit
			order.TIF = contract.GTC
			order.LimitPrice = contract.Ticks(rapid.Int64Range(9900, 10120).Draw(t, "takerLimit"))
		}

		result := matcher.Match(order, "B_T", 999)

		for tradeIndex, trade := range result.Trades {
			if !clamp.PriceAllowed(trade.Price) {
				t.Fatalf("takerSide=%v market=%v trade[%d] at price=%d violates clamp [%d, %d]",
					takerSide, takerIsMarket, tradeIndex, trade.Price, lower, upper)
			}
		}

		// Invariant 6 still holds under clamp halts: LastPriceMoved iff any
		// trade printed; when moved, the value tracks the final trade.
		if result.LastPriceMoved != (len(result.Trades) > 0) {
			t.Fatalf("LastPriceMoved=%v vs len(Trades)=%d", result.LastPriceMoved, len(result.Trades))
		}
		if result.LastPriceMoved && result.LastPrice != result.Trades[len(result.Trades)-1].Price {
			t.Fatalf("LastPrice=%d, want %d", result.LastPrice, result.Trades[len(result.Trades)-1].Price)
		}
	})
}
