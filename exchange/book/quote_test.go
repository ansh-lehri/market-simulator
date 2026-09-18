package book

import (
	"testing"

	"simulator/contract"
)

// assertQuoteClearing pins down EXCHANGE_11 §7.1's clearing rule: when a Has*
// flag is false the paired value fields MUST be exactly 0 (never stale from a
// previous non-empty state). A stale value under a cleared flag would be
// indistinguishable from a real quote at that price/qty for any reader that
// only checks the flag.
func assertQuoteClearing(t *testing.T, quote QuoteState) {
	t.Helper()
	if !quote.HasBid && (quote.BestBid != 0 || quote.BidQty != 0) {
		t.Fatalf("HasBid=false but BestBid=%d BidQty=%d (both must be 0)", quote.BestBid, quote.BidQty)
	}
	if !quote.HasAsk && (quote.BestAsk != 0 || quote.AskQty != 0) {
		t.Fatalf("HasAsk=false but BestAsk=%d AskQty=%d (both must be 0)", quote.BestAsk, quote.AskQty)
	}
	if !quote.HasLastPrice && quote.LastPrice != 0 {
		t.Fatalf("HasLastPrice=false but LastPrice=%d (must be 0)", quote.LastPrice)
	}
}

func seed(book *Book, orders ...*contract.RestingOrder) {
	for _, order := range orders {
		book.Insert(order)
	}
}

// quoteExpectation is the read half of a state-transition row. Each ask-side
// case has a symmetric bid-side sibling; the tables below are laid out to make
// that symmetry visible on the page.
type quoteExpectation struct {
	hasBid   bool
	bestBid  contract.Ticks
	bidQty   contract.Qty
	hasAsk   bool
	bestAsk  contract.Ticks
	askQty   contract.Qty
}

func assertMatches(t *testing.T, got QuoteState, want quoteExpectation) {
	t.Helper()
	if got.HasBid != want.hasBid || got.BestBid != want.bestBid || got.BidQty != want.bidQty {
		t.Fatalf("bid: got (has=%v price=%d qty=%d), want (has=%v price=%d qty=%d)",
			got.HasBid, got.BestBid, got.BidQty, want.hasBid, want.bestBid, want.bidQty)
	}
	if got.HasAsk != want.hasAsk || got.BestAsk != want.bestAsk || got.AskQty != want.askQty {
		t.Fatalf("ask: got (has=%v price=%d qty=%d), want (has=%v price=%d qty=%d)",
			got.HasAsk, got.BestAsk, got.AskQty, want.hasAsk, want.bestAsk, want.askQty)
	}
	assertQuoteClearing(t, got)
}

// --- state-transition table for the ask side ----------------------------

func TestQuote_AskStateTransitions(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*Book)
		event func(*Book)
		want  quoteExpectation
	}{
		{
			name:  "insert into empty side flips HasAsk true and populates",
			setup: func(*Book) {},
			event: func(book *Book) {
				book.Insert(makeResting("A", contract.Sell, 10021, 50))
			},
			want: quoteExpectation{hasAsk: true, bestAsk: 10021, askQty: 50},
		},
		{
			name: "insert at existing top level grows AskQty, price unchanged",
			setup: func(book *Book) {
				seed(book, makeResting("A", contract.Sell, 10021, 50))
			},
			event: func(book *Book) {
				book.Insert(makeResting("B", contract.Sell, 10021, 30))
			},
			want: quoteExpectation{hasAsk: true, bestAsk: 10021, askQty: 80},
		},
		{
			name: "insert at strictly better price shifts BestAsk down",
			setup: func(book *Book) {
				seed(book, makeResting("A", contract.Sell, 10025, 50))
			},
			event: func(book *Book) {
				book.Insert(makeResting("B", contract.Sell, 10023, 30))
			},
			want: quoteExpectation{hasAsk: true, bestAsk: 10023, askQty: 30},
		},
		{
			name: "insert at strictly worse price leaves top untouched",
			setup: func(book *Book) {
				seed(book, makeResting("A", contract.Sell, 10021, 50))
			},
			event: func(book *Book) {
				book.Insert(makeResting("B", contract.Sell, 10025, 30))
			},
			want: quoteExpectation{hasAsk: true, bestAsk: 10021, askQty: 50},
		},
		{
			name: "cancel one of many at top shrinks AskQty",
			setup: func(book *Book) {
				seed(book,
					makeResting("A", contract.Sell, 10021, 50),
					makeResting("B", contract.Sell, 10021, 30),
				)
			},
			event: func(book *Book) { book.Cancel("A") },
			want:  quoteExpectation{hasAsk: true, bestAsk: 10021, askQty: 30},
		},
		{
			name: "cancel sole order at top with next level below shifts top up",
			setup: func(book *Book) {
				seed(book,
					makeResting("A", contract.Sell, 10021, 50),
					makeResting("B", contract.Sell, 10023, 30),
				)
			},
			event: func(book *Book) { book.Cancel("A") },
			want:  quoteExpectation{hasAsk: true, bestAsk: 10023, askQty: 30},
		},
		{
			name: "cancel sole order at only level clears HasAsk (and zeros fields)",
			setup: func(book *Book) {
				seed(book, makeResting("A", contract.Sell, 10021, 50))
			},
			event: func(book *Book) { book.Cancel("A") },
			want:  quoteExpectation{}, // all zero, all flags false
		},
		{
			name: "cancel sole order at NON-top level leaves quote unchanged",
			setup: func(book *Book) {
				seed(book,
					makeResting("A", contract.Sell, 10021, 50), // top
					makeResting("B", contract.Sell, 10023, 30),
				)
			},
			event: func(book *Book) { book.Cancel("B") },
			want:  quoteExpectation{hasAsk: true, bestAsk: 10021, askQty: 50},
		},
		{
			name: "ReduceFront partial shrinks AskQty by the fill amount",
			setup: func(book *Book) {
				seed(book, makeResting("A", contract.Sell, 10021, 50))
			},
			event: func(book *Book) {
				level, _ := book.asks.LevelAt(10021)
				book.ReduceFront(level, 20)
			},
			want: quoteExpectation{hasAsk: true, bestAsk: 10021, askQty: 30},
		},
		{
			name: "ReduceFront full-drain of front with orders behind keeps top price",
			setup: func(book *Book) {
				seed(book,
					makeResting("A", contract.Sell, 10021, 50),
					makeResting("B", contract.Sell, 10021, 30),
				)
			},
			event: func(book *Book) {
				level, _ := book.asks.LevelAt(10021)
				book.ReduceFront(level, 50)
			},
			want: quoteExpectation{hasAsk: true, bestAsk: 10021, askQty: 30},
		},
		{
			name: "ReduceFront full-drain of only order at top with next level below shifts top",
			setup: func(book *Book) {
				seed(book,
					makeResting("A", contract.Sell, 10021, 50),
					makeResting("B", contract.Sell, 10023, 30),
				)
			},
			event: func(book *Book) {
				level, _ := book.asks.LevelAt(10021)
				book.ReduceFront(level, 50)
			},
			want: quoteExpectation{hasAsk: true, bestAsk: 10023, askQty: 30},
		},
		{
			name: "ReduceFront full-drain of only order at only level clears HasAsk",
			setup: func(book *Book) {
				seed(book, makeResting("A", contract.Sell, 10021, 50))
			},
			event: func(book *Book) {
				level, _ := book.asks.LevelAt(10021)
				book.ReduceFront(level, 50)
			},
			want: quoteExpectation{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			book := NewBook("ACME")
			tc.setup(book)
			tc.event(book)
			assertMatches(t, book.Quote(), tc.want)
		})
	}
}

// --- state-transition table for the bid side (mirror) -------------------

func TestQuote_BidStateTransitions(t *testing.T) {
	// Same event set as the ask table, mirrored: "better" means HIGHER for bids.
	// Kept side-by-side so any drift between the two sides shows up in review.
	tests := []struct {
		name  string
		setup func(*Book)
		event func(*Book)
		want  quoteExpectation
	}{
		{
			name:  "insert into empty side flips HasBid true and populates",
			setup: func(*Book) {},
			event: func(book *Book) {
				book.Insert(makeResting("X", contract.Buy, 10020, 100))
			},
			want: quoteExpectation{hasBid: true, bestBid: 10020, bidQty: 100},
		},
		{
			name: "insert at existing top level grows BidQty, price unchanged",
			setup: func(book *Book) {
				seed(book, makeResting("X", contract.Buy, 10020, 100))
			},
			event: func(book *Book) {
				book.Insert(makeResting("Y", contract.Buy, 10020, 40))
			},
			want: quoteExpectation{hasBid: true, bestBid: 10020, bidQty: 140},
		},
		{
			name: "insert at strictly better price shifts BestBid up",
			setup: func(book *Book) {
				seed(book, makeResting("X", contract.Buy, 10018, 100))
			},
			event: func(book *Book) {
				book.Insert(makeResting("Y", contract.Buy, 10020, 40))
			},
			want: quoteExpectation{hasBid: true, bestBid: 10020, bidQty: 40},
		},
		{
			name: "insert at strictly worse price leaves top untouched",
			setup: func(book *Book) {
				seed(book, makeResting("X", contract.Buy, 10020, 100))
			},
			event: func(book *Book) {
				book.Insert(makeResting("Y", contract.Buy, 10018, 40))
			},
			want: quoteExpectation{hasBid: true, bestBid: 10020, bidQty: 100},
		},
		{
			name: "cancel one of many at top shrinks BidQty",
			setup: func(book *Book) {
				seed(book,
					makeResting("X", contract.Buy, 10020, 100),
					makeResting("Y", contract.Buy, 10020, 40),
				)
			},
			event: func(book *Book) { book.Cancel("X") },
			want:  quoteExpectation{hasBid: true, bestBid: 10020, bidQty: 40},
		},
		{
			name: "cancel sole order at top with next level below shifts top down",
			setup: func(book *Book) {
				seed(book,
					makeResting("X", contract.Buy, 10020, 100),
					makeResting("Y", contract.Buy, 10019, 50),
				)
			},
			event: func(book *Book) { book.Cancel("X") },
			want:  quoteExpectation{hasBid: true, bestBid: 10019, bidQty: 50},
		},
		{
			name: "cancel sole order at only level clears HasBid (and zeros fields)",
			setup: func(book *Book) {
				seed(book, makeResting("X", contract.Buy, 10020, 100))
			},
			event: func(book *Book) { book.Cancel("X") },
			want:  quoteExpectation{},
		},
		{
			name: "cancel sole order at NON-top level leaves quote unchanged",
			setup: func(book *Book) {
				seed(book,
					makeResting("X", contract.Buy, 10020, 100), // top
					makeResting("Y", contract.Buy, 10018, 40),
				)
			},
			event: func(book *Book) { book.Cancel("Y") },
			want:  quoteExpectation{hasBid: true, bestBid: 10020, bidQty: 100},
		},
		{
			name: "ReduceFront partial shrinks BidQty by the fill amount",
			setup: func(book *Book) {
				seed(book, makeResting("X", contract.Buy, 10020, 100))
			},
			event: func(book *Book) {
				level, _ := book.bids.LevelAt(10020)
				book.ReduceFront(level, 30)
			},
			want: quoteExpectation{hasBid: true, bestBid: 10020, bidQty: 70},
		},
		{
			name: "ReduceFront full-drain of front with orders behind keeps top price",
			setup: func(book *Book) {
				seed(book,
					makeResting("X", contract.Buy, 10020, 100),
					makeResting("Y", contract.Buy, 10020, 40),
				)
			},
			event: func(book *Book) {
				level, _ := book.bids.LevelAt(10020)
				book.ReduceFront(level, 100)
			},
			want: quoteExpectation{hasBid: true, bestBid: 10020, bidQty: 40},
		},
		{
			name: "ReduceFront full-drain of only order at top with next level below shifts top",
			setup: func(book *Book) {
				seed(book,
					makeResting("X", contract.Buy, 10020, 100),
					makeResting("Y", contract.Buy, 10019, 50),
				)
			},
			event: func(book *Book) {
				level, _ := book.bids.LevelAt(10020)
				book.ReduceFront(level, 100)
			},
			want: quoteExpectation{hasBid: true, bestBid: 10019, bidQty: 50},
		},
		{
			name: "ReduceFront full-drain of only order at only level clears HasBid",
			setup: func(book *Book) {
				seed(book, makeResting("X", contract.Buy, 10020, 100))
			},
			event: func(book *Book) {
				level, _ := book.bids.LevelAt(10020)
				book.ReduceFront(level, 100)
			},
			want: quoteExpectation{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			book := NewBook("ACME")
			tc.setup(book)
			tc.event(book)
			assertMatches(t, book.Quote(), tc.want)
		})
	}
}

// --- SetLastPrice state machine -----------------------------------------

func TestQuote_SetLastPriceStateMachine(t *testing.T) {
	t.Run("fresh book: HasLastPrice false, LastPrice 0", func(t *testing.T) {
		book := NewBook("ACME")
		quote := book.Quote()
		if quote.HasLastPrice || quote.LastPrice != 0 {
			t.Fatalf("fresh book: got has=%v price=%d, want has=false price=0", quote.HasLastPrice, quote.LastPrice)
		}
		assertQuoteClearing(t, quote)
	})
	t.Run("first SetLastPrice flips HasLastPrice true", func(t *testing.T) {
		book := NewBook("ACME")
		book.SetLastPrice(10022)
		quote := book.Quote()
		if !quote.HasLastPrice || quote.LastPrice != 10022 {
			t.Fatalf("after first set: got has=%v price=%d, want has=true price=10022", quote.HasLastPrice, quote.LastPrice)
		}
	})
	t.Run("subsequent SetLastPrice updates without clearing the flag", func(t *testing.T) {
		book := NewBook("ACME")
		book.SetLastPrice(10022)
		book.SetLastPrice(10025)
		quote := book.Quote()
		if !quote.HasLastPrice || quote.LastPrice != 10025 {
			t.Fatalf("after second set: got has=%v price=%d, want has=true price=10025", quote.HasLastPrice, quote.LastPrice)
		}
	})
	t.Run("SetLastPrice on a book with no book state still works (last price is independent of book contents)", func(t *testing.T) {
		book := NewBook("ACME")
		book.SetLastPrice(10022)
		assertMatches(t, book.Quote(), quoteExpectation{}) // bid/ask untouched
		if !book.Quote().HasLastPrice || book.Quote().LastPrice != 10022 {
			t.Fatal("last-price must persist regardless of empty book state")
		}
	})
	t.Run("no book mutator ever writes LastPrice / HasLastPrice", func(t *testing.T) {
		// Run through every mutator path with a fresh book and confirm the
		// last-price half of the quote stays at its zero value.
		book := NewBook("ACME")
		seed(book,
			makeResting("A", contract.Sell, 10021, 50),
			makeResting("B", contract.Sell, 10021, 30),
			makeResting("X", contract.Buy, 10020, 100),
		)
		level, _ := book.asks.LevelAt(10021)
		book.ReduceFront(level, 20)
		book.Cancel("B")
		book.Cancel("X")
		book.Cancel("ghost") // unknown-id path
		quote := book.Quote()
		if quote.HasLastPrice || quote.LastPrice != 0 {
			t.Fatalf("mutators leaked into last price: has=%v price=%d", quote.HasLastPrice, quote.LastPrice)
		}
	})
}

// --- clearing invariant under a compound sequence -----------------------

// TestQuote_ClearingInvariantThroughCompoundSequence drives a book through a
// sequence that touches every clearing/re-populating transition on both sides
// and asserts the clearing invariant after every single step. This is the
// belt-and-braces version of the per-transition tables above: a bug that flips
// Has* to false but leaves BestX populated will fire here even if it slipped
// through a specific transition test.
func TestQuote_ClearingInvariantThroughCompoundSequence(t *testing.T) {
	book := NewBook("ACME")
	step := func(name string, apply func()) {
		t.Helper()
		apply()
		assertQuoteClearing(t, book.Quote())
		_ = name // kept so failure output can be extended if needed
	}

	step("insert first ask", func() {
		book.Insert(makeResting("A", contract.Sell, 10021, 50))
	})
	step("insert first bid", func() {
		book.Insert(makeResting("X", contract.Buy, 10020, 100))
	})
	step("insert worse-priced bid (no top shift)", func() {
		book.Insert(makeResting("Y", contract.Buy, 10018, 40))
	})
	step("drain top ask entirely — HasAsk clears", func() {
		level, _ := book.asks.LevelAt(10021)
		book.ReduceFront(level, 50)
	})
	step("drain top bid — top shifts down, HasBid stays true", func() {
		book.Cancel("X")
	})
	step("drain remaining bid — HasBid clears", func() {
		book.Cancel("Y")
	})
	step("SetLastPrice on empty book — bid/ask stay cleared, HasLastPrice flips", func() {
		book.SetLastPrice(10021)
	})
	step("re-populate ask — HasAsk flips back true", func() {
		book.Insert(makeResting("A2", contract.Sell, 10023, 20))
	})
	step("cancel unknown — no state change", func() {
		book.Cancel("ghost")
	})
}
