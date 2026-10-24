package reserve

import (
	"errors"
	"reflect"
	"testing"

	"simulator/contract"
	"simulator/economy/account"
)

// -----------------------------------------------------------------------------
// Test helpers scoped to reserve_test.go.
//
// Every test builds a fresh account with a known Cash + Positions shape,
// installs it in the harness's ledger, and then calls ReserveIfAvailable.
// Assertions cover: return value (nil vs specific typed sentinel via
// errors.Is), Account state (both success and no-mutation-on-failure
// paths), the ReservationBook entry, and the spy call sequences.
// -----------------------------------------------------------------------------

const (
	testAccountID    account.AccountID     = "acct-1"
	testSessionID    SessionID             = "sess-1"
	testOrderID      OrderID               = "order-A"
	testInstrumentID contract.InstrumentID = "ACME"
	testParticipant                        = "part-1"
)

// buildAccount returns a fresh Account with the given cash + positions,
// keyed under testAccountID. Empty positions map is initialised so callers
// can drop entries in without a nil-map allocation dance.
func buildAccount(cash account.Cash, positions map[contract.InstrumentID]*account.Position) *account.Account {
	if positions == nil {
		positions = map[contract.InstrumentID]*account.Position{}
	}
	return &account.Account{
		ID:        testAccountID,
		Cash:      cash,
		Positions: positions,
	}
}

// snapshotAccount returns a deep-enough copy of an Account for
// no-mutation assertions. Cash is a value, Positions is copied entry by
// entry (Lot slice cloned).
func snapshotAccount(acct *account.Account) *account.Account {
	positions := make(map[contract.InstrumentID]*account.Position, len(acct.Positions))
	for key, value := range acct.Positions {
		lots := make([]account.Lot, len(value.OpenLots))
		copy(lots, value.OpenLots)
		positions[key] = &account.Position{
			Available: value.Available,
			Reserved:  value.Reserved,
			OpenLots:  lots,
		}
	}
	return &account.Account{
		ID:        acct.ID,
		Cash:      acct.Cash,
		Positions: positions,
	}
}

// assertNoMutation deep-checks that the current account state matches a
// pre-captured snapshot. Used on every rejection path.
func assertNoMutation(t *testing.T, current, snapshot *account.Account) {
	t.Helper()
	if current.Cash != snapshot.Cash {
		t.Errorf("Cash mutated: got %+v, want %+v", current.Cash, snapshot.Cash)
	}
	if len(current.Positions) != len(snapshot.Positions) {
		t.Errorf("Positions size changed: got %d, want %d", len(current.Positions), len(snapshot.Positions))
	}
	for key, wantPosition := range snapshot.Positions {
		gotPosition, exists := current.Positions[key]
		if !exists {
			t.Errorf("Positions[%q] dropped", key)
			continue
		}
		if !reflect.DeepEqual(*gotPosition, *wantPosition) {
			t.Errorf("Positions[%q] mutated: got %+v, want %+v", key, *gotPosition, *wantPosition)
		}
	}
}

// -----------------------------------------------------------------------------
// Buy — sufficient path (limit + market).
// -----------------------------------------------------------------------------

// A buy-limit order with enough Available cash reserves limit_price*qty
// and writes a record with IsMarket=false. Cash split shifts exactly
// requiredCash from Available to Reserved; Settled and Bank untouched.
func TestReserveBuyLimit_Sufficient(t *testing.T) {
	harness := newHarness()
	acct := buildAccount(account.Cash{Available: 100_000, Reserved: 0, Settled: 0, Bank: 500}, nil)
	harness.ledger.put(acct)

	order := Order{
		OrderID:      testOrderID,
		AccountID:    testAccountID,
		SessionID:    testSessionID,
		InstrumentID: testInstrumentID,
		Side:         contract.Buy,
		Qty:          100,
		IsMarket:     false,
		LimitPrice:   50,
	}

	if err := harness.service.ReserveIfAvailable(Participant{ID: testParticipant}, order); err != nil {
		t.Fatalf("ReserveIfAvailable returned err=%v, want nil (admitted)", err)
	}

	// requiredCash = 50 ticks * 100 qty / 100 = 50 rupees
	wantCash := account.Cash{Available: 99_950, Reserved: 50, Settled: 0, Bank: 500}
	if acct.Cash != wantCash {
		t.Errorf("Cash after reserve = %+v, want %+v", acct.Cash, wantCash)
	}

	record := harness.service.book.get(testOrderID)
	if record == nil {
		t.Fatal("book record missing after admission")
	}
	wantRecord := ReservationRecord{
		OrderID:      testOrderID,
		AccountID:    testAccountID,
		SessionID:    testSessionID,
		InstrumentID: testInstrumentID,
		Side:         contract.Buy,
		IsMarket:     false,
		LimitPrice:   50,
		ReservedCash: 50,
		RemainingQty: 100,
		OrigQty:      100,
	}
	if *record != wantRecord {
		t.Errorf("record = %+v, want %+v", *record, wantRecord)
	}

	if len(harness.events.emitted) != 1 {
		t.Fatalf("events emitted = %d, want 1", len(harness.events.emitted))
	}
	gotEvent, ok := harness.events.emitted[0].(ReserveEvent)
	if !ok {
		t.Fatalf("event type = %T, want ReserveEvent", harness.events.emitted[0])
	}
	wantEvent := ReserveEvent{OrderID: testOrderID, AccountID: testAccountID, Cash: 50}
	if gotEvent != wantEvent {
		t.Errorf("event = %+v, want %+v", gotEvent, wantEvent)
	}

	if len(harness.persist.dirtyCalls) != 1 {
		t.Fatalf("dirty calls = %d, want 1", len(harness.persist.dirtyCalls))
	}
	if harness.persist.dirtyCalls[0] != (dirtyCall{SessionID: testSessionID, AccountID: testAccountID}) {
		t.Errorf("dirty call = %+v, want {sess-1 acct-1}", harness.persist.dirtyCalls[0])
	}
}

// LimitPrice is stored on the admitted ReservationRecord verbatim from
// Order.LimitPrice — across both Side values and both IsMarket values.
// The field is a display fact: 0 for market orders (buy or sell), the
// user's limit tick for limit orders (buy or sell). Phase-6 A11 reads
// this field via ReservationView without back-calculating from
// ReservedCash (unreliable on sells and market buys).
func TestReserve_LimitPrice_StoredOnRecord(t *testing.T) {
	tests := []struct {
		name       string
		side       contract.Side
		isMarket   bool
		limitPrice int64 // caller-supplied Order.LimitPrice
		// For sell cases we need an existing position; buy cases skip it.
		positions map[contract.InstrumentID]*account.Position
		// Order sizing args (market buys use BestAsk/SlippageBuffer).
		bestAsk        int64
		slippageBuffer int64
		wantLimitPrice int64 // what should land on the record
	}{
		{
			name:           "buy-limit stores LimitPrice",
			side:           contract.Buy,
			isMarket:       false,
			limitPrice:     50,
			wantLimitPrice: 50,
		},
		{
			name:           "buy-market stores LimitPrice=0 (caller sent 0)",
			side:           contract.Buy,
			isMarket:       true,
			limitPrice:     0,
			bestAsk:        52,
			slippageBuffer: 200,
			wantLimitPrice: 0,
		},
		{
			name:       "sell-limit stores LimitPrice",
			side:       contract.Sell,
			isMarket:   false,
			limitPrice: 75,
			positions: map[contract.InstrumentID]*account.Position{
				testInstrumentID: {Available: 200, OpenLots: []account.Lot{{EntryPrice: 50, RemainingQty: 200}}},
			},
			wantLimitPrice: 75,
		},
		{
			name:       "sell-market stores LimitPrice=0 (caller sent 0)",
			side:       contract.Sell,
			isMarket:   true,
			limitPrice: 0,
			positions: map[contract.InstrumentID]*account.Position{
				testInstrumentID: {Available: 200, OpenLots: []account.Lot{{EntryPrice: 50, RemainingQty: 200}}},
			},
			wantLimitPrice: 0,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			harness := newHarness()
			acct := buildAccount(account.Cash{Available: 1_000_000}, testCase.positions)
			harness.ledger.put(acct)

			order := Order{
				OrderID:        testOrderID,
				AccountID:      testAccountID,
				SessionID:      testSessionID,
				InstrumentID:   testInstrumentID,
				Side:           testCase.side,
				Qty:            10,
				IsMarket:       testCase.isMarket,
				LimitPrice:     testCase.limitPrice,
				BestAsk:        testCase.bestAsk,
				SlippageBuffer: testCase.slippageBuffer,
			}

			if err := harness.service.ReserveIfAvailable(Participant{ID: testParticipant}, order); err != nil {
				t.Fatalf("ReserveIfAvailable err = %v", err)
			}

			record := harness.service.book.get(testOrderID)
			if record == nil {
				t.Fatal("record missing after admission")
			}
			if record.LimitPrice != testCase.wantLimitPrice {
				t.Errorf("record.LimitPrice = %d, want %d",
					record.LimitPrice, testCase.wantLimitPrice)
			}
		})
	}
}

// A market buy reserves best_ask*qty + slippage_buffer and marks the
// record IsMarket=true (which stage 4's ReleaseExcess uses to spot
// market-buy true-ups).
func TestReserveBuyMarket_Sufficient(t *testing.T) {
	harness := newHarness()
	acct := buildAccount(account.Cash{Available: 100_000}, nil)
	harness.ledger.put(acct)

	order := Order{
		OrderID:        testOrderID,
		AccountID:      testAccountID,
		SessionID:      testSessionID,
		InstrumentID:   testInstrumentID,
		Side:           contract.Buy,
		Qty:            100,
		IsMarket:       true,
		BestAsk:        52,
		SlippageBuffer: 200,
	}

	if err := harness.service.ReserveIfAvailable(Participant{ID: testParticipant}, order); err != nil {
		t.Fatalf("ReserveIfAvailable returned err=%v, want nil (admitted)", err)
	}

	// requiredCash = (52*100 + 200) / 100 = 54 rupees
	wantRequired := int64((52*100 + 200) / 100) // 54
	wantCash := account.Cash{Available: 100_000 - wantRequired, Reserved: wantRequired}
	if acct.Cash != wantCash {
		t.Errorf("Cash = %+v, want %+v", acct.Cash, wantCash)
	}

	record := harness.service.book.get(testOrderID)
	if record == nil {
		t.Fatal("record missing")
	}
	if !record.IsMarket {
		t.Errorf("record.IsMarket = false, want true")
	}
	if record.ReservedCash != wantRequired {
		t.Errorf("record.ReservedCash = %d, want %d", record.ReservedCash, wantRequired)
	}
}

// -----------------------------------------------------------------------------
// Sell — sufficient path.
// -----------------------------------------------------------------------------

// A sell with enough Available position reserves qty units. Cash is
// entirely untouched; PosAvailable shrinks and PosReserved grows.
func TestReserveSell_Sufficient(t *testing.T) {
	harness := newHarness()
	acct := buildAccount(
		account.Cash{Available: 100_000},
		map[contract.InstrumentID]*account.Position{
			testInstrumentID: {Available: 200, Reserved: 0, OpenLots: []account.Lot{{EntryPrice: 50, RemainingQty: 200}}},
		},
	)
	harness.ledger.put(acct)

	order := Order{
		OrderID:      testOrderID,
		AccountID:    testAccountID,
		SessionID:    testSessionID,
		InstrumentID: testInstrumentID,
		Side:         contract.Sell,
		Qty:          100,
	}

	if err := harness.service.ReserveIfAvailable(Participant{ID: testParticipant}, order); err != nil {
		t.Fatalf("ReserveIfAvailable returned err=%v, want nil (admitted)", err)
	}

	// Cash untouched.
	if acct.Cash != (account.Cash{Available: 100_000}) {
		t.Errorf("Cash mutated on sell reserve: got %+v", acct.Cash)
	}

	position := acct.Positions[testInstrumentID]
	if position.Available != 100 || position.Reserved != 100 {
		t.Errorf("Position after reserve = {Available:%d, Reserved:%d}, want {100, 100}",
			position.Available, position.Reserved)
	}

	record := harness.service.book.get(testOrderID)
	if record == nil {
		t.Fatal("record missing")
	}
	if record.ReservedUnits != 100 || record.ReservedCash != 0 {
		t.Errorf("record = %+v, want ReservedUnits=100 ReservedCash=0", *record)
	}
	if record.Side != contract.Sell {
		t.Errorf("record.Side = %v, want Sell", record.Side)
	}
	if record.OrigQty != 100 || record.RemainingQty != 100 {
		t.Errorf("record OrigQty=%d RemainingQty=%d, want both 100", record.OrigQty, record.RemainingQty)
	}

	if len(harness.events.emitted) != 1 {
		t.Fatalf("events = %d, want 1", len(harness.events.emitted))
	}
	gotEvent := harness.events.emitted[0].(ReserveEvent)
	if gotEvent != (ReserveEvent{OrderID: testOrderID, AccountID: testAccountID, Units: 100}) {
		t.Errorf("event = %+v, want {order-A acct-1 0 100}", gotEvent)
	}
}

// -----------------------------------------------------------------------------
// Buy — insufficient funds (no mutation, both order types, both Option-B branches).
// -----------------------------------------------------------------------------

// Table across (IsMarket true/false) × (positive-Available and Option-B
// negative-Available). Every case must return ErrInsufficientFunds via
// errors.Is, leave Cash/Positions untouched, write NO record, and emit
// NO events / dirty marks.
func TestReserveBuy_InsufficientFunds_NoMutation(t *testing.T) {
	tests := []struct {
		name           string
		cashAvailable  int64
		isMarket       bool
		qty            int64
		limitPrice     int64
		bestAsk        int64
		slippageBuffer int64
	}{
		{
			// requiredCash = 100*100/100 = 100 > 40
			name:          "buy-limit, positive Available below required",
			cashAvailable: 40,
			isMarket:      false,
			qty:           100,
			limitPrice:    100,
		},
		{
			// requiredCash = 200*1/100 = 2 > 0
			name:          "buy-limit, zero Available",
			cashAvailable: 0,
			isMarket:      false,
			qty:           1,
			limitPrice:    200,
		},
		{
			// requiredCash = 100*1/100 = 1 > -500
			name:          "buy-limit, Option B negative Available (already in debt)",
			cashAvailable: -500,
			isMarket:      false,
			qty:           1,
			limitPrice:    100,
		},
		{
			// requiredCash = (200*100+0)/100 = 200 > 100
			name:           "buy-market, positive Available below required",
			cashAvailable:  100,
			isMarket:       true,
			qty:            100,
			bestAsk:        200,
			slippageBuffer: 0,
		},
		{
			// requiredCash = (200*1+0)/100 = 2 > -100
			name:           "buy-market, Option B negative Available",
			cashAvailable:  -100,
			isMarket:       true,
			qty:            1,
			bestAsk:        200,
			slippageBuffer: 0,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			harness := newHarness()
			acct := buildAccount(account.Cash{Available: testCase.cashAvailable, Bank: 700}, nil)
			harness.ledger.put(acct)
			snapshot := snapshotAccount(acct)

			order := Order{
				OrderID:        testOrderID,
				AccountID:      testAccountID,
				SessionID:      testSessionID,
				InstrumentID:   testInstrumentID,
				Side:           contract.Buy,
				Qty:            testCase.qty,
				IsMarket:       testCase.isMarket,
				LimitPrice:     testCase.limitPrice,
				BestAsk:        testCase.bestAsk,
				SlippageBuffer: testCase.slippageBuffer,
			}

			err := harness.service.ReserveIfAvailable(Participant{ID: testParticipant}, order)
			if !errors.Is(err, ErrInsufficientFunds) {
				t.Fatalf("err = %v, want errors.Is ErrInsufficientFunds", err)
			}
			// Isolation: must NOT chain to the sell-side sentinel.
			if errors.Is(err, ErrInsufficientPosition) {
				t.Errorf("err chains to ErrInsufficientPosition — must not")
			}

			assertNoMutation(t, acct, snapshot)

			if record := harness.service.book.get(testOrderID); record != nil {
				t.Errorf("book wrote record %+v on rejection path, want none", *record)
			}
			if len(harness.events.emitted) != 0 {
				t.Errorf("events emitted on rejection: %+v", harness.events.emitted)
			}
			if len(harness.persist.dirtyCalls) != 0 {
				t.Errorf("dirty calls on rejection: %+v", harness.persist.dirtyCalls)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Sell — insufficient position (no mutation, present-but-short + missing).
// -----------------------------------------------------------------------------

// Two shapes of insufficient position, both must reject with
// ErrInsufficientPosition and mutate nothing.
func TestReserveSell_InsufficientPosition_NoMutation(t *testing.T) {
	tests := []struct {
		name      string
		positions map[contract.InstrumentID]*account.Position
	}{
		{
			name: "instrument held, less than sell qty",
			positions: map[contract.InstrumentID]*account.Position{
				testInstrumentID: {Available: 50, Reserved: 0, OpenLots: []account.Lot{{EntryPrice: 50, RemainingQty: 50}}},
			},
		},
		{
			name: "instrument entry present but PosAvailable zero (all reserved)",
			positions: map[contract.InstrumentID]*account.Position{
				testInstrumentID: {Available: 0, Reserved: 100, OpenLots: []account.Lot{{EntryPrice: 50, RemainingQty: 100}}},
			},
		},
		{
			name:      "missing instrument (empty Positions)",
			positions: map[contract.InstrumentID]*account.Position{},
		},
		{
			name: "missing instrument (other instrument held)",
			positions: map[contract.InstrumentID]*account.Position{
				"OTHER": {Available: 500, Reserved: 0, OpenLots: []account.Lot{{EntryPrice: 50, RemainingQty: 500}}},
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			harness := newHarness()
			acct := buildAccount(account.Cash{Available: 100_000}, testCase.positions)
			harness.ledger.put(acct)
			snapshot := snapshotAccount(acct)

			order := Order{
				OrderID:      testOrderID,
				AccountID:    testAccountID,
				SessionID:    testSessionID,
				InstrumentID: testInstrumentID,
				Side:         contract.Sell,
				Qty:          100,
			}

			err := harness.service.ReserveIfAvailable(Participant{ID: testParticipant}, order)
			if !errors.Is(err, ErrInsufficientPosition) {
				t.Fatalf("err = %v, want errors.Is ErrInsufficientPosition", err)
			}
			if errors.Is(err, ErrInsufficientFunds) {
				t.Errorf("err chains to ErrInsufficientFunds — must not")
			}

			assertNoMutation(t, acct, snapshot)

			if record := harness.service.book.get(testOrderID); record != nil {
				t.Errorf("book wrote record on rejection: %+v", *record)
			}
			if len(harness.events.emitted) != 0 {
				t.Errorf("events emitted on rejection: %+v", harness.events.emitted)
			}
			if len(harness.persist.dirtyCalls) != 0 {
				t.Errorf("dirty calls on rejection: %+v", harness.persist.dirtyCalls)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Ordering-invariant panics.
// -----------------------------------------------------------------------------

// A ReserveIfAvailable for an AccountID the ledger does not know about
// panics — Gate 4 would not admit an order for an unloaded account, so a
// nil resolution here is dev-fatal.
func TestReserveIfAvailable_UnknownAccount_Panics(t *testing.T) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected panic on unknown account, got none")
		}
		msg, ok := recovered.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", recovered, recovered)
		}
		want := `reserve: account "acct-1" not loaded (ordering invariant broken)`
		if msg != want {
			t.Errorf("panic msg = %q, want %q", msg, want)
		}
	}()

	harness := newHarness()
	// Deliberately do NOT put the account.
	order := Order{
		OrderID:    testOrderID,
		AccountID:  testAccountID,
		SessionID:  testSessionID,
		Side:       contract.Buy,
		Qty:        1,
		LimitPrice: 100,
	}
	_ = harness.service.ReserveIfAvailable(Participant{ID: testParticipant}, order)
}

// An unknown Side on the Order struct (e.g. a future ternary value) is
// dev-fatal. Kept as a defensive panic against a caller that ignores the
// contract.Side enum.
func TestReserveIfAvailable_UnknownSide_Panics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on unknown side, got none")
		}
	}()

	harness := newHarness()
	acct := buildAccount(account.Cash{Available: 1_000_000}, nil)
	harness.ledger.put(acct)

	order := Order{
		OrderID:   testOrderID,
		AccountID: testAccountID,
		SessionID: testSessionID,
		Side:      contract.Side(42), // not Buy or Sell
		Qty:       1,
	}
	_ = harness.service.ReserveIfAvailable(Participant{ID: testParticipant}, order)
}

// -----------------------------------------------------------------------------
// Determinism replay — the same admission on two fresh services yields
// identical Account state + book + spy call log. Guards against any
// hidden clock/random pull inside the reserve path.
// -----------------------------------------------------------------------------

func TestReserve_DeterminismReplay(t *testing.T) {
	run := func() (*harness, *account.Account) {
		harness := newHarness()
		acct := buildAccount(
			account.Cash{Available: 100_000, Bank: 200},
			map[contract.InstrumentID]*account.Position{
				testInstrumentID: {Available: 200, Reserved: 0, OpenLots: []account.Lot{{EntryPrice: 50, RemainingQty: 200}}},
			},
		)
		harness.ledger.put(acct)

		// A mixed sequence: a buy-limit admission, a sell admission on a
		// second order-id, then a rejected buy-market on a third.
		if err := harness.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
			OrderID: "o-1", AccountID: testAccountID, SessionID: testSessionID, InstrumentID: testInstrumentID,
			Side: contract.Buy, Qty: 10, LimitPrice: 100,
		}); err != nil {
			t.Fatalf("o-1 reserve failed: %v", err)
		}
		if err := harness.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
			OrderID: "o-2", AccountID: testAccountID, SessionID: testSessionID, InstrumentID: testInstrumentID,
			Side: contract.Sell, Qty: 50,
		}); err != nil {
			t.Fatalf("o-2 reserve failed: %v", err)
		}
		if err := harness.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
			OrderID: "o-3", AccountID: testAccountID, SessionID: testSessionID, InstrumentID: testInstrumentID,
			Side: contract.Buy, Qty: 1_000_000, IsMarket: true, BestAsk: 1_000_000, SlippageBuffer: 0,
		}); !errors.Is(err, ErrInsufficientFunds) {
			t.Fatalf("o-3 expected ErrInsufficientFunds, got %v", err)
		}
		return harness, acct
	}

	harnessOne, acctOne := run()
	harnessTwo, acctTwo := run()

	if acctOne.Cash != acctTwo.Cash {
		t.Errorf("Cash diverged: run1=%+v run2=%+v", acctOne.Cash, acctTwo.Cash)
	}
	if !reflect.DeepEqual(acctOne.Positions, acctTwo.Positions) {
		t.Errorf("Positions diverged")
	}
	if !reflect.DeepEqual(harnessOne.persist.dirtyCalls, harnessTwo.persist.dirtyCalls) {
		t.Errorf("dirty call log diverged: run1=%+v run2=%+v", harnessOne.persist.dirtyCalls, harnessTwo.persist.dirtyCalls)
	}
	if !reflect.DeepEqual(harnessOne.events.emitted, harnessTwo.events.emitted) {
		t.Errorf("events diverged: run1=%+v run2=%+v", harnessOne.events.emitted, harnessTwo.events.emitted)
	}
	// Book: compare records under the same OrderIDs.
	for _, orderID := range []OrderID{"o-1", "o-2", "o-3"} {
		recordOne := harnessOne.service.book.get(orderID)
		recordTwo := harnessTwo.service.book.get(orderID)
		if (recordOne == nil) != (recordTwo == nil) {
			t.Errorf("book divergence at %q: run1=%v run2=%v", orderID, recordOne, recordTwo)
			continue
		}
		if recordOne != nil && *recordOne != *recordTwo {
			t.Errorf("record %q diverged: run1=%+v run2=%+v", orderID, *recordOne, *recordTwo)
		}
	}
}
