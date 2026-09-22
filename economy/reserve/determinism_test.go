package reserve

import (
	"reflect"
	"testing"

	"simulator/contract"
	"simulator/economy/account"
)

// TestDeterminism_FullLifecycleReplay drives all four reserve methods —
// ReserveIfAvailable, Convert, Release, ReleaseExcess — across multiple
// order-ids on a single account, twice, on freshly-built services. Every
// piece of observable state must be byte-identical between the two runs:
//
//   - Account.Cash (all four buckets)
//   - Account.Positions (per-instrument Available/Reserved/OpenLots)
//   - the ReservationBook's remaining records (should be empty at end)
//   - the persistence spy's MarkSnapshotDirty call log (order + args)
//   - the events sink's emit log (all ConvertEvent / ReleaseEvent / ReserveEvent)
//   - the realizer spy's OnSellFill input log
//
// If a hidden clock, random, or map-iteration-order dependency slipped
// into the reserve package, this test flakes; it should never flake.
//
// Scenario shape (one linear script, executed twice):
//
//   1. buy limit  → partial fill → qty-decrease modify (partial release)
//                → terminal fill (record deleted, brokerage fires in Convert)
//   2. sell       → terminal fill (position flat, brokerage fires in Convert)
//   3. buy market → cheaper fill leaves residue → ReleaseExcess drains it
//                (brokerage fired inside Convert, ReleaseExcess only trues cash)
//   4. unfilled buy  → cancel (brokerage never fires)
//   5. unfilled sell → cancel (brokerage never fires)
//
// Uses the same India cost fixture as stage 3/4 tests so the numbers are
// stable and readable when the assertion fails.
func TestDeterminism_FullLifecycleReplay(t *testing.T) {
	runOnce := func() *harness {
		harness := newIndiaHarness()

		// Seed a rich account: enough cash for two buys + a market buy,
		// enough position for a terminal sell and an unfilled-sell cancel.
		primeAccount(harness, account.Cash{Available: 500_000_000}, map[contract.InstrumentID]*account.Position{
			testInstrumentID: {
				Available: 200, Reserved: 0,
				OpenLots: []account.Lot{{EntryPrice: 5_000, EntryAt: testExitAt, RemainingQty: 200}},
			},
		})

		// (1) buy limit — partial fill, then qty-decrease modify, then terminal fill.
		if err := harness.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
			OrderID: "buy-limit", AccountID: testAccountID, SessionID: testSessionID,
			InstrumentID: testInstrumentID, Side: contract.Buy, Qty: 50, LimitPrice: 100,
		}); err != nil {
			t.Fatalf("(1) reserve: %v", err)
		}
		if err := harness.service.Convert("buy-limit", 20, 100, testExitAt, "trade-1a"); err != nil {
			t.Fatalf("(1) convert #1: %v", err)
		}
		// Partial modify: reduce the remaining 30 by 10.
		if err := harness.service.Release("buy-limit", 10); err != nil {
			t.Fatalf("(1) release partial: %v", err)
		}
		// Fill the remaining 20 to terminate.
		if err := harness.service.Convert("buy-limit", 20, 100, testExitAt, "trade-1b"); err != nil {
			t.Fatalf("(1) convert #2: %v", err)
		}

		// (2) sell terminal — position drains.
		if err := harness.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
			OrderID: "sell-limit", AccountID: testAccountID, SessionID: testSessionID,
			InstrumentID: testInstrumentID, Side: contract.Sell, Qty: 40,
		}); err != nil {
			t.Fatalf("(2) reserve: %v", err)
		}
		if err := harness.service.Convert("sell-limit", 40, 6_000, testExitAt, "trade-2"); err != nil {
			t.Fatalf("(2) convert: %v", err)
		}

		// (3) market buy with residue — fill 10 units cheaper than BestAsk;
		// buffer + savings remain in Reserved, ReleaseExcess trues them up.
		if err := harness.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
			OrderID: "buy-market", AccountID: testAccountID, SessionID: testSessionID,
			InstrumentID: testInstrumentID, Side: contract.Buy, Qty: 10,
			IsMarket: true, BestAsk: 200, SlippageBuffer: 100,
		}); err != nil {
			t.Fatalf("(3) reserve: %v", err)
		}
		if err := harness.service.Convert("buy-market", 10, 180, testExitAt, "trade-3"); err != nil {
			t.Fatalf("(3) convert: %v", err)
		}
		if err := harness.service.ReleaseExcess("buy-market"); err != nil {
			t.Fatalf("(3) release_excess: %v", err)
		}

		// (4) unfilled buy — cancel with zero brokerage.
		if err := harness.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
			OrderID: "buy-cancel", AccountID: testAccountID, SessionID: testSessionID,
			InstrumentID: testInstrumentID, Side: contract.Buy, Qty: 5, LimitPrice: 90,
		}); err != nil {
			t.Fatalf("(4) reserve: %v", err)
		}
		if err := harness.service.Release("buy-cancel", 0); err != nil {
			t.Fatalf("(4) release: %v", err)
		}

		// (5) unfilled sell — cancel with zero brokerage.
		if err := harness.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
			OrderID: "sell-cancel", AccountID: testAccountID, SessionID: testSessionID,
			InstrumentID: testInstrumentID, Side: contract.Sell, Qty: 5,
		}); err != nil {
			t.Fatalf("(5) reserve: %v", err)
		}
		if err := harness.service.Release("sell-cancel", 0); err != nil {
			t.Fatalf("(5) release: %v", err)
		}

		return harness
	}

	runOne := runOnce()
	runTwo := runOnce()

	acctOne := runOne.ledger.accounts[testAccountID]
	acctTwo := runTwo.ledger.accounts[testAccountID]

	// Account Cash — the whole four-bucket split must match.
	if acctOne.Cash != acctTwo.Cash {
		t.Errorf("Cash diverged:\n  run1=%+v\n  run2=%+v", acctOne.Cash, acctTwo.Cash)
	}

	// Positions — per-instrument, including OpenLots slice order.
	if !reflect.DeepEqual(acctOne.Positions, acctTwo.Positions) {
		t.Errorf("Positions diverged:\n  run1=%+v\n  run2=%+v", acctOne.Positions, acctTwo.Positions)
	}

	// Book — every OrderID in this script should be terminal by the end.
	// Both runs must show identical empty books (or, defensively, identical
	// residues if the script ever leaves some).
	if !reflect.DeepEqual(runOne.service.book.records, runTwo.service.book.records) {
		t.Errorf("book diverged:\n  run1=%+v\n  run2=%+v",
			runOne.service.book.records, runTwo.service.book.records)
	}
	for orderID := range runOne.service.book.records {
		t.Errorf("book still holds record for order %q — script should have terminated it", orderID)
	}

	// Spy logs — exact same call sequence, arg-by-arg.
	if !reflect.DeepEqual(runOne.persist.dirtyCalls, runTwo.persist.dirtyCalls) {
		t.Errorf("MarkSnapshotDirty call log diverged")
	}
	if !reflect.DeepEqual(runOne.events.emitted, runTwo.events.emitted) {
		t.Errorf("events emitted diverged (len run1=%d, run2=%d)",
			len(runOne.events.emitted), len(runTwo.events.emitted))
	}
	if !reflect.DeepEqual(runOne.realiz.fills, runTwo.realiz.fills) {
		t.Errorf("realizer OnSellFill log diverged")
	}
}
