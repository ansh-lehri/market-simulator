package reserve

import (
	"testing"

	"pgregory.net/rapid"

	"simulator/contract"
	"simulator/economy/account"
)

// -----------------------------------------------------------------------------
// Property test for the core invariant (doc §0.1):
//
//   The reservation covers exactly the still-unfilled quantity;
//   debits/credits cover exactly the filled quantity;
//   the two never overlap.
//
// We drive one order per rapid iteration through a randomised lifecycle
// (reserve → mixed sequence of Convert / partial Release / full Release
// → terminal). After every operation we assert:
//
//   1. Sync — the Reserved bucket on the account is byte-identical to
//      the record's ReservedCash (buy) or ReservedUnits (sell).
//   2. Qty conservation — filled + released + record.RemainingQty
//      always equals OrigQty while the record lives; equals OrigQty
//      exactly at terminal (record deleted).
//   3. No zombie — while the record lives, RemainingQty > 0.
//      (Scenario deliberately excludes market-buy residue so this holds.)
//   4. Monotonic flags — EverFilled and BrokerageCharged never flip
//      true→false.
//   5. Brokerage-implies-fill — BrokerageCharged==true forces
//      EverFilled==true at the same moment.
//
// After the loop we assert the "brokerage fires at most once" invariant
// by counting brokerage ConvertEvents in the emit log: never more than 1,
// exactly 1 iff EverFilled ever became true and the order reached terminal
// (record deleted), exactly 0 if the order never filled or never
// terminated in this run.
//
// Scenario shape: LIMIT orders only (buy or sell), fills at exactly
// LimitPrice. That keeps Reserved draining cleanly to zero on full fill
// with no market residue — the residue path is exercised by explicit
// stage-4 goldens (TestReleaseExcess_MarketBuyResidue), not here.
// -----------------------------------------------------------------------------

const propertyMaxOps = 12 // hard cap so a run that keeps drawing tiny partials still terminates

// invariantChecker bundles the running scenario counters + the account
// pointer so the assertion helper reads without dozens of params.
type invariantChecker struct {
	rt                    *rapid.T
	harness               *harness
	acct                  *account.Account
	isSell                bool
	origQty               int64
	filledQty             int64 // Σ Convert fillQty across the run
	releasedQty           int64 // Σ Release release qty (partial's argument, or full's snapshot of RemainingQty)
	everFilledSeenTrue    bool  // once observed true, must stay true
	brokerageSeenTrue     bool  // once observed true, must stay true
}

// check runs after every lifecycle op (and after the initial Reserve).
// stepLabel goes into the failure message so rapid's shrinking trail
// points at the exact step.
func (checker *invariantChecker) check(stepLabel string) {
	record := checker.harness.service.book.get(testOrderID)

	if record != nil {
		// (1) Sync — Reserved bucket == record.Reserved*.
		if checker.isSell {
			position := checker.acct.Positions[testInstrumentID]
			if position == nil {
				checker.rt.Fatalf("[%s] Position missing while record still lives", stepLabel)
			}
			if position.Reserved != record.ReservedUnits {
				checker.rt.Fatalf("[%s] Position.Reserved=%d != record.ReservedUnits=%d",
					stepLabel, position.Reserved, record.ReservedUnits)
			}
		} else {
			if checker.acct.Cash.Reserved != record.ReservedCash {
				checker.rt.Fatalf("[%s] Cash.Reserved=%d != record.ReservedCash=%d",
					stepLabel, checker.acct.Cash.Reserved, record.ReservedCash)
			}
		}
		// (2) Qty conservation.
		if checker.filledQty+checker.releasedQty+record.RemainingQty != checker.origQty {
			checker.rt.Fatalf("[%s] filled(%d) + released(%d) + remaining(%d) != origQty(%d)",
				stepLabel, checker.filledQty, checker.releasedQty, record.RemainingQty, checker.origQty)
		}
		// (3) No zombie.
		if record.RemainingQty <= 0 {
			checker.rt.Fatalf("[%s] zombie record: RemainingQty=%d while retained (no residue expected)",
				stepLabel, record.RemainingQty)
		}
		// (4) Flag monotonicity.
		if checker.everFilledSeenTrue && !record.EverFilled {
			checker.rt.Fatalf("[%s] EverFilled went true → false", stepLabel)
		}
		if checker.brokerageSeenTrue && !record.BrokerageCharged {
			checker.rt.Fatalf("[%s] BrokerageCharged went true → false", stepLabel)
		}
		// (5) Brokerage implies fill.
		if record.BrokerageCharged && !record.EverFilled {
			checker.rt.Fatalf("[%s] BrokerageCharged=true but EverFilled=false", stepLabel)
		}
		if record.EverFilled {
			checker.everFilledSeenTrue = true
		}
		if record.BrokerageCharged {
			checker.brokerageSeenTrue = true
		}
		return
	}

	// Record deleted: order terminal. Reserved bucket must be 0.
	if checker.isSell {
		position := checker.acct.Positions[testInstrumentID]
		if position != nil && position.Reserved != 0 {
			checker.rt.Fatalf("[%s] record gone but Position.Reserved=%d", stepLabel, position.Reserved)
		}
	} else {
		if checker.acct.Cash.Reserved != 0 {
			checker.rt.Fatalf("[%s] record gone but Cash.Reserved=%d", stepLabel, checker.acct.Cash.Reserved)
		}
	}
	if checker.filledQty+checker.releasedQty != checker.origQty {
		checker.rt.Fatalf("[%s] terminal: filled(%d) + released(%d) != origQty(%d)",
			stepLabel, checker.filledQty, checker.releasedQty, checker.origQty)
	}
}

// countBrokerageEvents scans the emit log for ConvertEvents that carry
// a "brokerage" line — that is the fingerprint of the per-executed-order
// charge fired by chargeOrderCost.
func countBrokerageEvents(events []Event) int {
	count := 0
	for _, event := range events {
		convertEvent, ok := event.(ConvertEvent)
		if !ok {
			continue
		}
		if _, hasBrokerage := convertEvent.Cost.Lines["brokerage"]; hasBrokerage {
			count++
		}
	}
	return count
}

func TestProperty_CoreInvariant_LimitOrderLifecycle(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		isSell := rapid.Bool().Draw(rt, "isSell")
		origQty := rapid.Int64Range(1, 20).Draw(rt, "origQty")
		// Minimum 100 ticks (₹1) so that limitPrice*origQty/100 >= 1 and
		// Reserve never rejects a valid property draw as sub-rupee.
		limitPrice := rapid.Int64Range(100, 10000).Draw(rt, "limitPrice")

		harness := newIndiaHarness()
		initialAvailable := int64(100_000_000_000) // large enough that fill costs + brokerage never bite

		var acct *account.Account
		if isSell {
			// Seed enough units so the sell can reserve OrigQty on any draw.
			acct = primeAccount(harness, account.Cash{Available: initialAvailable}, map[contract.InstrumentID]*account.Position{
				testInstrumentID: {
					Available: origQty, Reserved: 0,
					OpenLots: []account.Lot{{EntryPrice: limitPrice, EntryAt: testExitAt, RemainingQty: origQty}},
				},
			})
		} else {
			acct = primeAccount(harness, account.Cash{Available: initialAvailable}, nil)
		}

		side := contract.Buy
		if isSell {
			side = contract.Sell
		}
		order := Order{
			OrderID:      testOrderID,
			AccountID:    testAccountID,
			SessionID:    testSessionID,
			InstrumentID: testInstrumentID,
			Side:         side,
			Qty:          origQty,
			IsMarket:     false,
			LimitPrice:   limitPrice,
		}
		if err := harness.service.ReserveIfAvailable(Participant{ID: testParticipant}, order); err != nil {
			rt.Fatalf("initial Reserve failed: %v", err)
		}

		checker := &invariantChecker{rt: rt, harness: harness, acct: acct, isSell: isSell, origQty: origQty}
		checker.check("after reserve")

		for step := 0; step < propertyMaxOps; step++ {
			record := harness.service.book.get(testOrderID)
			if record == nil {
				break
			}
			remaining := record.RemainingQty

			// Op menu:
			//   0 = Convert(fillQty ∈ [1, remaining])
			//   1 = partial Release (only when remaining >= 2, releaseQty ∈ [1, remaining-1])
			//   2 = full Release (releaseQty=0)
			options := []int{0, 2}
			if remaining >= 2 {
				options = append(options, 1)
			}
			opChoice := rapid.SampledFrom(options).Draw(rt, "op")

			switch opChoice {
			case 0:
				fillQty := rapid.Int64Range(1, remaining).Draw(rt, "fillQty")
				if err := harness.service.Convert(testOrderID, fillQty, limitPrice, testExitAt, ""); err != nil {
					rt.Fatalf("Convert(%d) failed: %v", fillQty, err)
				}
				checker.filledQty += fillQty
			case 1:
				releaseAmt := rapid.Int64Range(1, remaining-1).Draw(rt, "releaseAmt")
				if err := harness.service.Release(testOrderID, releaseAmt); err != nil {
					rt.Fatalf("Release partial(%d) failed: %v", releaseAmt, err)
				}
				checker.releasedQty += releaseAmt
			case 2:
				// Full release — quantity being released is the current RemainingQty.
				if err := harness.service.Release(testOrderID, 0); err != nil {
					rt.Fatalf("Release full failed: %v", err)
				}
				checker.releasedQty += remaining
			}
			checker.check("after op")
		}

		// Final: brokerage "fires at most once" across the entire run,
		// and fires EXACTLY once iff the order both filled and reached
		// terminal. If the run hit propertyMaxOps without terminating,
		// the order might still be resting — brokerage is 0 (hasn't
		// fired yet, correctly).
		//
		// Ground truth for "has been filled" is checker.filledQty > 0
		// (test's own tally). Reading record.EverFilled would race with
		// record deletion at terminal Convert — a full-fill in one shot
		// flips EverFilled AND deletes the record inside a single call,
		// so an in-loop sample never observes the flag true.
		brokerageCount := countBrokerageEvents(harness.events.emitted)
		if brokerageCount > 1 {
			rt.Fatalf("brokerage fired %d times, must fire at most once", brokerageCount)
		}
		record := harness.service.book.get(testOrderID)
		if record == nil {
			// Terminal reached.
			if checker.filledQty > 0 && brokerageCount != 1 {
				rt.Fatalf("terminal + filled(%d): brokerage count = %d, want 1", checker.filledQty, brokerageCount)
			}
			if checker.filledQty == 0 && brokerageCount != 0 {
				rt.Fatalf("terminal + never filled: brokerage count = %d, want 0", brokerageCount)
			}
		}
	})
}
