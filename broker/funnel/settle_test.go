package funnel

import (
	"strings"
	"testing"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/reserve"
)

// -----------------------------------------------------------------------------
// settle_test.go — focused unit coverage for the settlement body that the
// Stage-2 settle goldens (settle_golden_test.go) do NOT already pin:
//
//   - tradeID pure-formatter table.
//   - handleApplyResult dispatch for an UNKNOWN Outcome (dev-fatal panic).
//   - settleCancelRejected on an absent track (early-return path — Golden
//     15 pre-seeds the track).
//   - finalizeTrack dirty-mark reaches persistence.Component.
//   - finalizeTrack defensive branch when the session was torn down
//     between place-time and settlement.
//   - markDiverted preserves ownership (does not finalize).
//
// The six outcome branches, cross-order-type scenarios, event sequences,
// and release-completeness invariant are already covered by the settle
// goldens. This file complements, does not duplicate.
// -----------------------------------------------------------------------------

func TestTradeID_Formats(t *testing.T) {
	cases := []struct {
		name    string
		orderID OrderID
		seq     contract.Seq
		want    string
	}{
		{"basic", "acct-G:1", 1, "acct-G:1:1"},
		{"large seq", "acct-G:1", 9_999_999, "acct-G:1:9999999"},
		{"zero seq", "acct-G:7", 0, "acct-G:7:0"},
		{"account with colon", "foo:bar:42", 7, "foo:bar:42:7"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := string(tradeID(tc.orderID, tc.seq))
			if got != tc.want {
				t.Errorf("tradeID(%q, %d) = %q, want %q", tc.orderID, tc.seq, got, tc.want)
			}
		})
	}
}

func TestHandleApplyResult_UnknownOutcome_Panics(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 1_000_000}, nil)

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatalf("unknown Outcome did not panic")
		}
		msg, ok := recovered.(string)
		if !ok {
			t.Fatalf("panic value type = %T, want string", recovered)
		}
		if !strings.Contains(msg, "unknown outcome") {
			t.Errorf("panic message = %q, want substring 'unknown outcome'", msg)
		}
	}()

	// contract.Outcome values 0..5 are defined; 99 is unknown.
	harness.shard.handleApplyResult(ApplyResult{
		AccountID: goldenAccountID,
		Result: contract.EngineResult{
			Outcome:  contract.Outcome(99),
			OrderID:  goldenOrderID,
			BrokerID: goldenBrokerID,
		},
	})
}

func TestSettleCancelRejected_UnknownOrder_IsSafeNoop(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 1_000_000}, nil)
	// NO track seeded, NO reservation seeded — the "track already gone"
	// path that channel order makes the common case.

	harness.shard.handleApplyResult(ApplyResult{
		AccountID: goldenAccountID,
		Result: contract.EngineResult{
			Outcome:  contract.OutcomeCancelRejected,
			OrderID:  "acct-G:never-placed",
			BrokerID: goldenBrokerID,
		},
	})

	if got := len(harness.events.emitted); got != 0 {
		t.Errorf("events emitted on absent-track CancelRejected = %d, want 0", got)
	}
	if got := len(harness.shard.tracks); got != 0 {
		t.Errorf("tracks map size = %d, want 0 (no track touched)", got)
	}
}

func TestFinalizeTrack_TerminalDeletes(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 1_000_000}, nil)
	harness.seedTrack(goldenOrderID, OrderStatusFilled, false)

	harness.shard.finalizeTrack(goldenAccountID, goldenOrderID)

	if _, exists := harness.shard.tracks[goldenOrderID]; exists {
		t.Errorf("terminal track survived finalizeTrack, want delete")
	}
}

func TestFinalizeTrack_NonTerminalRetains(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 1_000_000}, nil)
	harness.seedTrack(goldenOrderID, OrderStatusWorking, false)

	harness.shard.finalizeTrack(goldenAccountID, goldenOrderID)

	track, exists := harness.shard.tracks[goldenOrderID]
	if !exists {
		t.Fatalf("non-terminal track removed by finalizeTrack, want retained")
	}
	if track.Status != OrderStatusWorking {
		t.Errorf("Status mutated by finalizeTrack = %v, want OrderStatusWorking (unchanged)", track.Status)
	}
}

func TestFinalizeTrack_MarksSnapshotDirty(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 1_000_000}, nil)
	harness.seedTrack(goldenOrderID, OrderStatusFilled, false)

	harness.shard.finalizeTrack(goldenAccountID, goldenOrderID)

	// assertDirty (defined in cancel_test.go) walks DrainDirty's slice for
	// the (SessionID, AccountID) key — same assertion pattern cancel uses.
	assertDirty(t, harness, goldenSessionID, goldenAccountID)
}

func TestFinalizeTrack_NoLiveSession_SkipsDirty(t *testing.T) {
	harness := newGoldenHarness(t)
	// Seed track directly without seeding a session — simulates a
	// post-tear-down settlement arrival (user disconnected mid-flight).
	harness.shard.tracks[goldenOrderID] = &OrderTrack{
		OrderID:    goldenOrderID,
		AccountID:  goldenAccountID,
		Instrument: goldenInstrument,
		Status:     OrderStatusFilled,
	}

	harness.shard.finalizeTrack(goldenAccountID, goldenOrderID)

	// Track still deleted (terminal cleanup is independent of session liveness).
	if _, exists := harness.shard.tracks[goldenOrderID]; exists {
		t.Errorf("terminal track survived no-session finalizeTrack, want delete")
	}
	// Dirty mark skipped (no session → no SessionID to pair with AccountID).
	if dirty := harness.shard.bundle.Persist.DrainDirty(); len(dirty) != 0 {
		t.Errorf("dirty set = %+v, want empty (no session, no mark)", dirty)
	}
}

func TestMarkDiverted_NoFinalize(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 1_000_000}, nil)
	harness.seedReservation(reserve.Order{
		OrderID: goldenOrderID, AccountID: goldenAccountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 10, IsMarket: false,
		LimitPrice: 10_000,
	})
	harness.seedTrack(goldenOrderID, OrderStatusWorking, false)
	harness.events.reset()

	harness.shard.handleApplyResult(ApplyResult{
		AccountID: goldenAccountID,
		Result: contract.EngineResult{
			Outcome: contract.OutcomeDiverted, OrderID: goldenOrderID, BrokerID: goldenBrokerID,
		},
	})

	track, exists := harness.shard.tracks[goldenOrderID]
	if !exists {
		t.Fatalf("markDiverted finalized the track, want retained for auction clear")
	}
	if track.Status != OrderStatusWorking {
		t.Errorf("Status = %v, want OrderStatusWorking (hold)", track.Status)
	}
	// Diverted path emits zero reserve events (reservation held).
	if got := len(harness.events.emitted); got != 0 {
		t.Errorf("events emitted on Diverted = %d, want 0", got)
	}
}
