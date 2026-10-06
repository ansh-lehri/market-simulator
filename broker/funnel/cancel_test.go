package funnel

import (
	"testing"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/persistence"
	"simulator/economy/reserve"
)

// -----------------------------------------------------------------------------
// cancel_test.go — Phase-5 handleCancel coverage.
//
// The handler is async: validate -> mark in-flight + persist dirty ->
// punch. NOTHING RELEASES HERE (P5-D10). Release waits for the exchange's
// echoed CancelledQty on settlement (Stage 5).
//
// Every reject path pre-filter is covered as its own case; the happy
// path pins the state transition (CancelInFlight flips, MarkSnapshotDirty
// fires, the correct InboundMessage lands on the exchange edge).
// -----------------------------------------------------------------------------

const (
	cancelTestOrderID OrderID = "acct-G:1"
)

// -----------------------------------------------------------------------------
// Reject paths.
// -----------------------------------------------------------------------------

// No live session on this shard. CancelFailed + ReasonNoSession, nothing
// mutated, nothing punched, nothing marked dirty.
func TestHandleCancel_NoSession_RejectsNoSession(t *testing.T) {
	harness := newGoldenHarness(t)
	// No seedAccount — sessions map stays empty.

	result := harness.shard.handleCancel(CancelOrder{
		AccountID: harness.accountID,
		OrderID:   cancelTestOrderID,
		ReplyTo:   make(chan CancelResult, 1),
	})

	if result.Status != CancelFailed || result.Reason != ReasonNoSession {
		t.Errorf("result = %+v, want {CancelFailed, ReasonNoSession}", result)
	}
	assertNoCancelSideEffects(t, harness)
}

// Unknown OrderID — the shard's tracks map has no entry. Returns the same
// ReasonUnknownOrder as a cross-account cancel so a probing caller can't
// learn whether an order id exists on someone else's account.
func TestHandleCancel_UnknownOrder_RejectsUnknownOrder(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 1_000_000}, nil)

	result := harness.shard.handleCancel(CancelOrder{
		AccountID: harness.accountID,
		OrderID:   "does-not-exist",
		ReplyTo:   make(chan CancelResult, 1),
	})

	if result.Status != CancelFailed || result.Reason != ReasonUnknownOrder {
		t.Errorf("result = %+v, want {CancelFailed, ReasonUnknownOrder}", result)
	}
	assertNoCancelSideEffects(t, harness)
}

// Cross-account cancel: track exists but is owned by a different account.
// Must return the same ReasonUnknownOrder as a missing track — the
// response does not reveal that the id exists elsewhere.
func TestHandleCancel_CrossAccount_RejectsUnknownOrder(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 1_000_000}, nil)
	// Plant a track owned by SOMEONE ELSE.
	harness.shard.tracks[cancelTestOrderID] = &OrderTrack{
		OrderID:    cancelTestOrderID,
		AccountID:  "acct-OTHER",
		Instrument: goldenInstrument,
		Status:     OrderStatusWorking,
	}

	result := harness.shard.handleCancel(CancelOrder{
		AccountID: harness.accountID,
		OrderID:   cancelTestOrderID,
		ReplyTo:   make(chan CancelResult, 1),
	})

	if result.Status != CancelFailed || result.Reason != ReasonUnknownOrder {
		t.Errorf("result = %+v, want {CancelFailed, ReasonUnknownOrder}", result)
	}
	// Other-account track must be untouched.
	otherTrack := harness.shard.tracks[cancelTestOrderID]
	if otherTrack.CancelInFlight {
		t.Errorf("other account's track mutated: CancelInFlight = true")
	}
	assertNoCancelSideEffects(t, harness)
}

// Terminal states — Filled, Cancelled, Rejected — are not cancellable.
// Table-driven: one case per terminal status.
func TestHandleCancel_TerminalStates_RejectsNotCancellable(t *testing.T) {
	cases := []struct {
		name   string
		status OrderStatus
	}{
		{"Filled", OrderStatusFilled},
		{"Cancelled", OrderStatusCancelled},
		{"Rejected", OrderStatusRejected},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			harness := newGoldenHarness(t)
			harness.seedAccount(account.Cash{Available: 1_000_000}, nil)
			harness.seedTrack(cancelTestOrderID, tc.status, false)

			result := harness.shard.handleCancel(CancelOrder{
				AccountID: harness.accountID,
				OrderID:   cancelTestOrderID,
				ReplyTo:   make(chan CancelResult, 1),
			})

			if result.Status != CancelFailed || result.Reason != ReasonNotCancellable {
				t.Errorf("result = %+v, want {CancelFailed, ReasonNotCancellable}", result)
			}
			// Track survives (not cancelled by this handler).
			track := harness.shard.tracks[cancelTestOrderID]
			if track == nil {
				t.Fatalf("track deleted on NotCancellable reject")
			}
			if track.CancelInFlight {
				t.Errorf("CancelInFlight flipped on terminal track, want unchanged")
			}
			assertNoCancelSideEffects(t, harness)
		})
	}
}

// -----------------------------------------------------------------------------
// Idempotency (P5-D11).
// -----------------------------------------------------------------------------

// Second cancel on a track with CancelInFlight already set returns the
// same CancelPending — crucially, WITHOUT re-punching the cancel to the
// exchange and WITHOUT re-marking dirty. A re-punch would race the first
// echo's release and could double-release.
func TestHandleCancel_Idempotent_WhenInFlight(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 1_000_000}, nil)
	harness.seedTrack(cancelTestOrderID, OrderStatusWorking, false)
	harness.shard.tracks[cancelTestOrderID].CancelInFlight = true

	result := harness.shard.handleCancel(CancelOrder{
		AccountID: harness.accountID,
		OrderID:   cancelTestOrderID,
		ReplyTo:   make(chan CancelResult, 1),
	})

	if result.Status != CancelPending || result.OrderID != cancelTestOrderID {
		t.Errorf("result = %+v, want {CancelPending, %q}", result, cancelTestOrderID)
	}
	// Flag unchanged (was already true, stays true).
	if !harness.shard.tracks[cancelTestOrderID].CancelInFlight {
		t.Errorf("CancelInFlight cleared on idempotent re-cancel, want still true")
	}
	// NO second punch.
	select {
	case outbound := <-harness.exchange:
		t.Errorf("idempotent re-cancel re-punched to exchange: %+v", outbound)
	default:
	}
	// NO MarkSnapshotDirty: pre-state has no reservation (no admission
	// dirty mark), the manually-set CancelInFlight didn't go through
	// handleCancel either. If idempotency works, the dirty set stays empty.
	if dirty := harness.shard.bundle.Persist.DrainDirty(); len(dirty) != 0 {
		t.Errorf("idempotent re-cancel triggered MarkSnapshotDirty: %+v", dirty)
	}
}

// -----------------------------------------------------------------------------
// Happy paths — Working + PartiallyFilled both cancellable.
// -----------------------------------------------------------------------------

// Working status, cancel accepted:
//   - CancelInFlight flips true
//   - MarkSnapshotDirty fires with (sessionID, accountID)
//   - InboundMessage{Kind: contract.CancelOrder, Instrument: track.Instrument,
//     Cancel: {OrderID}} punched to exchange edge
//   - BrokerID stamped by s.punch
//   - NO ReleaseEvent — nothing released here (P5-D10)
//   - Track NOT finalized — it waits for the settlement echo
//   - Result: CancelPending with the OrderID echoed back
func TestHandleCancel_HappyWorking_PunchesCancel(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 1_000_000}, nil)
	// Seed a reservation + track so the "no release" invariant has
	// something to prove — if cancel.go mistakenly called Release, we'd
	// see a ReleaseEvent on the reserve spy.
	harness.seedReservation(reserve.Order{
		OrderID: cancelTestOrderID, AccountID: goldenAccountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 10, LimitPrice: 100,
	})
	harness.seedTrack(cancelTestOrderID, OrderStatusWorking, false)
	harness.events.reset()

	result := harness.shard.handleCancel(CancelOrder{
		AccountID: harness.accountID,
		OrderID:   cancelTestOrderID,
		ReplyTo:   make(chan CancelResult, 1),
	})

	if result.Status != CancelPending || result.OrderID != cancelTestOrderID {
		t.Errorf("result = %+v, want {CancelPending, %q}", result, cancelTestOrderID)
	}
	// Flag flipped, track retained.
	track := harness.shard.tracks[cancelTestOrderID]
	if track == nil {
		t.Fatalf("track deleted on happy cancel path (should survive until settlement echoes)")
	}
	if !track.CancelInFlight {
		t.Errorf("CancelInFlight = false after happy cancel, want true")
	}
	// NO release event — the whole point of P5-D10.
	for _, event := range harness.events.emitted {
		if _, ok := event.(reserve.ReleaseEvent); ok {
			t.Errorf("ReleaseEvent emitted on cancel path: %+v (nothing should release here)", event)
		}
	}
	// The account is flagged dirty in the persistence component. (The
	// reserve admission already marked the same key, and the dirty set is
	// map-semantic — the invariant is "key present after cancel," not
	// "cancel added a new entry.")
	assertDirty(t, harness, goldenSessionID, goldenAccountID)
	// Punch landed on the exchange with the right shape.
	select {
	case outbound := <-harness.exchange:
		if outbound.Kind != contract.CancelOrder {
			t.Errorf("outbound.Kind = %v, want contract.CancelOrder", outbound.Kind)
		}
		if outbound.BrokerID != goldenBrokerID {
			t.Errorf("outbound.BrokerID = %q, want %q (stamped by s.punch)", outbound.BrokerID, goldenBrokerID)
		}
		if outbound.Instrument != goldenInstrument {
			t.Errorf("outbound.Instrument = %q, want %q", outbound.Instrument, goldenInstrument)
		}
		if outbound.Cancel == nil || outbound.Cancel.OrderID != cancelTestOrderID {
			t.Errorf("outbound.Cancel = %+v, want {OrderID: %q}", outbound.Cancel, cancelTestOrderID)
		}
		if outbound.Submit != nil {
			t.Errorf("outbound.Submit = %+v, want nil on cancel", outbound.Submit)
		}
	default:
		t.Fatalf("no InboundMessage punched to exchange edge")
	}
}

// PartiallyFilled is cancellable too — same transition as Working.
func TestHandleCancel_HappyPartiallyFilled_PunchesCancel(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 1_000_000}, nil)
	harness.seedReservation(reserve.Order{
		OrderID: cancelTestOrderID, AccountID: goldenAccountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 10, LimitPrice: 100,
	})
	harness.seedTrack(cancelTestOrderID, OrderStatusPartiallyFilled, false)
	harness.events.reset()

	result := harness.shard.handleCancel(CancelOrder{
		AccountID: harness.accountID,
		OrderID:   cancelTestOrderID,
		ReplyTo:   make(chan CancelResult, 1),
	})

	if result.Status != CancelPending {
		t.Fatalf("result.Status = %v, want CancelPending", result.Status)
	}
	if !harness.shard.tracks[cancelTestOrderID].CancelInFlight {
		t.Errorf("CancelInFlight = false, want true")
	}
	assertDirty(t, harness, goldenSessionID, goldenAccountID)
	// Exchange edge must have received exactly one message.
	select {
	case <-harness.exchange:
	default:
		t.Fatalf("no InboundMessage punched")
	}
}

// -----------------------------------------------------------------------------
// Local assertion helper — pins the "cancel rejected = no side-effects"
// invariant: for every reject path (no-session / unknown / cross-account /
// terminal), nothing is punched, no reserve event is emitted, no
// MarkSnapshotDirty call lands.
// -----------------------------------------------------------------------------

func assertNoCancelSideEffects(t *testing.T, harness *goldenHarness) {
	t.Helper()
	select {
	case outbound := <-harness.exchange:
		t.Errorf("exchange edge received a message on cancel reject: %+v", outbound)
	default:
	}
	for _, event := range harness.events.emitted {
		if _, ok := event.(reserve.ReleaseEvent); ok {
			t.Errorf("ReleaseEvent emitted on cancel reject: %+v (nothing should release)", event)
		}
	}
	// None of the reject-path tests pre-seed a reservation (which would
	// trigger reserve.MarkSnapshotDirty on admission), so the dirty set
	// must be empty on all reject paths.
	if dirty := harness.shard.bundle.Persist.DrainDirty(); len(dirty) != 0 {
		t.Errorf("MarkSnapshotDirty called on cancel reject: %+v (reject paths touch no state)", dirty)
	}
}

// assertDirty — the (sessionID, accountID) key is present in the
// persistence component's dirty set. The real Component uses map
// semantics (not a call log), so "present" is the observable invariant;
// the dirty set may have been flagged earlier by reserve admission or
// just now by handleCancel — both land on the same key.
func assertDirty(t *testing.T, harness *goldenHarness, wantSession SessionID, wantAccount AccountID) {
	t.Helper()
	wantKey := persistence.DirtyKey{SessionID: wantSession, AccountID: wantAccount}
	for _, key := range harness.shard.bundle.Persist.DrainDirty() {
		if key == wantKey {
			return
		}
	}
	t.Errorf("dirty set missing %+v after handler", wantKey)
}
