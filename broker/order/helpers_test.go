package order

import (
	"context"
	"errors"

	"simulator/broker/funnel"
)

// errUnexpected is a sentinel used by the 500 branch tests — a broker
// error that is neither ErrBrokerTimeout nor nil. The HTTP handler
// must map any such error to 500 after logging; the exact identity
// does not matter, only that it is not the two typed outcomes.
var errUnexpected = errors.New("broker/order: unexpected broker error (test sentinel)")

// -----------------------------------------------------------------------------
// helpers_test.go — shared test fixtures for the three HTTP handlers
// (place / cancel / modify). All three use the same stubBroker (one
// interface satisfies all three) and the same Config.
//
// stubBroker is a hand-rolled double rather than a generated mock:
//
//   - captures what the HTTP handler sent (account id, order spec,
//     order id, session id) so the test can assert the wire→funnel
//     translation
//   - returns a pre-canned (PlaceResult / CancelResult, error) tuple
//     so every status-table branch is deterministic
//   - supports a "block until ctx cancels then return ErrBrokerTimeout"
//     mode for the 504 branch — the test uses a 1 ms OrderTimeout and
//     the stub blocks on ctx.Done, so the deadline fires fast
//   - supports a "panic on call" mode for the 500 recover branch
// -----------------------------------------------------------------------------

type stubBroker struct {
	// Configured return values.
	placeResult  funnel.PlaceResult
	placeErr     error
	cancelResult funnel.CancelResult
	cancelErr    error

	// Captured last-call args — a single call per test is the pattern
	// (place_test / cancel_test) so a slice is overkill; modify_test
	// uses placeCount / cancelCount to pin "both methods were invoked
	// in the right order".
	placeAccountID funnel.AccountID
	placeSessionID funnel.SessionID
	placeOrder     funnel.OrderSpec

	cancelAccountID funnel.AccountID
	cancelOrderID   funnel.OrderID

	// Call counters — used by modify_test to assert the compose path
	// (cancel first, then place) and by cancel-failed-gate to assert
	// place was NOT called.
	placeCount  int
	cancelCount int

	// placeBlocks / cancelBlocks: when set, the matching method waits on
	// ctx.Done() instead of returning, then returns ErrBrokerTimeout. The
	// 504 branch tests use these plus a 1 ms OrderTimeout to drive the
	// handler's ctx deadline. Separate per-method flags are required by
	// the modify compose path — we need to time out ONE leg (cancel or
	// place) at a time, with the other succeeding normally.
	placeBlocks  bool
	cancelBlocks bool

	// panicValue: if non-nil, the next PlaceOrder / CancelOrder call
	// panics with this value instead of returning. The 500 recover
	// branch test uses this to prove the HTTP handler's defer catches
	// a bubbled panic and writes a clean 500 rather than tearing the
	// connection.
	panicValue any
}

func (stub *stubBroker) PlaceOrder(
	ctx context.Context,
	accountID funnel.AccountID,
	sessionID funnel.SessionID,
	order funnel.OrderSpec,
) (funnel.PlaceResult, error) {
	stub.placeCount++
	stub.placeAccountID = accountID
	stub.placeSessionID = sessionID
	stub.placeOrder = order
	if stub.panicValue != nil {
		panic(stub.panicValue)
	}
	if stub.placeBlocks {
		<-ctx.Done()
		return funnel.PlaceResult{}, funnel.ErrBrokerTimeout
	}
	return stub.placeResult, stub.placeErr
}

func (stub *stubBroker) CancelOrder(
	ctx context.Context,
	accountID funnel.AccountID,
	orderID funnel.OrderID,
) (funnel.CancelResult, error) {
	stub.cancelCount++
	stub.cancelAccountID = accountID
	stub.cancelOrderID = orderID
	if stub.panicValue != nil {
		panic(stub.panicValue)
	}
	if stub.cancelBlocks {
		<-ctx.Done()
		return funnel.CancelResult{}, funnel.ErrBrokerTimeout
	}
	return stub.cancelResult, stub.cancelErr
}
