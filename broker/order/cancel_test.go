package order

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"simulator/broker/funnel"
)

// -----------------------------------------------------------------------------
// cancel_test.go — POST /v1/orders/cancel handler coverage.
//
// The §9.4 "all cancel-fails → 409" rule makes this endpoint simpler
// than place: no 422 bucket, no NoSession special-case. The status
// table that gets exercised:
//   202 — CancelPending (echoed order_id)
//   400 — bad json, missing account_id, missing order_id
//   405 — non-POST (Allow: POST)
//   409 — EVERY CancelFailed reason (no_session, unknown_order,
//         not_cancellable); reason tag in body
//   504 — ErrBrokerTimeout (stub blocks on ctx.Done)
//   500 — unexpected broker error + bubbled panic (recover branch)
// -----------------------------------------------------------------------------

const (
	testCancelAccount = "acct-7"
	testCancelOrderID = "acct-7:3"
)

func newCancelHandlerForTest(t *testing.T, stub *stubBroker, orderTimeout time.Duration) http.Handler {
	t.Helper()
	if orderTimeout == 0 {
		orderTimeout = 100 * time.Millisecond
	}
	return NewCancelHandler(stub, Config{OrderTimeout: orderTimeout})
}

func goodCancelBody() map[string]any {
	return map[string]any{
		"account_id": testCancelAccount,
		"order_id":   testCancelOrderID,
	}
}

func doCancel(t *testing.T, handler http.Handler, body any) *httptest.ResponseRecorder {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal test body: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, CancelPath, bytes.NewReader(encoded))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

// -----------------------------------------------------------------------------
// 202 — happy path
// -----------------------------------------------------------------------------

func TestCancel_HappyPending_Returns202WithOrderID(t *testing.T) {
	stub := &stubBroker{
		cancelResult: funnel.CancelResult{Status: funnel.CancelPending, OrderID: testCancelOrderID},
	}
	handler := newCancelHandlerForTest(t, stub, 0)

	recorder := doCancel(t, handler, goodCancelBody())

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", recorder.Code)
	}
	var body cancelAcceptedBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.OrderID != testCancelOrderID {
		t.Errorf("OrderID echoed = %q, want %q", body.OrderID, testCancelOrderID)
	}
	if stub.cancelCount != 1 {
		t.Errorf("broker.CancelOrder call count = %d, want 1", stub.cancelCount)
	}
}

// -----------------------------------------------------------------------------
// Wire→funnel translation
// -----------------------------------------------------------------------------

func TestCancel_TranslatesWireToIDs(t *testing.T) {
	stub := &stubBroker{
		cancelResult: funnel.CancelResult{Status: funnel.CancelPending, OrderID: testCancelOrderID},
	}
	handler := newCancelHandlerForTest(t, stub, 0)

	doCancel(t, handler, goodCancelBody())

	if stub.cancelAccountID != testCancelAccount {
		t.Errorf("accountID sent = %q, want %q", stub.cancelAccountID, testCancelAccount)
	}
	if stub.cancelOrderID != testCancelOrderID {
		t.Errorf("orderID sent = %q, want %q", stub.cancelOrderID, testCancelOrderID)
	}
}

// -----------------------------------------------------------------------------
// 400 — pre-funnel body-shape rejects
// -----------------------------------------------------------------------------

func TestCancel_BadJSON_Returns400(t *testing.T) {
	stub := &stubBroker{}
	handler := newCancelHandlerForTest(t, stub, 0)

	request := httptest.NewRequest(http.MethodPost, CancelPath, bytes.NewReader([]byte("{not json")))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	if stub.cancelCount != 0 {
		t.Errorf("broker.CancelOrder called on bad JSON, want 0 calls")
	}
}

func TestCancel_PreFunnelBadInputs_Return400(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(body map[string]any)
	}{
		{"missing account_id", func(body map[string]any) { body["account_id"] = "" }},
		{"missing order_id", func(body map[string]any) { body["order_id"] = "" }},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			stub := &stubBroker{}
			handler := newCancelHandlerForTest(t, stub, 0)

			body := goodCancelBody()
			testCase.mutate(body)
			recorder := doCancel(t, handler, body)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body=%s)", recorder.Code, recorder.Body.String())
			}
			if stub.cancelCount != 0 {
				t.Errorf("broker.CancelOrder called on 400 branch, want 0 calls")
			}
		})
	}
}

func TestCancel_NonPOST_Returns405(t *testing.T) {
	stub := &stubBroker{}
	handler := newCancelHandlerForTest(t, stub, 0)

	request := httptest.NewRequest(http.MethodDelete, CancelPath, nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", recorder.Code)
	}
	if got := recorder.Header().Get("Allow"); got != http.MethodPost {
		t.Errorf("Allow header = %q, want POST", got)
	}
}

// -----------------------------------------------------------------------------
// 409 — the "every CancelFailed reason" uniform bucket
// -----------------------------------------------------------------------------

func TestCancel_CancelFailed_Return409WithReason(t *testing.T) {
	cases := []struct {
		reason     funnel.RejectReason
		wantReason string
	}{
		{funnel.ReasonNoSession, "no_session"},
		{funnel.ReasonUnknownOrder, "unknown_order"},
		{funnel.ReasonNotCancellable, "not_cancellable"},
	}
	for _, testCase := range cases {
		t.Run(testCase.wantReason, func(t *testing.T) {
			stub := &stubBroker{
				cancelResult: funnel.CancelResult{Status: funnel.CancelFailed, Reason: testCase.reason},
			}
			handler := newCancelHandlerForTest(t, stub, 0)

			recorder := doCancel(t, handler, goodCancelBody())

			if recorder.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409", recorder.Code)
			}
			var body reasonBody
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if body.Reason != testCase.wantReason {
				t.Errorf("reason = %q, want %q", body.Reason, testCase.wantReason)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// 504 — ErrBrokerTimeout
// -----------------------------------------------------------------------------

func TestCancel_BrokerTimeout_Returns504(t *testing.T) {
	stub := &stubBroker{cancelBlocks: true}
	handler := newCancelHandlerForTest(t, stub, 1*time.Millisecond)

	recorder := doCancel(t, handler, goodCancelBody())

	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", recorder.Code)
	}
}

// -----------------------------------------------------------------------------
// 500 — unexpected error + bubbled panic
// -----------------------------------------------------------------------------

func TestCancel_UnexpectedBrokerError_Returns500(t *testing.T) {
	stub := &stubBroker{cancelErr: errUnexpected}
	handler := newCancelHandlerForTest(t, stub, 0)

	recorder := doCancel(t, handler, goodCancelBody())

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", recorder.Code)
	}
}

func TestCancel_BrokerPanic_Recovers_Returns500(t *testing.T) {
	stub := &stubBroker{panicValue: "simulated broker panic (invariant violation)"}
	handler := newCancelHandlerForTest(t, stub, 0)

	recorder := doCancel(t, handler, goodCancelBody())

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (recovered)", recorder.Code)
	}
}
