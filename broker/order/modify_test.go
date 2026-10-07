package order

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"simulator/broker/funnel"
	"simulator/contract"
)

// -----------------------------------------------------------------------------
// modify_test.go — POST /v1/orders/modify handler coverage.
//
// Three composed outcome branches (BROKER_READ_SURFACES §9.4):
//   1. cancel-failed (gate) → 409 modify_status=rejected, replace=null,
//      and the invariant "place was NEVER called" is proved via
//      stub.placeCount == 0.
//   2. cancel-pending + place-accepted → 200 modify_status=ok with
//      both leg order_ids.
//   3. cancel-pending + place-rejected → 200 modify_status=partial
//      with the pinned warning string (§9.3 partial seam, reported
//      not rolled back).
//
// Plus the normal 400 / 405 / 504 / 500 branches, with 504 covering
// EITHER leg separately (modify is the one endpoint where that
// distinction matters — the stub's placeBlocks / cancelBlocks pair
// drives the two cases).
//
// Wire shape translation: is_market=true → Type=Market, TIF=IOC;
// is_market=false → Type=Limit, TIF=GTC + LimitPrice=new_price.
// -----------------------------------------------------------------------------

const (
	testModifyAccount     = "acct-M"
	testModifyOldOrderID  = "acct-M:1"
	testModifyNewOrderID  = "acct-M:2"
	testModifyInstrument  = "ACME"
)

func newModifyHandlerForTest(t *testing.T, stub *stubBroker, orderTimeout time.Duration) http.Handler {
	t.Helper()
	if orderTimeout == 0 {
		orderTimeout = 100 * time.Millisecond
	}
	return NewModifyHandler(stub, Config{OrderTimeout: orderTimeout})
}

// goodModifyBody is a well-formed limit-buy modify body — the "ok"
// starting point every happy / branch-specific test customises.
func goodModifyBody() map[string]any {
	return map[string]any{
		"account_id": testModifyAccount,
		"order_id":   testModifyOldOrderID,
		"new_qty":    int64(50),
		"new_price":  int64(10_050),
		"side":       "buy",
		"instrument": testModifyInstrument,
		"is_market":  false,
	}
}

func doModify(t *testing.T, handler http.Handler, body any) *httptest.ResponseRecorder {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal test body: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, ModifyPath, bytes.NewReader(encoded))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

// -----------------------------------------------------------------------------
// 200 — happy "ok" branch (both legs locally accepted)
// -----------------------------------------------------------------------------

func TestModify_HappyOK_Returns200WithBothLegOrderIDs(t *testing.T) {
	stub := &stubBroker{
		cancelResult: funnel.CancelResult{Status: funnel.CancelPending, OrderID: testModifyOldOrderID},
		placeResult:  funnel.PlaceResult{Status: funnel.PlaceAccepted, OrderID: testModifyNewOrderID},
	}
	handler := newModifyHandlerForTest(t, stub, 0)

	recorder := doModify(t, handler, goodModifyBody())

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", recorder.Code, recorder.Body.String())
	}
	var body modifyResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.ModifyStatus != modifyStatusOK {
		t.Errorf("modify_status = %q, want %q", body.ModifyStatus, modifyStatusOK)
	}
	if body.Cancel == nil || body.Cancel.Status != cancelLegPending || body.Cancel.OrderID != testModifyOldOrderID {
		t.Errorf("cancel leg = %+v, want {pending, %q}", body.Cancel, testModifyOldOrderID)
	}
	if body.Replace == nil || body.Replace.Status != replaceLegAccepted || body.Replace.OrderID != testModifyNewOrderID {
		t.Errorf("replace leg = %+v, want {accepted, %q}", body.Replace, testModifyNewOrderID)
	}
	if body.Warning != "" {
		t.Errorf("warning = %q, want empty on happy ok", body.Warning)
	}

	// Both legs invoked exactly once, in the right order (cancel first).
	if stub.cancelCount != 1 || stub.placeCount != 1 {
		t.Errorf("call counts = cancel:%d place:%d, want 1 each", stub.cancelCount, stub.placeCount)
	}
}

// -----------------------------------------------------------------------------
// 409 — gate rejected (cancel failed, place NEVER called)
// -----------------------------------------------------------------------------

func TestModify_CancelFailed_GateRejects_PlaceNeverCalled(t *testing.T) {
	stub := &stubBroker{
		cancelResult: funnel.CancelResult{Status: funnel.CancelFailed, Reason: funnel.ReasonUnknownOrder},
		// placeResult left unset — if the handler called place, it
		// would see PlaceStatusUnknown and we would detect that in
		// the response assertion.
	}
	handler := newModifyHandlerForTest(t, stub, 0)

	recorder := doModify(t, handler, goodModifyBody())

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (body=%s)", recorder.Code, recorder.Body.String())
	}
	var body modifyResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.ModifyStatus != modifyStatusRejected {
		t.Errorf("modify_status = %q, want %q", body.ModifyStatus, modifyStatusRejected)
	}
	if body.Cancel == nil || body.Cancel.Status != cancelLegFailed || body.Cancel.Reason != "unknown_order" {
		t.Errorf("cancel leg = %+v, want {failed, unknown_order}", body.Cancel)
	}
	if body.Replace != nil {
		t.Errorf("replace leg = %+v, want null (gate rejected)", body.Replace)
	}

	// Load-bearing invariant: the gate prevented the place punch.
	if stub.placeCount != 0 {
		t.Errorf("broker.PlaceOrder called %d times after gate rejection, want 0 — the §9.2 Option-1 gate failed", stub.placeCount)
	}
	if stub.cancelCount != 1 {
		t.Errorf("broker.CancelOrder call count = %d, want 1", stub.cancelCount)
	}
}

// Both other CancelFailed reasons route through the same gate path;
// one subtest per reason to prove the switch is reason-blind.
func TestModify_CancelFailed_EveryReasonRoutesTo409(t *testing.T) {
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
			handler := newModifyHandlerForTest(t, stub, 0)

			recorder := doModify(t, handler, goodModifyBody())

			if recorder.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409", recorder.Code)
			}
			var body modifyResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if body.Cancel == nil || body.Cancel.Reason != testCase.wantReason {
				t.Errorf("cancel leg reason = %+v, want %q", body.Cancel, testCase.wantReason)
			}
			if stub.placeCount != 0 {
				t.Errorf("place called on %s gate, want 0", testCase.wantReason)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// 200 — partial branch (cancel pending, place rejected at Gate 4)
// -----------------------------------------------------------------------------

func TestModify_CancelPending_PlaceRejected_ReturnsPartialWithWarning(t *testing.T) {
	stub := &stubBroker{
		cancelResult: funnel.CancelResult{Status: funnel.CancelPending, OrderID: testModifyOldOrderID},
		placeResult:  funnel.PlaceResult{Status: funnel.PlaceRejected, Reason: funnel.ReasonInsufficientFunds},
	}
	handler := newModifyHandlerForTest(t, stub, 0)

	recorder := doModify(t, handler, goodModifyBody())

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (partial)", recorder.Code)
	}
	var body modifyResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.ModifyStatus != modifyStatusPartial {
		t.Errorf("modify_status = %q, want %q", body.ModifyStatus, modifyStatusPartial)
	}
	if body.Cancel == nil || body.Cancel.Status != cancelLegPending || body.Cancel.OrderID != testModifyOldOrderID {
		t.Errorf("cancel leg = %+v, want {pending, %q}", body.Cancel, testModifyOldOrderID)
	}
	if body.Replace == nil || body.Replace.Status != replaceLegRejected || body.Replace.Reason != "insufficient_funds" {
		t.Errorf("replace leg = %+v, want {rejected, insufficient_funds}", body.Replace)
	}
	if body.Warning != modifyPartialWarning {
		t.Errorf("warning = %q, want %q", body.Warning, modifyPartialWarning)
	}
}

// -----------------------------------------------------------------------------
// Wire→funnel translation — is_market drives Type / TIF correctly
// -----------------------------------------------------------------------------

func TestModify_WireTranslation_LimitBuy(t *testing.T) {
	stub := &stubBroker{
		cancelResult: funnel.CancelResult{Status: funnel.CancelPending, OrderID: testModifyOldOrderID},
		placeResult:  funnel.PlaceResult{Status: funnel.PlaceAccepted, OrderID: testModifyNewOrderID},
	}
	handler := newModifyHandlerForTest(t, stub, 0)

	body := goodModifyBody()
	body["side"] = "buy"
	body["is_market"] = false
	body["new_qty"] = int64(25)
	body["new_price"] = int64(10_200)

	doModify(t, handler, body)

	wantSpec := funnel.OrderSpec{
		Instrument: contract.InstrumentID(testModifyInstrument),
		Side:       contract.Buy,
		Type:       contract.Limit,
		TIF:        contract.GTC,
		Qty:        25,
		LimitPrice: 10_200,
	}
	if stub.placeOrder != wantSpec {
		t.Errorf("OrderSpec sent on place leg = %+v, want %+v", stub.placeOrder, wantSpec)
	}
	if stub.cancelOrderID != testModifyOldOrderID {
		t.Errorf("cancelOrderID = %q, want %q", stub.cancelOrderID, testModifyOldOrderID)
	}
	if stub.cancelAccountID != testModifyAccount {
		t.Errorf("cancelAccountID = %q, want %q", stub.cancelAccountID, testModifyAccount)
	}
}

func TestModify_WireTranslation_MarketSell(t *testing.T) {
	stub := &stubBroker{
		cancelResult: funnel.CancelResult{Status: funnel.CancelPending, OrderID: testModifyOldOrderID},
		placeResult:  funnel.PlaceResult{Status: funnel.PlaceAccepted, OrderID: testModifyNewOrderID},
	}
	handler := newModifyHandlerForTest(t, stub, 0)

	body := goodModifyBody()
	body["side"] = "sell"
	body["is_market"] = true
	body["new_qty"] = int64(10)
	// new_price intentionally left in the body — ignored because
	// is_market=true, same as contract.FundedOrder.LimitPrice note.
	body["new_price"] = int64(0)

	doModify(t, handler, body)

	wantSpec := funnel.OrderSpec{
		Instrument: contract.InstrumentID(testModifyInstrument),
		Side:       contract.Sell,
		Type:       contract.Market,
		TIF:        contract.IOC,
		Qty:        10,
		LimitPrice: 0,
	}
	if stub.placeOrder != wantSpec {
		t.Errorf("OrderSpec sent on place leg = %+v, want %+v (market→IOC)", stub.placeOrder, wantSpec)
	}
}

// -----------------------------------------------------------------------------
// 400 — pre-funnel body-shape rejects
// -----------------------------------------------------------------------------

func TestModify_BadJSON_Returns400(t *testing.T) {
	stub := &stubBroker{}
	handler := newModifyHandlerForTest(t, stub, 0)

	request := httptest.NewRequest(http.MethodPost, ModifyPath, bytes.NewReader([]byte("{not json")))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	if stub.cancelCount+stub.placeCount != 0 {
		t.Errorf("broker called on bad JSON branch, want 0 calls")
	}
}

func TestModify_PreFunnelBadInputs_Return400(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(body map[string]any)
	}{
		{"missing account_id", func(body map[string]any) { body["account_id"] = "" }},
		{"missing order_id", func(body map[string]any) { body["order_id"] = "" }},
		{"missing instrument", func(body map[string]any) { body["instrument"] = "" }},
		{"unknown side", func(body map[string]any) { body["side"] = "long" }},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			stub := &stubBroker{}
			handler := newModifyHandlerForTest(t, stub, 0)

			body := goodModifyBody()
			testCase.mutate(body)
			recorder := doModify(t, handler, body)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body=%s)", recorder.Code, recorder.Body.String())
			}
			if stub.cancelCount+stub.placeCount != 0 {
				t.Errorf("broker called on 400 branch, want 0 calls")
			}
		})
	}
}

func TestModify_NonPOST_Returns405(t *testing.T) {
	stub := &stubBroker{}
	handler := newModifyHandlerForTest(t, stub, 0)

	request := httptest.NewRequest(http.MethodPut, ModifyPath, nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", recorder.Code)
	}
	if got := recorder.Header().Get("Allow"); got != http.MethodPost {
		t.Errorf("Allow = %q, want POST", got)
	}
}

// -----------------------------------------------------------------------------
// 504 — timeout on EITHER leg (modify-specific split)
// -----------------------------------------------------------------------------

func TestModify_CancelLegTimeout_Returns504(t *testing.T) {
	stub := &stubBroker{cancelBlocks: true}
	handler := newModifyHandlerForTest(t, stub, 1*time.Millisecond)

	recorder := doModify(t, handler, goodModifyBody())

	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504 (cancel leg)", recorder.Code)
	}
	if stub.placeCount != 0 {
		t.Errorf("place called after cancel-leg timeout, want 0")
	}
}

func TestModify_PlaceLegTimeout_Returns504(t *testing.T) {
	stub := &stubBroker{
		cancelResult: funnel.CancelResult{Status: funnel.CancelPending, OrderID: testModifyOldOrderID},
		placeBlocks:  true,
	}
	handler := newModifyHandlerForTest(t, stub, 20*time.Millisecond)

	recorder := doModify(t, handler, goodModifyBody())

	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504 (place leg)", recorder.Code)
	}
	if stub.cancelCount != 1 {
		t.Errorf("cancel call count = %d, want 1 (cancel succeeded; place timed out)", stub.cancelCount)
	}
	if stub.placeCount != 1 {
		t.Errorf("place call count = %d, want 1 (started then blocked on ctx)", stub.placeCount)
	}
}

// -----------------------------------------------------------------------------
// 500 — unexpected errors on each leg + bubbled panic
// -----------------------------------------------------------------------------

func TestModify_UnexpectedCancelError_Returns500(t *testing.T) {
	stub := &stubBroker{cancelErr: errUnexpected}
	handler := newModifyHandlerForTest(t, stub, 0)

	recorder := doModify(t, handler, goodModifyBody())

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (cancel error)", recorder.Code)
	}
	if stub.placeCount != 0 {
		t.Errorf("place called after cancel 500, want 0")
	}
}

func TestModify_UnexpectedPlaceError_Returns500(t *testing.T) {
	stub := &stubBroker{
		cancelResult: funnel.CancelResult{Status: funnel.CancelPending, OrderID: testModifyOldOrderID},
		placeErr:     errUnexpected,
	}
	handler := newModifyHandlerForTest(t, stub, 0)

	recorder := doModify(t, handler, goodModifyBody())

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (place error)", recorder.Code)
	}
}

func TestModify_BrokerPanic_Recovers_Returns500(t *testing.T) {
	// Panic value fires on the FIRST call — which is cancel. The
	// recover guard writes 500 and the server keeps serving (if the
	// recover were missing, the test binary would crash).
	stub := &stubBroker{panicValue: "simulated broker panic (invariant violation)"}
	handler := newModifyHandlerForTest(t, stub, 0)

	recorder := doModify(t, handler, goodModifyBody())

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (recovered)", recorder.Code)
	}
}
