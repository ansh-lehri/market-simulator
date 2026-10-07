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
// place_test.go — POST /v1/orders handler coverage.
//
// Every branch of the P5-D27 status table against a stubBroker:
//   202 — PlaceAccepted happy path (both order types)
//   400 — bad json, missing account_id, missing instrument,
//         unknown side/type/tif
//   405 — method not allowed
//   409 — PlaceRejected{ReasonNoSession}
//   422 — PlaceRejected{all other reasons} table
//   504 — ErrBrokerTimeout (stub blocks on ctx.Done)
//   500 — unexpected broker error + broker panic (recover)
//
// Plus one wire→funnel translation test pinning that the OrderSpec
// handed to the broker is byte-exact to the parsed body.
// -----------------------------------------------------------------------------

const (
	testPlaceAccount    = "acct-1"
	testPlaceInstrument = "ACME"
	testPlaceOrderID    = "acct-1:1"
)

// newPlaceHandlerForTest builds a handler + its stub for one test.
// Default OrderTimeout is 100 ms — long enough that no happy test flakes
// on a hyper-slow runner, short enough that the 504 test resolves fast.
func newPlaceHandlerForTest(t *testing.T, stub *stubBroker, orderTimeout time.Duration) http.Handler {
	t.Helper()
	if orderTimeout == 0 {
		orderTimeout = 100 * time.Millisecond
	}
	return NewPlaceHandler(stub, Config{OrderTimeout: orderTimeout})
}

// goodPlaceBody is the shared well-formed-limit-buy body — tests that
// care only about one specific reject reason can start from this and
// override the one field under test. account_id, instrument, side,
// type, tif, qty, limit_price all present and valid.
func goodPlaceBody() map[string]any {
	return map[string]any{
		"account_id":  testPlaceAccount,
		"instrument":  testPlaceInstrument,
		"side":        "buy",
		"type":        "limit",
		"tif":         "gtc",
		"qty":         int64(100),
		"limit_price": int64(10_021),
	}
}

func doPlace(t *testing.T, handler http.Handler, body any) *httptest.ResponseRecorder {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal test body: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, PlacePath, bytes.NewReader(encoded))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

// -----------------------------------------------------------------------------
// 202 — happy paths
// -----------------------------------------------------------------------------

func TestPlace_HappyLimitBuy_Returns202WithOrderID(t *testing.T) {
	stub := &stubBroker{
		placeResult: funnel.PlaceResult{Status: funnel.PlaceAccepted, OrderID: testPlaceOrderID},
	}
	handler := newPlaceHandlerForTest(t, stub, 0)

	recorder := doPlace(t, handler, goodPlaceBody())

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", recorder.Code)
	}
	var body placeAcceptedBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.OrderID != testPlaceOrderID {
		t.Errorf("OrderID = %q, want %q", body.OrderID, testPlaceOrderID)
	}
	if stub.placeCount != 1 {
		t.Errorf("broker.PlaceOrder call count = %d, want 1", stub.placeCount)
	}
}

func TestPlace_HappyMarketSell_Returns202(t *testing.T) {
	stub := &stubBroker{
		placeResult: funnel.PlaceResult{Status: funnel.PlaceAccepted, OrderID: testPlaceOrderID},
	}
	handler := newPlaceHandlerForTest(t, stub, 0)

	marketSell := goodPlaceBody()
	marketSell["side"] = "sell"
	marketSell["type"] = "market"
	marketSell["tif"] = "ioc"
	delete(marketSell, "limit_price")

	recorder := doPlace(t, handler, marketSell)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body=%s)", recorder.Code, recorder.Body.String())
	}
}

// -----------------------------------------------------------------------------
// Wire→funnel translation — the OrderSpec handed to the broker is exact
// -----------------------------------------------------------------------------

func TestPlace_TranslatesWireToOrderSpec(t *testing.T) {
	stub := &stubBroker{
		placeResult: funnel.PlaceResult{Status: funnel.PlaceAccepted, OrderID: testPlaceOrderID},
	}
	handler := newPlaceHandlerForTest(t, stub, 0)

	body := goodPlaceBody()
	body["side"] = "sell"
	body["type"] = "limit"
	body["tif"] = "ioc"
	body["qty"] = int64(42)
	body["limit_price"] = int64(9_999)

	doPlace(t, handler, body)

	if stub.placeAccountID != testPlaceAccount {
		t.Errorf("accountID sent = %q, want %q", stub.placeAccountID, testPlaceAccount)
	}
	if stub.placeSessionID != "" {
		t.Errorf("sessionID sent = %q, want empty (vestigial per §4.3)", stub.placeSessionID)
	}
	wantSpec := funnel.OrderSpec{
		Instrument: contract.InstrumentID(testPlaceInstrument),
		Side:       contract.Sell,
		Type:       contract.Limit,
		TIF:        contract.IOC,
		Qty:        42,
		LimitPrice: 9_999,
	}
	if stub.placeOrder != wantSpec {
		t.Errorf("OrderSpec sent = %+v, want %+v", stub.placeOrder, wantSpec)
	}
}

// -----------------------------------------------------------------------------
// 400 — pre-funnel body-shape rejects
// -----------------------------------------------------------------------------

func TestPlace_BadJSON_Returns400(t *testing.T) {
	stub := &stubBroker{}
	handler := newPlaceHandlerForTest(t, stub, 0)

	request := httptest.NewRequest(http.MethodPost, PlacePath, bytes.NewReader([]byte("{not json")))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	if stub.placeCount != 0 {
		t.Errorf("broker.PlaceOrder called %d times on bad JSON, want 0", stub.placeCount)
	}
}

func TestPlace_PreFunnelBadInputs_Return400(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(body map[string]any)
		errHint string
	}{
		{"missing account_id", func(body map[string]any) { body["account_id"] = "" }, "account_id"},
		{"missing instrument", func(body map[string]any) { body["instrument"] = "" }, "instrument"},
		{"unknown side", func(body map[string]any) { body["side"] = "long" }, "side"},
		{"unknown type", func(body map[string]any) { body["type"] = "stop" }, "type"},
		{"unknown tif", func(body map[string]any) { body["tif"] = "day" }, "tif"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			stub := &stubBroker{}
			handler := newPlaceHandlerForTest(t, stub, 0)

			body := goodPlaceBody()
			testCase.mutate(body)
			recorder := doPlace(t, handler, body)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body=%s)", recorder.Code, recorder.Body.String())
			}
			if stub.placeCount != 0 {
				t.Errorf("broker.PlaceOrder called on 400 branch, want 0 calls")
			}
		})
	}
}

func TestPlace_NonPOST_Returns405(t *testing.T) {
	stub := &stubBroker{}
	handler := newPlaceHandlerForTest(t, stub, 0)

	request := httptest.NewRequest(http.MethodGet, PlacePath, nil)
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
// 409 — ReasonNoSession special-cased
// -----------------------------------------------------------------------------

func TestPlace_NoSession_Returns409(t *testing.T) {
	stub := &stubBroker{
		placeResult: funnel.PlaceResult{Status: funnel.PlaceRejected, Reason: funnel.ReasonNoSession},
	}
	handler := newPlaceHandlerForTest(t, stub, 0)

	recorder := doPlace(t, handler, goodPlaceBody())

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", recorder.Code)
	}
	var body reasonBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Reason != "no_session" {
		t.Errorf("reason = %q, want %q", body.Reason, "no_session")
	}
}

// -----------------------------------------------------------------------------
// 422 — the "every other PlaceRejected reason" bucket
// -----------------------------------------------------------------------------

func TestPlace_PlaceRejected_Return422WithReason(t *testing.T) {
	cases := []struct {
		reason     funnel.RejectReason
		wantReason string
	}{
		{funnel.ReasonUnknownInstrument, "unknown_instrument"},
		{funnel.ReasonBadQty, "bad_qty"},
		{funnel.ReasonBadSide, "bad_side"},
		{funnel.ReasonBadPrice, "bad_price"},
		{funnel.ReasonBadType, "bad_type"},
		{funnel.ReasonInsufficientFunds, "insufficient_funds"},
		{funnel.ReasonNoMarketPrice, "no_market_price"},
	}
	for _, testCase := range cases {
		t.Run(testCase.wantReason, func(t *testing.T) {
			stub := &stubBroker{
				placeResult: funnel.PlaceResult{Status: funnel.PlaceRejected, Reason: testCase.reason},
			}
			handler := newPlaceHandlerForTest(t, stub, 0)

			recorder := doPlace(t, handler, goodPlaceBody())

			if recorder.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422", recorder.Code)
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

func TestPlace_BrokerTimeout_Returns504(t *testing.T) {
	stub := &stubBroker{placeBlocks: true}
	// 1 ms timeout — stub blocks on ctx.Done, deadline fires fast.
	handler := newPlaceHandlerForTest(t, stub, 1*time.Millisecond)

	recorder := doPlace(t, handler, goodPlaceBody())

	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", recorder.Code)
	}
}

// -----------------------------------------------------------------------------
// 500 — unexpected broker error + bubbled panic
// -----------------------------------------------------------------------------

func TestPlace_UnexpectedBrokerError_Returns500(t *testing.T) {
	stub := &stubBroker{
		placeErr: errUnexpected,
	}
	handler := newPlaceHandlerForTest(t, stub, 0)

	recorder := doPlace(t, handler, goodPlaceBody())

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", recorder.Code)
	}
}

func TestPlace_BrokerPanic_Recovers_Returns500(t *testing.T) {
	stub := &stubBroker{panicValue: "simulated broker panic (invariant violation)"}
	handler := newPlaceHandlerForTest(t, stub, 0)

	// If recoverPanic were missing, this would crash the test binary. The
	// fact that doPlace returns at all is half the assertion; the status
	// code is the other half.
	recorder := doPlace(t, handler, goodPlaceBody())

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (recovered)", recorder.Code)
	}
}
