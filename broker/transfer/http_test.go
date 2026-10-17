package transfer

// Stub-based HTTP tests (BROKER_PHASE3 §2.4). Fast, no DB: drive the
// ports with hand-rolled stubs to cover 400 pre-funnel paths, 504
// timeout, 500 fallthroughs, no-409-ever, panic recovery, and
// status/body shape symmetry between live-ok and durable-ok (mode-blind).
//
// Live-runtime tests live in live_integration_test.go under the external
// `package transfer_test` — they import broker/bringup, which (now that
// bringup registers the transfer handler on its mux) would create an
// import cycle if co-located here. External test packages compile
// separately and break that cycle cleanly.
//
// Assertions trace to the Phase-3 §2.4 status table, not to the code's
// output.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"simulator/broker/funnel"
	"simulator/broker/store"
	"simulator/economy/account"
)

// -------------------------------------------------------------------------
// Stub ports — satisfy brokerPort / storePort in-memory. Tests inject these
// to reach branches the live runtime can't force deterministically.
// -------------------------------------------------------------------------

type stubBroker struct {
	// If TransferFn is set, it is called; otherwise returns a canned
	// result configured by the fields below.
	TransferFn func(ctx context.Context, aid funnel.AccountID, from, to account.Bucket, amt int64) (funnel.TransferResult, error)
	Result     funnel.TransferResult
	Err        error
	Calls      int
}

func (stub *stubBroker) Transfer(
	ctx context.Context,
	accountID funnel.AccountID,
	fromBucket account.Bucket,
	toBucket account.Bucket,
	amount int64,
) (funnel.TransferResult, error) {
	stub.Calls++
	if stub.TransferFn != nil {
		return stub.TransferFn(ctx, accountID, fromBucket, toBucket, amount)
	}
	return stub.Result, stub.Err
}

type stubStore struct {
	// If DurableFn is set, it is called; otherwise returns the canned
	// (Available, Bank, Err) below.
	DurableFn func(ctx context.Context, aid store.AccountID, dir store.TransferDirection, amt int64) (int64, int64, error)
	Available int64
	Bank      int64
	Err       error
	Calls     int
}

func (stub *stubStore) DurableTransfer(
	ctx context.Context,
	accountID store.AccountID,
	direction store.TransferDirection,
	amount int64,
) (int64, int64, error) {
	stub.Calls++
	if stub.DurableFn != nil {
		return stub.DurableFn(ctx, accountID, direction, amount)
	}
	return stub.Available, stub.Bank, stub.Err
}

// defaultStubCfg: a 2-second timeout keeps the fast tests fast while
// leaving room for the 504 case to prove it fires without flake.
func defaultStubCfg() Config {
	return Config{TransferTimeout: 2 * time.Second}
}

// postJSON issues one POST against the given handler. Returns the
// response status + decoded body (as a map so error vs success bodies
// are both inspectable without pre-committing to a shape).
func postJSON(t *testing.T, handler http.Handler, body any) (int, map[string]any) {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, Path, bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	responseRecorder := httptest.NewRecorder()
	handler.ServeHTTP(responseRecorder, request)

	responseBody := map[string]any{}
	if responseRecorder.Body.Len() > 0 {
		if err := json.Unmarshal(responseRecorder.Body.Bytes(), &responseBody); err != nil {
			t.Fatalf("decode response body: %v (raw=%q)", err, responseRecorder.Body.String())
		}
	}
	return responseRecorder.Code, responseBody
}

// -------------------------------------------------------------------------
// Pre-funnel 400 paths (H9/H10/H11) — stubs never called.
// -------------------------------------------------------------------------

func TestHTTP_400s_PreFunnel(t *testing.T) {
	cases := []struct {
		name string
		body map[string]any
	}{
		{"missing_account_id", map[string]any{"direction": "to_bank", "amount": 100, "unit": "paise"}},
		{"empty_account_id", map[string]any{"account_id": "", "direction": "to_bank", "amount": 100}},
		{"illegal_direction", map[string]any{"account_id": "acct", "direction": "sideways", "amount": 100}},
		{"missing_direction", map[string]any{"account_id": "acct", "amount": 100}},
		{"amount_zero", map[string]any{"account_id": "acct", "direction": "to_bank", "amount": 0}},
		{"amount_negative", map[string]any{"account_id": "acct", "direction": "to_bank", "amount": -1}},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			broker := &stubBroker{}
			durableStore := &stubStore{}
			handler := NewHandler(broker, durableStore, defaultStubCfg())

			status, body := postJSON(t, handler, testCase.body)

			if status != http.StatusBadRequest {
				t.Fatalf("status: got %d, want 400 (body=%v)", status, body)
			}
			if _, ok := body["error"].(string); !ok {
				t.Errorf("error body: got %v, want {\"error\":<string>}", body)
			}
			if broker.Calls != 0 {
				t.Errorf("broker calls: got %d, want 0 (400 must be pre-funnel)", broker.Calls)
			}
			if durableStore.Calls != 0 {
				t.Errorf("store calls: got %d, want 0 (400 must be pre-funnel)", durableStore.Calls)
			}
		})
	}
}

// Invalid JSON body (not a map, not valid at all) is also a 400 — this
// case is distinct from the field-level 400s above because it fires
// before field inspection.
func TestHTTP_400_InvalidJSON(t *testing.T) {
	broker := &stubBroker{}
	durableStore := &stubStore{}
	handler := NewHandler(broker, durableStore, defaultStubCfg())

	request := httptest.NewRequest(http.MethodPost, Path, strings.NewReader("not-json"))
	responseRecorder := httptest.NewRecorder()
	handler.ServeHTTP(responseRecorder, request)

	if responseRecorder.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", responseRecorder.Code)
	}
	if broker.Calls != 0 || durableStore.Calls != 0 {
		t.Errorf("ports touched on bad-json 400: broker=%d store=%d", broker.Calls, durableStore.Calls)
	}
}

// -------------------------------------------------------------------------
// 504 broker timeout (H12) — hanging stub + tiny timeout.
// -------------------------------------------------------------------------

func TestHTTP_504_BrokerTimeout(t *testing.T) {
	broker := &stubBroker{
		TransferFn: func(ctx context.Context, aid funnel.AccountID, from, to account.Bucket, amt int64) (funnel.TransferResult, error) {
			<-ctx.Done()
			return funnel.TransferResult{}, funnel.ErrBrokerTimeout
		},
	}
	durableStore := &stubStore{}
	handler := NewHandler(broker, durableStore, Config{TransferTimeout: 50 * time.Millisecond})

	status, body := postJSON(t, handler, map[string]any{
		"account_id": "acct-wedged",
		"direction":  "to_bank",
		"amount":     100,
	})

	if status != http.StatusGatewayTimeout {
		t.Fatalf("status: got %d, want 504 (body=%v)", status, body)
	}
	if durableStore.Calls != 0 {
		t.Errorf("store calls on 504: got %d, want 0 (timeout must not fall through to durable)", durableStore.Calls)
	}
}

// -------------------------------------------------------------------------
// Live-ok and live-reject (H1-H4 via stubs). Live-path assertions:
//   - Present:true + Err:nil        → 200 {available, bank}
//   - Present:true + ErrInsufficientFunds → 422 (no durable call)
//   - route-first-never-falls-when-live: a live insufficient MUST NOT
//     trigger a durable call (H13).
// -------------------------------------------------------------------------

func TestHTTP_200_LiveSuccess(t *testing.T) {
	broker := &stubBroker{
		Result: funnel.TransferResult{Present: true, Available: 7_000, Bank: 8_000},
	}
	durableStore := &stubStore{}
	handler := NewHandler(broker, durableStore, defaultStubCfg())

	status, body := postJSON(t, handler, map[string]any{
		"account_id": "acct-live",
		"direction":  "to_bank",
		"amount":     3_000,
	})

	if status != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body=%v)", status, body)
	}
	if gotAvailable, ok := body["available"].(float64); !ok || int64(gotAvailable) != 7_000 {
		t.Errorf("available: got %v, want 7000", body["available"])
	}
	if gotBank, ok := body["bank"].(float64); !ok || int64(gotBank) != 8_000 {
		t.Errorf("bank: got %v, want 8000", body["bank"])
	}
	if durableStore.Calls != 0 {
		t.Errorf("durable called on live 200: got %d, want 0", durableStore.Calls)
	}
}

func TestHTTP_422_LiveInsufficient_DoesNotFallToDurable(t *testing.T) {
	broker := &stubBroker{
		Result: funnel.TransferResult{Present: true, Err: account.ErrInsufficient{Bucket: account.Available, Have: 1_000, Want: 5_000}},
	}
	durableStore := &stubStore{}
	handler := NewHandler(broker, durableStore, defaultStubCfg())

	status, body := postJSON(t, handler, map[string]any{
		"account_id": "acct-live-insufficient",
		"direction":  "to_bank",
		"amount":     5_000,
	})

	if status != http.StatusUnprocessableEntity {
		t.Fatalf("status: got %d, want 422 (body=%v)", status, body)
	}
	// Mode-blind body: plain {"error": "insufficient funds"}. Must NOT
	// carry have/want (would be a mode leak vs the durable 422).
	if msg, _ := body["error"].(string); msg != "insufficient funds" {
		t.Errorf("error body: got %v, want {\"error\":\"insufficient funds\"}", body)
	}
	if _, has := body["have"]; has {
		t.Errorf("422 body leaked have/want: %v (must be mode-blind)", body)
	}
	// H13 route-first-never-falls-when-live.
	if durableStore.Calls != 0 {
		t.Errorf("durable called on live 422: got %d, want 0 (route-first-never-falls-when-live)", durableStore.Calls)
	}
}

// Live-reject with a non-ErrInsufficientFunds err → 500 fallthrough.
// Covers the "else → 500" clause of the Phase-3 status table on the live
// branch (unexpected L5 error).
func TestHTTP_500_LiveUnexpectedReject(t *testing.T) {
	broker := &stubBroker{
		Result: funnel.TransferResult{Present: true, Err: errors.New("some unexpected L5 fault")},
	}
	durableStore := &stubStore{}
	handler := NewHandler(broker, durableStore, defaultStubCfg())

	status, _ := postJSON(t, handler, map[string]any{
		"account_id": "acct", "direction": "to_bank", "amount": 100,
	})

	if status != http.StatusInternalServerError {
		t.Fatalf("status: got %d, want 500", status)
	}
	if durableStore.Calls != 0 {
		t.Errorf("durable called on 500 live fallthrough: got %d, want 0", durableStore.Calls)
	}
}

// -------------------------------------------------------------------------
// Idle paths (Present:false → durable) via stubs. H5-H8 + 503 (ErrLoadFailed).
// -------------------------------------------------------------------------

func TestHTTP_200_DurableSuccess_ModeBlindBody(t *testing.T) {
	broker := &stubBroker{Result: funnel.TransferResult{Present: false}}
	durableStore := &stubStore{Available: 4_000, Bank: 6_000}
	handler := NewHandler(broker, durableStore, defaultStubCfg())

	status, body := postJSON(t, handler, map[string]any{
		"account_id": "acct-idle", "direction": "to_bank", "amount": 1_000,
	})

	if status != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body=%v)", status, body)
	}
	// Mode-blind: identical body shape to TestHTTP_200_LiveSuccess.
	if _, hasAvail := body["available"]; !hasAvail {
		t.Errorf("durable 200 body missing 'available' (mode-blind shape broken): %v", body)
	}
	if _, hasBank := body["bank"]; !hasBank {
		t.Errorf("durable 200 body missing 'bank' (mode-blind shape broken): %v", body)
	}
	if durableStore.Calls != 1 {
		t.Errorf("durable calls: got %d, want 1", durableStore.Calls)
	}
}

func TestHTTP_422_DurableInsufficient(t *testing.T) {
	broker := &stubBroker{Result: funnel.TransferResult{Present: false}}
	durableStore := &stubStore{Err: store.ErrInsufficient}
	handler := NewHandler(broker, durableStore, defaultStubCfg())

	status, body := postJSON(t, handler, map[string]any{
		"account_id": "acct-idle", "direction": "to_trading", "amount": 999_999,
	})

	if status != http.StatusUnprocessableEntity {
		t.Fatalf("status: got %d, want 422 (body=%v)", status, body)
	}
	// Mode-blind: same error string as live 422.
	if msg, _ := body["error"].(string); msg != "insufficient funds" {
		t.Errorf("error body: got %v, want {\"error\":\"insufficient funds\"}", body)
	}
}

func TestHTTP_404_DurableNoAccount(t *testing.T) {
	broker := &stubBroker{Result: funnel.TransferResult{Present: false}}
	durableStore := &stubStore{Err: store.ErrNoAccount}
	handler := NewHandler(broker, durableStore, defaultStubCfg())

	status, _ := postJSON(t, handler, map[string]any{
		"account_id": "acct-missing", "direction": "to_bank", "amount": 1,
	})

	if status != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", status)
	}
}

func TestHTTP_400_DurableBadTransfer_Defensive(t *testing.T) {
	broker := &stubBroker{Result: funnel.TransferResult{Present: false}}
	durableStore := &stubStore{Err: fmt.Errorf("wrapped: %w", store.ErrBadTransfer)}
	handler := NewHandler(broker, durableStore, defaultStubCfg())

	status, _ := postJSON(t, handler, map[string]any{
		"account_id": "acct", "direction": "to_bank", "amount": 1,
	})

	if status != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400 (defensive)", status)
	}
}

func TestHTTP_503_DurableLoadFailed(t *testing.T) {
	broker := &stubBroker{Result: funnel.TransferResult{Present: false}}
	durableStore := &stubStore{Err: fmt.Errorf("pool closed: %w", store.ErrLoadFailed)}
	handler := NewHandler(broker, durableStore, defaultStubCfg())

	status, _ := postJSON(t, handler, map[string]any{
		"account_id": "acct", "direction": "to_bank", "amount": 1,
	})

	if status != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d, want 503", status)
	}
}

// -------------------------------------------------------------------------
// No-409-ever (H14). Exhaustive-across-outcomes guard: scan the handler's
// behavior under every port outcome configuration we test above, assert
// 409 is never emitted and Present:false never leaks as its own status.
// -------------------------------------------------------------------------

func TestHTTP_No409_EverEmitted(t *testing.T) {
	outcomes := []struct {
		name         string
		brokerResult funnel.TransferResult
		brokerErr    error
		storeErr     error
	}{
		{"live_ok", funnel.TransferResult{Present: true, Available: 1, Bank: 1}, nil, nil},
		{"live_insufficient", funnel.TransferResult{Present: true, Err: account.ErrInsufficient{}}, nil, nil},
		{"live_unexpected", funnel.TransferResult{Present: true, Err: errors.New("boom")}, nil, nil},
		{"idle_ok", funnel.TransferResult{Present: false}, nil, nil},
		{"idle_insufficient", funnel.TransferResult{Present: false}, nil, store.ErrInsufficient},
		{"idle_no_account", funnel.TransferResult{Present: false}, nil, store.ErrNoAccount},
		{"idle_bad_transfer", funnel.TransferResult{Present: false}, nil, fmt.Errorf("%w", store.ErrBadTransfer)},
		{"idle_load_failed", funnel.TransferResult{Present: false}, nil, fmt.Errorf("%w", store.ErrLoadFailed)},
	}

	for _, outcome := range outcomes {
		t.Run(outcome.name, func(t *testing.T) {
			broker := &stubBroker{Result: outcome.brokerResult, Err: outcome.brokerErr}
			durableStore := &stubStore{Err: outcome.storeErr}
			handler := NewHandler(broker, durableStore, defaultStubCfg())

			status, _ := postJSON(t, handler, map[string]any{
				"account_id": "acct", "direction": "to_bank", "amount": 1,
			})

			if status == http.StatusConflict {
				t.Fatalf("%s: handler emitted 409 (P3-D8 violation)", outcome.name)
			}
		})
	}
}

// -------------------------------------------------------------------------
// Panic recovery. A broker that panics on Transfer must land as a 500,
// not kill the process or leave the connection hanging. The explicit
// recover is Phase-3's HTTP-boundary panic policy.
// -------------------------------------------------------------------------

func TestHTTP_PanicRecover_500(t *testing.T) {
	broker := &stubBroker{
		TransferFn: func(ctx context.Context, aid funnel.AccountID, from, to account.Bucket, amt int64) (funnel.TransferResult, error) {
			panic("simulated broker invariant violation")
		},
	}
	durableStore := &stubStore{}
	handler := NewHandler(broker, durableStore, defaultStubCfg())

	status, body := postJSON(t, handler, map[string]any{
		"account_id": "acct", "direction": "to_bank", "amount": 1,
	})

	if status != http.StatusInternalServerError {
		t.Fatalf("status: got %d, want 500 after panic", status)
	}
	if msg, _ := body["error"].(string); msg == "" {
		t.Errorf("panic 500 body: got %v, want non-empty error", body)
	}
}

// -------------------------------------------------------------------------
// Method guard — anything other than POST is 405.
// -------------------------------------------------------------------------

func TestHTTP_405_WrongMethod(t *testing.T) {
	broker := &stubBroker{}
	durableStore := &stubStore{}
	handler := NewHandler(broker, durableStore, defaultStubCfg())

	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		t.Run(method, func(t *testing.T) {
			request := httptest.NewRequest(method, Path, nil)
			responseRecorder := httptest.NewRecorder()
			handler.ServeHTTP(responseRecorder, request)
			if responseRecorder.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s: got %d, want 405", method, responseRecorder.Code)
			}
			if broker.Calls != 0 {
				t.Errorf("%s: broker called on wrong-method rejection", method)
			}
		})
	}
}

// -------------------------------------------------------------------------
// NewHandler construction guards.
// -------------------------------------------------------------------------

func TestNewHandler_PanicsOnNilBroker(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("NewHandler(nil, store, cfg) must panic")
		}
	}()
	_ = NewHandler(nil, &stubStore{}, defaultStubCfg())
}

func TestNewHandler_PanicsOnNilStore(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("NewHandler(broker, nil, cfg) must panic")
		}
	}()
	_ = NewHandler(&stubBroker{}, nil, defaultStubCfg())
}

func TestNewHandler_PanicsOnZeroTimeout(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("NewHandler with TransferTimeout=0 must panic")
		}
	}()
	_ = NewHandler(&stubBroker{}, &stubStore{}, Config{TransferTimeout: 0})
}
