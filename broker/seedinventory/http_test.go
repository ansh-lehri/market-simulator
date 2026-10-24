package seedinventory

// Stub-based HTTP tests (BROKER_SEED_INVENTORY §5), mirroring
// broker/transfer's test shape minus the durable-fallback half (this
// endpoint has none — SI-D1). Fast, no DB: drive brokerPort with a
// hand-rolled stub to cover 400 pre-funnel paths, 409 no-live-session,
// 500 unexpected-reject, 504 timeout, panic recovery, and the 200 body
// shape.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"simulator/broker/funnel"
)

type stubBroker struct {
	SeedFn func(ctx context.Context, aid funnel.AccountID, instr funnel.InstrumentID, qty, price int64) (funnel.SeedInventoryResult, error)
	Result funnel.SeedInventoryResult
	Err    error
	Calls  int
}

func (stub *stubBroker) SeedInventory(
	ctx context.Context,
	accountID funnel.AccountID,
	instrument funnel.InstrumentID,
	qty int64,
	entryPrice int64,
) (funnel.SeedInventoryResult, error) {
	stub.Calls++
	if stub.SeedFn != nil {
		return stub.SeedFn(ctx, accountID, instrument, qty, entryPrice)
	}
	return stub.Result, stub.Err
}

func defaultStubCfg() Config {
	return Config{SeedTimeout: 2 * time.Second}
}

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
// Pre-funnel 400 paths — stub never called.
// -------------------------------------------------------------------------

func TestHTTP_400s_PreFunnel(t *testing.T) {
	cases := []struct {
		name string
		body map[string]any
	}{
		{"missing_account_id", map[string]any{"instrument": "ACME", "qty": 100, "entry_price": 250}},
		{"empty_account_id", map[string]any{"account_id": "", "instrument": "ACME", "qty": 100, "entry_price": 250}},
		{"missing_instrument", map[string]any{"account_id": "acct", "qty": 100, "entry_price": 250}},
		{"empty_instrument", map[string]any{"account_id": "acct", "instrument": "", "qty": 100, "entry_price": 250}},
		{"qty_zero", map[string]any{"account_id": "acct", "instrument": "ACME", "qty": 0, "entry_price": 250}},
		{"qty_negative", map[string]any{"account_id": "acct", "instrument": "ACME", "qty": -1, "entry_price": 250}},
		{"entry_price_zero", map[string]any{"account_id": "acct", "instrument": "ACME", "qty": 100, "entry_price": 0}},
		{"entry_price_negative", map[string]any{"account_id": "acct", "instrument": "ACME", "qty": 100, "entry_price": -5}},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			broker := &stubBroker{}
			handler := NewHandler(broker, defaultStubCfg())

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
		})
	}
}

func TestHTTP_400_InvalidJSON(t *testing.T) {
	broker := &stubBroker{}
	handler := NewHandler(broker, defaultStubCfg())

	request := httptest.NewRequest(http.MethodPost, Path, strings.NewReader("not-json"))
	responseRecorder := httptest.NewRecorder()
	handler.ServeHTTP(responseRecorder, request)

	if responseRecorder.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", responseRecorder.Code)
	}
	if broker.Calls != 0 {
		t.Errorf("broker called on bad-json 400: got %d, want 0", broker.Calls)
	}
}

// -------------------------------------------------------------------------
// 504 broker timeout.
// -------------------------------------------------------------------------

func TestHTTP_504_BrokerTimeout(t *testing.T) {
	broker := &stubBroker{
		SeedFn: func(ctx context.Context, aid funnel.AccountID, instr funnel.InstrumentID, qty, price int64) (funnel.SeedInventoryResult, error) {
			<-ctx.Done()
			return funnel.SeedInventoryResult{}, funnel.ErrBrokerTimeout
		},
	}
	handler := NewHandler(broker, Config{SeedTimeout: 50 * time.Millisecond})

	status, body := postJSON(t, handler, map[string]any{
		"account_id": "acct-wedged", "instrument": "ACME", "qty": 100, "entry_price": 250,
	})

	if status != http.StatusGatewayTimeout {
		t.Fatalf("status: got %d, want 504 (body=%v)", status, body)
	}
}

// -------------------------------------------------------------------------
// 409 no live session (SI-D1 — hard error, no durable fallback exists).
// -------------------------------------------------------------------------

func TestHTTP_409_NoLiveSession(t *testing.T) {
	broker := &stubBroker{Result: funnel.SeedInventoryResult{Present: false}}
	handler := NewHandler(broker, defaultStubCfg())

	status, body := postJSON(t, handler, map[string]any{
		"account_id": "acct-idle", "instrument": "ACME", "qty": 100, "entry_price": 250,
	})

	if status != http.StatusConflict {
		t.Fatalf("status: got %d, want 409 (body=%v)", status, body)
	}
}

// -------------------------------------------------------------------------
// 200 success.
// -------------------------------------------------------------------------

func TestHTTP_200_Success(t *testing.T) {
	broker := &stubBroker{
		Result: funnel.SeedInventoryResult{Present: true, Available: 140},
	}
	handler := NewHandler(broker, defaultStubCfg())

	status, body := postJSON(t, handler, map[string]any{
		"account_id": "acct-live", "instrument": "ACME", "qty": 100, "entry_price": 250,
	})

	if status != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (body=%v)", status, body)
	}
	if gotInstrument, _ := body["instrument"].(string); gotInstrument != "ACME" {
		t.Errorf("instrument: got %v, want ACME", body["instrument"])
	}
	if gotAvailable, ok := body["available"].(float64); !ok || int64(gotAvailable) != 140 {
		t.Errorf("available: got %v, want 140", body["available"])
	}
}

// -------------------------------------------------------------------------
// 500 on an unexpected live reject (SI-D2 — reaching this means a caller
// bypassed the endpoint's own pre-funnel validation).
// -------------------------------------------------------------------------

func TestHTTP_500_UnexpectedLiveReject(t *testing.T) {
	broker := &stubBroker{
		Result: funnel.SeedInventoryResult{Present: true, Err: errors.New("some unexpected L5 fault")},
	}
	handler := NewHandler(broker, defaultStubCfg())

	status, _ := postJSON(t, handler, map[string]any{
		"account_id": "acct", "instrument": "ACME", "qty": 100, "entry_price": 250,
	})

	if status != http.StatusInternalServerError {
		t.Fatalf("status: got %d, want 500", status)
	}
}

func TestHTTP_500_UnexpectedBrokerTransportError(t *testing.T) {
	broker := &stubBroker{Err: errors.New("transport fault, not a timeout")}
	handler := NewHandler(broker, defaultStubCfg())

	status, _ := postJSON(t, handler, map[string]any{
		"account_id": "acct", "instrument": "ACME", "qty": 100, "entry_price": 250,
	})

	if status != http.StatusInternalServerError {
		t.Fatalf("status: got %d, want 500", status)
	}
}

// -------------------------------------------------------------------------
// Panic recovery.
// -------------------------------------------------------------------------

func TestHTTP_PanicRecover_500(t *testing.T) {
	broker := &stubBroker{
		SeedFn: func(ctx context.Context, aid funnel.AccountID, instr funnel.InstrumentID, qty, price int64) (funnel.SeedInventoryResult, error) {
			panic("simulated broker invariant violation")
		},
	}
	handler := NewHandler(broker, defaultStubCfg())

	status, body := postJSON(t, handler, map[string]any{
		"account_id": "acct", "instrument": "ACME", "qty": 100, "entry_price": 250,
	})

	if status != http.StatusInternalServerError {
		t.Fatalf("status: got %d, want 500 after panic", status)
	}
	if msg, _ := body["error"].(string); msg == "" {
		t.Errorf("panic 500 body: got %v, want non-empty error", body)
	}
}

// -------------------------------------------------------------------------
// Method guard.
// -------------------------------------------------------------------------

func TestHTTP_405_WrongMethod(t *testing.T) {
	broker := &stubBroker{}
	handler := NewHandler(broker, defaultStubCfg())

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
			t.Fatalf("NewHandler(nil, cfg) must panic")
		}
	}()
	_ = NewHandler(nil, defaultStubCfg())
}

func TestNewHandler_PanicsOnZeroTimeout(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("NewHandler with SeedTimeout=0 must panic")
		}
	}()
	_ = NewHandler(&stubBroker{}, Config{SeedTimeout: 0})
}
