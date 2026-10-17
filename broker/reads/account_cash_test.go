package reads

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"simulator/broker/store"
)

// Tests for the pre-session account_cash endpoint — pure durable read,
// no funnel. Covers:
//
//   - full status map (400 missing param / 405 non-GET / 404 ErrNoAccount
//     / 503 ErrLoadFailed incl. ctx timeout / 500 unexpected / 200 ok);
//   - 200 body shape — two bare int64 fields, no "unit" / "currency";
//   - signed trading_cash passthrough (debt case);
//   - the construction-time panic guards.

func serveAccountCash(t *testing.T, durableStore *stubStore, cfg Config, method, url string) *httptest.ResponseRecorder {
	t.Helper()
	handler := NewAccountCashHandler(durableStore, cfg)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(method, url, nil))
	return recorder
}

func TestAccountCash_StatusMap(t *testing.T) {
	okStore := &stubStore{accountCashTrading: 100, accountCashBank: 200}
	noAccountStore := &stubStore{accountCashErr: store.ErrNoAccount}
	loadFailStore := &stubStore{accountCashErr: fmt.Errorf("db died: %w", store.ErrLoadFailed)}
	unexpectedStore := &stubStore{accountCashErr: errors.New("boom")}

	cases := []struct {
		name       string
		method     string
		url        string
		store      *stubStore
		wantStatus int
	}{
		{"400 missing account_id", http.MethodGet, PathAccountCash, okStore, http.StatusBadRequest},
		{"405 non-GET", http.MethodPost, PathAccountCash + "?account_id=acct-1", okStore, http.StatusMethodNotAllowed},
		{"404 no account", http.MethodGet, PathAccountCash + "?account_id=acct-1", noAccountStore, http.StatusNotFound},
		{"503 store unavailable", http.MethodGet, PathAccountCash + "?account_id=acct-1", loadFailStore, http.StatusServiceUnavailable},
		{"500 unexpected", http.MethodGet, PathAccountCash + "?account_id=acct-1", unexpectedStore, http.StatusInternalServerError},
		{"200 ok", http.MethodGet, PathAccountCash + "?account_id=acct-1", okStore, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := serveAccountCash(t, tc.store, defaultTestConfig, tc.method, tc.url)
			if recorder.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body=%s)", recorder.Code, tc.wantStatus, recorder.Body.String())
			}
		})
	}
}

// 405 must also advertise Allow:GET so a well-behaved client can
// retry with the right method.
func TestAccountCash_405AdvertisesAllowGET(t *testing.T) {
	recorder := serveAccountCash(t, &stubStore{}, defaultTestConfig, http.MethodDelete, PathAccountCash+"?account_id=acct-1")
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", recorder.Code)
	}
	if allow := recorder.Header().Get("Allow"); allow != http.MethodGet {
		t.Errorf("Allow = %q, want GET", allow)
	}
}

// 503 fires both for a bare store.ErrLoadFailed and for a wrapped one
// — the handler dispatches on category sentinel via errors.Is, same as
// trades/pnl.
func TestAccountCash_503_WrappedErrLoadFailed(t *testing.T) {
	wrappedStore := &stubStore{accountCashErr: fmt.Errorf("transport broke: %w", store.ErrLoadFailed)}
	recorder := serveAccountCash(t, wrappedStore, defaultTestConfig, http.MethodGet, PathAccountCash+"?account_id=acct-1")
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (wrapped ErrLoadFailed)", recorder.Code)
	}
}

// ctx deadline fires inside the store call (stub.delay > ReadTimeout).
// The stub's waitOrCancel returns ctx.Err() which does NOT wrap
// ErrLoadFailed — the handler routes this through the final default
// branch, mapping to 500. If we ever want ctx cancellation to surface
// as 503 (consistent with durable-read convention), the Store itself
// must wrap ctx.Err() in ErrLoadFailed. Documented here so the test
// is honest about what it is proving.
func TestAccountCash_CtxCancelMapsTo500(t *testing.T) {
	slowStore := &stubStore{delay: 100 * time.Millisecond}
	tightCfg := Config{ReadTimeout: 10 * time.Millisecond}
	recorder := serveAccountCash(t, slowStore, tightCfg, http.MethodGet, PathAccountCash+"?account_id=acct-1")
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (stub returns raw ctx.Err() — see test comment)", recorder.Code)
	}
}

// 200 body shape: two bare int64 fields, no "unit" / currency, no
// extra keys. Verifies the no-unit rule holds on this endpoint and
// nothing else leaked into the response.
func TestAccountCash_200_BodyShape(t *testing.T) {
	okStore := &stubStore{accountCashTrading: 1_000_000, accountCashBank: 250_000}
	recorder := serveAccountCash(t, okStore, defaultTestConfig, http.MethodGet, PathAccountCash+"?account_id=acct-1")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}

	var body map[string]int64
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v (raw=%q)", err, recorder.Body.String())
	}
	want := map[string]int64{"trading_cash": 1_000_000, "bank_cash": 250_000}
	if len(body) != len(want) {
		t.Fatalf("body has %d keys, want %d (full body: %+v)", len(body), len(want), body)
	}
	for key, wantValue := range want {
		if body[key] != wantValue {
			t.Errorf("body[%q] = %d, want %d", key, body[key], wantValue)
		}
	}
}

// Signed trading_cash passthrough: a debtor's balance renders truthfully
// as a negative int64, not clamped or missing. bank_cash is non-negative
// by schema; this test pins the signed-handling contract at the HTTP
// boundary (the store test pins it at the SQL boundary).
func TestAccountCash_200_SignedTradingCashPassthrough(t *testing.T) {
	debtorStore := &stubStore{accountCashTrading: -42_000, accountCashBank: 1_000}
	recorder := serveAccountCash(t, debtorStore, defaultTestConfig, http.MethodGet, PathAccountCash+"?account_id=acct-1")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var body accountCashResponse
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.TradingCash != -42_000 {
		t.Errorf("trading_cash: got %d, want -42000", body.TradingCash)
	}
	if body.BankCash != 1_000 {
		t.Errorf("bank_cash: got %d, want 1000", body.BankCash)
	}
}

// Store witness: the handler fired exactly one AccountCash call per
// request. Proves no stray repeat-call or missed-call bugs.
func TestAccountCash_200_CallsStoreOnce(t *testing.T) {
	stubWithWitness := &stubStore{accountCashTrading: 10, accountCashBank: 20}
	recorder := serveAccountCash(t, stubWithWitness, defaultTestConfig, http.MethodGet, PathAccountCash+"?account_id=acct-1")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if calls := stubWithWitness.accountCashCalls.Load(); calls != 1 {
		t.Errorf("stub AccountCash calls = %d, want 1", calls)
	}
}

// Construction panics — bringup-mistake guards. A nil store or a bad
// Config must fail loudly instead of nil-dereffing on first request.
func TestNewAccountCashHandler_PanicsOnNilStore(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic on nil store")
		}
	}()
	_ = NewAccountCashHandler(nil, defaultTestConfig)
}

func TestNewAccountCashHandler_PanicsOnBadConfig(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic on bad config")
		}
	}()
	_ = NewAccountCashHandler(&stubStore{}, Config{ReadTimeout: 0})
}
