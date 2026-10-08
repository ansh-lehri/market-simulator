package reads

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"
)

// Shared HTTP helpers for the eight Phase-6 read endpoints (BROKER_PHASE6
// §1.2). The handlers themselves (balance.go, positions.go, orders.go,
// observation.go, trades.go, costs.go) all compose from these primitives:
//
//   defer recoverPanic(w, path)
//   if !requireGET(w, r, path) { return }
//   accountID, ok := parseAccountID(r); if !ok { ... writeError 400 ... }
//   limit, ok := parseLimit(r, defaultLimit)
//   before, ok := parseBefore(r)
//   writeJSON(w, 200, body, path) / writeError(w, status, msg, path)
//
// Mirrors broker/order and broker/transfer's shared-helper shape — same
// idiom: tiny free functions, path threaded through for attributable log
// lines, no shared mutex / shared state.

// errorBody is the plain error envelope every non-success response uses.
// Mirrors broker/order's errorBody: one "error" field, no leaking of
// internal state. (The reads family has no "reason" bucket — all
// business shaping is per-endpoint response bodies, not short tags.)
type errorBody struct {
	Error string `json:"error"`
}

// writeJSON writes payload as JSON with the given status. A body-write
// error at this stage is a dead or walked-away client; log at debug (not
// error) because there is nothing actionable to do.
//
// WriteHeader fires BEFORE Encode so the status code lands even if the
// body write dies mid-flight.
//
// path is the handler's own route so the log line is attributable
// without walking the stack.
func writeJSON(responseWriter http.ResponseWriter, status int, payload any, path string) {
	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.WriteHeader(status)
	if err := json.NewEncoder(responseWriter).Encode(payload); err != nil {
		slog.Debug("broker/reads: body write failed",
			"path", path,
			"status", status,
			"err", err,
		)
	}
}

// writeError writes an errorBody with the given status. Used for every
// non-success response: 400 malformed query / 404 (none in Phase 6; the
// durable branch never 404s) / 409 no live session / 500 internal / 501
// not implemented (A16 US) / 503 store unavailable / 504 broker timeout.
func writeError(responseWriter http.ResponseWriter, status int, message string, path string) {
	writeJSON(responseWriter, status, errorBody{Error: message}, path)
}

// recoverPanic is the HTTP-boundary panic guard every read handler
// installs as its FIRST defer. Rationale same as broker/order's: an
// invariant-violation panic on the HTTP goroutine (vs the shard
// goroutine, which crashes the shard) surfaces as a clean 500 instead
// of a silently-torn connection.
//
// A panic on the SHARD goroutine (reserve-hand invariant, etc.) crashes
// the shard, not the request — net/http's own per-request recover would
// NOT catch that. This recover only catches request-local panics (the
// Broker facade's buffered-1 reply select, the Store SQL path).
//
// path is the handler's own route for attributable logging.
func recoverPanic(responseWriter http.ResponseWriter, path string) {
	recovered := recover()
	if recovered == nil {
		return
	}
	slog.Error("broker/reads: handler panic",
		"path", path,
		"cause", recovered,
		"stack", string(debug.Stack()),
	)
	writeError(responseWriter, http.StatusInternalServerError, "internal error", path)
}

// requireGET is the method guard every read handler calls first (after
// recoverPanic defers). Writes 405 + Allow:GET on a non-GET and returns
// false; returns true on a GET so the handler proceeds. All eight
// Phase-6 endpoints are GET (BROKER_PHASE6 §0.5).
func requireGET(responseWriter http.ResponseWriter, request *http.Request, path string) bool {
	if request.Method == http.MethodGet {
		return true
	}
	responseWriter.Header().Set("Allow", http.MethodGet)
	slog.Debug("broker/reads: 405 method not allowed",
		"path", path,
		"method", request.Method,
	)
	writeError(responseWriter, http.StatusMethodNotAllowed, "method not allowed", path)
	return false
}

// parseAccountID reads the required ?account_id= query param. account_id
// is the trusted param in the no-auth demo surface (BROKER_PHASE6 §0.5);
// an empty value is a client-side error — the caller writes 400.
//
// Returns (value, true) on present+non-empty, ("", false) when absent or
// empty.
func parseAccountID(request *http.Request) (string, bool) {
	accountID := request.URL.Query().Get("account_id")
	return accountID, accountID != ""
}

// parseBefore reads the optional ?before= RFC3339 keyset-pagination
// cursor used by A10 (trades) and A16 (costs). Three cases:
//
//   - absent / empty     → (nil, true)       — newest-page request
//   - valid RFC3339      → (&parsedTime, true)
//   - unparseable value  → (nil, false)      — caller writes 400
//
// The Store's TradePage / CostsPerTradeIN methods take `before *time.Time`
// where nil means "newest page" — we hand them exactly that type.
func parseBefore(request *http.Request) (*time.Time, bool) {
	raw := request.URL.Query().Get("before")
	if raw == "" {
		return nil, true
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil, false
	}
	return &parsed, true
}

// parseLimit reads the optional ?limit= paging dial used by A10 and A16.
// Three cases:
//
//   - absent / empty    → (defaultLimit, true)
//   - valid positive    → (parsed, true)
//   - unparseable OR    → (0, false) — caller writes 400
//     non-positive
//
// Non-positive values are rejected because the Store treats n<=0 as a
// shortcut ([] with no query); a client that passed limit=0 almost
// certainly meant "default" (handled by the absent branch), not "empty
// result" — so we surface the ambiguity as 400 instead of silently
// returning []. Upper bound is NOT enforced here: the Store caps
// internally at maxRecent, so a request for limit=10000 degrades to the
// Store's cap without an error at the HTTP boundary.
func parseLimit(request *http.Request, defaultLimit int) (int, bool) {
	raw := request.URL.Query().Get("limit")
	if raw == "" {
		return defaultLimit, true
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		return 0, false
	}
	return parsed, true
}
