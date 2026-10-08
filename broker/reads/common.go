package reads

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"
)

// Shared HTTP helpers for every read endpoint in this package. Each
// handler's ServeHTTP composes from these primitives:
//
//   defer recoverPanic(w, path)
//   if !requireGET(w, r, path) { return }
//   accountID, ok := parseAccountID(r); if !ok { writeError 400 ... }
//   limit,     ok := parseLimit(r, defaultLimit)
//   before,    ok := parseBefore(r)
//   writeJSON(w, 200, body, path) / writeError(w, status, msg, path)
//
// Same pattern as broker/order and broker/transfer: tiny free
// functions, path threaded through for attributable log lines, no
// shared mutex, no shared state.

// errorBody is the plain error envelope every non-success response
// uses. One "error" field, no leaking of internal state.
type errorBody struct {
	Error string `json:"error"`
}

// writeJSON writes payload as JSON with the given status. A body-write
// error at this stage is a client that walked away mid-response; logged
// at debug (not error) because there's nothing actionable to do.
//
// WriteHeader fires before Encode so the status code lands even if the
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

// writeError writes an errorBody with the given status.
func writeError(responseWriter http.ResponseWriter, status int, message string, path string) {
	writeJSON(responseWriter, status, errorBody{Error: message}, path)
}

// recoverPanic is the HTTP-boundary panic guard every handler installs
// as its first defer. A panic here (on the request goroutine) becomes
// a clean 500 instead of a silently-torn connection.
//
// Note the scope: a panic on the shard goroutine (an invariant
// violation inside the reserve or realize hands) crashes the shard,
// not the request — net/http's own per-request recover would not
// catch that. This recover only catches panics on the request
// goroutine itself (the broker-facade select loop, the Store SQL path).
//
// path is the handler's own route so the log line is attributable.
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

// requireGET is the method guard. On a non-GET it writes 405 with
// Allow:GET and returns false; on a GET it returns true so the handler
// proceeds. Every read endpoint in this package is a GET.
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

// parseAccountID reads the required ?account_id= query param. The
// no-auth demo surface trusts account_id as the identity param; an
// empty value is a client error — the caller writes 400.
//
// Returns (value, true) when present and non-empty, ("", false) when
// absent or empty.
func parseAccountID(request *http.Request) (string, bool) {
	accountID := request.URL.Query().Get("account_id")
	return accountID, accountID != ""
}

// parseBefore reads the optional ?before= RFC3339 keyset-pagination
// cursor used by the trades and costs endpoints. Three cases:
//
//   - absent / empty    → (nil, true)       — newest-page request
//   - valid RFC3339     → (&parsedTime, true)
//   - unparseable value → (nil, false)      — caller writes 400
//
// The Store's paginated reads take `before *time.Time` where nil
// means "newest page", so we hand them exactly that type.
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

// parseLimit reads the optional ?limit= paging dial. Three cases:
//
//   - absent / empty    → (defaultLimit, true)
//   - valid positive    → (parsed, true)
//   - unparseable OR    → (0, false) — caller writes 400
//     non-positive
//
// Non-positive is rejected because the Store treats n<=0 as a shortcut
// that returns [] with no query — a client that passed limit=0 almost
// certainly meant "default" (handled by the absent branch), not "empty
// result". Surfacing it as 400 keeps the two intents distinguishable.
//
// No upper-bound check here: the Store caps internally, so a request
// for limit=10000 silently degrades to the Store's cap rather than
// erroring at the HTTP boundary.
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
