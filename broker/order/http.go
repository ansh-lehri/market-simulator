package order

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime/debug"
)

// errorBody is the plain error envelope every non-success response uses.
// Mirrors broker/transfer's errorBody: one "error" field, no leaking of
// internal state. For business rejections the per-endpoint response
// shape carries a "reason" field instead (place/cancel) — see each
// endpoint for details.
type errorBody struct {
	Error string `json:"error"`
}

// reasonBody is the shape every business-reject response uses: a short
// snake_case reason tag that the caller can switch on. Used for 409 (no
// session, cancel failures) and 422 (place business rejects) — the two
// buckets P5-D27 defines, keyed by exact reason (not per-reason code).
type reasonBody struct {
	Reason string `json:"reason"`
}

// writeJSON writes `payload` as JSON with the given status, swallowing
// any encode error at debug level (the connection is almost always
// gone by the time Encode fails). WriteHeader fires first so the
// status code lands even if the body write dies mid-flight.
func writeJSON(responseWriter http.ResponseWriter, status int, payload any, path string) {
	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.WriteHeader(status)
	if err := json.NewEncoder(responseWriter).Encode(payload); err != nil {
		slog.Debug("broker/order: body write failed",
			"path", path,
			"status", status,
			"err", err,
		)
	}
}

// writeError writes an errorBody with the given status. Used for the
// non-reason non-success responses: 400 bad body, 504 timeout, 500
// internal.
func writeError(responseWriter http.ResponseWriter, status int, message string, path string) {
	writeJSON(responseWriter, status, errorBody{Error: message}, path)
}

// writeReason writes a reasonBody with the given status. Used for the
// two business-rejection buckets P5-D27 defines: 409 no_session / cancel
// failures and 422 place business rejects.
func writeReason(responseWriter http.ResponseWriter, status int, reason string, path string) {
	writeJSON(responseWriter, status, reasonBody{Reason: reason}, path)
}

// recoverPanic is the HTTP-boundary panic guard every ServeHTTP method
// installs as its FIRST defer. Reserve / realize / lifecycle hands can
// panic on invariant violations (dev-fatal per root CLAUDE.md), and
// though those typically panic on the SHARD goroutine (crashing the
// shard, not the HTTP request), the entry-point Broker.PlaceOrder /
// Broker.CancelOrder also run on the HTTP goroutine itself — a race
// between request serving and broker shutdown (send to a closed inbox)
// surfaces here. Without this guard a single late request would crash
// the whole server; with it, the request gets a clean 500 and the
// server keeps serving.
//
// This is standard net/http hygiene — the stdlib has its own per-request
// recover that writes nothing to the client; this explicit recover gives
// the caller a 500 instead of a silently-torn connection.
//
// path is the handler's own route so the log line is attributable
// without walking the stack.
func recoverPanic(responseWriter http.ResponseWriter, path string) {
	recovered := recover()
	if recovered == nil {
		return
	}
	slog.Error("broker/order: handler panic",
		"path", path,
		"cause", recovered,
		"stack", string(debug.Stack()),
	)
	writeError(responseWriter, http.StatusInternalServerError, "internal error", path)
}
