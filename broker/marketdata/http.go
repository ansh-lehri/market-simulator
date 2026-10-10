package marketdata

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime/debug"
)

// Shared HTTP primitives for every marketdata endpoint — the candle
// history handler today, the WebSocket upgrade handler in the next
// stage. Same pattern as broker/reads/common.go: tiny free functions,
// path threaded through for attributable log lines, no shared state.

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
// body write dies mid-flight. path is the handler's own route so the
// log line is attributable without walking the stack.
func writeJSON(responseWriter http.ResponseWriter, status int, payload any, path string) {
	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.WriteHeader(status)
	if err := json.NewEncoder(responseWriter).Encode(payload); err != nil {
		slog.Debug("broker/marketdata: body write failed",
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
// as its first defer. A panic on the request goroutine becomes a clean
// 500 instead of a silently-torn connection (net/http's own per-request
// recover catches the panic but writes nothing to the client).
func recoverPanic(responseWriter http.ResponseWriter, path string) {
	recovered := recover()
	if recovered == nil {
		return
	}
	slog.Error("broker/marketdata: handler panic",
		"path", path,
		"cause", recovered,
		"stack", string(debug.Stack()),
	)
	writeError(responseWriter, http.StatusInternalServerError, "internal error", path)
}

// requireGET is the method guard. On a non-GET it writes 405 with
// Allow:GET and returns false; on a GET it returns true so the handler
// proceeds.
func requireGET(responseWriter http.ResponseWriter, request *http.Request, path string) bool {
	if request.Method == http.MethodGet {
		return true
	}
	responseWriter.Header().Set("Allow", http.MethodGet)
	slog.Debug("broker/marketdata: 405 method not allowed",
		"path", path,
		"method", request.Method,
	)
	writeError(responseWriter, http.StatusMethodNotAllowed, "method not allowed", path)
	return false
}
