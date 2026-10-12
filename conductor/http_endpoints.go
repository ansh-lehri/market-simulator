package conductor

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"

	"simulator/broker/funnel"
	"simulator/economy/lifecycle"
)

// PathSessionCanonical is the one route for admitting a user into a live
// canonical market. Owned by MountRoutes so bringup never has to
// duplicate the string.
const PathSessionCanonical = "/session/canonical"

// --- JSON wire shapes -------------------------------------------------

// joinRequestBody is the POST /session/canonical request envelope. The
// identity packet is nested so the frontend can grow it (new fields on
// user{}) without reshaping the top level. user_id is the identity key
// the conductor trusts verbatim; email is attribution only — never
// validated here (admission is anonymous per the domain constitution).
type joinRequestBody struct {
	User   joinRequestUser `json:"user"`
	Market string          `json:"market"`
}

type joinRequestUser struct {
	Email  string `json:"email"`
	UserID string `json:"user_id"`
}

// joinResponseBody is the 200 payload. Status is a fixed literal
// ("active") — the handler only reaches this branch when JoinCanonical
// returned nil, which means the broker has installed the session on its
// owning shard.
type joinResponseBody struct {
	SessionID     string `json:"session_id"`
	ParticipantID string `json:"participant_id"`
	Status        string `json:"status"`
}

// errorBody is the shape every non-2xx response uses. One short string;
// internal state (wrapped stack details, prefixes, etc.) is passed
// through verbatim so a developer looking at a failing request can see
// what the flow actually surfaced.
type errorBody struct {
	Error string `json:"error"`
}

// --- status mapper (Stage 1; left in place) --------------------------

// joinStatusFor maps a JoinCanonical return error to the HTTP status
// the handler writes. The categories:
//
//   - 200: success (nil error).
//   - 409: a conflict the client can act on — the market isn't live for
//     the requested tag, the user already holds a session, or the
//     broker's per-account XOR refused the install.
//   - 504: StartSession could not deliver to the owning shard before the
//     handler's context deadline fired.
//   - 503: StartSession reached the shard but the durable load or Redis
//     snapshot seed failed; retriable after infra recovery.
//   - 500: everything else — EnsureAccount failures (lifecycle
//     validation, store load / retry-exhausted miss, create failure),
//     funnel.ErrBadBinding / ErrBadParticipant (the conductor fills both
//     fields, so these indicate a conductor bug not a client error), and
//     any wrapped or unknown error.
//
// Dispatch uses errors.Is so a %w-wrapped error is matched the same as
// the bare sentinel.
//
// Note: lifecycle.ErrNoAccount (from StartSession's loader → 503) and
// store.ErrNoAccount (from EnsureAccount's retry-exhausted miss → 500)
// are distinct sentinel values in distinct packages — the mapper
// discriminates by identity, not by stage context.
func joinStatusFor(err error) int {
	if err == nil {
		return http.StatusOK
	}
	switch {
	case errors.Is(err, ErrMarketNotLive),
		errors.Is(err, ErrAlreadyInSession),
		errors.Is(err, funnel.ErrSessionAlreadyLive):
		return http.StatusConflict
	case errors.Is(err, funnel.ErrBrokerTimeout):
		return http.StatusGatewayTimeout
	case errors.Is(err, lifecycle.ErrNoAccount),
		errors.Is(err, lifecycle.ErrLoadFailed),
		errors.Is(err, lifecycle.ErrSnapshotWriteFailed):
		return http.StatusServiceUnavailable
	}
	return http.StatusInternalServerError
}

// --- shared HTTP helpers ---------------------------------------------

// writeJSON writes payload as JSON with the given status. Any Encode
// error is logged at debug (the connection is almost always gone by
// then) so a dying response never kills the server. WriteHeader fires
// first so the status code lands even if the body write dies mid-flight.
func writeJSON(responseWriter http.ResponseWriter, status int, payload any, path string) {
	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.WriteHeader(status)
	if encodeErr := json.NewEncoder(responseWriter).Encode(payload); encodeErr != nil {
		slog.Debug("conductor: response body write failed",
			"path", path,
			"status", status,
			"err", encodeErr,
		)
	}
}

// writeError writes an errorBody with the given status.
func writeError(responseWriter http.ResponseWriter, status int, message string, path string) {
	writeJSON(responseWriter, status, errorBody{Error: message}, path)
}

// recoverPanic is the HTTP-boundary panic guard every handler installs
// as its FIRST defer. The conductor's call-outs can panic on invariant
// violations (dev-fatal per the root constitution); without this guard
// a single late panic would crash the whole server. With it the client
// sees a clean 500 and the server keeps serving. path is the handler's
// own route so the log line is attributable without walking the stack.
//
// Uses slog.Default() rather than threading the Conductor's logger
// through — the recover path must not depend on receiver state in case
// the receiver itself is what panicked.
func recoverPanic(responseWriter http.ResponseWriter, path string) {
	recovered := recover()
	if recovered == nil {
		return
	}
	slog.Error("conductor: handler panic",
		"path", path,
		"cause", recovered,
		"stack", string(debug.Stack()),
	)
	writeError(responseWriter, http.StatusInternalServerError, "internal error", path)
}

// --- POST /session/canonical ----------------------------------------

// handleJoinCanonical is the canonical-join HTTP ingress. Decodes the
// identity packet, validates the two required fields, wraps the
// request context with the configured JoinTimeout, calls JoinCanonical,
// and maps the result through joinStatusFor to the HTTP response. The
// handler owns the deadline (not the flow) so a slow EnsureAccount or
// StartSession surfaces as 504 even when the flow itself has no
// explicit timeout dial.
func (conductor *Conductor) handleJoinCanonical(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathSessionCanonical)

	if request.Method != http.MethodPost {
		writeError(responseWriter, http.StatusMethodNotAllowed, "method not allowed", PathSessionCanonical)
		return
	}

	var requestBody joinRequestBody
	if decodeErr := json.NewDecoder(request.Body).Decode(&requestBody); decodeErr != nil {
		conductor.log.Info("conductor: handleJoinCanonical bad body",
			"err", decodeErr,
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusBadRequest, "invalid JSON body", PathSessionCanonical)
		return
	}
	if requestBody.User.UserID == "" || requestBody.Market == "" {
		conductor.log.Info("conductor: handleJoinCanonical missing required fields",
			"has_user_id", requestBody.User.UserID != "",
			"has_market", requestBody.Market != "",
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusBadRequest, "user_id and market required", PathSessionCanonical)
		return
	}

	deadlineCtx, cancelDeadline := context.WithTimeout(request.Context(), conductor.joinTimeout)
	defer cancelDeadline()

	result, joinErr := conductor.JoinCanonical(
		deadlineCtx,
		UserID(requestBody.User.UserID),
		MarketTag(requestBody.Market),
	)
	status := joinStatusFor(joinErr)
	if joinErr != nil {
		writeError(responseWriter, status, joinErr.Error(), PathSessionCanonical)
		return
	}

	writeJSON(responseWriter, status, joinResponseBody{
		SessionID:     string(result.SessionID),
		ParticipantID: string(result.ParticipantID),
		Status:        "active",
	}, PathSessionCanonical)
}
