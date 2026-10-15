package conductor

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"

	"simulator/broker/funnel"
	"simulator/broker/marketdata"
	"simulator/economy/lifecycle"
)

// PathSessionCanonical is the one route for admitting a user into a live
// canonical market. Owned by MountRoutes so bringup never has to
// duplicate the string.
const PathSessionCanonical = "/session/canonical"

// PathMarkets is the catalog + liveness + canonical session-window read
// the pre-join screen hits. Reads config + a brief registry scan;
// returns identity and schedule only (no prices — those are the
// market-data WebSocket stream's domain).
const PathMarkets = "/v1/markets"

// PathInstruments is the per-market static instrument list. Config-only
// read; works whether the market is live or closed; never serves
// price/quote/depth.
const PathInstruments = "/v1/instruments"

// PathAccountOpen is the signup/login handshake. One sole call into
// broker.EnsureAccount; idempotent (200 for both signup and login);
// money-blind (returns only the account handle, never balances).
const PathAccountOpen = "/v1/account/open"

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
		errors.Is(err, ErrLiveElsewhere),
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

// --- GET /v1/ws (dispatcher) ----------------------------------------

// authFromRequest extracts the carrier identifying which session the
// WebSocket connection belongs to. Today only the ?session= query param
// is recognized; a longer-lived reconnect token (surviving a browser
// refresh) is parked for a later phase and lands here as an additional
// source.
//
// Returns the empty string when no carrier is present — the dispatcher
// writes 401 before any Upgrade (gorilla's Upgrade hijacks the
// connection, after which the conductor can no longer write a status).
func authFromRequest(request *http.Request) SessionID {
	return SessionID(request.URL.Query().Get("session"))
}

// fanoutForSession is the dispatcher's registry resolve: sessionID →
// Session.Market → Market.fanout. The one registry mutex is taken for
// the microsecond map lookups and released BEFORE the fanout is
// returned — the caller must not hold the mutex across the shipped WS
// handler's ServeHTTP (which blocks on its write-loop-done signal for
// the entire connection life; holding the lock across it would freeze
// every other join and dispatcher resolve for as long as any socket is
// open).
//
// Returns nil when the session doesn't exist, isn't Active (a Starting
// reservation hasn't finalized yet), is pointing at a market that no
// longer exists, or is pointing at a market that isn't Live (teardown
// in flight). The dispatcher writes 404 in each of these cases.
//
// Deliberately no defer unlock: the lock→copy→unlock→return sequence
// is written out so a reader can see directly that unlock precedes
// return — the load-bearing concurrency property of the whole file.
func (conductor *Conductor) fanoutForSession(sid SessionID) *marketdata.FanOut {
	conductor.reg.mu.Lock()
	session := conductor.reg.sessions[sid]
	if session == nil || session.State != Active {
		conductor.reg.mu.Unlock()
		return nil
	}
	market := conductor.reg.markets[session.Market]
	if market == nil || market.State != Live {
		conductor.reg.mu.Unlock()
		return nil
	}
	fanout := market.fanout
	conductor.reg.mu.Unlock()
	return fanout
}

// handleWS is the WebSocket dispatcher. Validates the carrier, resolves
// the session's fanout, and hands the connection off to the shipped WS
// handler. Both reject branches (401 empty session, 404 unresolvable
// session) MUST write their status BEFORE the delegate — once gorilla's
// Upgrade hijacks the connection, the conductor can no longer write an
// HTTP status.
//
// Delegation blocks for the whole connection life (readLoop runs on
// this goroutine; writeLoop runs on a spawned goroutine; ServeHTTP
// returns only after both exit). The registry mutex is released inside
// fanoutForSession, so this blocking does not freeze the registry.
func (conductor *Conductor) handleWS(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, marketdata.PathWS)

	if request.Method != http.MethodGet {
		writeError(responseWriter, http.StatusMethodNotAllowed, "method not allowed", marketdata.PathWS)
		return
	}

	sessionID := authFromRequest(request)
	if sessionID == "" {
		conductor.log.Info("conductor: handleWS rejected — no session carrier",
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusUnauthorized, "no session", marketdata.PathWS)
		return
	}

	fanout := conductor.fanoutForSession(sessionID)
	if fanout == nil {
		conductor.log.Info("conductor: handleWS rejected — no live session",
			"session_id", sessionID,
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusNotFound, "no live session", marketdata.PathWS)
		return
	}

	conductor.log.Info("conductor: handleWS delegating to shipped WS handler",
		"session_id", sessionID,
	)
	conductor.wsHandlerFactory(fanout, conductor.wsCfg).ServeHTTP(responseWriter, request)
}

