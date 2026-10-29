package conductor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strconv"
	"time"

	"simulator/broker/funnel"
)

// PathSessionOnDemand is the on-demand trigger endpoint. Held through
// the deterministic setup (gate + populate + construct + wire + Start
// + seal) and returns once at warm-up-start with absolute
// warmup_ends_at + session_ends_at the UI counts down against.
const PathSessionOnDemand = "/session/ondemand"

// PathSessionStatus is the fallback/recovery endpoint — a pure
// registry read that returns the current state of one session. The
// frontend's happy path never hits this; the trigger response's
// warmup_ends_at/session_ends_at are what drives the UI. This
// endpoint exists for cases where the held POST was lost mid-flight
// (page refresh, flaky network) and the frontend needs to re-verify
// "do I actually have a live session, and if so, when does it end?"
//
// The carrier mirrors the WebSocket: ?session=<sid>. Deliberately
// chosen over a path parameter (/session/{id}/status) because every
// other shipped route in this package is a flat constant string and
// the frontend already knows the ?session= carrier shape from the
// WebSocket connect.
const PathSessionStatus = "/session/status"

// Session status label literals the handler writes verbatim. One
// label per observable Session state. Centralized so a future doc
// says "ready" and the response says the same word.
const (
	sessionStatusLabelWarmingUp = "warming_up"
	sessionStatusLabelReady     = "ready"
	sessionStatusLabelFailed    = "failed"
	sessionStatusLabelEnded     = "ended"
)

// On-demand duration bounds (integer minutes). The HTTP layer enforces
// these before the flow runs — a value outside the range is a client
// bug mapped to 400 (not a setup failure).
const (
	onDemandDurationMinMinutes     = 5
	onDemandDurationDefaultMinutes = 30
	onDemandDurationMaxMinutes     = 60
)

// --- JSON wire shapes -------------------------------------------------

// onDemandRequestBody is the POST /session/ondemand envelope. Reuses
// joinRequestUser from http_endpoints.go so the frontend sends the
// same identity packet to canonical join and on-demand triggers; one
// shape, one validation path, zero divergence.
//
// Condition is the optional "practice condition" the frontend's
// contract (contract/openapi.yaml in synthetic-mkt-ui, PracticeCondition
// schema) added to OnDemandRequest. Omitted entirely by the frontend
// when the user didn't pick one (not sent as null/""). This decoder is
// plain json.Decode (no DisallowUnknownFields), so an older frontend
// build that never sends this field continues to decode exactly as
// before — adding a recognized field here is backward compatible by
// construction.
type onDemandRequestBody struct {
	User      joinRequestUser `json:"user"`
	Condition string          `json:"condition,omitempty"`
}

// Practice-condition wire values. Mirrors the frontend/contract's
// PracticeCondition enum as of this writing (contract/openapi.yaml in
// synthetic-mkt-ui) — but that enum is explicitly NOT locked/final, so
// this package never hardcodes a fixed allowlist into validation logic.
// defaultPracticeConditions is only the fallback Conductor.
// practiceConditions resolves to when Config.PracticeConditions is left
// nil (see NewConductor in conductor.go); a deployment tracking a
// different contract revision overrides the whole list there, with no
// code change here.
const (
	practiceConditionNormalDay     = "normal"
	practiceConditionCalmRange     = "calm"
	practiceConditionTrendingDay   = "trending_day"
	practiceConditionWhipsaw       = "whipsaw"
	practiceConditionUnstableDay   = "unstable_day"
	practiceConditionThinLiquidity = "thin_liquidity"
)

var defaultPracticeConditions = []string{
	practiceConditionNormalDay,
	practiceConditionCalmRange,
	practiceConditionTrendingDay,
	practiceConditionWhipsaw,
	practiceConditionUnstableDay,
	practiceConditionThinLiquidity,
}

// buildPracticeConditionSet turns a flat allowlist into the O(1)-lookup
// shape parsePracticeCondition needs. Exposed at package scope (not a
// method) so both NewConductor and tests can build one without needing
// a live *Conductor.
func buildPracticeConditionSet(allowed []string) map[string]bool {
	set := make(map[string]bool, len(allowed))
	for _, condition := range allowed {
		set[condition] = true
	}
	return set
}

// parsePracticeCondition validates the optional condition field against
// allowed — the Conductor's configured set (Config.PracticeConditions,
// or defaultPracticeConditions if that was left nil), never a fixed
// package-level list.
//
//   - "" (field omitted) is legal and means "no condition requested" —
//     passed through as empty, not defaulted to normal. What "no
//     scenario" means downstream is the agent-orchestrator's call, not
//     this HTTP layer's.
//   - a non-empty value outside the configured set is a client bug:
//     400, same convention as place-order's side/type/tif validation
//     (broker/order/mapping.go).
func parsePracticeCondition(raw string, allowed map[string]bool) (string, error) {
	if raw == "" {
		return "", nil
	}
	if !allowed[raw] {
		return "", fmt.Errorf("condition %q not recognized", raw)
	}
	return raw, nil
}

// onDemandWarmingUpBody is the gate-pass 200 response. The UI reads
// warmup_ends_at to drive its countdown and session_ends_at to drive
// the session timer after warm-up.
type onDemandWarmingUpBody struct {
	Status        string `json:"status"`
	SessionID     string `json:"session_id"`
	WarmupEndsAt  string `json:"warmup_ends_at"`
	SessionEndsAt string `json:"session_ends_at"`
}

// onDemandInsufficientFundsBody is the gate-fail 200 response. The
// user has an account but not enough trading cash to meet the
// configured on-demand floor; the UI surfaces the message verbatim.
type onDemandInsufficientFundsBody struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

// onDemandFailedBody is the setup-fail 503 response. Carries the raw
// reason string the flow produced so a developer looking at a failing
// request sees exactly what went wrong — surface area the regular
// errorBody shape would hide behind "error".
type onDemandFailedBody struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
}

// sessionStatusBody is the status endpoint's response envelope.
// Three optional fields keep the shape consistent across all four
// Status values while hiding fields that would carry zero-values:
//
//   - "warming_up": warmup_ends_at and session_ends_at are included
//     only if the Market record exists (a Starting session with no
//     Market yet omits both — this is the brief window between the
//     reservation and the Market-create lock take).
//   - "ready": both times are always included.
//   - "failed": reason is always included; times are omitted (the
//     Market record was deleted at failOnDemand).
//   - "ended": no extras (reserved for Tier-3 teardown states).
type sessionStatusBody struct {
	Status        string `json:"status"`
	WarmupEndsAt  string `json:"warmup_ends_at,omitempty"`
	SessionEndsAt string `json:"session_ends_at,omitempty"`
	Reason        string `json:"reason,omitempty"`
}

// --- status mapper ----------------------------------------------------

// onDemandStatusFor categorizes a StartOnDemand return into the HTTP
// status the handler writes. Dispatch uses errors.Is so a %w-wrapped
// error matches the same as a bare sentinel. The two happy-path
// result Kinds both map to 200 — the response body shape
// discriminates between them.
//
// Categories:
//
//   - 200: nil error. Caller then inspects result.Kind to pick the
//     response body shape (warming_up vs insufficient_funds).
//   - 400: unknown market tag (loadConfig hit a missing file —
//     os.ErrNotExist descends through fmt.Errorf's %w wrap). Treated
//     as a client bug: the frontend should only POST tags that
//     /v1/markets catalogued.
//   - 409: ErrAlreadyInSession (per-user XOR front-gate refused a
//     second concurrent trigger) or funnel.ErrSessionAlreadyLive (the
//     broker's per-account backstop fired, meaning the on-demand XOR
//     desynced with the broker's).
//   - 503: ErrOnDemandSetupFailed (populate/construct/wire/start/seal
//     failure) or context.DeadlineExceeded (the handler's
//     SetupTimeout fired before the flow completed).
//   - 500: funnel.ErrBadBinding / ErrBadParticipant (the conductor
//     fills both fields in SessionStartCmd — these indicate a
//     conductor bug, not a client error), and every other wrapped or
//     unknown error.
//
// Note the deliberate asymmetry with joinStatusFor: lifecycle
// sentinels (ErrNoAccount, ErrLoadFailed, ErrSnapshotWriteFailed)
// surface through StartSession here and map to 503 via
// ErrOnDemandSetupFailed (the whole setup-phase category), where join
// routes them to 503 by their individual sentinel. The on-demand path
// does not need per-sentinel routing — the whole setup-fail class is
// one bucket because the UI treats them identically ("show error,
// retry").
func onDemandStatusFor(err error) int {
	if err == nil {
		return http.StatusOK
	}
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return http.StatusBadRequest
	case errors.Is(err, ErrAlreadyInSession),
		errors.Is(err, funnel.ErrSessionAlreadyLive):
		return http.StatusConflict
	// Conductor-side bugs must be classified BEFORE the setup-failed
	// bucket: ErrBadBinding and ErrBadParticipant can only fire if the
	// conductor filled SessionStartCmd.ExchangeInbound or Participant
	// incorrectly, which is a programming error, not infra flakiness.
	// StartOnDemand wraps them with ErrOnDemandSetupFailed because they
	// surface from the setup-phase StartSession call, so a case ordered
	// below the setup bucket would never fire — errors.Is matches both
	// wraps simultaneously.
	case errors.Is(err, funnel.ErrBadBinding),
		errors.Is(err, funnel.ErrBadParticipant):
		return http.StatusInternalServerError
	case errors.Is(err, ErrOnDemandSetupFailed),
		errors.Is(err, context.DeadlineExceeded):
		return http.StatusServiceUnavailable
	}
	return http.StatusInternalServerError
}

// --- duration parsing -------------------------------------------------

// parseOnDemandDuration reads the ?duration= query param and returns
// the number of minutes to use for the session, or an error the
// caller surfaces as HTTP 400.
//
// Rules (locked by the design):
//
//   - absent (empty value, including a missing param) → use the
//     configured default (30 minutes).
//   - present but non-integer → error (e.g. "abc", "5m", "1.5").
//   - present integer below min (5) or above max (60) → error.
//   - 0 is not a special case: it fails the min guard and errors with
//     a clear out-of-range message.
//
// The caller never maps the returned error through onDemandStatusFor;
// a parse / out-of-range failure is a client bug that goes straight
// to 400 with the error string as the body.
func parseOnDemandDuration(query string) (int, error) {
	if query == "" {
		return onDemandDurationDefaultMinutes, nil
	}
	parsed, parseErr := strconv.Atoi(query)
	if parseErr != nil {
		return 0, errors.New("duration must be an integer number of minutes")
	}
	if parsed < onDemandDurationMinMinutes || parsed > onDemandDurationMaxMinutes {
		return 0, errors.New("duration out of range (must be between 5 and 60 minutes)")
	}
	return parsed, nil
}

// --- handler ----------------------------------------------------------

// handleStartOnDemand is the on-demand trigger HTTP ingress. Decodes
// the identity packet, validates the three required inputs (user_id,
// market, duration), installs a setupTimeout context around
// StartOnDemand, and writes one of three response shapes based on the
// flow's result and the status mapper.
//
// The handler owns the deadline (not the flow or the broker) so a
// wedged orchestrator or shard reply surfaces as 503 regardless of
// whether any internal step has its own timeout dial.
func (conductor *Conductor) handleStartOnDemand(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathSessionOnDemand)

	if request.Method != http.MethodPost {
		writeError(responseWriter, http.StatusMethodNotAllowed, "method not allowed", PathSessionOnDemand)
		return
	}

	marketTag := request.URL.Query().Get("market")
	if marketTag == "" {
		conductor.log.Info("conductor: handleStartOnDemand missing market tag",
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusBadRequest, "market query parameter required", PathSessionOnDemand)
		return
	}

	durationMinutes, durationErr := parseOnDemandDuration(request.URL.Query().Get("duration"))
	if durationErr != nil {
		conductor.log.Info("conductor: handleStartOnDemand bad duration",
			"raw", request.URL.Query().Get("duration"),
			"err", durationErr,
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusBadRequest, durationErr.Error(), PathSessionOnDemand)
		return
	}

	var requestBody onDemandRequestBody
	if decodeErr := json.NewDecoder(request.Body).Decode(&requestBody); decodeErr != nil {
		conductor.log.Info("conductor: handleStartOnDemand bad body",
			"err", decodeErr,
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusBadRequest, "invalid JSON body", PathSessionOnDemand)
		return
	}
	if requestBody.User.UserID == "" {
		conductor.log.Info("conductor: handleStartOnDemand missing user_id",
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusBadRequest, "user_id required", PathSessionOnDemand)
		return
	}

	condition, conditionErr := parsePracticeCondition(requestBody.Condition, conductor.practiceConditions)
	if conditionErr != nil {
		conductor.log.Info("conductor: handleStartOnDemand bad condition",
			"raw", requestBody.Condition,
			"err", conditionErr,
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusBadRequest, conditionErr.Error(), PathSessionOnDemand)
		return
	}

	deadlineCtx, cancelDeadline := context.WithTimeout(request.Context(), conductor.setupTimeout)
	defer cancelDeadline()

	result, startErr := conductor.StartOnDemand(
		deadlineCtx,
		UserID(requestBody.User.UserID),
		MarketTag(marketTag),
		durationMinutes,
		condition,
	)
	status := onDemandStatusFor(startErr)

	if startErr != nil {
		switch status {
		case http.StatusServiceUnavailable:
			writeJSON(responseWriter, status, onDemandFailedBody{
				Status: "failed",
				Reason: startErr.Error(),
			}, PathSessionOnDemand)
		default:
			writeError(responseWriter, status, startErr.Error(), PathSessionOnDemand)
		}
		return
	}

	switch result.Kind {
	case OnDemandWarmingUp:
		writeJSON(responseWriter, status, onDemandWarmingUpBody{
			Status:        sessionStatusLabelWarmingUp,
			SessionID:     string(result.SessionID),
			WarmupEndsAt:  result.WarmupEndsAt.UTC().Format(time.RFC3339Nano),
			SessionEndsAt: result.SessionEndsAt.UTC().Format(time.RFC3339Nano),
		}, PathSessionOnDemand)
	case OnDemandInsufficientFunds:
		writeJSON(responseWriter, status, onDemandInsufficientFundsBody{
			Status:  "insufficient_funds",
			Message: result.Message,
		}, PathSessionOnDemand)
	default:
		// Defensive — a new OnDemandResultKind added without a handler
		// branch would otherwise silently write an empty body. Surface
		// as 500 so the oversight is loud.
		conductor.log.Error("conductor: handleStartOnDemand unknown result Kind",
			"kind", result.Kind,
		)
		writeError(responseWriter, http.StatusInternalServerError, "unknown result kind", PathSessionOnDemand)
	}
}

// --- GET /session/status (fallback / recovery) -----------------------

// sessionStatusSnapshot is the pure registry read the status handler
// delegates to. Takes the one registry mutex for the microsecond map
// lookups, copies out everything the response body needs, releases
// before formatting or writing. Zero mutations anywhere — a snapshot
// of the registry before and after the call is byte-identical.
//
// Returns found=false if the session doesn't exist (handler writes
// 404). Returns found=true with:
//
//   - statusLabel: the user-visible string ("warming_up" / "ready" /
//     "failed" / "ended") derived from Session.State. The two
//     intermediate SessionStates (Stopping, Ended) both map to
//     "ended" — the UI only cares that the session is no longer
//     operable.
//   - warmupEndsAt / closeAt: zero time.Time values if the Market
//     record does not exist or has not yet stamped them (a Starting
//     session before the finalize-Live lock take). Handler then omits
//     the fields from the response body via the json ",omitempty"
//     tag.
//   - failReason: the FailReason text when the session is in Failed
//     state; empty string otherwise.
//
// Deliberately NO defer-unlock: the lock→copy→unlock sequence is
// written out so a reader sees directly that the mutex is released
// before any further work (string formatting, response write)
// happens. Matches the fanoutForSession pattern in http_endpoints.go.
func (conductor *Conductor) sessionStatusSnapshot(sessionID SessionID) (
	statusLabel string,
	warmupEndsAt time.Time,
	closeAt time.Time,
	failReason string,
	found bool,
) {
	conductor.reg.mu.Lock()
	session := conductor.reg.sessions[sessionID]
	if session == nil {
		conductor.reg.mu.Unlock()
		return "", time.Time{}, time.Time{}, "", false
	}
	sessionState := session.State
	failReason = session.FailReason
	marketID := session.Market
	if marketRecord := conductor.reg.markets[marketID]; marketRecord != nil {
		warmupEndsAt = marketRecord.warmupEndsAt
		closeAt = marketRecord.closeAt
	}
	conductor.reg.mu.Unlock()

	switch sessionState {
	case Starting:
		statusLabel = sessionStatusLabelWarmingUp
	case Active:
		statusLabel = sessionStatusLabelReady
	case Failed:
		statusLabel = sessionStatusLabelFailed
	case Stopping, Ended:
		statusLabel = sessionStatusLabelEnded
	default:
		// Unknown SessionState (should not happen — enum is sealed).
		// Return "ended" as a safe fallback so a future enum addition
		// without a case update does not expose a zero-value label.
		statusLabel = sessionStatusLabelEnded
	}
	return statusLabel, warmupEndsAt, closeAt, failReason, true
}

// handleSessionStatus is the status endpoint. Reads the sessionID
// from the ?session= query (same carrier as the WebSocket, via the
// shared authFromRequest helper), looks up the registry, and writes
// one of three response shapes:
//
//   - 200 with sessionStatusBody on a known session.
//   - 400 if the carrier is absent (empty ?session=).
//   - 404 if the sessionID isn't in the registry.
//   - 405 for non-GET.
//
// Pure read. The registry is not mutated — a status call cannot
// cause state to change. Multiple concurrent status reads serialize
// through the one mutex but each spends only the microseconds the
// map lookups take.
func (conductor *Conductor) handleSessionStatus(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathSessionStatus)

	if request.Method != http.MethodGet {
		writeError(responseWriter, http.StatusMethodNotAllowed, "method not allowed", PathSessionStatus)
		return
	}

	sessionID := authFromRequest(request)
	if sessionID == "" {
		conductor.log.Info("conductor: handleSessionStatus missing session carrier",
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusBadRequest, "session query parameter required", PathSessionStatus)
		return
	}

	statusLabel, warmupEndsAt, closeAt, failReason, found := conductor.sessionStatusSnapshot(sessionID)
	if !found {
		conductor.log.Info("conductor: handleSessionStatus unknown session",
			"session_id", sessionID,
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusNotFound, "unknown session", PathSessionStatus)
		return
	}

	responseBody := sessionStatusBody{Status: statusLabel}
	if !warmupEndsAt.IsZero() {
		responseBody.WarmupEndsAt = warmupEndsAt.UTC().Format(time.RFC3339Nano)
	}
	if !closeAt.IsZero() {
		responseBody.SessionEndsAt = closeAt.UTC().Format(time.RFC3339Nano)
	}
	if statusLabel == sessionStatusLabelFailed {
		responseBody.Reason = failReason
	}
	writeJSON(responseWriter, http.StatusOK, responseBody, PathSessionStatus)
}
