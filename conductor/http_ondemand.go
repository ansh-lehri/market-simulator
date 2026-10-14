package conductor

import (
	"context"
	"encoding/json"
	"errors"
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

// On-demand duration bounds (integer minutes). The HTTP layer enforces
// these before the flow runs — a value outside the range is a client
// bug mapped to 400 (not a setup failure).
const (
	onDemandDurationMinMinutes     = 5
	onDemandDurationDefaultMinutes = 60
	onDemandDurationMaxMinutes     = 120
)

// --- JSON wire shapes -------------------------------------------------

// onDemandRequestBody is the POST /session/ondemand envelope. Reuses
// joinRequestUser from http_endpoints.go so the frontend sends the
// same identity packet to canonical join and on-demand triggers; one
// shape, one validation path, zero divergence.
type onDemandRequestBody struct {
	User joinRequestUser `json:"user"`
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
//     configured default (60 minutes).
//   - present but non-integer → error (e.g. "abc", "5m", "1.5").
//   - present integer below min (5) or above max (120) → error.
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
		return 0, errors.New("duration out of range (must be between 5 and 120 minutes)")
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

	deadlineCtx, cancelDeadline := context.WithTimeout(request.Context(), conductor.setupTimeout)
	defer cancelDeadline()

	result, startErr := conductor.StartOnDemand(
		deadlineCtx,
		UserID(requestBody.User.UserID),
		MarketTag(marketTag),
		durationMinutes,
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
			Status:        "warming_up",
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
