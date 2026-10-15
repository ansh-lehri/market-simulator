package conductor

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"simulator/broker/funnel"
	"simulator/economy/lifecycle"
)

// PathSessionExit is the session-exit endpoint. Carrier shape:
// POST /session/exit?session=<sid>. Chosen over /session/{id}/exit so
// every session-level HTTP surface in this package shares one carrier
// (the WebSocket, /session/status, and this endpoint all read
// ?session=<sid>).
//
// The handler (handleSessionExit) is MODE-BRANCHED: a canonical session
// takes the detach-keep path (leaveCanonicalSession, this file); an
// on-demand session takes the full-teardown path (added in the sibling
// on-demand teardown stage in this same file). Until that lands, an
// on-demand session returns 501.
const PathSessionExit = "/session/exit"

// --- JSON wire shapes ------------------------------------------------

// sessionExitResponseBody is the 200 payload. One short literal — the
// handler only reaches here on a clean detach, so no additional fields
// are needed. Clients that need to see what state the session landed in
// after leave re-trigger /session/current.
type sessionExitResponseBody struct {
	Status string `json:"status"` // "ended" on success
}

// --- status mapper ---------------------------------------------------

// exitStatusFor maps a leaveCanonicalSession return error to an HTTP
// status. Categories:
//
//   - 200: success (nil error). The session was uninstalled on the shard
//     AND reaped from the conductor registry.
//   - 404: ErrSessionNotFound — no record of the SessionID.
//   - 409: ErrSessionNotActive — session exists but is Starting /
//     Stopping / Ended. Likely a double-leave or a leave racing a
//     market close.
//   - 503: lifecycle load/snapshot failures surfaced from the broker's
//     EndSession path.
//   - 504: funnel.ErrBrokerTimeout — shard did not reply before the
//     handler-owned deadline fired.
//   - 500: anything else — conductor-bug seams (funnel.ErrBadAccountID /
//     ErrBadSessionID — conductor fills both, so seeing them is a bug),
//     funnel.ErrSessionMismatch (registry drift), unknown errors.
//
// Dispatch is errors.Is on category sentinels so a %w-wrapped error is
// matched the same as the bare sentinel.
func exitStatusFor(err error) int {
	if err == nil {
		return http.StatusOK
	}
	switch {
	case errors.Is(err, ErrSessionNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrSessionNotActive):
		return http.StatusConflict
	case errors.Is(err, funnel.ErrBrokerTimeout):
		return http.StatusGatewayTimeout
	case errors.Is(err, lifecycle.ErrLoadFailed),
		errors.Is(err, lifecycle.ErrSnapshotWriteFailed),
		errors.Is(err, lifecycle.ErrNoAccount):
		return http.StatusServiceUnavailable
	}
	return http.StatusInternalServerError
}

// --- core flow -------------------------------------------------------

// leaveCanonicalSession runs the canonical detach-keep teardown for one
// session. Shape (matching THE RULE — the registry mutex is never held
// across a call-out):
//
//  1. RESERVE (lock, µs): look up the session + its market; refuse if
//     the market is on-demand (sibling phase handles it); refuse if
//     the session is not Active (double-leave guard); flip the session
//     from Active → Stopping as the leave-in-flight guard; snapshot
//     {AccountID, UserID} and unlock.
//
//  2. OFF-LOCK: broker.EndSession(SessionEndCmd{AccountID, SessionID,
//     Canonical}). Synchronous: the shard uninstalls the binding,
//     frees the per-account XOR, parks the LoadedAccount in its
//     `detached` map. No persist, no square, no unload — those happen
//     later at FinalizeMarket.
//
//  3. REAP OR REVERT (lock, µs):
//     - On nil (or ErrNoLiveSession — idempotent): delete the session
//       record, drop its member entry from the market, and free the
//       byUser slot. Return nil → handler 200.
//     - On any other error: revert Stopping → Active so the user is
//       never half-detached, then return the error → handler 503/504/500.
//
// Nothing in this function touches market-data or the exchange handle.
// The browser's socket close + the shipped market-data Disconnect own
// the feed-side cleanup; the broker owns the account-side cleanup.
func (conductor *Conductor) leaveCanonicalSession(ctx context.Context, sessionID SessionID) error {
	// ---- RESERVE (lock take #1 — microseconds) ----
	conductor.reg.mu.Lock()
	session := conductor.reg.sessions[sessionID]
	if session == nil {
		conductor.reg.mu.Unlock()
		conductor.log.Info("conductor: leaveCanonicalSession rejected — unknown session",
			"session_id", sessionID,
		)
		return ErrSessionNotFound
	}
	market := conductor.reg.markets[session.Market]
	// Mode guard. The on-demand branch is the sibling teardown phase's
	// job; here we refuse so a stray canonical-leave call against an
	// on-demand session cannot damage its state. The handler maps this
	// to a 501 Not Implemented at the HTTP layer.
	marketMode := Canonical
	if market != nil {
		marketMode = market.Mode
	}
	if marketMode != Canonical {
		conductor.reg.mu.Unlock()
		conductor.log.Info("conductor: leaveCanonicalSession rejected — session is not canonical",
			"session_id", sessionID,
			"mode", marketMode,
		)
		return fmt.Errorf("conductor: leaveCanonicalSession: session mode %s not implemented in this phase", marketMode)
	}
	if session.State != Active {
		snapshotState := session.State
		conductor.reg.mu.Unlock()
		conductor.log.Info("conductor: leaveCanonicalSession rejected — session not Active",
			"session_id", sessionID,
			"state", snapshotState,
		)
		return ErrSessionNotActive
	}
	session.State = Stopping // guard: a concurrent leave trips the Active check above
	accountID := session.Account
	userID := session.User
	marketID := session.Market
	conductor.reg.mu.Unlock()
	conductor.log.Info("conductor: leaveCanonicalSession reserved Stopping",
		"session_id", sessionID,
		"user", userID,
		"account_id", accountID,
		"market_id", marketID,
	)

	// ---- OFF-LOCK: broker.EndSession ----
	endErr := conductor.broker.EndSession(ctx, funnel.SessionEndCmd{
		AccountID: funnel.AccountID(accountID),
		SessionID: funnel.SessionID(sessionID),
		Mode:      lifecycle.Canonical,
	})

	// A broker-reported ErrNoLiveSession means the shard already has no
	// binding for the account under this SessionID — effectively the
	// detach-keep's post-condition. Treat as success for reap purposes
	// so a retried leave (lost network, double-click) collapses to the
	// same idempotent 200 instead of leaving the user stuck on a
	// Stopping session the conductor never reaps.
	if endErr != nil && errors.Is(endErr, funnel.ErrNoLiveSession) {
		conductor.log.Warn("conductor: leaveCanonicalSession broker reports no live session — treating as idempotent success",
			"session_id", sessionID,
			"account_id", accountID,
		)
		endErr = nil
	}

	if endErr != nil {
		// REVERT: lock, flip Stopping back to Active so the user is
		// never half-detached, then surface the broker error. The
		// handler maps it to 503/504/500 per exitStatusFor.
		conductor.reg.mu.Lock()
		if revertTarget := conductor.reg.sessions[sessionID]; revertTarget != nil && revertTarget.State == Stopping {
			revertTarget.State = Active
		}
		conductor.reg.mu.Unlock()
		conductor.log.Error("conductor: leaveCanonicalSession broker.EndSession failed — reverted to Active",
			"session_id", sessionID,
			"account_id", accountID,
			"err", endErr,
		)
		return fmt.Errorf("conductor: leaveCanonicalSession: end session: %w", endErr)
	}

	// ---- REAP (lock take #2 — microseconds) ----
	conductor.reg.mu.Lock()
	if liveSession := conductor.reg.sessions[sessionID]; liveSession != nil {
		delete(conductor.reg.sessions, sessionID)
		if liveMarket := conductor.reg.markets[liveSession.Market]; liveMarket != nil {
			delete(liveMarket.members, sessionID)
		}
		if byUserHit := conductor.reg.byUser[userID]; byUserHit == liveSession {
			delete(conductor.reg.byUser, userID)
		}
	}
	conductor.reg.mu.Unlock()
	conductor.log.Info("conductor: leaveCanonicalSession detach-keep complete",
		"session_id", sessionID,
		"user", userID,
		"account_id", accountID,
		"market_id", marketID,
	)
	return nil
}

// --- HTTP handler ----------------------------------------------------

// handleSessionExit is the POST /session/exit?session=<sid> ingress for
// both canonical and on-demand sessions. Carrier mirrors
// /session/status: the SessionID rides as a query parameter so the
// frontend uses the same shape for both endpoints.
//
// The handler is MODE-BRANCHED off the registry's Market record so a
// stray canonical-leave call against an on-demand session (or vice
// versa) cannot damage either account:
//
//   - Canonical session → leaveCanonicalSession (detach-keep: broker
//     uninstalls the binding, keeps the LoadedAccount parked for
//     market-close FinalizeMarket). This file.
//   - OnDemand session  → full teardown (ephemeral account dropped,
//     Redis snapshot deleted). Added in the on-demand teardown stage;
//     until that lands this arm returns 501.
func (conductor *Conductor) handleSessionExit(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathSessionExit)

	if request.Method != http.MethodPost {
		writeError(responseWriter, http.StatusMethodNotAllowed, "method not allowed", PathSessionExit)
		return
	}

	sessionID := SessionID(request.URL.Query().Get("session"))
	if sessionID == "" {
		conductor.log.Info("conductor: handleSessionExit missing session carrier",
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusBadRequest, "session required", PathSessionExit)
		return
	}

	// Pre-flight on mode: if the session is on-demand, bail with 501
	// before holding the lock any longer. leaveCanonicalSession also
	// guards (defense-in-depth); doing it here keeps the error legible.
	conductor.reg.mu.Lock()
	preCheckSession := conductor.reg.sessions[sessionID]
	var preCheckMode Mode
	if preCheckSession != nil {
		if market := conductor.reg.markets[preCheckSession.Market]; market != nil {
			preCheckMode = market.Mode
		}
	}
	conductor.reg.mu.Unlock()
	if preCheckSession != nil && preCheckMode != Canonical {
		conductor.log.Info("conductor: handleSessionExit on-demand session routed to sibling (not implemented)",
			"session_id", sessionID,
			"mode", preCheckMode,
		)
		writeError(responseWriter, http.StatusNotImplemented, "on-demand teardown is in the sibling flow", PathSessionExit)
		return
	}

	deadlineCtx, cancelDeadline := context.WithTimeout(request.Context(), conductor.joinTimeout)
	defer cancelDeadline()

	leaveErr := conductor.leaveCanonicalSession(deadlineCtx, sessionID)
	status := exitStatusFor(leaveErr)
	if leaveErr != nil {
		writeError(responseWriter, status, leaveErr.Error(), PathSessionExit)
		return
	}

	writeJSON(responseWriter, status, sessionExitResponseBody{Status: "ended"}, PathSessionExit)
}
