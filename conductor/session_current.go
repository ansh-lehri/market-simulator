package conductor

import (
	"encoding/json"
	"net/http"
	"time"
)

// PathSessionCurrent is the reconnect-resolve endpoint: given the user
// identity packet, return the user's current session (if any) so the
// frontend can re-open the market-data WebSocket after losing local
// state (full page reload). Pure registry read — no broker call, no
// market-data call, one µs lock take.
//
// The frontend's typical use: on a cold load, POST here first. A 404
// means "no live session — show Join." A 200 means "resume the shown
// session on this market." The two optional ends-at timers are populated
// only for on-demand sessions (where the UI counts down against them);
// canonical sessions carry no end timer the client needs to see here.
const PathSessionCurrent = "/session/current"

// sessionCurrentRequestBody mirrors the shape of joinRequestBody so the
// frontend ships ONE identity packet across join / current / exit. Only
// user_id is required; email is attribution only.
type sessionCurrentRequestBody struct {
	User joinRequestUser `json:"user"`
}

// sessionCurrentResponseBody is the 200 payload. The two ends-at fields
// are omitted from the wire when empty (canonical sessions have no
// user-visible end timer at this door).
type sessionCurrentResponseBody struct {
	SessionID     string `json:"session_id"`
	Mode          string `json:"mode"`
	Market        string `json:"market"`
	Status        string `json:"status"`
	WarmupEndsAt  string `json:"warmup_ends_at,omitempty"`
	SessionEndsAt string `json:"session_ends_at,omitempty"`
}

// currentSessionSnapshot is the value shape resolveCurrentSession returns
// before the handler formats it into the response body. Decoupling the
// read from the formatting keeps the lock-held section tiny and lets a
// future caller (another handler, a test) reuse the resolver without
// paying the JSON encode cost.
type currentSessionSnapshot struct {
	SessionID     SessionID
	Mode          Mode
	Market        MarketTag
	Status        SessionState
	WarmupEndsAt  time.Time
	SessionEndsAt time.Time
}

// resolveCurrentSession is the pure-read helper: byUser → Session →
// Market, snapshot the five fields the handler needs, release the lock
// before any further work. Returns found=false if the user has no
// session in byUser.
//
// Deliberately no defer unlock: the lock→copy→unlock sequence is
// written out so a reader sees directly that the mutex is released
// before the formatting work in the caller runs — the load-bearing
// concurrency property of the file (matches fanoutForSession and
// sessionStatusSnapshot).
func (conductor *Conductor) resolveCurrentSession(userID UserID) (currentSessionSnapshot, bool) {
	conductor.reg.mu.Lock()
	session := conductor.reg.byUser[userID]
	if session == nil {
		conductor.reg.mu.Unlock()
		return currentSessionSnapshot{}, false
	}
	snapshot := currentSessionSnapshot{
		SessionID: session.ID,
		Status:    session.State,
	}
	// Market may be nil in the rare window where a teardown deleted the
	// market record but the session's byUser slot survives (reap is still
	// in flight). Honor that: return the session identity + status, leave
	// the market-sourced fields zeroed so the handler omits them.
	if market := conductor.reg.markets[session.Market]; market != nil {
		snapshot.Mode = market.Mode
		snapshot.Market = market.Tag
		snapshot.WarmupEndsAt = market.warmupEndsAt
		snapshot.SessionEndsAt = market.closeAt
	}
	conductor.reg.mu.Unlock()
	return snapshot, true
}

// handleSessionCurrent is the HTTP ingress. POST only. Decodes the
// identity packet, validates user_id, resolves the session, writes the
// response.
func (conductor *Conductor) handleSessionCurrent(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathSessionCurrent)

	if request.Method != http.MethodPost {
		writeError(responseWriter, http.StatusMethodNotAllowed, "method not allowed", PathSessionCurrent)
		return
	}

	var requestBody sessionCurrentRequestBody
	if decodeErr := json.NewDecoder(request.Body).Decode(&requestBody); decodeErr != nil {
		conductor.log.Info("conductor: handleSessionCurrent bad body",
			"err", decodeErr,
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusBadRequest, "invalid JSON body", PathSessionCurrent)
		return
	}
	if requestBody.User.UserID == "" {
		conductor.log.Info("conductor: handleSessionCurrent missing user_id",
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusBadRequest, "user_id required", PathSessionCurrent)
		return
	}

	snapshot, found := conductor.resolveCurrentSession(UserID(requestBody.User.UserID))
	if !found {
		conductor.log.Info("conductor: handleSessionCurrent miss",
			"user", requestBody.User.UserID,
		)
		writeError(responseWriter, http.StatusNotFound, "no session for user", PathSessionCurrent)
		return
	}

	responseBody := sessionCurrentResponseBody{
		SessionID: string(snapshot.SessionID),
		Mode:      snapshot.Mode.String(),
		Market:    string(snapshot.Market),
		Status:    snapshot.Status.String(),
	}
	// Populate the two ends-at timers only for on-demand sessions; the
	// zero-time check filters out canonical sessions AND the rare window
	// where a Market record was deleted between the slot hit and the
	// snapshot (the market.* fields stayed zero in that case).
	if snapshot.Mode == OnDemand {
		if !snapshot.WarmupEndsAt.IsZero() {
			responseBody.WarmupEndsAt = snapshot.WarmupEndsAt.UTC().Format(time.RFC3339Nano)
		}
		if !snapshot.SessionEndsAt.IsZero() {
			responseBody.SessionEndsAt = snapshot.SessionEndsAt.UTC().Format(time.RFC3339Nano)
		}
	}

	conductor.log.Info("conductor: handleSessionCurrent hit",
		"user", requestBody.User.UserID,
		"session_id", snapshot.SessionID,
		"mode", snapshot.Mode,
		"market", snapshot.Market,
		"status", snapshot.Status,
	)
	writeJSON(responseWriter, http.StatusOK, responseBody, PathSessionCurrent)
}
