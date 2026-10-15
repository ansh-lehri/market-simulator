package conductor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

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

	// Pre-flight resolves the session's Mode under one µs lock-take so
	// the branch below picks the right teardown shape without the body
	// having to defensively re-check. A session reaped between this
	// read and the body (e.g. the on-demand reaper fired in the gap)
	// is handled by the body's own idempotent guards and surfaces as
	// 404 at the status mapper — no race window where we route the
	// wrong teardown against the wrong account.
	conductor.reg.mu.Lock()
	preCheckSession := conductor.reg.sessions[sessionID]
	var preCheckMode Mode
	if preCheckSession != nil {
		if market := conductor.reg.markets[preCheckSession.Market]; market != nil {
			preCheckMode = market.Mode
		}
	}
	conductor.reg.mu.Unlock()

	deadlineCtx, cancelDeadline := context.WithTimeout(request.Context(), conductor.joinTimeout)
	defer cancelDeadline()

	// On-demand branch: full teardown via tearDownOnDemandInstance.
	// Status table is just 200/404 — the discard body log+proceeds on
	// every error after the flip, so the only non-success exit from
	// exitOnDemandSession is "session was already gone at preflight."
	if preCheckSession != nil && preCheckMode == OnDemand {
		exitErr := conductor.exitOnDemandSession(deadlineCtx, sessionID)
		status := onDemandExitStatusFor(exitErr)
		if exitErr != nil {
			writeError(responseWriter, status, exitErr.Error(), PathSessionExit)
			return
		}
		writeJSON(responseWriter, status, sessionExitResponseBody{Status: "ended"}, PathSessionExit)
		return
	}

	// Canonical branch (also the fall-through for an unknown session —
	// leaveCanonicalSession returns ErrSessionNotFound → 404 at
	// exitStatusFor, same shape the on-demand branch would produce).
	leaveErr := conductor.leaveCanonicalSession(deadlineCtx, sessionID)
	status := exitStatusFor(leaveErr)
	if leaveErr != nil {
		writeError(responseWriter, status, leaveErr.Error(), PathSessionExit)
		return
	}

	writeJSON(responseWriter, status, sessionExitResponseBody{Status: "ended"}, PathSessionExit)
}

// --- on-demand teardown ----------------------------------------------

// tearDownOnDemandInstance runs the full teardown for one on-demand
// instance. This is the on-demand sibling of closeCanonicalMarket, but
// collapsed to one ephemeral user+account rather than a market-wide
// sweep:
//
//  1. FLIP + SNAPSHOT (lock, µs): look up the market; nil OR State !=
//     Live → return. The second trigger in an exit-vs-reaper race
//     observes State == TearingDown here and no-ops. From the flip
//     onward, fanoutForSession returns nil for this market's session
//     so a WebSocket attempt lands as 404.
//
//  2. OFF-LOCK (THE RULE — the registry mutex is never held across
//     any of these call-outs):
//     - broker.EndSession(Mode=OnDemand): routes a DiscardSession to
//       the owning shard, which drops the ephemeral LoadedAccount
//       and deletes the Redis snapshot. LOG + PROCEED on error —
//       the ephemeral account is gone regardless; a stale snapshot
//       is TTL-cleaned; the user's intent to end the session is
//       honored by reaching Stop + reap unconditionally.
//     - agents.WindDown: fire-and-forget signal to the agent
//       orchestrator so the throwaway population unwinds.
//     - exchange.Stop: shuts the exchange handle; tape close lets
//       market-data self-clean its FanOut.
//
//  3. REAP (lock, µs): delete the single member session, delete the
//     market, free the per-user byUser slot.
//
// Zero touch of market-data from this function — same Model-A split
// closeCanonicalMarket follows (the socket close + shipped
// Disconnect own the feed-side cleanup).
func (conductor *Conductor) tearDownOnDemandInstance(ctx context.Context, marketID MarketID) {
	// ---- FLIP + SNAPSHOT (lock take #1) ----
	conductor.reg.mu.Lock()
	market := conductor.reg.markets[marketID]
	if market == nil || market.State != Live {
		var observedState MarketState
		if market != nil {
			observedState = market.State
		}
		conductor.reg.mu.Unlock()
		conductor.log.Info("conductor: tearDownOnDemandInstance skipped — market not Live",
			"market_id", marketID,
			"state", observedState,
			"market_present", market != nil,
		)
		return
	}
	market.State = TearingDown

	// On-demand carries exactly one member (the single user whose
	// session this ephemeral market was spun up for). Snapshot it so
	// the off-lock EndSession can target the account.
	var singleMemberSessionID SessionID
	for sessionID := range market.members {
		singleMemberSessionID = sessionID
		break
	}
	memberSession := conductor.reg.sessions[singleMemberSessionID]
	if memberSession == nil {
		// Market was Live but carried no corresponding session record
		// — a wiring bug upstream, but the teardown's reap can still
		// drop the orphan market. Log loudly and continue.
		exchangeHandleForTeardown := market.exch
		marketTagForTeardown := market.Tag
		conductor.reg.mu.Unlock()
		conductor.log.Error("conductor: tearDownOnDemandInstance market has no corresponding session record — reaping orphan market",
			"market_id", marketID,
			"snapshot_session_id", singleMemberSessionID,
		)
		if exchangeHandleForTeardown != nil {
			exchangeHandleForTeardown.Stop()
		}
		conductor.agents.WindDown(marketTagForTeardown, OnDemand)
		conductor.reg.mu.Lock()
		delete(conductor.reg.markets, marketID)
		conductor.reg.mu.Unlock()
		return
	}
	accountID := memberSession.Account
	userID := memberSession.User
	exchangeHandleForTeardown := market.exch
	marketTagForTeardown := market.Tag
	conductor.reg.mu.Unlock()
	conductor.log.Info("conductor: tearDownOnDemandInstance flipped to TearingDown",
		"market_id", marketID,
		"tag", marketTagForTeardown,
		"session_id", singleMemberSessionID,
		"user", userID,
		"account_id", accountID,
	)

	// ---- OFF-LOCK: broker.EndSession (OnDemand = discard) ----
	// Forward-progress on any error — the on-demand teardown contract
	// is "always reach Stop + reap." Log the error for operability;
	// the ephemeral account is dropped from the shard regardless, so
	// nothing is left half-ended.
	endErr := conductor.broker.EndSession(ctx, funnel.SessionEndCmd{
		AccountID: funnel.AccountID(accountID),
		SessionID: funnel.SessionID(singleMemberSessionID),
		Mode:      lifecycle.OnDemand,
	})
	if endErr != nil {
		conductor.log.Error("conductor: tearDownOnDemandInstance broker.EndSession incomplete; proceeding to wind-down + stop",
			"market_id", marketID,
			"session_id", singleMemberSessionID,
			"account_id", accountID,
			"err", endErr,
		)
	}

	// ---- OFF-LOCK: agent wind-down (fire-and-forget) ----
	conductor.agents.WindDown(marketTagForTeardown, OnDemand)

	// ---- OFF-LOCK: exchange stop ----
	if exchangeHandleForTeardown != nil {
		exchangeHandleForTeardown.Stop()
	}

	// ---- REAP (lock take #2) ----
	conductor.reg.mu.Lock()
	if liveSession := conductor.reg.sessions[singleMemberSessionID]; liveSession != nil {
		delete(conductor.reg.sessions, singleMemberSessionID)
		if byUserHit := conductor.reg.byUser[liveSession.User]; byUserHit == liveSession {
			delete(conductor.reg.byUser, liveSession.User)
		}
	}
	delete(conductor.reg.markets, marketID)
	conductor.reg.mu.Unlock()
	conductor.log.Info("conductor: tearDownOnDemandInstance reap complete",
		"market_id", marketID,
		"session_id", singleMemberSessionID,
		"user", userID,
	)
}

// onDemandReaperDelay computes the sleep duration between now and the
// market's scheduled end-of-session instant. Pure function so tests can
// pin "the reaper is armed off closeAt, not now+duration" without
// probing a running goroutine. A non-positive result means closeAt is
// already in the past — the reaper fires immediately.
func onDemandReaperDelay(now, closeAt time.Time) time.Duration {
	return closeAt.Sub(now)
}

// spawnOnDemandReaper launches the single-session reaper goroutine. One
// goroutine per on-demand instance, armed off the market's closeAt (the
// session-end instant computed at spin-up finalize = construct-instant
// + warm-up window + session duration). Fires tearDownOnDemandInstance
// at closeAt, or exits cleanly on process shutdown.
//
// Registered on conductor.wg so Conductor.Stop's wg.Wait includes
// reapers — a Stop in flight joins every outstanding reaper before
// closing the OHLCV pool.
//
// Safe against a late fire: if the user's exit (or any other path)
// already tore the market down, the reaper's tearDownOnDemandInstance
// call enters, sees markets[marketID] == nil, and returns a no-op.
func (conductor *Conductor) spawnOnDemandReaper(marketID MarketID, closeAt time.Time) {
	conductor.wg.Add(1)
	go func() {
		defer conductor.wg.Done()
		delay := onDemandReaperDelay(conductor.now(), closeAt)
		conductor.log.Info("conductor: on-demand reaper armed",
			"market_id", marketID,
			"close_at", closeAt,
			"delay", delay,
		)
		if !conductor.sleepOrQuit(delay) {
			conductor.log.Info("conductor: on-demand reaper exiting on shutdown signal",
				"market_id", marketID,
				"close_at", closeAt,
			)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), conductor.closeTimeout)
		defer cancel()
		conductor.log.Info("conductor: on-demand reaper firing",
			"market_id", marketID,
			"close_at", closeAt,
		)
		conductor.tearDownOnDemandInstance(ctx, marketID)
	}()
}

// exitOnDemandSession is the HTTP-driven wrapper around
// tearDownOnDemandInstance: resolve sessionID → marketID under one
// µs lock-take, then invoke the teardown body. The user-initiated
// trigger from handleSessionExit's on-demand branch.
//
// A session that disappears between the preflight read and this
// resolve returns ErrSessionNotFound — the reaper or another exit
// already reaped it, and the HTTP response is 404 (the status table
// deviation flagged in orientation: the body's own idempotency flip
// no-ops cleanly, but the preflight-level resolve honestly reports
// "no such session").
func (conductor *Conductor) exitOnDemandSession(ctx context.Context, sessionID SessionID) error {
	conductor.reg.mu.Lock()
	session := conductor.reg.sessions[sessionID]
	if session == nil {
		conductor.reg.mu.Unlock()
		conductor.log.Info("conductor: exitOnDemandSession rejected — unknown session",
			"session_id", sessionID,
		)
		return ErrSessionNotFound
	}
	marketID := session.Market
	conductor.reg.mu.Unlock()

	conductor.tearDownOnDemandInstance(ctx, marketID)
	return nil
}

// onDemandExitStatusFor maps an exitOnDemandSession return to an HTTP
// status. The surface is deliberately flat — the on-demand teardown
// body forward-progresses on every error after the flip, so the only
// non-success return from exitOnDemandSession is ErrSessionNotFound
// (preflight resolve miss → 404). Anything else — including a race
// where the reaper fired between preflight and resolve — folds into
// the same 404 or a clean 200 depending on timing.
func onDemandExitStatusFor(err error) int {
	if err == nil {
		return http.StatusOK
	}
	if errors.Is(err, ErrSessionNotFound) {
		return http.StatusNotFound
	}
	return http.StatusInternalServerError
}
