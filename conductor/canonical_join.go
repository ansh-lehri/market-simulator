package conductor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"simulator/broker/funnel"
	"simulator/contract"
	"simulator/economy/lifecycle"
)

// JoinResult is the success payload JoinCanonical returns on a clean
// admission — the two opaque identifiers the caller stamps into its HTTP
// response (and the frontend sends back on the WebSocket as ?session=).
type JoinResult struct {
	SessionID     SessionID
	ParticipantID ParticipantID
}

// JoinCanonical admits one user into one live canonical market. The flow
// is fast + blocking because the market is already Live — there is no
// market-open wait. Three phases, with the one registry mutex taken
// exactly twice and never across a broker call-out:
//
//  1. reserve (locked, microseconds): resolve the Live canonical market
//     for the tag, enforce the per-user front-gate, and reserve a
//     Starting Session record. The Starting record itself IS the
//     reservation — a second concurrent trigger for the same user is
//     refused here (ErrAlreadyInSession) without any broker I/O.
//
//  2. slow work (off-lock): EnsureAccount (one durable upsert round-trip)
//     then StartSession (one shard round-trip plus the account's load
//     from durable store + Redis snapshot seed). Either failure rolls
//     the reservation back. The account itself is NOT rolled back —
//     EnsureAccount is idempotent and durable, so a re-join just
//     resolves the same handle.
//
//  3. finalize (locked, microseconds): write Account / ParticipantID /
//     Active onto the reservation and attach the session to the market's
//     member set. The market record is re-looked-up under the lock in
//     case teardown removed it between reserve and finalize (the
//     attachment is dropped in that case; the join still succeeds —
//     teardown will reap the session on its own pass).
//
// Returns JoinResult{SessionID, ParticipantID} on success. Error paths:
//
//   - ErrMarketNotLive       — no Live canonical market for tag.
//   - ErrAlreadyInSession    — the user already holds a session.
//   - wrapped broker error   — any EnsureAccount or StartSession failure,
//     surfaced with fmt.Errorf("…: %w", err) so joinStatusFor can
//     classify it with errors.Is.
func (conductor *Conductor) JoinCanonical(ctx context.Context, userID UserID, tag MarketTag) (JoinResult, error) {
	conductor.log.Info("conductor: JoinCanonical begin",
		"user", userID,
		"tag", tag,
	)

	// ---- RESERVE (lock take #1 — microseconds) ----
	conductor.reg.mu.Lock()
	liveMarket := findLiveCanonical(conductor.reg, tag)
	if liveMarket == nil {
		conductor.reg.mu.Unlock()
		conductor.log.Info("conductor: JoinCanonical rejected — no live canonical market",
			"user", userID,
			"tag", tag,
		)
		return JoinResult{}, ErrMarketNotLive
	}
	if _, alreadyPresent := conductor.reg.byUser[userID]; alreadyPresent {
		conductor.reg.mu.Unlock()
		conductor.log.Info("conductor: JoinCanonical rejected — user already in session",
			"user", userID,
			"tag", tag,
		)
		return JoinResult{}, ErrAlreadyInSession
	}
	sessionID := SessionID(newUUIDv4())
	reservedSession := &Session{
		ID:          sessionID,
		User:        userID,
		Market:      liveMarket.ID,
		AccountType: Durable,
		State:       Starting,
	}
	conductor.reg.sessions[sessionID] = reservedSession
	conductor.reg.byUser[userID] = reservedSession
	marketInbound := liveMarket.inbound
	marketID := liveMarket.ID
	conductor.reg.mu.Unlock()
	conductor.log.Info("conductor: JoinCanonical reserved Starting session",
		"session_id", sessionID,
		"user", userID,
		"market_id", marketID,
	)

	// ---- OFF-LOCK: EnsureAccount ----
	accountID, ensureErr := conductor.broker.EnsureAccount(ctx, lifecycle.OwnerHuman, string(userID))
	if ensureErr != nil {
		conductor.rollbackJoin(sessionID, userID)
		conductor.log.Error("conductor: JoinCanonical ensure-account failed",
			"session_id", sessionID,
			"user", userID,
			"err", ensureErr,
		)
		return JoinResult{}, fmt.Errorf("conductor: JoinCanonical: ensure account: %w", ensureErr)
	}

	// ---- OFF-LOCK: StartSession ----
	participantID := ParticipantID(newUUIDv4())
	startErr := conductor.broker.StartSession(ctx, funnel.SessionStartCmd{
		AccountID:       accountID,
		SessionID:       funnel.SessionID(sessionID),
		Mode:            lifecycle.Canonical,
		ExchangeID:      funnel.ExchangeID(marketID),
		ExchangeInbound: marketInbound,
		Participant:     contract.ParticipantID(participantID),
	})
	if startErr != nil {
		conductor.rollbackJoin(sessionID, userID)
		conductor.log.Error("conductor: JoinCanonical start-session failed",
			"session_id", sessionID,
			"account_id", accountID,
			"err", startErr,
		)
		return JoinResult{}, fmt.Errorf("conductor: JoinCanonical: start session: %w", startErr)
	}

	// ---- FINALIZE (lock take #2 — microseconds) ----
	conductor.reg.mu.Lock()
	reservedSession.Account = AccountID(string(accountID))
	reservedSession.Participant = participantID
	reservedSession.State = Active
	if freshMarketRecord := conductor.reg.markets[marketID]; freshMarketRecord != nil {
		freshMarketRecord.members[sessionID] = struct{}{}
	}
	conductor.reg.mu.Unlock()
	conductor.log.Info("conductor: JoinCanonical committed Active",
		"session_id", sessionID,
		"participant_id", participantID,
		"account_id", accountID,
		"market_id", marketID,
	)
	return JoinResult{SessionID: sessionID, ParticipantID: participantID}, nil
}

// findLiveCanonical scans the registry for the one Live canonical market
// with the given tag. Returns nil if no such record exists. Caller must
// hold reg.mu.
//
// There is at most one Live record per tag at any instant (the spin-up
// phase enforces this through its reserve-step duplicate check), so the
// first match is also the only match — the scan is O(markets), cold-path
// (one call per JoinCanonical invocation).
func findLiveCanonical(registry *Registry, tag MarketTag) *Market {
	for _, candidateMarket := range registry.markets {
		if candidateMarket.Tag == tag && candidateMarket.Mode == Canonical && candidateMarket.State == Live {
			return candidateMarket
		}
	}
	return nil
}

// rollbackJoin undoes the reservation step on an off-lock failure.
// Deletes the Starting session record and, if the byUser slot still
// points at that session, frees the slot too. The byUser guard matters
// if some other code path moved on from this user (today none does; the
// guard is a correctness margin, not a current need).
//
// Does NOT roll back the EnsureAccount result: the account is durable
// and idempotent, so a successful EnsureAccount followed by a failed
// StartSession leaves a reusable account that a retry will pick up.
func (conductor *Conductor) rollbackJoin(sessionID SessionID, userID UserID) {
	conductor.reg.mu.Lock()
	reservedSession := conductor.reg.sessions[sessionID]
	delete(conductor.reg.sessions, sessionID)
	if reservedSession != nil && conductor.reg.byUser[userID] == reservedSession {
		delete(conductor.reg.byUser, userID)
	}
	conductor.reg.mu.Unlock()
	conductor.log.Info("conductor: JoinCanonical rollback complete",
		"session_id", sessionID,
		"user", userID,
	)
}

// newUUIDv4 mints a random UUID v4 string (RFC 4122 variant). Reads 16
// bytes from crypto/rand, patches the version + variant nibbles, and
// hex-formats 8-4-4-4-12.
//
// Panics on crypto/rand.Read failure: the OS entropy source failing is
// an invariant violation per the project constitution (dev-fatal; not a
// business error to surface to the caller).
func newUUIDv4() string {
	var randomBytes [16]byte
	if _, readErr := rand.Read(randomBytes[:]); readErr != nil {
		panic(fmt.Sprintf("conductor: newUUIDv4: crypto/rand.Read failed: %v", readErr))
	}
	randomBytes[6] = (randomBytes[6] & 0x0f) | 0x40 // version 4
	randomBytes[8] = (randomBytes[8] & 0x3f) | 0x80 // RFC 4122 variant
	var formatted [36]byte
	hex.Encode(formatted[0:8], randomBytes[0:4])
	formatted[8] = '-'
	hex.Encode(formatted[9:13], randomBytes[4:6])
	formatted[13] = '-'
	hex.Encode(formatted[14:18], randomBytes[6:8])
	formatted[18] = '-'
	hex.Encode(formatted[19:23], randomBytes[8:10])
	formatted[23] = '-'
	hex.Encode(formatted[24:36], randomBytes[10:16])
	return string(formatted[:])
}
