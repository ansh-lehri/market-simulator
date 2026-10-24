package conductor

import (
	"context"
	"fmt"

	"simulator/broker/funnel"
	"simulator/contract"
	"simulator/economy/lifecycle"
)

// JoinOnDemand admits one agent into one already-live on-demand market —
// the market a human's StartOnDemand call already created. Structurally
// identical to JoinCanonical (same reserve/off-lock/finalize shape, same
// per-user byUser XOR/reconnect check — an agent must be bound to at
// most one live session anywhere in the registry, same as a human); the
// only differences are forced by what "on-demand" already means
// elsewhere in this package:
//
//   - the market is resolved by exact marketID (on-demand markets are
//     not looked up by tag — see onDemandMarketIDPrefix), requiring
//     Mode == OnDemand && State == Live, instead of findLiveCanonical's
//     tag scan.
//   - the reserved Session carries AccountType: Ephemeral and
//     Mode: lifecycle.OnDemand, matching the convention StartOnDemand
//     already uses for the human's own session on this same market.
//
// This endpoint's only caller is L2 — a human never calls it, since
// StartOnDemand already creates the human's own session inline.
// ownerType is the caller's resolved identity class: real agents send
// "agent" (the HTTP handler defaults an omitted wire value to this),
// liquidity-seeder identities send "agent_operator". Passed straight
// through to EnsureAccount — getting this wrong means the session
// installs on a DIFFERENT account row than the one the caller already
// opened (EnsureAccount is keyed by (owner_type, owner_id), so the
// same owner_id under two owner_types is two separate accounts), and
// every subsequent order/seed-inventory call against the caller's
// real account 409s with "no session" even though *a* session was
// installed — just not on that account.
//
// Returns JoinResult{SessionID, ParticipantID} on success. Error paths:
//
//   - ErrMarketNotLive  — no on-demand market Live under marketID.
//   - ErrLiveElsewhere  — agent is live on a different market, or has a
//     transient session on this one.
//   - wrapped broker error — any EnsureAccount or StartSession failure.
func (conductor *Conductor) JoinOnDemand(ctx context.Context, agentUserID UserID, marketID MarketID, ownerType lifecycle.OwnerType) (JoinResult, error) {
	conductor.log.Info("conductor: JoinOnDemand begin",
		"user", agentUserID,
		"market_id", marketID,
		"owner_type", ownerType,
	)

	// ---- RESERVE (lock take #1 — microseconds) ----
	conductor.reg.mu.Lock()
	liveMarket := conductor.reg.markets[marketID]
	if liveMarket == nil || liveMarket.Mode != OnDemand || liveMarket.State != Live {
		conductor.reg.mu.Unlock()
		conductor.log.Info("conductor: JoinOnDemand rejected — no live on-demand market",
			"user", agentUserID,
			"market_id", marketID,
		)
		return JoinResult{}, ErrMarketNotLive
	}
	if existingSession := conductor.reg.byUser[agentUserID]; existingSession != nil {
		existingMarket := existingSession.Market
		existingState := existingSession.State
		existingSessionID := existingSession.ID
		existingParticipantID := existingSession.Participant
		if existingMarket == liveMarket.ID && existingState == Active {
			conductor.reg.mu.Unlock()
			conductor.log.Info("conductor: JoinOnDemand reconnect — returning existing session",
				"user", agentUserID,
				"market_id", marketID,
				"session_id", existingSessionID,
				"participant_id", existingParticipantID,
			)
			return JoinResult{SessionID: existingSessionID, ParticipantID: existingParticipantID}, nil
		}
		conductor.reg.mu.Unlock()
		conductor.log.Info("conductor: JoinOnDemand rejected — user live elsewhere or transient",
			"user", agentUserID,
			"market_id", marketID,
			"existing_market", existingMarket,
			"existing_state", existingState,
		)
		return JoinResult{}, ErrLiveElsewhere
	}
	sessionID := SessionID(newUUIDv4())
	reservedSession := &Session{
		ID:          sessionID,
		User:        agentUserID,
		Market:      liveMarket.ID,
		AccountType: Ephemeral,
		State:       Starting,
	}
	conductor.reg.sessions[sessionID] = reservedSession
	conductor.reg.byUser[agentUserID] = reservedSession
	marketInbound := liveMarket.inbound
	resolvedMarketID := liveMarket.ID
	conductor.reg.mu.Unlock()
	conductor.log.Info("conductor: JoinOnDemand reserved Starting session",
		"session_id", sessionID,
		"user", agentUserID,
		"market_id", resolvedMarketID,
	)

	// ---- OFF-LOCK: EnsureAccount ----
	accountID, ensureErr := conductor.broker.EnsureAccount(ctx, ownerType, string(agentUserID))
	if ensureErr != nil {
		conductor.rollbackJoin(sessionID, agentUserID)
		conductor.log.Error("conductor: JoinOnDemand ensure-account failed",
			"session_id", sessionID,
			"user", agentUserID,
			"err", ensureErr,
		)
		return JoinResult{}, fmt.Errorf("conductor: JoinOnDemand: ensure account: %w", ensureErr)
	}

	// ---- OFF-LOCK: StartSession ----
	participantID := ParticipantID(newUUIDv4())
	startErr := conductor.broker.StartSession(ctx, funnel.SessionStartCmd{
		AccountID:       accountID,
		SessionID:       funnel.SessionID(sessionID),
		Mode:            lifecycle.OnDemand,
		ExchangeID:      funnel.ExchangeID(resolvedMarketID),
		ExchangeInbound: marketInbound,
		Participant:     contract.ParticipantID(participantID),
	})
	if startErr != nil {
		conductor.rollbackJoin(sessionID, agentUserID)
		conductor.log.Error("conductor: JoinOnDemand start-session failed",
			"session_id", sessionID,
			"account_id", accountID,
			"err", startErr,
		)
		return JoinResult{}, fmt.Errorf("conductor: JoinOnDemand: start session: %w", startErr)
	}

	// ---- FINALIZE (lock take #2 — microseconds) ----
	conductor.reg.mu.Lock()
	reservedSession.Account = AccountID(string(accountID))
	reservedSession.Participant = participantID
	reservedSession.State = Active
	if freshMarketRecord := conductor.reg.markets[resolvedMarketID]; freshMarketRecord != nil {
		freshMarketRecord.members[sessionID] = struct{}{}
	}
	conductor.reg.mu.Unlock()
	conductor.log.Info("conductor: JoinOnDemand committed Active",
		"session_id", sessionID,
		"participant_id", participantID,
		"account_id", accountID,
		"market_id", resolvedMarketID,
	)
	return JoinResult{SessionID: sessionID, ParticipantID: participantID}, nil
}
