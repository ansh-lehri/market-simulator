package conductor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"simulator/economy/lifecycle"
)

// PathSessionOnDemandJoin is the per-agent on-demand join route. Distinct
// from PathSessionOnDemand (which spins up a brand-new on-demand market):
// this route attaches one more participant to a market that already
// exists, identified by exact exchange_id. L2 is the only caller — a
// human never hits this, since StartOnDemand already creates the
// human's own session inline as part of spin-up.
const PathSessionOnDemandJoin = "/session/ondemand/join"

// ondemandJoinRequestBody is the POST /session/ondemand/join envelope.
// No email (unlike joinRequestBody) — this door is never human-facing.
// owner_type IS present, unlike the original design here: this door's
// callers are L2's real agents (send "agent", or omit — defaults to
// "agent") and L2's liquidity-seeder identities (send
// "agent_operator") — two different owner types hitting the one
// endpoint, same shape as canonical's human/agent split, just agent
// vs agent_operator instead of human vs agent.
type ondemandJoinRequestBody struct {
	User       ondemandJoinRequestUser `json:"user"`
	ExchangeID string                  `json:"exchange_id"`
}

type ondemandJoinRequestUser struct {
	UserID    string `json:"user_id"`
	OwnerType string `json:"owner_type"`
}

// resolveOnDemandJoinOwnerType mirrors resolveOwnerType's shape, but
// an omitted field here defaults to OwnerAgent, not OwnerHuman — this
// door has no human caller at all.
func resolveOnDemandJoinOwnerType(wireValue string) (lifecycle.OwnerType, error) {
	if wireValue == "" {
		return lifecycle.OwnerAgent, nil
	}
	return lifecycle.ParseOwnerType(wireValue)
}

// onDemandJoinStatusFor maps a JoinOnDemand return error to an HTTP
// status. Delegates to joinStatusFor for everything except
// ErrMarketNotLive, which gets 503 instead of 409. The distinction
// matters: on-demand agents always join a market that was just created
// moments ago — "not live" here means StartSession's Redis write is
// still in flight (a transient initialization gap of ~1s), not a
// permanent conflict. 503 signals "retry shortly"; 409 would signal
// "give up" and the agent client treats 4xx as non-retryable.
func onDemandJoinStatusFor(err error) int {
	if errors.Is(err, ErrMarketNotLive) {
		return http.StatusServiceUnavailable
	}
	return joinStatusFor(err)
}

// handleJoinOnDemand is the on-demand per-agent join HTTP ingress.
// Mirrors handleJoinCanonical's shape (decode, validate required
// fields, resolve owner_type, timeout-wrap, call the flow, map through
// onDemandJoinStatusFor).
func (conductor *Conductor) handleJoinOnDemand(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathSessionOnDemandJoin)

	if request.Method != http.MethodPost {
		writeError(responseWriter, http.StatusMethodNotAllowed, "method not allowed", PathSessionOnDemandJoin)
		return
	}

	var requestBody ondemandJoinRequestBody
	if decodeErr := json.NewDecoder(request.Body).Decode(&requestBody); decodeErr != nil {
		conductor.log.Info("conductor: handleJoinOnDemand bad body",
			"err", decodeErr,
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusBadRequest, "invalid JSON body", PathSessionOnDemandJoin)
		return
	}
	if requestBody.User.UserID == "" || requestBody.ExchangeID == "" {
		conductor.log.Info("conductor: handleJoinOnDemand missing required fields",
			"has_user_id", requestBody.User.UserID != "",
			"has_exchange_id", requestBody.ExchangeID != "",
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusBadRequest, "user_id and exchange_id required", PathSessionOnDemandJoin)
		return
	}
	ownerType, ownerTypeErr := resolveOnDemandJoinOwnerType(requestBody.User.OwnerType)
	if ownerTypeErr != nil {
		conductor.log.Info("conductor: handleJoinOnDemand bad owner_type",
			"owner_type", requestBody.User.OwnerType,
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusBadRequest, "owner_type must be one of {agent, agent_operator}", PathSessionOnDemandJoin)
		return
	}

	deadlineCtx, cancelDeadline := context.WithTimeout(request.Context(), conductor.joinTimeout)
	defer cancelDeadline()

	result, joinErr := conductor.JoinOnDemand(
		deadlineCtx,
		UserID(requestBody.User.UserID),
		MarketID(requestBody.ExchangeID),
		ownerType,
	)
	status := onDemandJoinStatusFor(joinErr)
	if joinErr != nil {
		conductor.log.Error("conductor: handleJoinOnDemand failed",
			"user_id", requestBody.User.UserID,
			"exchange_id", requestBody.ExchangeID,
			"status", status,
			"err", joinErr,
		)
		writeError(responseWriter, status, joinErr.Error(), PathSessionOnDemandJoin)
		return
	}

	conductor.log.Info("conductor: handleJoinOnDemand committed",
		"user_id", requestBody.User.UserID,
		"exchange_id", requestBody.ExchangeID,
		"session_id", result.SessionID,
		"participant_id", result.ParticipantID,
	)
	writeJSON(responseWriter, status, joinResponseBody{
		SessionID:     string(result.SessionID),
		ParticipantID: string(result.ParticipantID),
		Status:        "active",
	}, PathSessionOnDemandJoin)
}
