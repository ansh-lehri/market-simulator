package conductor

import (
	"context"
	"encoding/json"
	"net/http"
)

// PathSessionOnDemandJoin is the per-agent on-demand join route. Distinct
// from PathSessionOnDemand (which spins up a brand-new on-demand market):
// this route attaches one more participant to a market that already
// exists, identified by exact exchange_id. L2 is the only caller — a
// human never hits this, since StartOnDemand already creates the
// human's own session inline as part of spin-up.
const PathSessionOnDemandJoin = "/session/ondemand/join"

// ondemandJoinRequestBody is the POST /session/ondemand/join envelope.
// Deliberately narrower than joinRequestBody: no email, no owner_type —
// the only caller is L2, so the handler resolves OwnerAgent itself
// (see JoinOnDemand) rather than taking it on the wire.
type ondemandJoinRequestBody struct {
	User       ondemandJoinRequestUser `json:"user"`
	ExchangeID string                  `json:"exchange_id"`
}

type ondemandJoinRequestUser struct {
	UserID string `json:"user_id"`
}

// handleJoinOnDemand is the on-demand per-agent join HTTP ingress.
// Mirrors handleJoinCanonical's shape (decode, validate required
// fields, timeout-wrap, call the flow, map through joinStatusFor) minus
// the owner_type resolution step canonical's shared human/agent door
// needs — this door's only caller is L2.
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

	deadlineCtx, cancelDeadline := context.WithTimeout(request.Context(), conductor.joinTimeout)
	defer cancelDeadline()

	result, joinErr := conductor.JoinOnDemand(
		deadlineCtx,
		UserID(requestBody.User.UserID),
		MarketID(requestBody.ExchangeID),
	)
	status := joinStatusFor(joinErr)
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
