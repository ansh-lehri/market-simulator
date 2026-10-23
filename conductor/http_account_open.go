package conductor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"simulator/broker/funnel"
	"simulator/broker/store"
	"simulator/economy/lifecycle"
)

// accountOpenRequestBody is the POST /v1/account/open envelope. Same
// user packet shape the join flow accepts (joinRequestUser), so the
// frontend uses one identity payload across both calls. email is
// attribution only; user_id is the identity key passed verbatim to
// the broker as owner_id (admission is anonymous — no validation).
// owner_type is resolved the same way as the join flow (omitted →
// human); L2's agent account-open call always sends "agent".
type accountOpenRequestBody struct {
	User joinRequestUser `json:"user"`
}

// accountOpenResponseBody is the 200 success envelope. One field: the
// opaque broker account handle the frontend sends on every subsequent
// money-domain read (/v1/account/balance, /v1/account/trades,
// /v1/account/pnl). The money-blind rule means this handler never
// returns anything beyond the handle itself — balances live in the
// broker.
type accountOpenResponseBody struct {
	AccountID string `json:"account_id"`
}

// accountOpenStatusFor maps an EnsureAccount return error to the HTTP
// status the handler writes. Dispatch uses errors.Is so a %w-wrapped
// error is matched the same as the bare sentinel. Categories:
//
//   - 200: success (nil).
//   - 400: lifecycle.ErrBadOwnerID — the user_id slipped past the
//     empty-string guard but failed the broker's deeper validation.
//     Client-correctable: fix the identity packet.
//   - 503: store.ErrNoAccount (retry-exhausted — the create-first
//     INSERT lost the race and the follow-up SELECT then missed, twice
//     in a row; infra/timing anomaly) or lifecycle.ErrCreateFailed
//     (the INSERT itself failed on a driver/constraint error). Both
//     are retryable after the backing Postgres recovers.
//   - 504: context.DeadlineExceeded — the handler's own
//     accountOpenTimeout fired before EnsureAccount returned.
//   - 500: lifecycle.ErrBadOwnerType (unreachable in practice — the
//     handler resolves and validates owner_type itself before ever
//     calling OpenAccount, rejecting a bad wire value with 400; this
//     sentinel surfacing here would mean that guard was bypassed, a
//     conductor bug) or ErrNegativeStartingCapital /
//     ErrNegativeStartingBank (the starting-capital/bank live in
//     broker-side server config, not in the client's request — so a
//     negative value here is a server misconfig). Fallthrough is also
//     500 — a leaked unknown error must never be reported as
//     client-fixable.
//
// Kept as its own function instead of merging into joinStatusFor: the
// same lifecycle sentinel maps to different statuses in the two flows
// — ErrBadOwnerID is 400 here (first touchpoint for user_id), 500 in
// the join flow (where the user_id has already passed the empty-string
// guard before admission reaches EnsureAccount).
func accountOpenStatusFor(err error) int {
	if err == nil {
		return http.StatusOK
	}
	switch {
	case errors.Is(err, lifecycle.ErrBadOwnerID):
		return http.StatusBadRequest
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout
	case errors.Is(err, store.ErrNoAccount),
		errors.Is(err, lifecycle.ErrCreateFailed):
		return http.StatusServiceUnavailable
	}
	return http.StatusInternalServerError
}

// OpenAccount is the one-line pass-through into the broker. No
// registry touch, no per-user front-gate, no XOR — opening an account
// is not starting a session, so none of the admission state applies.
// EnsureAccount is idempotent and durable, so a client retry (or a
// re-login after a signup) safely resolves to the same handle.
//
// Returns funnel.AccountID verbatim; the handler stringifies for the
// wire. The conductor's own AccountID named type (used in Session
// records) is NOT applied here — this call does not touch the
// registry, so there is no Session field to carry.
func (conductor *Conductor) OpenAccount(ctx context.Context, userID UserID, ownerType lifecycle.OwnerType) (funnel.AccountID, error) {
	return conductor.broker.EnsureAccount(ctx, ownerType, string(userID))
}

// handleAccountOpen is the signup/login HTTP ingress. Both flows hit
// this same endpoint and both get 200 back with an account_id — the
// create-vs-fetch distinction is not surfaced on the wire (the
// frontend's auth layer already knows which button the user pressed,
// and the broker's EnsureAccount is idempotent so the handle is
// identical either way).
//
// The handler owns the request-scoped deadline (not the flow or the
// broker) so a slow durable upsert surfaces as 504 even when the
// broker itself has no explicit timeout dial.
func (conductor *Conductor) handleAccountOpen(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathAccountOpen)

	if request.Method != http.MethodPost {
		writeError(responseWriter, http.StatusMethodNotAllowed, "method not allowed", PathAccountOpen)
		return
	}

	var requestBody accountOpenRequestBody
	if decodeErr := json.NewDecoder(request.Body).Decode(&requestBody); decodeErr != nil {
		conductor.log.Info("conductor: handleAccountOpen bad body",
			"err", decodeErr,
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusBadRequest, "invalid JSON body", PathAccountOpen)
		return
	}
	if requestBody.User.UserID == "" {
		conductor.log.Info("conductor: handleAccountOpen missing user_id",
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusBadRequest, "user_id required", PathAccountOpen)
		return
	}
	ownerType, ownerTypeErr := resolveOwnerType(requestBody.User.OwnerType)
	if ownerTypeErr != nil {
		conductor.log.Info("conductor: handleAccountOpen bad owner_type",
			"owner_type", requestBody.User.OwnerType,
			"remote_addr", request.RemoteAddr,
		)
		writeError(responseWriter, http.StatusBadRequest, "owner_type must be one of {agent, human}", PathAccountOpen)
		return
	}

	deadlineCtx, cancelDeadline := context.WithTimeout(request.Context(), conductor.accountOpenTimeout)
	defer cancelDeadline()

	accountID, ensureErr := conductor.OpenAccount(deadlineCtx, UserID(requestBody.User.UserID), ownerType)
	status := accountOpenStatusFor(ensureErr)
	if ensureErr != nil {
		conductor.log.Error("conductor: handleAccountOpen EnsureAccount failed",
			"user_id", requestBody.User.UserID,
			"status", status,
			"err", ensureErr,
		)
		writeError(responseWriter, status, ensureErr.Error(), PathAccountOpen)
		return
	}

	conductor.log.Info("conductor: handleAccountOpen resolved account handle",
		"user_id", requestBody.User.UserID,
		"account_id", accountID,
	)
	writeJSON(responseWriter, status, accountOpenResponseBody{
		AccountID: string(accountID),
	}, PathAccountOpen)
}
