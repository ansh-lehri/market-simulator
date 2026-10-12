package conductor

import (
	"errors"
	"net/http"

	"simulator/broker/funnel"
	"simulator/economy/lifecycle"
)

// joinStatusFor maps a JoinCanonical return error to the HTTP status
// the handler writes. The categories:
//
//   - 200: success (nil error).
//   - 409: a conflict the client can act on — the market isn't live for
//     the requested tag, the user already holds a session, or the
//     broker's per-account XOR refused the install.
//   - 504: StartSession could not deliver to the owning shard before the
//     handler's context deadline fired.
//   - 503: StartSession reached the shard but the durable load or Redis
//     snapshot seed failed; retriable after infra recovery.
//   - 500: everything else — EnsureAccount failures (lifecycle
//     validation, store load / retry-exhausted miss, create failure),
//     funnel.ErrBadBinding / ErrBadParticipant (the conductor fills both
//     fields, so these indicate a conductor bug not a client error), and
//     any wrapped or unknown error.
//
// Dispatch uses errors.Is so a %w-wrapped error is matched the same as
// the bare sentinel.
//
// Note: lifecycle.ErrNoAccount (from StartSession's loader → 503) and
// store.ErrNoAccount (from EnsureAccount's retry-exhausted miss → 500)
// are distinct sentinel values in distinct packages — the mapper
// discriminates by identity, not by stage context.
func joinStatusFor(err error) int {
	if err == nil {
		return http.StatusOK
	}
	switch {
	case errors.Is(err, ErrMarketNotLive),
		errors.Is(err, ErrAlreadyInSession),
		errors.Is(err, funnel.ErrSessionAlreadyLive):
		return http.StatusConflict
	case errors.Is(err, funnel.ErrBrokerTimeout):
		return http.StatusGatewayTimeout
	case errors.Is(err, lifecycle.ErrNoAccount),
		errors.Is(err, lifecycle.ErrLoadFailed),
		errors.Is(err, lifecycle.ErrSnapshotWriteFailed):
		return http.StatusServiceUnavailable
	}
	return http.StatusInternalServerError
}
