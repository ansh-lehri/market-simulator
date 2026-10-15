package conductor

import "errors"

// ErrLiveElsewhere is returned by the canonical-join reserve step when the
// requesting user already holds a session on a DIFFERENT market, or on the
// same market in a non-Active state (Starting / Stopping).
//
// Distinct from ErrAlreadyInSession: a same-market, Active byUser hit is
// NOT an error on the canonical-join door — it is a reconnect, returning
// the existing {SessionID, ParticipantID} unchanged. On-demand keeps the
// strict XOR semantic and continues to surface ErrAlreadyInSession.
//
// Mapped to 409 by the canonical-join status mapper. Callers inspect with
// errors.Is.
var ErrLiveElsewhere = errors.New("conductor: user already has a live session on a different market or in a transient state")
