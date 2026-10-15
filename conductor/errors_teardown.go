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

// ErrSessionNotFound is returned by the leave flow when the requested
// SessionID has no entry in the conductor's registry. The leave handler
// maps it to HTTP 404. Treating as "nothing to do, respond cleanly" is
// deliberate: a client replaying a stale leave request (lost network)
// against a session the server already reaped should not see a 500.
var ErrSessionNotFound = errors.New("conductor: session not found")

// ErrSessionNotActive is returned by the leave flow when the session
// exists but is NOT in the Active state (Starting / Stopping / Ended).
// Prevents a double-leave (a second POST while the first is in flight),
// a leave against an in-flight close (session Stopping because
// closeCanonicalMarket is sweeping), or a stale rejoin confusion. The
// handler maps it to HTTP 409 so the client can refresh its view.
var ErrSessionNotActive = errors.New("conductor: session is not in Active state")
