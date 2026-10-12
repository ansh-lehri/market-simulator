package conductor

// Conductor-owned opaque identifier types. Named types (not aliases)
// so the type checker rejects slot mix-ups (a MarketID in a UserID
// slot won't compile). All resolve to string underneath.
//
// AccountID is a NEW named type here — deliberately NOT a type alias
// to funnel.AccountID — so that registry/session code can declare an
// AccountID field without pulling broker/funnel into this file. The
// broker still owns the identity; the conductor just carries the
// handle. The two or three broker touchpoints that cross back convert
// explicitly with funnel.AccountID(string(sess.Account)) at the call
// site.
type (
	MarketID      string
	MarketTag     string
	SessionID     string
	UserID        string
	ParticipantID string
	AccountID     string
)
