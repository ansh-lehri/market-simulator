package persistence

import "errors"

// ErrNoAccount is returned by loadAccount / LoadAccount when the row for the
// requested accountID does not exist. Callers use errors.Is to detect it. It
// is a business error (the account simply has not been created yet), not an
// invariant violation.
var ErrNoAccount = errors.New("persistence: account not found")

// ErrUnknownCostLine is returned by translateClosedTrade when the cost
// breakdown carries a key that is not in the India cost-line allowlist
// (D1: "an unrecognized key is an error, not a silent drop"). Callers at the
// seam (EnqueueClosedTrade, added in Stage 3) treat this as a programming
// error — a cost line without a column is a bug in cost/realize, not
// something to swallow.
var ErrUnknownCostLine = errors.New("persistence: unknown cost line")
