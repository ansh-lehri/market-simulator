package conductor

import "errors"

// ErrOnDemandSetupFailed marks every failure of the held on-demand
// setup path after the per-user reservation succeeded — anything from
// agent populate to exchange construct, wire, Start, or the broker
// StartSession seal. The underlying cause (an orchestrator transport
// error, a broker lifecycle sentinel, a context deadline) is kept via
// %w so callers inspecting with errors.Is can still see it; the
// specific-cause string is also mirrored onto Session.FailReason so a
// fallback status read surfaces what went wrong without re-walking the
// error chain.
//
// The HTTP mapper routes this sentinel to 503 with a {status:"failed",
// reason:"..."} envelope — the setup-timeout path and the real
// mid-setup failure both map here. Gate failure (QualifyOnDemand
// returning false, nil) is NOT a setup failure — it returns a
// first-class result variant and maps to 200 insufficient_funds.
var ErrOnDemandSetupFailed = errors.New("conductor: on-demand setup failed")
