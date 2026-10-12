package conductor

import "errors"

// Sentinel errors for the canonical-bring-up flow and the remote-call
// ports it drives. Callers inspect categories with errors.Is, so each
// sentinel is a stable comparable value; adapters wrap transport
// specifics on top with %w.
//
// ErrAlreadyUp is returned by the registry's reserve step when a second
// bring-up trigger finds an existing record for the same market id.
// Not a failure — the caller treats it as "nothing to do."
//
// The three orchestrator errors mirror the three error categories a
// real HTTP adapter would surface against the (future, out-of-process)
// agent orchestrator:
//
//   - ErrOrchestratorUnavailable: transport failure or a 5xx response —
//     the orchestrator could not be reached or failed to answer coherently.
//     Retriable at the next arm.
//   - ErrOrchestratorTimeout: the request did not complete within the
//     configured BringUpTimeout, or the caller's context was already
//     cancelled when the call started. Retriable.
//   - ErrAgentBringUpRejected: the orchestrator was reached and
//     explicitly refused — a 4xx response. Not retriable without
//     changing the request shape.
var (
	ErrAlreadyUp               = errors.New("conductor: market already provisioning or live")
	ErrOrchestratorUnavailable = errors.New("conductor: agent orchestrator unavailable")
	ErrOrchestratorTimeout     = errors.New("conductor: agent orchestrator timeout")
	ErrAgentBringUpRejected    = errors.New("conductor: agent bring-up rejected")
)
