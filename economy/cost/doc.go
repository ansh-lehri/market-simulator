// Package cost is Layer 5.3 — the market-agnostic cost & tax engine.
//
// It computes the friction (fees + taxes) a trade incurs, beyond the share
// price itself. It is pure arithmetic — turnover × injected rates — that
// returns a breakdown; it never mutates, never persists, never touches an
// account. The caller debits what this package returns.
//
// The single architectural rule: every country implements the same
// CostModule interface with the same inputs and outputs. A new country is a
// new module plus a new rulebook — never a change to callers or to the
// generic types in this package.
//
// Spec: docs/economy/LAYER_5_3_COST_ENGINE.md.
package cost
