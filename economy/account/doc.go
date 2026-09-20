// Package account is Layer 5.1 — the account state model plus the six write
// hands and two hot-path reads that mutate/query one participant's live money
// truth during a session.
//
// It is a leaf package: no economy deps, no I/O, no goroutine of its own. It
// assumes single-writer serialization by the broker funnel and does not enforce
// it. Money is integer coins (int64); prices are integer ticks (int64); every
// amount/units argument is strictly positive — direction is carried by which
// hand and which buckets, never by the sign of an argument.
//
// Two §0.6 coupling invariants are asserted (panic on violation, per root
// constitution):
//
//  1. Cash conservation across pure transfers — asserted in MoveCash.
//  2. Position–lot coupling: Available+Reserved == Σ Lot.RemainingQty —
//     asserted in RemovePosition.
//
// Spec: docs/economy/LAYER_5_1_OPERATIONS.md §0–§1 and
// docs/economy/LAYER_5_1_READ_OPERATIONS.md items 14/15.
package account
