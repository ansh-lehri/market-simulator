// Package lifecycle is Layer 5.2 Tier-2 orchestration: it establishes,
// persists, and restores the in-memory account.Account across session
// boundaries by calling economy/persistence.Component. It decides sequence
// and policy; it holds NO SQL and touches NO driver.
//
// Spec authorities (read in this order, they all apply):
//   1. /CLAUDE.md                          — root project constitution
//   2. /economy/CLAUDE.md                  — money-truth (L5) constitution
//   3. /economy/lifecycle/CLAUDE.md        — this package's constitution
//   4. docs/economy/LAYER_5_2_LIFECYCLE_FLOWS.md — the six flows (18–23)
//   5. docs/economy/LAYER_5_1_READ_OPERATIONS.md — item 16 (read_balance_snapshot) only
//
// The concrete *persistence.Component is the AUTHORITY for the store surface
// and the snapshot shape. Where the lifecycle doc's inline SQL / Redis-key
// sketch differs from persistence's code, the code wins.
//
// Owns (Stages B–F, in build order):
//   - read_balance_snapshot   (item 16, B3)
//   - session_load            (item 19, C1)
//   - session_save            (item 21, C2)
//   - ondemand_session_load   (item 20, D1)
//   - session_discard         (item 22, D2)
//   - open_account            (item 18, E1)
//   - crash_recovery_reload   (item 23, F1 — L5 slice only; cross-layer deferred)
//
// Does NOT: import pgx / redis / reserve / cost; run any goroutine; touch
// per-trade persistence (that's realize → persistence.EnqueueClosedTrade
// during the session; lifecycle is session boundaries only).
package lifecycle
