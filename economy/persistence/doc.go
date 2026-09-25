// Package persistence is economy's imperative shell — the sole owner of the
// SQL for economy's three durable tables (accounts, participant_trades,
// trade_costs) and the sole writer to Redis for the snapshot mirror and the
// closed-trade write-behind buffer.
//
// Spec: docs/economy/LAYER_5_PERSISTENCE.md is the authority for this
// component; the schema doc docs/economy/LAYER_5_2_SCHEMA.md fixes the exact
// columns/constraints. Everything here binds verbatim to those two.
//
// Stage 1 is I/O-free: types, keys, translation, encoding. Stage 2 adds the
// SQL layer; Stage 3 the seam impl; Stage 4 the async drainer.
package persistence
