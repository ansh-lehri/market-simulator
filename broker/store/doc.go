// Package store is the broker-owned, shard-independent durable layer
// (BROKER_PHASE1 §2 / §5). It owns its own Postgres pool, parallel to the
// per-shard bundles' pools, and hosts every off-shard durable operation:
//
//   - AccountHandle              — owner-identity → account_id lookup
//   - RecentTrades / TradePage   — closed-trade reads (Stage B)
//   - PnLSummary                 — projection over participant_trades
//   - CostsPerTradeIN /
//     CostsAggregateIN           — India-only cost reads (country knowledge
//                                   quarantined inside these two methods)
//   - DurableTransfer            — off-session bank↔trading move (Stage C)
//
// Load-bearing rules (read-order: /CLAUDE.md → broker/CLAUDE.md →
// broker/store/CLAUDE.md → BROKER_PHASE1_IMPLEMENTATION_DESIGN.md):
//
//   - Own pool. No Redis, no shard references, no economy calls. The pool
//     is handed in by the wiring root (bringup, Phase-1 Stage D) and closed
//     LAST on shutdown, after every per-shard bundle is torn down.
//   - Pure durable reads. Nothing persisted. All reads return raw rows and
//     stored projections — never a recomputed P&L or cost figure. Shaping
//     and formatting are the Phase-6 response layer's job, never here.
//   - Off-session sole-writer. DurableTransfer is the only mutator of the
//     accounts cash columns outside a live session; combined with the
//     live-ON-shard ⊕ off-session XOR invariant, conservation holds without
//     a lock (BROKER_PHASE1 §4.5).
package store
