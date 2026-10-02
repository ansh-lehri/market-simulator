// Package store is the broker-owned, shard-independent durable layer
// (BROKER_PHASE1 §2 / §5 + Phase-2 supersession P2-1·D3). It owns its own
// Postgres pool, its own Redis client, and an UNSTARTED
// economy/lifecycle.Service — all three parallel to the per-shard bundles'
// handles — and hosts every off-shard durable operation:
//
//   - AccountHandle              — owner-identity → account_id lookup (§2)
//   - RecentTrades / TradePage   — closed-trade reads (§3)
//   - PnLSummary                 — projection over participant_trades (§3)
//   - CostsPerTradeIN /
//     CostsAggregateIN           — India-only cost reads (country knowledge
//                                   quarantined inside these two methods)
//   - DurableTransfer            — off-session bank↔trading move (§4)
//   - TradingCash                — Phase-2 signed-balance read, QualifyOnDemand gate (§2 of Phase-2)
//   - Lifecycle()                — exposes the stored *lifecycle.Service so
//                                   Phase-2 EnsureAccount / StartSession can
//                                   call OpenAccount / SessionLoad /
//                                   OndemandSessionLoad off-shard
//
// Load-bearing rules (read-order: /CLAUDE.md → broker/CLAUDE.md →
// broker/store/CLAUDE.md → BROKER_PHASE1_IMPLEMENTATION_DESIGN.md →
// BROKER_PHASE2_IMPLEMENTATION_DESIGN.md):
//
//   - Own pool, own redis, own lifecycle service. No shard references, no
//     economy money math. All three handles are torn down by Store.Close in
//     the correct internal order (redis then pool); the wiring root makes
//     one Store.Close call after Broker.Stop and every per-shard bundle
//     has been torn down.
//   - The persistence.Component the lifecycle service wraps is UNSTARTED by
//     design (P2-1·D3): the Store never calls EnqueueClosedTrade, the
//     closed-trade drainer belongs on the per-shard bundles. No goroutine
//     spawns here; GC reclaims the Component when Runtime drops.
//   - Pure durable reads. Nothing persisted on a read path. All reads return
//     raw rows and stored projections — never a recomputed P&L or cost
//     figure. Shaping and formatting are the Phase-6 response layer's job,
//     never here.
//   - Off-session sole-writer. DurableTransfer is the only mutator of the
//     accounts cash columns outside a live session; combined with the
//     live-ON-shard ⊕ off-session XOR invariant, conservation holds without
//     a lock (BROKER_PHASE1 §4.5). EnsureAccount's OpenAccount and
//     StartSession's loaders respect this XOR by construction (birth
//     precedes any session; loaders only read Postgres and write Redis).
package store
