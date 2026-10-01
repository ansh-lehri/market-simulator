// Package funnel is the broker's Tier-0 concurrency funnel: the sharded
// single-writer machine every money-touching flow runs inside.
//
// It owns the machine only — the shard pool, the router, the serial loop,
// the reply mechanism, the co-located session binding, and the per-shard
// L5 wiring seam. Every handler body (order intake, cancel, settlement,
// bank moves, reads, stop fires) lives in a later tier; the funnel keeps
// those seams empty and clearly marked.
//
// The load-bearing rule is single-writer per account: an AccountID hashes
// deterministically to one shard, and only that shard's goroutine ever
// touches the account's state, its OrderTracks, or the L5 hands for it.
// Callers hit Broker methods; the shard/router split is internal.
//
// See docs/broker/BROKER_TIER0_IMPLEMENTATION_DESIGN.md for the full
// design, and broker/funnel/CLAUDE.md for the domain rules.
package funnel
