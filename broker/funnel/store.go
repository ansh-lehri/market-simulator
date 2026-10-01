package funnel

// Store is the broker-owned durable layer — a single, shard-independent
// component that owns its own Postgres pool and hosts every off-shard
// durable operation (BROKER_PHASE1 §2 / §5). Its full shape and methods
// (AccountHandle, RecentTrades / TradePage, PnLSummary,
// CostsPerTradeIN / CostsAggregateIN, DurableTransfer) are Phase 1.
//
// Phase 0 declares only the type so NewBroker's signature is stable —
// callers pass either a nil *Store (Phase-0 tests, no durable ops) or a
// real *Store constructed by Phase-1's BringUp. Adding a field or method
// here is a Phase-1 change, not a Phase-0 one.
type Store struct{}
