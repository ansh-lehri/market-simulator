package reads

import (
	"context"
	"time"

	"simulator/broker/funnel"
	"simulator/broker/store"
)

// brokerPort is the minimal shape the live-read HTTP handlers need from
// the funnel broker (BROKER_PHASE6 §1.1 ports.go sketch). Four methods —
// each a one-to-one mirror of a Phase-6 funnel facade: A8 ReadBalance, A9
// ReadPositions (with the optional single-instrument filter), A11
// ReadOpenOrders, A14 ReadObservation.
//
// Why one joined interface for all four endpoints: A14's handler also
// fans out to RecentTrades, so it needs both brokerPort and storePort;
// the single-read handlers each need exactly one method. One joined
// interface stays dependency-honest via Go's "unused method on a stub
// is fine" idiom — same precedent as broker/order's brokerPort.
//
// *funnel.Broker satisfies this interface implicitly; nothing on the
// funnel changes.
//
// Scope note: the interface lives on the consumer side (broker/reads/),
// not inside broker/funnel/. Go idiom + same rationale as broker/transfer
// and broker/order: interfaces belong on the caller; the producer stays
// unaware of the HTTP shape.
type brokerPort interface {
	// ReadBalance forwards to the owning shard's handleReadBalance,
	// blocking on the reply or ctx deadline. Returns funnel.ErrBrokerTimeout
	// on ctx.Done.
	ReadBalance(
		ctx context.Context,
		accountID funnel.AccountID,
	) (funnel.BalanceSnapshot, error)

	// ReadPositions forwards to handleReadPositions with the optional
	// single-instrument filter (""==all held).
	ReadPositions(
		ctx context.Context,
		accountID funnel.AccountID,
		instrument funnel.InstrumentID,
	) (funnel.PositionsSnapshot, error)

	// ReadOpenOrders forwards to handleReadOpenOrders. Live-only; A10
	// serves history (durable).
	ReadOpenOrders(
		ctx context.Context,
		accountID funnel.AccountID,
	) (funnel.OpenOrdersSnapshot, error)

	// ReadObservation forwards to handleReadObservation — one atomic
	// shard visit assembling balance + positions + open orders. The
	// durable recent-trades half of A14 is handled by the HTTP handler,
	// not here.
	ReadObservation(
		ctx context.Context,
		accountID funnel.AccountID,
	) (funnel.ObservationLive, error)
}

// storePort is the minimal shape the durable-read HTTP handlers need
// from the broker Store (BROKER_PHASE6 §1.1). Five methods, each a
// one-to-one mirror of a built store method:
//
//   - RecentTrades       — A14 Observation (clamped at 20 by the handler)
//   - TradePage          — A10 trades endpoint (keyset-paginated)
//   - PnLSummary         — A10 pnl endpoint
//   - CostsPerTradeIN    — A16 India per-trade cost breakdown
//   - CostsAggregateIN   — A16 India account-level cost total
//
// Country knowledge (CostsPerTradeIN / CostsAggregateIN method names) is
// deliberate at this seam: the Store already quarantines India column
// knowledge in those two methods, and the A16 handler dispatches to
// them under country=="IN". Adding a new country (US / etc.) means a
// new Store method + a new dispatch case — no change to the generic
// handler shape (BROKER_PHASE6 §6, "country-agnostic handler").
//
// *store.Store satisfies this interface implicitly.
type storePort interface {
	// RecentTrades returns the N newest closed trades for the account,
	// ordered by exit_at DESC. The handler passes 20 for A14 (the Store
	// also caps internally).
	RecentTrades(
		ctx context.Context,
		accountID store.AccountID,
		requestedN int,
	) ([]store.TradeRow, error)

	// TradePage returns one keyset-paginated page of closed trades for
	// the account. before=nil means "newest page"; a non-nil before
	// means "trades strictly older than this exit_at" (next_before
	// cursor from the previous response).
	TradePage(
		ctx context.Context,
		accountID store.AccountID,
		requestedLimit int,
		before *time.Time,
	) ([]store.TradeRow, error)

	// PnLSummary returns the account-wide aggregate (sums of gross,
	// costs, net; win/loss/trade counts). Projection over already-
	// stored columns — no recomputation.
	PnLSummary(
		ctx context.Context,
		accountID store.AccountID,
	) (store.PnLSummary, error)

	// CostsPerTradeIN returns India per-trade cost breakdowns, keyset-
	// paginated identically to TradePage. India-only by Store design;
	// A16's handler routes here on country=="IN".
	CostsPerTradeIN(
		ctx context.Context,
		accountID store.AccountID,
		requestedLimit int,
		before *time.Time,
	) ([]store.TradeCostBreakdown, error)

	// CostsAggregateIN returns the India account-level cost total +
	// trade count. Projection, no recomputation.
	CostsAggregateIN(
		ctx context.Context,
		accountID store.AccountID,
	) (store.CostAggregate, error)
}
