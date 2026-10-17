package reads

import (
	"context"
	"time"

	"simulator/broker/funnel"
	"simulator/broker/store"
)

// brokerPort is the minimal shape the live-read handlers need from the
// funnel broker — one method per endpoint, each a one-to-one mirror of
// a facade on *funnel.Broker.
//
// One joined interface rather than four small ones: the observation
// handler calls ReadObservation and also talks to the store, so it
// already needs both ports; splitting brokerPort would force it to
// depend on two broker-side interfaces that must happen to be
// satisfied by the same object (an awkwardness Go doesn't express
// well). A handler that only uses one of the four methods stays
// dependency-honest via the "unused method on a stub is fine" idiom.
//
// *funnel.Broker satisfies this interface implicitly; nothing on the
// funnel changes.
//
// The interface lives on the consumer side (this package), not inside
// broker/funnel/. Go idiom: interfaces belong on the caller, so the
// producer stays unaware of the HTTP shape — same pattern as the
// transfer and order packages.
type brokerPort interface {
	// ReadBalance forwards to the owning shard's balance handler and
	// blocks on the reply or the ctx deadline, returning
	// funnel.ErrBrokerTimeout on ctx.Done.
	ReadBalance(
		ctx context.Context,
		accountID funnel.AccountID,
	) (funnel.BalanceSnapshot, error)

	// ReadPositions forwards to the positions handler with an optional
	// single-instrument filter — "" means every held instrument.
	ReadPositions(
		ctx context.Context,
		accountID funnel.AccountID,
		instrument funnel.InstrumentID,
	) (funnel.PositionsSnapshot, error)

	// ReadOpenOrders forwards to the open-orders handler. Live-only —
	// trade history lives behind the durable trades endpoint.
	ReadOpenOrders(
		ctx context.Context,
		accountID funnel.AccountID,
	) (funnel.OpenOrdersSnapshot, error)

	// ReadObservation runs balance, positions and open-orders in one
	// visit to the owning shard so the three sub-payloads cannot
	// interleave with a fill. The durable "recent closed trades" half
	// of the observation body is merged in by the HTTP handler, not
	// here.
	ReadObservation(
		ctx context.Context,
		accountID funnel.AccountID,
	) (funnel.ObservationLive, error)
}

// storePort is the minimal shape the durable-read handlers need from
// the Store — five methods, each a one-to-one mirror of a built store
// method.
//
// Country-specific method names (CostsPerTradeIN / CostsAggregateIN)
// surface here deliberately: the Store already quarantines India
// column knowledge in those two methods, and the costs handler
// dispatches to them under country=="IN". Adding a new country means
// a new Store method plus a new dispatch case — the generic handler
// shape stays country-agnostic.
//
// *store.Store satisfies this interface implicitly.
type storePort interface {
	// RecentTrades returns the N newest closed trades for the account,
	// ordered newest-first. The observation handler asks for 20; the
	// Store also caps internally.
	RecentTrades(
		ctx context.Context,
		accountID store.AccountID,
		requestedN int,
	) ([]store.TradeRow, error)

	// TradePage returns one keyset-paginated page. before=nil requests
	// the newest page; a non-nil before means "trades strictly older
	// than this exit_at" (the next-page cursor the previous response
	// handed out).
	TradePage(
		ctx context.Context,
		accountID store.AccountID,
		requestedLimit int,
		before *time.Time,
	) ([]store.TradeRow, error)

	// PnLSummary returns the account-wide aggregate — a projection
	// over already-stored columns, no recomputation.
	PnLSummary(
		ctx context.Context,
		accountID store.AccountID,
	) (store.PnLSummary, error)

	// CostsPerTradeIN returns India per-trade cost breakdowns, keyset-
	// paginated identically to TradePage. India-only by Store design;
	// the costs handler routes here on country=="IN".
	CostsPerTradeIN(
		ctx context.Context,
		accountID store.AccountID,
		requestedLimit int,
		before *time.Time,
	) ([]store.TradeCostBreakdown, error)

	// CostsAggregateIN returns the India account-level cost total plus
	// a trade count. Projection; no recomputation.
	CostsAggregateIN(
		ctx context.Context,
		accountID store.AccountID,
	) (store.CostAggregate, error)

	// AccountCash returns the durable (trading_cash, bank_cash) pair for
	// one account — the pre-session cash view. Serves the signup/login
	// flow where the user has an account_id but no live session yet, so
	// the live balance read (ReadBalance → 409 without a session) cannot
	// answer. Pure Postgres read; the Store returns raw signed int64 for
	// trading_cash (debt is legal) and shaping stays on the HTTP side.
	AccountCash(
		ctx context.Context,
		accountID store.AccountID,
	) (tradingCash int64, bankCash int64, err error)
}
