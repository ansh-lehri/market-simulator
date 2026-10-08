package reads

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"simulator/broker/store"
)

// Routes this file owns:
//
//   PathTrades  — one page of closed trades, keyset-paginated.
//   PathPnL     — the account-wide P&L aggregate.
//
// Both are pure durable reads. The handler holds only a storePort —
// no funnel, no shard, no live-session concept. A new/unknown account
// returns zero rows (or a zero-filled summary) with 200; empty is
// never an error here. ErrLoadFailed (any store-level failure,
// including a ctx timeout) maps to 503, matching the transfer
// package's durable branch — durable reads never surface as 504.
const (
	PathTrades = "/v1/account/trades"
	PathPnL    = "/v1/account/pnl"
)

// defaultTradePageLimit is the default row count when the client
// omits ?limit=. The Store caps internally, so a client requesting
// more degrades silently to the Store's cap.
const defaultTradePageLimit = 50

// NewTradesHandler builds the paginated trades endpoint. durableStore
// is enough on its own — no funnel involvement.
func NewTradesHandler(durableStore storePort, cfg Config) http.Handler {
	if durableStore == nil {
		panic("broker/reads: NewTradesHandler requires a non-nil storePort")
	}
	if err := cfg.Validate(); err != nil {
		panic("broker/reads: NewTradesHandler: " + err.Error())
	}
	return &tradesHandler{durableStore: durableStore, cfg: cfg}
}

// NewPnLHandler builds the account-wide P&L summary endpoint. It is a
// separate handler so the aggregate isn't recomputed on every trade
// page the client walks.
func NewPnLHandler(durableStore storePort, cfg Config) http.Handler {
	if durableStore == nil {
		panic("broker/reads: NewPnLHandler requires a non-nil storePort")
	}
	if err := cfg.Validate(); err != nil {
		panic("broker/reads: NewPnLHandler: " + err.Error())
	}
	return &pnlHandler{durableStore: durableStore, cfg: cfg}
}

type tradesHandler struct {
	durableStore storePort
	cfg          Config
}

// ServeHTTP walks one page of closed trades for the account. Reads
// account_id (required → 400), an optional limit (default 50; a
// client-passed 0 or negative is rejected to keep the two intents
// — "default" and "empty" — distinguishable), and an optional
// RFC3339 before cursor.
//
// Pagination: when a full page comes back the response advertises
// next_before = last row's exit_at so the client can walk older
// trades. A short or empty page returns next_before:null meaning
// "no more rows beyond this cursor" — the walk ends.
func (handlerInstance *tradesHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathTrades)

	if !requireGET(responseWriter, request, PathTrades) {
		return
	}
	accountID, ok := parseAccountID(request)
	if !ok {
		slog.Debug("broker/reads: 400 missing account_id", "path", PathTrades)
		writeError(responseWriter, http.StatusBadRequest, "account_id required", PathTrades)
		return
	}
	limit, ok := parseLimit(request, defaultTradePageLimit)
	if !ok {
		slog.Debug("broker/reads: 400 bad limit", "path", PathTrades, "raw", request.URL.Query().Get("limit"))
		writeError(responseWriter, http.StatusBadRequest, "limit must be a positive integer", PathTrades)
		return
	}
	before, ok := parseBefore(request)
	if !ok {
		slog.Debug("broker/reads: 400 bad before cursor", "path", PathTrades, "raw", request.URL.Query().Get("before"))
		writeError(responseWriter, http.StatusBadRequest, "before must be an RFC3339 timestamp", PathTrades)
		return
	}

	requestCtx, cancel := context.WithTimeout(request.Context(), handlerInstance.cfg.ReadTimeout)
	defer cancel()

	slog.Debug("broker/reads: trades begin",
		"account_id", accountID,
		"limit", limit,
		"before", before,
	)
	rows, readErr := handlerInstance.durableStore.TradePage(requestCtx, store.AccountID(accountID), limit, before)

	switch {
	case errors.Is(readErr, store.ErrLoadFailed):
		slog.Error("broker/reads: 503 trades store unavailable", "account_id", accountID, "err", readErr)
		writeError(responseWriter, http.StatusServiceUnavailable, "store unavailable", PathTrades)
	case readErr != nil:
		slog.Error("broker/reads: 500 trades unexpected error", "account_id", accountID, "err", readErr)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", PathTrades)
	default:
		var nextBefore *time.Time
		// A full page means more rows may exist past this cursor; a
		// short page (or an empty one) means we've reached the end.
		if len(rows) == limit && len(rows) > 0 {
			lastExitAt := rows[len(rows)-1].ExitAt
			nextBefore = &lastExitAt
		}
		slog.Info("broker/reads: 200 trades",
			"account_id", accountID,
			"limit", limit,
			"row_count", len(rows),
			"has_more", nextBefore != nil,
		)
		writeJSON(responseWriter, http.StatusOK,
			map[string]any{
				"trades":      shapeTrades(rows),
				"next_before": nextBefore,
			},
			PathTrades)
	}
}

type pnlHandler struct {
	durableStore storePort
	cfg          Config
}

// ServeHTTP returns the account-wide P&L aggregate — sums of gross
// P&L, costs, net P&L, plus trade / win / loss counts. Breakevens
// (net_pnl == 0) are counted in neither win nor loss, matching the
// Store's SQL. A new/unknown account returns a zero-filled summary
// with 200.
func (handlerInstance *pnlHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathPnL)

	if !requireGET(responseWriter, request, PathPnL) {
		return
	}
	accountID, ok := parseAccountID(request)
	if !ok {
		slog.Debug("broker/reads: 400 missing account_id", "path", PathPnL)
		writeError(responseWriter, http.StatusBadRequest, "account_id required", PathPnL)
		return
	}

	requestCtx, cancel := context.WithTimeout(request.Context(), handlerInstance.cfg.ReadTimeout)
	defer cancel()

	slog.Debug("broker/reads: pnl begin", "account_id", accountID)
	summary, readErr := handlerInstance.durableStore.PnLSummary(requestCtx, store.AccountID(accountID))

	switch {
	case errors.Is(readErr, store.ErrLoadFailed):
		slog.Error("broker/reads: 503 pnl store unavailable", "account_id", accountID, "err", readErr)
		writeError(responseWriter, http.StatusServiceUnavailable, "store unavailable", PathPnL)
	case readErr != nil:
		slog.Error("broker/reads: 500 pnl unexpected error", "account_id", accountID, "err", readErr)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", PathPnL)
	default:
		slog.Info("broker/reads: 200 pnl",
			"account_id", accountID,
			"trade_count", summary.TradeCount,
			"win_count", summary.WinCount,
			"loss_count", summary.LossCount,
		)
		writeJSON(responseWriter, http.StatusOK,
			map[string]any{
				"trade_count": summary.TradeCount,
				"gross_pnl":   summary.GrossPnl,
				"total_costs": summary.TotalCosts,
				"net_pnl":     summary.NetPnl,
				"win_count":   summary.WinCount,
				"loss_count":  summary.LossCount,
			},
			PathPnL)
	}
}
