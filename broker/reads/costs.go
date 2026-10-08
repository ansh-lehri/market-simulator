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
//   PathCosts         — per-trade cost breakdown, keyset-paginated.
//   PathCostsSummary  — account-wide cost aggregate.
//
// Both endpoints require a ?country= param and dispatch on it. The
// handler is deliberately country-agnostic: it sees only the generic
// Line / TradeCostBreakdown / CostAggregate shapes. Column names and
// tax-rate knowledge stay quarantined in the Store + the country
// packages. Adding a new country means a new Store method pair plus
// a new case in the dispatch switch; nothing in the response shape
// changes.
//
// Country handling policy:
//   - "IN" → routes to the built India methods.
//   - "US" → honest 501 (the slot is defined; no implementation yet).
//   - anything else → 400 (not in the allowlist).
//
// Pure durable reads, same error mapping as the trades endpoints:
// ErrLoadFailed → 503, no 504 on durable paths.
const (
	PathCosts        = "/v1/account/costs"
	PathCostsSummary = "/v1/account/costs/summary"
)

// defaultCostsPageLimit mirrors the trades endpoint — the Store caps
// internally, so a larger client limit degrades silently.
const defaultCostsPageLimit = 50

// NewCostsHandler builds the per-trade cost breakdown endpoint.
func NewCostsHandler(durableStore storePort, cfg Config) http.Handler {
	if durableStore == nil {
		panic("broker/reads: NewCostsHandler requires a non-nil storePort")
	}
	if err := cfg.Validate(); err != nil {
		panic("broker/reads: NewCostsHandler: " + err.Error())
	}
	return &costsHandler{durableStore: durableStore, cfg: cfg}
}

// NewCostsSummaryHandler builds the account-wide cost aggregate
// endpoint. Separate handler so the aggregate isn't recomputed on
// every per-trade page the client walks.
func NewCostsSummaryHandler(durableStore storePort, cfg Config) http.Handler {
	if durableStore == nil {
		panic("broker/reads: NewCostsSummaryHandler requires a non-nil storePort")
	}
	if err := cfg.Validate(); err != nil {
		panic("broker/reads: NewCostsSummaryHandler: " + err.Error())
	}
	return &costsSummaryHandler{durableStore: durableStore, cfg: cfg}
}

type costsHandler struct {
	durableStore storePort
	cfg          Config
}

// ServeHTTP walks one page of per-trade cost breakdowns. Pagination
// cursor is identical to the trades endpoint — the Store's query
// joins participant_trades, so pt.exit_at is the keyset key.
func (handlerInstance *costsHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathCosts)

	if !requireGET(responseWriter, request, PathCosts) {
		return
	}
	accountID, ok := parseAccountID(request)
	if !ok {
		slog.Debug("broker/reads: 400 missing account_id", "path", PathCosts)
		writeError(responseWriter, http.StatusBadRequest, "account_id required", PathCosts)
		return
	}
	country, ok := parseCountry(request)
	if !ok {
		slog.Debug("broker/reads: 400 missing country", "path", PathCosts)
		writeError(responseWriter, http.StatusBadRequest, "country required", PathCosts)
		return
	}
	limit, ok := parseLimit(request, defaultCostsPageLimit)
	if !ok {
		slog.Debug("broker/reads: 400 bad limit", "path", PathCosts, "raw", request.URL.Query().Get("limit"))
		writeError(responseWriter, http.StatusBadRequest, "limit must be a positive integer", PathCosts)
		return
	}
	before, ok := parseBefore(request)
	if !ok {
		slog.Debug("broker/reads: 400 bad before cursor", "path", PathCosts, "raw", request.URL.Query().Get("before"))
		writeError(responseWriter, http.StatusBadRequest, "before must be an RFC3339 timestamp", PathCosts)
		return
	}

	requestCtx, cancel := context.WithTimeout(request.Context(), handlerInstance.cfg.ReadTimeout)
	defer cancel()

	slog.Debug("broker/reads: costs begin",
		"account_id", accountID,
		"country", country,
		"limit", limit,
		"before", before,
	)

	var (
		breakdowns []store.TradeCostBreakdown
		readErr    error
	)
	switch country {
	case "IN":
		breakdowns, readErr = handlerInstance.durableStore.CostsPerTradeIN(requestCtx, store.AccountID(accountID), limit, before)
	case "US":
		slog.Info("broker/reads: 501 costs US not implemented", "account_id", accountID)
		writeError(responseWriter, http.StatusNotImplemented, "country not available", PathCosts)
		return
	default:
		slog.Debug("broker/reads: 400 unknown country", "country", country)
		writeError(responseWriter, http.StatusBadRequest, "unknown country", PathCosts)
		return
	}

	switch {
	case errors.Is(readErr, store.ErrLoadFailed):
		slog.Error("broker/reads: 503 costs store unavailable", "account_id", accountID, "err", readErr)
		writeError(responseWriter, http.StatusServiceUnavailable, "store unavailable", PathCosts)
	case readErr != nil:
		slog.Error("broker/reads: 500 costs unexpected error", "account_id", accountID, "err", readErr)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", PathCosts)
	default:
		var nextBefore *time.Time
		// A full page means older rows may still exist beyond this
		// cursor; a short or empty page means we've reached the end.
		// The Store orders by pt.exit_at DESC, so the last row on the
		// page carries the oldest exit_at — the cursor the client
		// feeds back as before= on the next request.
		if len(breakdowns) == limit && len(breakdowns) > 0 {
			lastExitAt := breakdowns[len(breakdowns)-1].ExitAt
			nextBefore = &lastExitAt
		}
		slog.Info("broker/reads: 200 costs",
			"account_id", accountID,
			"country", country,
			"limit", limit,
			"row_count", len(breakdowns),
			"has_more", nextBefore != nil,
		)
		writeJSON(responseWriter, http.StatusOK,
			map[string]any{
				"trades":      shapeCostBreakdowns(breakdowns),
				"next_before": nextBefore,
			},
			PathCosts)
	}
}

type costsSummaryHandler struct {
	durableStore storePort
	cfg          Config
}

// ServeHTTP returns the account-wide cost aggregate — trade count,
// per-line sums (zero lines dropped), and the summed total. The total
// is the stored trade_costs.total column (summed by SQL), never
// recomputed here.
func (handlerInstance *costsSummaryHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathCostsSummary)

	if !requireGET(responseWriter, request, PathCostsSummary) {
		return
	}
	accountID, ok := parseAccountID(request)
	if !ok {
		slog.Debug("broker/reads: 400 missing account_id", "path", PathCostsSummary)
		writeError(responseWriter, http.StatusBadRequest, "account_id required", PathCostsSummary)
		return
	}
	country, ok := parseCountry(request)
	if !ok {
		slog.Debug("broker/reads: 400 missing country", "path", PathCostsSummary)
		writeError(responseWriter, http.StatusBadRequest, "country required", PathCostsSummary)
		return
	}

	requestCtx, cancel := context.WithTimeout(request.Context(), handlerInstance.cfg.ReadTimeout)
	defer cancel()

	slog.Debug("broker/reads: costs summary begin",
		"account_id", accountID,
		"country", country,
	)

	var (
		aggregate store.CostAggregate
		readErr   error
	)
	switch country {
	case "IN":
		aggregate, readErr = handlerInstance.durableStore.CostsAggregateIN(requestCtx, store.AccountID(accountID))
	case "US":
		slog.Info("broker/reads: 501 costs summary US not implemented", "account_id", accountID)
		writeError(responseWriter, http.StatusNotImplemented, "country not available", PathCostsSummary)
		return
	default:
		slog.Debug("broker/reads: 400 unknown country", "country", country)
		writeError(responseWriter, http.StatusBadRequest, "unknown country", PathCostsSummary)
		return
	}

	switch {
	case errors.Is(readErr, store.ErrLoadFailed):
		slog.Error("broker/reads: 503 costs summary store unavailable", "account_id", accountID, "err", readErr)
		writeError(responseWriter, http.StatusServiceUnavailable, "store unavailable", PathCostsSummary)
	case readErr != nil:
		slog.Error("broker/reads: 500 costs summary unexpected error", "account_id", accountID, "err", readErr)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", PathCostsSummary)
	default:
		slog.Info("broker/reads: 200 costs summary",
			"account_id", accountID,
			"country", country,
			"trade_count", aggregate.TradeCount,
			"total", aggregate.Total,
		)
		writeJSON(responseWriter, http.StatusOK,
			map[string]any{
				"trade_count": aggregate.TradeCount,
				"lines":       shapeLines(aggregate.Lines),
				"total":       aggregate.Total,
			},
			PathCostsSummary)
	}
}

// parseCountry reads the required ?country= query param. Case-
// sensitive on purpose: the dispatch switch above uses the canonical
// two-letter form, and a forgiving match would hide client bugs
// (lowercase "in" is a client error, not a shorthand).
func parseCountry(request *http.Request) (string, bool) {
	country := request.URL.Query().Get("country")
	return country, country != ""
}
