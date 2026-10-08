package reads

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"

	"simulator/broker/funnel"
	"simulator/broker/store"
)

const PathObservation = "/v1/account/observation"

// observationRecentTradesLimit caps the recent-trades slice this endpoint
// requests. The Store caps internally too; pinning the value at the
// request site documents the limit next to the only call that uses it.
const observationRecentTradesLimit = 20

// NewObservationHandler takes both ports because this endpoint merges a
// live snapshot (balance + positions + working orders) from the funnel
// with recently closed trades from Postgres.
func NewObservationHandler(broker brokerPort, durableStore storePort, cfg Config) http.Handler {
	if broker == nil {
		panic("broker/reads: NewObservationHandler requires a non-nil brokerPort")
	}
	if durableStore == nil {
		panic("broker/reads: NewObservationHandler requires a non-nil storePort")
	}
	if err := cfg.Validate(); err != nil {
		panic("broker/reads: NewObservationHandler: " + err.Error())
	}
	return &observationHandler{broker: broker, durableStore: durableStore, cfg: cfg}
}

type observationHandler struct {
	broker       brokerPort
	durableStore storePort
	cfg          Config
}

// ServeHTTP fans the two reads out concurrently against a single
// request-scoped context. The live snapshot (balance + positions + open
// orders) is one atomic call to the owning shard — a single visit so
// the three sub-payloads cannot interleave with a fill. The recent-
// trades read hits Postgres on another goroutine.
//
// Only the live leg drives the error status: a shard timeout → 504, a
// shard failure → 500, a missing session → 409. The recent-trades leg
// degrades honestly: a durable failure returns an empty trades slice
// with trades_degraded:true in the body, still 200. The client always
// gets whatever live state is available.
func (handlerInstance *observationHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathObservation)

	if !requireGET(responseWriter, request, PathObservation) {
		return
	}
	accountID, ok := parseAccountID(request)
	if !ok {
		slog.Debug("broker/reads: 400 missing account_id", "path", PathObservation)
		writeError(responseWriter, http.StatusBadRequest, "account_id required", PathObservation)
		return
	}

	requestCtx, cancel := context.WithTimeout(request.Context(), handlerInstance.cfg.ReadTimeout)
	defer cancel()

	slog.Debug("broker/reads: observation begin", "account_id", accountID)

	var (
		live      funnel.ObservationLive
		liveErr   error
		trades    []store.TradeRow
		tradesErr error
	)
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)
	go func() {
		defer waitGroup.Done()
		live, liveErr = handlerInstance.broker.ReadObservation(requestCtx, funnel.AccountID(accountID))
	}()
	go func() {
		defer waitGroup.Done()
		trades, tradesErr = handlerInstance.durableStore.RecentTrades(requestCtx, store.AccountID(accountID), observationRecentTradesLimit)
	}()
	waitGroup.Wait()

	switch {
	case errors.Is(liveErr, funnel.ErrBrokerTimeout):
		slog.Warn("broker/reads: 504 observation timeout", "account_id", accountID)
		writeError(responseWriter, http.StatusGatewayTimeout, "observation read timed out", PathObservation)
		return
	case liveErr != nil:
		slog.Error("broker/reads: 500 unexpected broker error", "account_id", accountID, "err", liveErr)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", PathObservation)
		return
	case !live.Present:
		slog.Debug("broker/reads: 409 no live session", "account_id", accountID)
		writeError(responseWriter, http.StatusConflict, "no live session for account", PathObservation)
		return
	}

	response := map[string]any{
		"balance":   shapeBalance(live.Balance),
		"positions": shapePositions(live.Positions),
		"orders":    shapeOrders(live.OpenOrders),
	}
	if tradesErr != nil {
		slog.Warn("broker/reads: observation trades degraded",
			"account_id", accountID,
			"err", tradesErr,
		)
		response["recent_trades"] = []map[string]any{}
		response["trades_degraded"] = true
	} else {
		response["recent_trades"] = shapeTrades(trades)
		response["trades_degraded"] = false
	}
	slog.Info("broker/reads: 200 observation",
		"account_id", accountID,
		"position_rows", len(live.Positions),
		"order_rows", len(live.OpenOrders),
		"trade_rows", len(trades),
		"trades_degraded", tradesErr != nil,
	)
	writeJSON(responseWriter, http.StatusOK, response, PathObservation)
}
