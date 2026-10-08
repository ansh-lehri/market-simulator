package reads

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"simulator/broker/funnel"
)

const PathOrders = "/v1/account/orders"

func NewOrdersHandler(broker brokerPort, cfg Config) http.Handler {
	if broker == nil {
		panic("broker/reads: NewOrdersHandler requires a non-nil brokerPort")
	}
	if err := cfg.Validate(); err != nil {
		panic("broker/reads: NewOrdersHandler: " + err.Error())
	}
	return &ordersHandler{broker: broker, cfg: cfg}
}

type ordersHandler struct {
	broker brokerPort
	cfg    Config
}

// ServeHTTP returns the live working-set only — orders still working or
// partially filled. Terminal orders are deleted from the shard at
// settlement, so the shard already filters by construction. Trade
// history lives on the trades endpoint, which reads from Postgres.
func (handlerInstance *ordersHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathOrders)

	if !requireGET(responseWriter, request, PathOrders) {
		return
	}
	accountID, ok := parseAccountID(request)
	if !ok {
		slog.Debug("broker/reads: 400 missing account_id", "path", PathOrders)
		writeError(responseWriter, http.StatusBadRequest, "account_id required", PathOrders)
		return
	}

	requestCtx, cancel := context.WithTimeout(request.Context(), handlerInstance.cfg.ReadTimeout)
	defer cancel()

	slog.Debug("broker/reads: orders begin", "account_id", accountID)
	snapshot, readErr := handlerInstance.broker.ReadOpenOrders(requestCtx, funnel.AccountID(accountID))

	switch {
	case errors.Is(readErr, funnel.ErrBrokerTimeout):
		slog.Warn("broker/reads: 504 orders timeout", "account_id", accountID)
		writeError(responseWriter, http.StatusGatewayTimeout, "orders read timed out", PathOrders)
	case readErr != nil:
		slog.Error("broker/reads: 500 unexpected broker error", "account_id", accountID, "err", readErr)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", PathOrders)
	case !snapshot.Present:
		slog.Debug("broker/reads: 409 no live session", "account_id", accountID)
		writeError(responseWriter, http.StatusConflict, "no live session for account", PathOrders)
	default:
		slog.Info("broker/reads: 200 orders",
			"account_id", accountID,
			"row_count", len(snapshot.Orders),
		)
		writeJSON(responseWriter, http.StatusOK,
			map[string]any{"orders": shapeOrders(snapshot.Orders)},
			PathOrders)
	}
}
