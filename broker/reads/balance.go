package reads

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"simulator/broker/funnel"
)

// PathBalance is the GET route this handler owns. Exported so the mux
// wiring registers it without duplicating the string.
const PathBalance = "/v1/account/balance"

// NewBalanceHandler wires the balance endpoint. A nil broker or an
// invalid Config is a bringup mistake, caught here with a panic rather
// than left to nil-deref on the first request.
func NewBalanceHandler(broker brokerPort, cfg Config) http.Handler {
	if broker == nil {
		panic("broker/reads: NewBalanceHandler requires a non-nil brokerPort")
	}
	if err := cfg.Validate(); err != nil {
		panic("broker/reads: NewBalanceHandler: " + err.Error())
	}
	return &balanceHandler{broker: broker, cfg: cfg}
}

type balanceHandler struct {
	broker brokerPort
	cfg    Config
}

// ServeHTTP runs the balance read:
//   - panic guard, GET-only, account_id required (→ 400),
//   - round-trip to the funnel bounded by cfg.ReadTimeout,
//   - map the four possible outcomes:
//       504 — the shard didn't reply in time,
//       500 — any other unexpected error,
//       409 — the account has no live session on any shard,
//       200 — the four Cash buckets in the response body.
func (handlerInstance *balanceHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathBalance)

	if !requireGET(responseWriter, request, PathBalance) {
		return
	}
	accountID, ok := parseAccountID(request)
	if !ok {
		slog.Debug("broker/reads: 400 missing account_id", "path", PathBalance)
		writeError(responseWriter, http.StatusBadRequest, "account_id required", PathBalance)
		return
	}

	requestCtx, cancel := context.WithTimeout(request.Context(), handlerInstance.cfg.ReadTimeout)
	defer cancel()

	slog.Debug("broker/reads: balance begin", "account_id", accountID)
	snapshot, readErr := handlerInstance.broker.ReadBalance(requestCtx, funnel.AccountID(accountID))

	switch {
	case errors.Is(readErr, funnel.ErrBrokerTimeout):
		slog.Warn("broker/reads: 504 balance timeout", "account_id", accountID)
		writeError(responseWriter, http.StatusGatewayTimeout, "balance read timed out", PathBalance)
	case readErr != nil:
		slog.Error("broker/reads: 500 unexpected broker error", "account_id", accountID, "err", readErr)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", PathBalance)
	case !snapshot.Present:
		slog.Debug("broker/reads: 409 no live session", "account_id", accountID)
		writeError(responseWriter, http.StatusConflict, "no live session for account", PathBalance)
	default:
		slog.Info("broker/reads: 200 balance",
			"account_id", accountID,
			"available", snapshot.Available,
			"reserved", snapshot.Reserved,
			"settled", snapshot.Settled,
			"bank", snapshot.Bank,
		)
		writeJSON(responseWriter, http.StatusOK, shapeBalance(snapshot), PathBalance)
	}
}
