package reads

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"simulator/broker/funnel"
)

const PathPositions = "/v1/account/positions"

func NewPositionsHandler(broker brokerPort, cfg Config) http.Handler {
	if broker == nil {
		panic("broker/reads: NewPositionsHandler requires a non-nil brokerPort")
	}
	if err := cfg.Validate(); err != nil {
		panic("broker/reads: NewPositionsHandler: " + err.Error())
	}
	return &positionsHandler{broker: broker, cfg: cfg}
}

type positionsHandler struct {
	broker brokerPort
	cfg    Config
}

// ServeHTTP runs the positions read. Same preamble + status mapping as
// the balance endpoint; the one addition is an optional ?instrument=
// filter — empty means "every held instrument". A specified instrument
// the account doesn't hold returns a live-200 with an empty positions
// slice (flat on an instrument is not an error).
func (handlerInstance *positionsHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathPositions)

	if !requireGET(responseWriter, request, PathPositions) {
		return
	}
	accountID, ok := parseAccountID(request)
	if !ok {
		slog.Debug("broker/reads: 400 missing account_id", "path", PathPositions)
		writeError(responseWriter, http.StatusBadRequest, "account_id required", PathPositions)
		return
	}
	instrument := request.URL.Query().Get("instrument")

	requestCtx, cancel := context.WithTimeout(request.Context(), handlerInstance.cfg.ReadTimeout)
	defer cancel()

	slog.Debug("broker/reads: positions begin",
		"account_id", accountID,
		"instrument_filter", instrument,
	)
	snapshot, readErr := handlerInstance.broker.ReadPositions(requestCtx,
		funnel.AccountID(accountID), funnel.InstrumentID(instrument))

	switch {
	case errors.Is(readErr, funnel.ErrBrokerTimeout):
		slog.Warn("broker/reads: 504 positions timeout", "account_id", accountID)
		writeError(responseWriter, http.StatusGatewayTimeout, "positions read timed out", PathPositions)
	case readErr != nil:
		slog.Error("broker/reads: 500 unexpected broker error", "account_id", accountID, "err", readErr)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", PathPositions)
	case !snapshot.Present:
		slog.Debug("broker/reads: 409 no live session", "account_id", accountID)
		writeError(responseWriter, http.StatusConflict, "no live session for account", PathPositions)
	default:
		slog.Info("broker/reads: 200 positions",
			"account_id", accountID,
			"instrument_filter", instrument,
			"row_count", len(snapshot.Positions),
		)
		writeJSON(responseWriter, http.StatusOK,
			map[string]any{"positions": shapePositions(snapshot.Positions)},
			PathPositions)
	}
}
