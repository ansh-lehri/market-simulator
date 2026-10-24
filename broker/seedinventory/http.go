package seedinventory

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"

	"simulator/broker/funnel"
)

// Path is the one route this package owns (BROKER_SEED_INVENTORY §5).
// Exported so the mux-wiring stage can register without duplicating
// the string.
const Path = "/v1/account/seed-inventory"

// seedRequest is the on-wire request shape.
type seedRequest struct {
	AccountID  string `json:"account_id"`
	Instrument string `json:"instrument"`
	Qty        int64  `json:"qty"`
	EntryPrice int64  `json:"entry_price"`
}

// seedResponse is the 200 success body.
type seedResponse struct {
	Instrument string `json:"instrument"`
	Available  int64  `json:"available"`
}

// errorBody is the plain error envelope for every non-200 response.
type errorBody struct {
	Error string `json:"error"`
}

// NewHandler builds the one POST /v1/account/seed-inventory handler.
// The factory signature is deliberately narrow — a broker port and the
// one timeout dial. The port is satisfied by the real *funnel.Broker in
// production; tests inject a stub for the branches a live runtime
// can't reach deterministically (ctx-timeout 504, pre-funnel 400s).
//
// Nil broker at construction is a programming error at the caller
// (bringup); panic early rather than defer a nil-deref to the first
// request.
func NewHandler(broker brokerPort, cfg Config) http.Handler {
	if broker == nil {
		panic("broker/seedinventory: NewHandler requires a non-nil brokerPort")
	}
	if err := cfg.Validate(); err != nil {
		panic("broker/seedinventory: NewHandler: " + err.Error())
	}
	return &handler{broker: broker, cfg: cfg}
}

type handler struct {
	broker brokerPort
	cfg    Config
}

// ServeHTTP implements the full flow (BROKER_SEED_INVENTORY §5): method
// guard → recover → parse + pre-funnel validate → ctx-timeout →
// broker.SeedInventory → status dispatch.
//
// Panic policy: the shard loop runs on its own goroutine, so a
// request-side panic here does NOT corrupt shard state. We recover at
// the HTTP boundary, log the stack, write a 500, and do NOT re-panic.
func (handlerInstance *handler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("broker/seedinventory: handler panic",
				"cause", recovered,
				"stack", string(debug.Stack()),
			)
			writeError(responseWriter, http.StatusInternalServerError, "internal error")
		}
	}()

	if request.Method != http.MethodPost {
		responseWriter.Header().Set("Allow", http.MethodPost)
		writeError(responseWriter, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var requestBody seedRequest
	if err := json.NewDecoder(request.Body).Decode(&requestBody); err != nil {
		slog.Debug("broker/seedinventory: 400 bad json", "err", err)
		writeError(responseWriter, http.StatusBadRequest, "invalid json body")
		return
	}

	// Pre-funnel validation (BROKER_SEED_INVENTORY §5 step 3):
	// account_id present, instrument non-empty, qty > 0, entry_price > 0.
	// On any failure → 400 before the handler even looks at the broker.
	if requestBody.AccountID == "" {
		slog.Debug("broker/seedinventory: 400 missing account_id")
		writeError(responseWriter, http.StatusBadRequest, "account_id required")
		return
	}
	if requestBody.Instrument == "" {
		slog.Debug("broker/seedinventory: 400 missing instrument")
		writeError(responseWriter, http.StatusBadRequest, "instrument required")
		return
	}
	if requestBody.Qty <= 0 {
		slog.Debug("broker/seedinventory: 400 non-positive qty", "qty", requestBody.Qty)
		writeError(responseWriter, http.StatusBadRequest, "qty must be > 0")
		return
	}
	if requestBody.EntryPrice <= 0 {
		slog.Debug("broker/seedinventory: 400 non-positive entry_price", "entry_price", requestBody.EntryPrice)
		writeError(responseWriter, http.StatusBadRequest, "entry_price must be > 0")
		return
	}

	accountID := funnel.AccountID(requestBody.AccountID)
	instrument := funnel.InstrumentID(requestBody.Instrument)

	// The handler owns the deadline (mirrors transfer's P3-D4 stance).
	requestCtx, cancel := context.WithTimeout(request.Context(), handlerInstance.cfg.SeedTimeout)
	defer cancel()

	slog.Info("broker/seedinventory: begin",
		"account_id", accountID,
		"instrument", instrument,
		"qty", requestBody.Qty,
		"entry_price", requestBody.EntryPrice,
	)

	result, err := handlerInstance.broker.SeedInventory(requestCtx, accountID, instrument, requestBody.Qty, requestBody.EntryPrice)
	if errors.Is(err, funnel.ErrBrokerTimeout) {
		slog.Warn("broker/seedinventory: 504 broker timeout",
			"account_id", accountID,
			"instrument", instrument,
		)
		writeError(responseWriter, http.StatusGatewayTimeout, "broker timeout")
		return
	}
	if err != nil {
		slog.Error("broker/seedinventory: 500 unexpected broker error",
			"account_id", accountID,
			"instrument", instrument,
			"err", err,
		)
		writeError(responseWriter, http.StatusInternalServerError, "internal error")
		return
	}

	// No durable fallback exists for a mint (SI-D1) — Present:false is a
	// hard error, not a signal to fall back to a durable path.
	if !result.Present {
		slog.Info("broker/seedinventory: 409 no live session",
			"account_id", accountID,
			"instrument", instrument,
		)
		writeError(responseWriter, http.StatusConflict, "no live session for account")
		return
	}

	if result.Err != nil {
		// Only (Qty<=0 / EntryPrice<=0) ever reach here, and both are
		// already validated to 400 above — a non-nil Err means a caller
		// bypassed this endpoint. Return 500, don't take down the shard
		// (SI-D2).
		slog.Error("broker/seedinventory: 500 unexpected live reject",
			"account_id", accountID,
			"instrument", instrument,
			"err", result.Err,
		)
		writeError(responseWriter, http.StatusInternalServerError, "internal error")
		return
	}

	slog.Info("broker/seedinventory: 200 ok",
		"account_id", accountID,
		"instrument", instrument,
		"available", result.Available,
	)
	writeSuccess(responseWriter, requestBody.Instrument, result.Available)
}

// writeSuccess writes the 200 body.
func writeSuccess(responseWriter http.ResponseWriter, instrument string, available int64) {
	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(responseWriter).Encode(seedResponse{Instrument: instrument, Available: available}); err != nil {
		slog.Debug("broker/seedinventory: success body write failed", "err", err)
	}
}

// writeError writes the plain error envelope. Uses WriteHeader before
// Encode so the status code lands even if the body write fails mid-way.
func writeError(responseWriter http.ResponseWriter, status int, message string) {
	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.WriteHeader(status)
	if err := json.NewEncoder(responseWriter).Encode(errorBody{Error: message}); err != nil {
		slog.Debug("broker/seedinventory: error body write failed", "err", err, "status", status)
	}
}
