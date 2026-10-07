package order

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"simulator/broker/funnel"
)

// CancelPath is the one route this handler owns (BROKER_PHASE5 §4.4).
// Exported so the mux-wiring layer registers without duplicating the
// string.
//
// POST-with-body (not DELETE /orders/{id}) is a deliberate choice per
// BROKER_READ_SURFACES §0.1 / §4.4: the no-auth body convention keeps
// account_id as an explicit request field everywhere, and a URL-pathed
// variant would surface it as a path segment instead.
const CancelPath = "/v1/orders/cancel"

// cancelRequest is the on-wire request shape (BROKER_PHASE5 §4.4).
// Only two fields — the account owning the order and the order to
// cancel. No side/qty/price: a cancel is a whole-order action and the
// exchange looks up the resting entry by OrderID alone.
type cancelRequest struct {
	AccountID string `json:"account_id"`
	OrderID   string `json:"order_id"`
}

// cancelAcceptedBody is the 202 success response body. The echoed
// OrderID is the one the caller passed; it lets a client correlate the
// 202 with its own request without parsing the request body back out.
//
// "Accepted, not executed" (BROKER_READ_SURFACES §9.2): a 202 means
// the cancel was LOCALLY accepted (ownership + state pre-filter + flag
// set + punch sent), NEVER that the exchange has actually cancelled.
// The real outcome (Cancelled / CancelRejected) arrives async via
// settlement.
type cancelAcceptedBody struct {
	OrderID string `json:"order_id"`
}

type cancelHandler struct {
	broker brokerPort
	cfg    Config
}

// NewCancelHandler builds the POST /v1/orders/cancel handler. Same
// factory shape as NewPlaceHandler: one broker port, one Config.
// *funnel.Broker satisfies brokerPort implicitly; tests use the stub
// from helpers_test.go.
func NewCancelHandler(broker brokerPort, cfg Config) http.Handler {
	if broker == nil {
		panic("broker/order: NewCancelHandler requires a non-nil brokerPort")
	}
	if err := cfg.Validate(); err != nil {
		panic("broker/order: NewCancelHandler: " + err.Error())
	}
	return &cancelHandler{broker: broker, cfg: cfg}
}

// ServeHTTP is the cancel flow — simpler than place because the §9.4
// uniform-cancel-fail rule collapses ALL CancelFailed reasons to 409
// (no 422 bucket on this endpoint).
//
// Status mapping (BROKER_PHASE5 §4.5 / BROKER_READ_SURFACES §9.4):
//
//	202 — CancelPending (success)
//	400 — malformed/missing body (account_id or order_id)
//	409 — ANY CancelFailed reason (no_session / unknown_order /
//	      not_cancellable); exact reason tag in body
//	504 — ErrBrokerTimeout from the shard round-trip
//	500 — unexpected broker error OR a panic bubbled from downstream
func (handlerInstance *cancelHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, CancelPath)

	if request.Method != http.MethodPost {
		responseWriter.Header().Set("Allow", http.MethodPost)
		writeError(responseWriter, http.StatusMethodNotAllowed, "method not allowed", CancelPath)
		return
	}

	var requestBody cancelRequest
	if err := json.NewDecoder(request.Body).Decode(&requestBody); err != nil {
		slog.Debug("broker/order: cancel 400 bad json", "err", err)
		writeError(responseWriter, http.StatusBadRequest, "invalid json body", CancelPath)
		return
	}

	if requestBody.AccountID == "" {
		slog.Debug("broker/order: cancel 400 missing account_id")
		writeError(responseWriter, http.StatusBadRequest, "account_id required", CancelPath)
		return
	}
	if requestBody.OrderID == "" {
		slog.Debug("broker/order: cancel 400 missing order_id", "account_id", requestBody.AccountID)
		writeError(responseWriter, http.StatusBadRequest, "order_id required", CancelPath)
		return
	}

	requestCtx, cancelRequestCtx := context.WithTimeout(request.Context(), handlerInstance.cfg.OrderTimeout)
	defer cancelRequestCtx()

	accountID := funnel.AccountID(requestBody.AccountID)
	orderID := funnel.OrderID(requestBody.OrderID)
	slog.Info("broker/order: cancel begin",
		"account_id", accountID,
		"order_id", orderID,
	)

	cancelResult, cancelErr := handlerInstance.broker.CancelOrder(requestCtx, accountID, orderID)
	if errors.Is(cancelErr, funnel.ErrBrokerTimeout) {
		slog.Warn("broker/order: cancel 504 broker timeout",
			"account_id", accountID,
			"order_id", orderID,
		)
		writeError(responseWriter, http.StatusGatewayTimeout, "broker timeout", CancelPath)
		return
	}
	if cancelErr != nil {
		slog.Error("broker/order: cancel 500 unexpected broker error",
			"account_id", accountID,
			"order_id", orderID,
			"err", cancelErr,
		)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", CancelPath)
		return
	}

	switch cancelResult.Status {
	case funnel.CancelPending:
		slog.Info("broker/order: cancel 202 pending",
			"account_id", accountID,
			"order_id", cancelResult.OrderID,
		)
		writeJSON(responseWriter, http.StatusAccepted, cancelAcceptedBody{OrderID: string(cancelResult.OrderID)}, CancelPath)
	case funnel.CancelFailed:
		// §9.4 uniform cancel-fail → 409 for every reason (not split
		// across 409 for no_session and 422 for the rest, as place
		// does). The reason tag discriminates in the body.
		slog.Info("broker/order: cancel 409 failed",
			"account_id", accountID,
			"order_id", orderID,
			"reason", cancelResult.Reason,
		)
		writeReason(responseWriter, http.StatusConflict, cancelResult.Reason.String(), CancelPath)
	default:
		// A well-formed CancelResult always carries Pending or Failed
		// (funnel/results.go); CancelStatusUnknown means a programming
		// error upstream — 500, log hard.
		slog.Error("broker/order: cancel 500 unexpected status",
			"account_id", accountID,
			"order_id", orderID,
			"status", cancelResult.Status,
			"reason", cancelResult.Reason,
		)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", CancelPath)
	}
}
