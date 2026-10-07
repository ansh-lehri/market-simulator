package order

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"simulator/broker/funnel"
	"simulator/contract"
)

// ModifyPath is the one route this handler owns (BROKER_PHASE5 §4.4,
// BROKER_READ_SURFACES §9.1).
const ModifyPath = "/v1/orders/modify"

// modifyRequest is the on-wire request shape (BROKER_READ_SURFACES
// §9.1). Note the two shape deviations from the standalone place body:
//
//   - `is_market` bool instead of a `type` enum string
//   - `new_price` instead of `limit_price`
//
// Both are a locked choice in the authority doc (not negotiable from
// this file). The HTTP handler translates `is_market` into (Type, TIF)
// via typeForModify / tifForModify (mapping.go) and the price into
// OrderSpec.LimitPrice; downstream (funnel + exchange) sees the SAME
// OrderSpec shape as a standalone place.
//
// No `tif` field — modify always derives TIF from is_market (market →
// IOC, limit → GTC). A standalone place supports FOK on the wire; a
// modify does not. This is a deliberate wire asymmetry per §9.1.
type modifyRequest struct {
	AccountID  string `json:"account_id"`
	OrderID    string `json:"order_id"`
	NewQty     int64  `json:"new_qty"`
	NewPrice   int64  `json:"new_price"`
	Side       string `json:"side"`
	Instrument string `json:"instrument"`
	IsMarket   bool   `json:"is_market"`
}

// modifyCancelLeg is the "cancel" sub-object of modifyResponse. On the
// gate-rejected branch Status="failed" and Reason carries the reject
// tag; on the success+partial branches Status="pending" and OrderID
// echoes the OLD order id the caller passed.
type modifyCancelLeg struct {
	Status  string `json:"status"`
	OrderID string `json:"order_id,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

// modifyReplaceLeg is the "replace" sub-object. On accepted, OrderID
// is the NEW minted id; on rejected, Reason carries the funnel's
// RejectReason tag. The pointer-value shape in modifyResponse lets the
// gate-rejected branch marshal `replace: null` (Go JSON renders a nil
// pointer as JSON null, matching the §9.4 example).
type modifyReplaceLeg struct {
	Status  string `json:"status"`
	OrderID string `json:"order_id,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

// modifyResponse is the composed body returned on 200 and 409. The
// three modify_status values (ok/partial/rejected) exhaustively cover
// the three outcome branches of the Option-1 gate-on-local-acceptance
// flow — see ServeHTTP for the per-branch mapping.
type modifyResponse struct {
	ModifyStatus string            `json:"modify_status"`
	Cancel       *modifyCancelLeg  `json:"cancel"`
	Replace      *modifyReplaceLeg `json:"replace"`
	Warning      string            `json:"warning,omitempty"`
}

// String constants used verbatim in the response — single source of
// truth. Keeping them here (not in http.go) because they are specific
// to modify's composed shape; place and cancel use their own tags.
const (
	modifyStatusOK       = "ok"
	modifyStatusPartial  = "partial"
	modifyStatusRejected = "rejected"

	cancelLegPending = "pending"
	cancelLegFailed  = "failed"

	replaceLegAccepted = "accepted"
	replaceLegRejected = "rejected"

	// modifyPartialWarning is the exact warning string the §9.3 doc
	// pins for the partial-failure branch: cancel is in-flight (likely
	// to succeed) but the replacement was rejected at Gate 4. The
	// caller MUST surface this to the end-user — the client may end up
	// with no order at all.
	modifyPartialWarning = "original order is being cancelled; replacement was rejected — you may be left with no order"
)

type modifyHandler struct {
	broker brokerPort
	cfg    Config
}

// NewModifyHandler builds the POST /v1/orders/modify handler. Same
// factory shape as the other two endpoints; same panic-at-construction
// policy on nil broker / bad config.
func NewModifyHandler(broker brokerPort, cfg Config) http.Handler {
	if broker == nil {
		panic("broker/order: NewModifyHandler requires a non-nil brokerPort")
	}
	if err := cfg.Validate(); err != nil {
		panic("broker/order: NewModifyHandler: " + err.Error())
	}
	return &modifyHandler{broker: broker, cfg: cfg}
}

// ServeHTTP is the composed modify flow (BROKER_READ_SURFACES §9.2,
// Option 1: gate on fast LOCAL acceptance).
//
// Flow:
//
//  1. method guard, defer recover, decode body, pre-funnel validate
//  2. STEP A — punch cancel. On ErrBrokerTimeout → 504 (cancel leg).
//     On any other error → 500.
//  3. GATE — if cancel locally failed → 409 modify_status="rejected",
//     place is NEVER called. This is the whole point of Option 1: a
//     doomed cancel does not leak a naked replacement.
//  4. STEP B — punch place. On ErrBrokerTimeout → 504 (place leg).
//     On any other error → 500.
//  5. compose both local results into one body — modify_status is
//     "ok" if both locally accepted, "partial" if cancel-pending +
//     place-rejected (the §9.3 partial seam, reported never undone).
//
// Status mapping (§9.4):
//
//	200 — composed ok OR partial
//	409 — cancel locally failed (gate rejection)
//	400 — malformed/missing body or unparseable enum
//	504 — timeout on EITHER leg
//	500 — unexpected error on either leg OR panic recover
//
// Pre-funnel vs funnel split (same discipline as place): the HTTP
// layer rejects on wire-shape errors (missing account_id / order_id /
// instrument, unknown side); business validations on the place leg
// (bad_qty, bad_price, insufficient_funds, no_market_price) come back
// as PlaceRejected and surface INSIDE the partial response, not as a
// standalone 422 — modify's shape is composed, so a 422 on the place
// leg would hide the cancel's state.
func (handlerInstance *modifyHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, ModifyPath)

	if request.Method != http.MethodPost {
		responseWriter.Header().Set("Allow", http.MethodPost)
		writeError(responseWriter, http.StatusMethodNotAllowed, "method not allowed", ModifyPath)
		return
	}

	var requestBody modifyRequest
	if err := json.NewDecoder(request.Body).Decode(&requestBody); err != nil {
		slog.Debug("broker/order: modify 400 bad json", "err", err)
		writeError(responseWriter, http.StatusBadRequest, "invalid json body", ModifyPath)
		return
	}

	if requestBody.AccountID == "" {
		slog.Debug("broker/order: modify 400 missing account_id")
		writeError(responseWriter, http.StatusBadRequest, "account_id required", ModifyPath)
		return
	}
	if requestBody.OrderID == "" {
		slog.Debug("broker/order: modify 400 missing order_id", "account_id", requestBody.AccountID)
		writeError(responseWriter, http.StatusBadRequest, "order_id required", ModifyPath)
		return
	}
	if requestBody.Instrument == "" {
		slog.Debug("broker/order: modify 400 missing instrument", "account_id", requestBody.AccountID, "order_id", requestBody.OrderID)
		writeError(responseWriter, http.StatusBadRequest, "instrument required", ModifyPath)
		return
	}
	parsedSide, sideOk := parseSide(requestBody.Side)
	if !sideOk {
		slog.Debug("broker/order: modify 400 unknown side", "account_id", requestBody.AccountID, "side", requestBody.Side)
		writeError(responseWriter, http.StatusBadRequest, "side must be buy or sell", ModifyPath)
		return
	}

	// One ctx covers BOTH shard round-trips. A slow shard cannot
	// combine the two legs past OrderTimeout — if the first leg eats
	// 80 ms of a 100 ms budget, the second leg only sees 20 ms left,
	// which is the correct ceiling (BROKER_READ_SURFACES §9.4 "either
	// punch timed out → 504").
	requestCtx, cancelRequestCtx := context.WithTimeout(request.Context(), handlerInstance.cfg.OrderTimeout)
	defer cancelRequestCtx()

	accountID := funnel.AccountID(requestBody.AccountID)
	oldOrderID := funnel.OrderID(requestBody.OrderID)
	slog.Info("broker/order: modify begin",
		"account_id", accountID,
		"old_order_id", oldOrderID,
		"instrument", requestBody.Instrument,
		"side", requestBody.Side,
		"is_market", requestBody.IsMarket,
		"new_qty", requestBody.NewQty,
		"new_price", requestBody.NewPrice,
	)

	// STEP A — punch the cancel.
	cancelResult, cancelErr := handlerInstance.broker.CancelOrder(requestCtx, accountID, oldOrderID)
	if errors.Is(cancelErr, funnel.ErrBrokerTimeout) {
		slog.Warn("broker/order: modify 504 cancel leg timeout",
			"account_id", accountID,
			"old_order_id", oldOrderID,
		)
		writeError(responseWriter, http.StatusGatewayTimeout, "broker timeout (cancel leg)", ModifyPath)
		return
	}
	if cancelErr != nil {
		slog.Error("broker/order: modify 500 unexpected cancel error",
			"account_id", accountID,
			"old_order_id", oldOrderID,
			"err", cancelErr,
		)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", ModifyPath)
		return
	}

	// GATE (BROKER_READ_SURFACES §9.2 Option 1): a locally-failed
	// cancel means the replacement is NEVER punched. The original is
	// left exactly as it was; a "modify a filled order" does not
	// silently become a naked new order.
	if cancelResult.Status == funnel.CancelFailed {
		slog.Info("broker/order: modify 409 rejected (cancel failed)",
			"account_id", accountID,
			"old_order_id", oldOrderID,
			"reason", cancelResult.Reason,
		)
		writeJSON(responseWriter, http.StatusConflict, modifyResponse{
			ModifyStatus: modifyStatusRejected,
			Cancel:       &modifyCancelLeg{Status: cancelLegFailed, Reason: cancelResult.Reason.String()},
			Replace:      nil,
		}, ModifyPath)
		return
	}
	if cancelResult.Status != funnel.CancelPending {
		slog.Error("broker/order: modify 500 unexpected cancel status",
			"account_id", accountID,
			"old_order_id", oldOrderID,
			"status", cancelResult.Status,
			"reason", cancelResult.Reason,
		)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", ModifyPath)
		return
	}

	// STEP B — punch the replacement place. Same shard, same channel
	// → the exchange sees the cancel BEFORE the new place (§9.2 order-
	// preservation on same-shard punches).
	orderSpec := funnel.OrderSpec{
		Instrument: contract.InstrumentID(requestBody.Instrument),
		Side:       parsedSide,
		Type:       typeForModify(requestBody.IsMarket),
		TIF:        tifForModify(requestBody.IsMarket),
		Qty:        requestBody.NewQty,
		LimitPrice: requestBody.NewPrice,
	}
	placeResult, placeErr := handlerInstance.broker.PlaceOrder(requestCtx, accountID, "", orderSpec)
	if errors.Is(placeErr, funnel.ErrBrokerTimeout) {
		slog.Warn("broker/order: modify 504 place leg timeout",
			"account_id", accountID,
			"old_order_id", oldOrderID,
		)
		writeError(responseWriter, http.StatusGatewayTimeout, "broker timeout (place leg)", ModifyPath)
		return
	}
	if placeErr != nil {
		slog.Error("broker/order: modify 500 unexpected place error",
			"account_id", accountID,
			"old_order_id", oldOrderID,
			"err", placeErr,
		)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", ModifyPath)
		return
	}

	// Compose the final response. CancelPending is already proved; the
	// branch is now purely on the place leg's status.
	switch placeResult.Status {
	case funnel.PlaceAccepted:
		slog.Info("broker/order: modify 200 ok",
			"account_id", accountID,
			"old_order_id", oldOrderID,
			"new_order_id", placeResult.OrderID,
		)
		writeJSON(responseWriter, http.StatusOK, modifyResponse{
			ModifyStatus: modifyStatusOK,
			Cancel:       &modifyCancelLeg{Status: cancelLegPending, OrderID: string(cancelResult.OrderID)},
			Replace:      &modifyReplaceLeg{Status: replaceLegAccepted, OrderID: string(placeResult.OrderID)},
		}, ModifyPath)
	case funnel.PlaceRejected:
		// Partial seam (§9.3): cancel is in flight (likely to succeed)
		// but Gate 4 rejected the replacement. Reported, NEVER rolled
		// back — undoing a punched cancel would race the async echo
		// path and produce worse outcomes than the honest partial.
		slog.Warn("broker/order: modify 200 partial",
			"account_id", accountID,
			"old_order_id", oldOrderID,
			"place_reason", placeResult.Reason,
		)
		writeJSON(responseWriter, http.StatusOK, modifyResponse{
			ModifyStatus: modifyStatusPartial,
			Cancel:       &modifyCancelLeg{Status: cancelLegPending, OrderID: string(cancelResult.OrderID)},
			Replace:      &modifyReplaceLeg{Status: replaceLegRejected, Reason: placeResult.Reason.String()},
			Warning:      modifyPartialWarning,
		}, ModifyPath)
	default:
		slog.Error("broker/order: modify 500 unexpected place status",
			"account_id", accountID,
			"old_order_id", oldOrderID,
			"status", placeResult.Status,
			"reason", placeResult.Reason,
		)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", ModifyPath)
	}
}
