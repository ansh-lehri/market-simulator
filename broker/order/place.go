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

// PlacePath is the one route this handler owns (BROKER_PHASE5 §4.4).
// Exported so the mux-wiring layer (bringup) can register without
// duplicating the string.
const PlacePath = "/v1/orders"

// placeRequest is the on-wire request shape (BROKER_PHASE5 §4.2).
//
// Minor-unit integers only — ticks for LimitPrice, whole units for Qty
// (same discipline as the OrderSpec it is translated into). LimitPrice
// is `limit_price?` on the wire: present and > 0 iff type=="limit"; for
// market orders the field is ignored (contract.FundedOrder.LimitPrice:
// "set for Limit; ignored for Market"). Missing JSON int defaults to
// zero, which the funnel's validateSpec catches as ReasonBadPrice (422)
// on the limit branch and never looks at on the market branch.
type placeRequest struct {
	AccountID  string `json:"account_id"`
	Instrument string `json:"instrument"`
	Side       string `json:"side"`
	Type       string `json:"type"`
	TIF        string `json:"tif"`
	Qty        int64  `json:"qty"`
	LimitPrice int64  `json:"limit_price"`
}

// placeAcceptedBody is the 202 success response body. One field — the
// minted OrderID — nothing else. "Accepted, not executed" discipline
// (BROKER_READ_SURFACES §9.2): we never return fill state here, that
// comes async via settlement.
type placeAcceptedBody struct {
	OrderID string `json:"order_id"`
}

// placeHandler bundles the broker port and the shared Config into a
// struct that satisfies http.Handler. Not exported; callers construct
// via NewPlaceHandler.
type placeHandler struct {
	broker brokerPort
	cfg    Config
}

// NewPlaceHandler builds the POST /v1/orders handler. Narrow factory
// shape — one broker port, one Config — mirroring broker/transfer's
// NewHandler. *funnel.Broker satisfies brokerPort implicitly; tests
// inject a stub (see test_helpers_test.go).
//
// Nil broker or bad config at construction is a bringup programming
// error; panic early rather than defer the nil-deref to the first
// request.
func NewPlaceHandler(broker brokerPort, cfg Config) http.Handler {
	if broker == nil {
		panic("broker/order: NewPlaceHandler requires a non-nil brokerPort")
	}
	if err := cfg.Validate(); err != nil {
		panic("broker/order: NewPlaceHandler: " + err.Error())
	}
	return &placeHandler{broker: broker, cfg: cfg}
}

// ServeHTTP is the full place flow: method guard → defer recover →
// decode body → pre-funnel validate → ctx timeout → broker.PlaceOrder →
// shape response (BROKER_PHASE5 §4.4 / §4.5).
//
// Status mapping (P5-D27):
//
//	202 — PlaceAccepted (success)
//	400 — malformed/missing body or unparseable enum string
//	409 — ReasonNoSession (special-cased per §0.5)
//	422 — any other PlaceRejected reason (validation / insufficient /
//	      no_market_price); exact reason tag in the body
//	504 — ErrBrokerTimeout from the shard round-trip
//	500 — unexpected broker error OR a panic bubbled from downstream
//
// Pre-funnel vs funnel split: the HTTP layer only rejects on WIRE-SHAPE
// errors (missing field, unparseable enum string) → 400. Everything
// else — qty<=0, limit without price, unknown instrument, insufficient
// funds, no market price — lives in the funnel's validateSpec /
// ReserveIfAvailable and surfaces as PlaceRejected → 422. This matches
// the Phase-5 §4.5 "validation rejections are 422, not 400" rule.
func (handlerInstance *placeHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PlacePath)

	if request.Method != http.MethodPost {
		responseWriter.Header().Set("Allow", http.MethodPost)
		writeError(responseWriter, http.StatusMethodNotAllowed, "method not allowed", PlacePath)
		return
	}

	var requestBody placeRequest
	if err := json.NewDecoder(request.Body).Decode(&requestBody); err != nil {
		slog.Debug("broker/order: place 400 bad json", "err", err)
		writeError(responseWriter, http.StatusBadRequest, "invalid json body", PlacePath)
		return
	}

	if requestBody.AccountID == "" {
		slog.Debug("broker/order: place 400 missing account_id")
		writeError(responseWriter, http.StatusBadRequest, "account_id required", PlacePath)
		return
	}
	if requestBody.Instrument == "" {
		slog.Debug("broker/order: place 400 missing instrument", "account_id", requestBody.AccountID)
		writeError(responseWriter, http.StatusBadRequest, "instrument required", PlacePath)
		return
	}
	parsedSide, sideOk := parseSide(requestBody.Side)
	if !sideOk {
		slog.Debug("broker/order: place 400 unknown side", "account_id", requestBody.AccountID, "side", requestBody.Side)
		writeError(responseWriter, http.StatusBadRequest, "side must be buy or sell", PlacePath)
		return
	}
	parsedType, typeOk := parseType(requestBody.Type)
	if !typeOk {
		slog.Debug("broker/order: place 400 unknown type", "account_id", requestBody.AccountID, "type", requestBody.Type)
		writeError(responseWriter, http.StatusBadRequest, "type must be limit or market", PlacePath)
		return
	}
	parsedTIF, tifOk := parseTIF(requestBody.TIF)
	if !tifOk {
		slog.Debug("broker/order: place 400 unknown tif", "account_id", requestBody.AccountID, "tif", requestBody.TIF)
		writeError(responseWriter, http.StatusBadRequest, "tif must be gtc, ioc, or fok", PlacePath)
		return
	}

	orderSpec := funnel.OrderSpec{
		Instrument: contract.InstrumentID(requestBody.Instrument),
		Side:       parsedSide,
		Type:       parsedType,
		TIF:        parsedTIF,
		Qty:        requestBody.Qty,
		LimitPrice: requestBody.LimitPrice,
	}

	// The handler owns the deadline (P5-D27). One ctx bounds the shard
	// round-trip; a slow shard surfaces as 504 to the caller, not as a
	// hanging HTTP connection.
	requestCtx, cancelRequestCtx := context.WithTimeout(request.Context(), handlerInstance.cfg.OrderTimeout)
	defer cancelRequestCtx()

	accountID := funnel.AccountID(requestBody.AccountID)
	slog.Info("broker/order: place begin",
		"account_id", accountID,
		"instrument", requestBody.Instrument,
		"side", requestBody.Side,
		"type", requestBody.Type,
		"tif", requestBody.TIF,
		"qty", requestBody.Qty,
		"limit_price", requestBody.LimitPrice,
	)

	// sessionID is vestigial on the entry-point (BROKER_PHASE5 §4.3): the
	// shard resolves the live session via s.sessions[AccountID]. Pass "".
	placeResult, placeErr := handlerInstance.broker.PlaceOrder(requestCtx, accountID, "", orderSpec)
	if errors.Is(placeErr, funnel.ErrBrokerTimeout) {
		slog.Warn("broker/order: place 504 broker timeout", "account_id", accountID)
		writeError(responseWriter, http.StatusGatewayTimeout, "broker timeout", PlacePath)
		return
	}
	if placeErr != nil {
		slog.Error("broker/order: place 500 unexpected broker error",
			"account_id", accountID,
			"err", placeErr,
		)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", PlacePath)
		return
	}

	switch placeResult.Status {
	case funnel.PlaceAccepted:
		slog.Info("broker/order: place 202 accepted",
			"account_id", accountID,
			"order_id", placeResult.OrderID,
		)
		writeJSON(responseWriter, http.StatusAccepted, placeAcceptedBody{OrderID: string(placeResult.OrderID)}, PlacePath)
	case funnel.PlaceRejected:
		statusCode := http.StatusUnprocessableEntity
		if placeResult.Reason == funnel.ReasonNoSession {
			statusCode = http.StatusConflict
		}
		slog.Info("broker/order: place rejected",
			"account_id", accountID,
			"status", statusCode,
			"reason", placeResult.Reason,
		)
		writeReason(responseWriter, statusCode, placeResult.Reason.String(), PlacePath)
	default:
		// A well-formed PlaceResult always carries Accepted or Rejected
		// (funnel/results.go); PlaceStatusUnknown means a programming
		// error upstream — 500, log hard.
		slog.Error("broker/order: place 500 unexpected status",
			"account_id", accountID,
			"status", placeResult.Status,
			"reason", placeResult.Reason,
		)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", PlacePath)
	}
}
