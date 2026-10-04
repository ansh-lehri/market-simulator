package transfer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"

	"simulator/broker/funnel"
	"simulator/broker/store"
	"simulator/economy/account"
)

// Path is the one route this package owns (BROKER_PHASE3 §1.6). Exported
// so the mux-wiring stage (Tier-4-adjacent, not Phase-3) can register
// without duplicating the string.
const Path = "/v1/account/transfer"

// transferRequest is the on-wire request shape.
//
// `unit` is parsed but intentionally unused — the field stays in the
// wire so future Tier-4 unit vocabulary slots in without a wire break.
// Not validated; missing/empty is fine either way.
type transferRequest struct {
	AccountID string `json:"account_id"`
	Direction string `json:"direction"`
	Amount    int64  `json:"amount"`
	Unit      string `json:"unit"`
}

// transferResponse is the mode-blind success body (BROKER_PHASE3 P3-D7):
// identical shape whether the money moved on the live shard or on the
// durable row.
type transferResponse struct {
	Available int64 `json:"available"`
	Bank      int64 `json:"bank"`
}

// errorBody is the plain error envelope for every non-200 response. Kept
// mode-blind: the 422 insufficient body is the same string whether it
// came from the live account.ErrInsufficient or the durable
// store.ErrInsufficient, so a client cannot tell live from idle (P3-D7).
type errorBody struct {
	Error string `json:"error"`
}

// NewHandler builds the one POST /v1/account/transfer handler. The
// factory signature is deliberately narrow — a broker port, a store
// port, and the one timeout dial. Both ports are satisfied by the real
// *funnel.Broker and *store.Store in production; tests inject stubs for
// the branches a live runtime can't reach deterministically (ctx-timeout
// 504, pre-funnel 400s).
//
// Nil broker or store at construction is a programming error at the
// caller (bringup); panic early rather than defer a nil-deref to the
// first request.
func NewHandler(broker brokerPort, durableStore storePort, cfg Config) http.Handler {
	if broker == nil {
		panic("broker/transfer: NewHandler requires a non-nil brokerPort")
	}
	if durableStore == nil {
		panic("broker/transfer: NewHandler requires a non-nil storePort")
	}
	if err := cfg.Validate(); err != nil {
		panic("broker/transfer: NewHandler: " + err.Error())
	}
	return &handler{broker: broker, durableStore: durableStore, cfg: cfg}
}

type handler struct {
	broker       brokerPort
	durableStore storePort
	cfg          Config
}

// ServeHTTP implements the full flow: method guard → recover → parse +
// pre-funnel validate → ctx-timeout → broker.Transfer → status dispatch.
//
// Panic policy: the shard loop runs on its own goroutine, so a request-
// side panic here does NOT corrupt shard state (that is single-writer-
// owned and protected). We recover at the HTTP boundary, log the stack,
// write a 500, and do NOT re-panic. net/http's own per-request recover
// would also catch an un-recovered panic, but it writes nothing to the
// client; this explicit recover gives the caller a 500 instead of a
// silently-torn connection.
func (handlerInstance *handler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("broker/transfer: handler panic",
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

	var requestBody transferRequest
	if err := json.NewDecoder(request.Body).Decode(&requestBody); err != nil {
		slog.Debug("broker/transfer: 400 bad json", "err", err)
		writeError(responseWriter, http.StatusBadRequest, "invalid json body")
		return
	}

	// Pre-funnel validation (BROKER_PHASE3 §1.6): account_id present,
	// direction legal, amount > 0. On any failure → 400 before the
	// handler even looks at the broker. `unit` is parsed above and
	// intentionally ignored here per the deferred-vocabulary rule.
	if requestBody.AccountID == "" {
		slog.Debug("broker/transfer: 400 missing account_id")
		writeError(responseWriter, http.StatusBadRequest, "account_id required")
		return
	}
	fromBucket, toBucket, directionOk := bucketsFor(requestBody.Direction)
	if !directionOk {
		slog.Debug("broker/transfer: 400 illegal direction", "direction", requestBody.Direction)
		writeError(responseWriter, http.StatusBadRequest, "direction must be to_bank or to_trading")
		return
	}
	durableDirection, _ := directionFor(requestBody.Direction) // ok guaranteed by the check above
	if requestBody.Amount <= 0 {
		slog.Debug("broker/transfer: 400 non-positive amount", "amount", requestBody.Amount)
		writeError(responseWriter, http.StatusBadRequest, "amount must be > 0")
		return
	}

	accountID := funnel.AccountID(requestBody.AccountID)

	// The handler owns the deadline (P3-D4). One ctx bounds both the
	// live shard round-trip and (on miss) the durable UPDATE — so a
	// slow shard followed by a slow DB cannot combine past the dial.
	requestCtx, cancel := context.WithTimeout(request.Context(), handlerInstance.cfg.TransferTimeout)
	defer cancel()

	slog.Info("broker/transfer: begin",
		"account_id", accountID,
		"direction", requestBody.Direction,
		"amount", requestBody.Amount,
	)

	liveResult, liveErr := handlerInstance.broker.Transfer(requestCtx, accountID, fromBucket, toBucket, requestBody.Amount)
	if errors.Is(liveErr, funnel.ErrBrokerTimeout) {
		slog.Warn("broker/transfer: 504 broker timeout",
			"account_id", accountID,
			"direction", requestBody.Direction,
		)
		writeError(responseWriter, http.StatusGatewayTimeout, "broker timeout")
		return
	}
	if liveErr != nil {
		slog.Error("broker/transfer: 500 unexpected broker error",
			"account_id", accountID,
			"direction", requestBody.Direction,
			"err", liveErr,
		)
		writeError(responseWriter, http.StatusInternalServerError, "internal error")
		return
	}

	// Live branch handled it — live-ok OR live-reject (P3-D7/D8). Never
	// fall to durable from here; a wrong routing would corrupt a live
	// account.
	if liveResult.Present {
		if liveResult.Err == nil {
			slog.Info("broker/transfer: 200 live ok",
				"account_id", accountID,
				"direction", requestBody.Direction,
				"available", liveResult.Available,
				"bank", liveResult.Bank,
			)
			writeSuccess(responseWriter, liveResult.Available, liveResult.Bank)
			return
		}
		// Live-reject. The only business-expected failure on this branch
		// is account.ErrInsufficientFunds (the MoveCash guard); anything
		// else is a programming error upstream (amount<=0 /illegal edge
		// were validated pre-funnel), which maps to 500.
		if errors.Is(liveResult.Err, account.ErrInsufficientFunds) {
			slog.Info("broker/transfer: 422 live insufficient",
				"account_id", accountID,
				"direction", requestBody.Direction,
				"err", liveResult.Err,
			)
			writeError(responseWriter, http.StatusUnprocessableEntity, "insufficient funds")
			return
		}
		slog.Error("broker/transfer: 500 unexpected live reject",
			"account_id", accountID,
			"direction", requestBody.Direction,
			"err", liveResult.Err,
		)
		writeError(responseWriter, http.StatusInternalServerError, "internal error")
		return
	}

	// Present:false — a routine idle miss. Fall to the durable branch.
	// Never surfaces to the client as its own status; the client only
	// sees the durable branch's outcome. (P3-D8: no 409 ever.)
	available, bank, durableErr := handlerInstance.durableStore.DurableTransfer(requestCtx, accountID, durableDirection, requestBody.Amount)
	switch {
	case durableErr == nil:
		slog.Info("broker/transfer: 200 durable ok",
			"account_id", accountID,
			"direction", requestBody.Direction,
			"available", available,
			"bank", bank,
		)
		writeSuccess(responseWriter, available, bank)
	case errors.Is(durableErr, store.ErrInsufficient):
		slog.Info("broker/transfer: 422 durable insufficient",
			"account_id", accountID,
			"direction", requestBody.Direction,
		)
		writeError(responseWriter, http.StatusUnprocessableEntity, "insufficient funds")
	case errors.Is(durableErr, store.ErrNoAccount):
		slog.Info("broker/transfer: 404 no account",
			"account_id", accountID,
			"direction", requestBody.Direction,
		)
		writeError(responseWriter, http.StatusNotFound, "account not found")
	case errors.Is(durableErr, store.ErrBadTransfer):
		// Defensive: Point 1 pre-validated amount+direction, so this is a
		// programming-order anomaly. The Phase-3 doc still lists it in
		// the status table for completeness.
		slog.Error("broker/transfer: 400 durable bad transfer (defensive)",
			"account_id", accountID,
			"direction", requestBody.Direction,
			"err", durableErr,
		)
		writeError(responseWriter, http.StatusBadRequest, "bad transfer")
	case errors.Is(durableErr, store.ErrLoadFailed):
		// Collapsed: every ErrLoadFailed is transport-shaped (pool dead,
		// ctx cancelled, driver error). 503 across the board — the
		// Phase-3 doc's "500 / 503" is left unsplit.
		slog.Error("broker/transfer: 503 durable transport failure",
			"account_id", accountID,
			"direction", requestBody.Direction,
			"err", durableErr,
		)
		writeError(responseWriter, http.StatusServiceUnavailable, "store unavailable")
	default:
		slog.Error("broker/transfer: 500 unexpected durable error",
			"account_id", accountID,
			"direction", requestBody.Direction,
			"err", durableErr,
		)
		writeError(responseWriter, http.StatusInternalServerError, "internal error")
	}
}

// writeSuccess writes the mode-blind 200 body.
func writeSuccess(responseWriter http.ResponseWriter, available, bank int64) {
	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.WriteHeader(http.StatusOK)
	// Encoder error here is network-side (client walked away); nothing
	// to do but log at debug — the handler is otherwise complete.
	if err := json.NewEncoder(responseWriter).Encode(transferResponse{Available: available, Bank: bank}); err != nil {
		slog.Debug("broker/transfer: success body write failed", "err", err)
	}
}

// writeError writes the plain error envelope. Uses WriteHeader before
// Encode so the status code lands even if the body write fails mid-way.
func writeError(responseWriter http.ResponseWriter, status int, message string) {
	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.WriteHeader(status)
	if err := json.NewEncoder(responseWriter).Encode(errorBody{Error: message}); err != nil {
		slog.Debug("broker/transfer: error body write failed", "err", err, "status", status)
	}
}
