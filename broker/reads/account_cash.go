package reads

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"simulator/broker/store"
)

// PathAccountCash is the pre-session cash view. The user has an
// account_id (from the conductor's /v1/account/open handshake) but has
// not yet joined a market, so no live session exists on any shard and
// /v1/account/balance would return 409. This endpoint answers directly
// from Postgres — the durable trading_cash + bank_cash pair — so a
// freshly-signed-up or just-logged-in user can see their money without
// first entering a market.
const PathAccountCash = "/v1/account/cash"

// accountCashResponse is the 200 payload shape. Both fields are bare
// int64 paise/coins per the no-unit rule — no "unit" / "currency"
// field anywhere. trading_cash is the signed Postgres column (debt is
// legal); bank_cash is non-negative by schema.
type accountCashResponse struct {
	TradingCash int64 `json:"trading_cash"`
	BankCash    int64 `json:"bank_cash"`
}

// NewAccountCashHandler wires the pre-session cash read. durableStore
// is enough on its own — no funnel involvement, no live-session concept.
// A nil store or an invalid Config is a bringup mistake caught here
// with a panic rather than left to nil-deref on the first request
// (same stance as the other broker/reads constructors).
func NewAccountCashHandler(durableStore storePort, cfg Config) http.Handler {
	if durableStore == nil {
		panic("broker/reads: NewAccountCashHandler requires a non-nil storePort")
	}
	if err := cfg.Validate(); err != nil {
		panic("broker/reads: NewAccountCashHandler: " + err.Error())
	}
	return &accountCashHandler{durableStore: durableStore, cfg: cfg}
}

type accountCashHandler struct {
	durableStore storePort
	cfg          Config
}

// ServeHTTP runs the pre-session cash read:
//
//   - panic guard, GET-only, account_id required (→ 400),
//   - Postgres round-trip bounded by cfg.ReadTimeout,
//   - map the four outcomes:
//     404 — ErrNoAccount (the account_id does not resolve to a row),
//     503 — ErrLoadFailed (any store-level failure, including a ctx
//           deadline fire; durable reads never surface as 504),
//     500 — any other unexpected error,
//     200 — {trading_cash, bank_cash}.
//
// On the 404 call: broker/reads/CLAUDE.md's "no 404 on durable reads"
// rule was written for the list-shaped reads (trades/pnl/costs) where
// a missing account legitimately produces an empty list and 200 is
// correct. This endpoint is a singular object keyed by account_id — a
// missing row genuinely means "not found", and returning 200 with
// zero-filled cash would mislead the client into believing an empty
// account exists. Deliberate deviation; scope the no-404 rule to the
// list-shaped reads going forward.
func (handlerInstance *accountCashHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	defer recoverPanic(responseWriter, PathAccountCash)

	if !requireGET(responseWriter, request, PathAccountCash) {
		return
	}
	accountID, ok := parseAccountID(request)
	if !ok {
		slog.Debug("broker/reads: 400 missing account_id", "path", PathAccountCash)
		writeError(responseWriter, http.StatusBadRequest, "account_id required", PathAccountCash)
		return
	}

	requestCtx, cancel := context.WithTimeout(request.Context(), handlerInstance.cfg.ReadTimeout)
	defer cancel()

	slog.Debug("broker/reads: account_cash begin", "account_id", accountID)
	tradingCash, bankCash, readErr := handlerInstance.durableStore.AccountCash(requestCtx, store.AccountID(accountID))

	switch {
	case errors.Is(readErr, store.ErrNoAccount):
		slog.Info("broker/reads: 404 account_cash unknown account", "account_id", accountID)
		writeError(responseWriter, http.StatusNotFound, "account not found", PathAccountCash)
	case errors.Is(readErr, store.ErrLoadFailed):
		slog.Error("broker/reads: 503 account_cash store unavailable", "account_id", accountID, "err", readErr)
		writeError(responseWriter, http.StatusServiceUnavailable, "store unavailable", PathAccountCash)
	case readErr != nil:
		slog.Error("broker/reads: 500 account_cash unexpected error", "account_id", accountID, "err", readErr)
		writeError(responseWriter, http.StatusInternalServerError, "internal error", PathAccountCash)
	default:
		slog.Info("broker/reads: 200 account_cash",
			"account_id", accountID,
			"trading_cash", tradingCash,
			"bank_cash", bankCash,
		)
		writeJSON(responseWriter, http.StatusOK, accountCashResponse{
			TradingCash: tradingCash,
			BankCash:    bankCash,
		}, PathAccountCash)
	}
}
