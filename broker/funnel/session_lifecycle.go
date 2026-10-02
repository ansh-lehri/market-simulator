package funnel

import (
	"context"
	"errors"
	"log/slog"

	"simulator/broker/store"
	"simulator/economy/lifecycle"
)

// EnsureAccount is BROKER_PHASE2 §1 Point 1 — the get-or-create for a
// durable account. Every session entry (canonical and on-demand) begins
// here. Off-shard, runs on the caller's goroutine, Postgres-only; does
// NOT touch the funnel (no shard, no inbox, no command).
//
// Strategy is create-first, not check-first: ask lifecycle.OpenAccount to
// insert, and on the duplicate signal fall back to the raw Store lookup.
// The INSERT is the sole race arbiter (uq_accounts_owner UNIQUE on
// (owner_type, owner_id) — migration 0001), so two concurrent callers
// with the same owner cannot both create and cannot both miss — one
// creates, the other falls back to the same handle. No check-then-create
// window exists.
//
// Error contract (traces to BROKER_PHASE2 §1 error table):
//
//   - nil                             → account created, return new aid
//   - lifecycle.ErrAccountExists      → fall back to AccountHandle:
//       - nil                         → return the existing aid
//       - store.ErrNoAccount          → retry EnsureAccount ONCE (handles the
//                                       vanishingly rare delete-between-insert-
//                                       and-select racecourse); then surface
//                                       store.ErrNoAccount as a hard signal
//       - store.ErrLoadFailed         → propagate (transport failure)
//   - lifecycle.ErrBadOwnerType /
//     lifecycle.ErrBadOwnerID         → propagate (caller bug)
//   - lifecycle.ErrNegativeStarting*  → propagate (misconfigured dial)
//   - lifecycle.ErrCreateFailed       → propagate (driver failure)
//
// NEVER returns "not found" as a normal outcome — the method's job is to
// make the account exist. ErrNoAccount here is reserved for the single
// retry-exhausted fallback case above.
//
// Error dispatch uses errors.Is on category sentinels per
// [[feedback_error_category_sentinels]] — never errors.As on L5's struct
// carriers.
func (broker *Broker) EnsureAccount(
	ctx context.Context,
	ownerType lifecycle.OwnerType,
	ownerID string,
) (AccountID, error) {
	return broker.ensureAccount(ctx, ownerType, ownerID, true /*allowRetry*/)
}

// ensureAccount is the private workhorse. The allowRetry bool is the
// one-shot that bounds the retry at 1 — the second-pass call sets it to
// false so a repeated ErrNoAccount from AccountHandle becomes a terminal
// error rather than looping forever against a genuinely-missing row.
func (broker *Broker) ensureAccount(
	ctx context.Context,
	ownerType lifecycle.OwnerType,
	ownerID string,
	allowRetry bool,
) (AccountID, error) {
	slog.Debug("funnel: EnsureAccount begin",
		"owner_type", ownerType,
		"owner_id", ownerID,
		"starting_capital", broker.money.StartingCapital,
		"starting_bank", broker.money.StartingBank,
		"allow_retry", allowRetry,
	)

	accountID, openErr := broker.store.Lifecycle().OpenAccount(
		ctx,
		ownerType,
		ownerID,
		broker.money.StartingCapital,
		broker.money.StartingBank,
	)
	switch {
	case openErr == nil:
		slog.Info("funnel: EnsureAccount created",
			"account_id", accountID,
			"owner_type", ownerType,
			"owner_id", ownerID,
		)
		return accountID, nil

	case errors.Is(openErr, lifecycle.ErrAccountExists):
		// Fall back to the raw Store lookup. The INSERT above lost the
		// race (another caller got there first); the UNIQUE index has
		// already materialized the row, so AccountHandle sees it.
		existingID, handleErr := broker.store.AccountHandle(ctx, ownerType.String(), ownerID)
		switch {
		case handleErr == nil:
			slog.Info("funnel: EnsureAccount fell back to existing",
				"account_id", existingID,
				"owner_type", ownerType,
				"owner_id", ownerID,
			)
			return existingID, nil

		case errors.Is(handleErr, store.ErrNoAccount):
			if allowRetry {
				// The row visible to OpenAccount's INSERT is somehow not
				// visible to the follow-up SELECT. No code path deletes
				// an account today, so this is a vanishingly rare timing
				// anomaly — give the next attempt a chance to create
				// cleanly before escalating.
				slog.Warn("funnel: EnsureAccount fallback missed; retrying once",
					"owner_type", ownerType,
					"owner_id", ownerID,
				)
				return broker.ensureAccount(ctx, ownerType, ownerID, false)
			}
			slog.Error("funnel: EnsureAccount retry exhausted; account genuinely absent",
				"owner_type", ownerType,
				"owner_id", ownerID,
			)
			return "", store.ErrNoAccount

		default:
			slog.Error("funnel: EnsureAccount fallback failed",
				"owner_type", ownerType,
				"owner_id", ownerID,
				"err", handleErr,
			)
			return "", handleErr
		}

	default:
		// Genuine failure path: ErrBadOwnerType / ErrBadOwnerID /
		// ErrNegativeStartingCapital / ErrNegativeStartingBank /
		// ErrCreateFailed. Propagate verbatim so the caller's errors.Is
		// dispatch sees the original category.
		slog.Error("funnel: EnsureAccount OpenAccount failed",
			"owner_type", ownerType,
			"owner_id", ownerID,
			"err", openErr,
		)
		return "", openErr
	}
}

// QualifyOnDemand is BROKER_PHASE2 §2 Point 2 — the on-demand money gate.
// Pure durable read; off-shard; zero writes; no state. L7 asks the broker
// "does this account clear the on-demand bar?" and gets a bare boolean —
// the balance and the threshold stay inside the broker (L7 is money-blind
// per A2.8 / P2-2·D1).
//
// The gate has two conjoined rules:
//
//  1. Debt never qualifies. `trading_cash < 0` returns false regardless
//     of OnDemandMinimum. OnDemandMinimum is non-negative at bringup
//     (config validate >= 0), so the strict comparison alone would also
//     reject debt — but we assert the rule explicitly so a future
//     validation relaxation (negative floors, incentives, etc.) does not
//     silently let a debtor through. This is a load-bearing property of
//     the gate, not an implementation detail of the min.
//
//  2. The threshold is strict: `trading_cash > OnDemandMinimum`. A
//     balance equal to the floor does NOT qualify — the user must be
//     strictly above the bar. Zero min therefore means "any positive
//     balance qualifies," not "any non-negative balance qualifies."
//
// Returns:
//
//   - (true,  nil)                if cash >= 0 AND cash >  OnDemandMinimum
//   - (false, nil)                if cash <  0 OR  cash <= OnDemandMinimum
//                                   — a valid "no," NOT an error (an
//                                   under-balance or a debt is a legitimate
//                                   state the user can top up out of; the
//                                   broker does not treat it as a failure)
//   - (false, store.ErrNoAccount) if the account row is missing — treated
//                                   as a bug signal (EnsureAccount is the
//                                   caller's prerequisite, per the Phase-2
//                                   doc §2 error table), not a routine
//                                   outcome
//   - (false, store.ErrLoadFailed) on any transport / driver failure
//
// Error dispatch is errors.Is on the category sentinels per
// [[feedback_error_category_sentinels]] — never errors.As on the Store's
// wrapped-cause strings.
func (broker *Broker) QualifyOnDemand(ctx context.Context, accountID AccountID) (bool, error) {
	slog.Debug("funnel: QualifyOnDemand begin",
		"account_id", accountID,
		"on_demand_minimum", broker.money.OnDemandMinimum,
	)

	cash, readErr := broker.store.TradingCash(ctx, accountID)
	switch {
	case readErr == nil:
		// Rule 1: debt never qualifies. Explicit — do not rely on
		// OnDemandMinimum's non-negative invariant to make this true by
		// side-effect.
		if cash < 0 {
			slog.Info("funnel: QualifyOnDemand ok (debt rejected)",
				"account_id", accountID,
				"qualified", false,
			)
			return false, nil
		}
		// Rule 2: strict greater-than. Equal-to-floor does not qualify.
		qualified := cash > broker.money.OnDemandMinimum
		slog.Info("funnel: QualifyOnDemand ok",
			"account_id", accountID,
			"qualified", qualified,
		)
		return qualified, nil

	case errors.Is(readErr, store.ErrNoAccount):
		// Caller must have run EnsureAccount first — a miss here is a
		// programming-order bug, not a routine outcome. Return the
		// category sentinel so the caller's errors.Is dispatch sees it.
		slog.Error("funnel: QualifyOnDemand ErrNoAccount (EnsureAccount prerequisite missed)",
			"account_id", accountID,
		)
		return false, store.ErrNoAccount

	default:
		// Transport failure (pool closed, ctx cancelled, driver error).
		// Collapse to the category sentinel; the Store already logged
		// the wrapped cause.
		slog.Error("funnel: QualifyOnDemand ErrLoadFailed",
			"account_id", accountID,
			"err", readErr,
		)
		return false, store.ErrLoadFailed
	}
}
