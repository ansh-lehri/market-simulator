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
