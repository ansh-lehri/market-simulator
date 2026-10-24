package funnel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"simulator/broker/store"
	"simulator/contract"
	"simulator/economy/lifecycle"
)

// liquiditySeederFundingMultiple scales the server's flat
// StartingCapital/StartingBank for OwnerAgentOperator accounts only —
// the liquidity-seeder identities need enough capital to rest asks
// and lift them as takers across several instruments without
// starving, well beyond a normal agent's funding. A placeholder
// calibration value like every other money dial, not fitted.
const liquiditySeederFundingMultiple = 20

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
	startingCapital, startingBank := broker.money.StartingCapital, broker.money.StartingBank
	if ownerType == lifecycle.OwnerAgentOperator {
		startingCapital *= liquiditySeederFundingMultiple
		startingBank *= liquiditySeederFundingMultiple
	}
	slog.Debug("funnel: EnsureAccount begin",
		"owner_type", ownerType,
		"owner_id", ownerID,
		"starting_capital", startingCapital,
		"starting_bank", startingBank,
		"allow_retry", allowRetry,
	)

	accountID, openErr := broker.store.Lifecycle().OpenAccount(
		ctx,
		ownerType,
		ownerID,
		startingCapital,
		startingBank,
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

// SessionStartCmd is L7's payload for Phase-2 Point 3's StartSession. L7
// fills these fields from its own registry at session trigger time; the
// broker owns the shape and the delivery onto the owning shard.
//
// Mode selects the loader (Canonical → SessionLoad, OnDemand →
// OndemandSessionLoad) — the single divergence between the two flows
// inside the broker. ExchangeInbound is the per-session outbound half of
// the exchange seam L7 injects; the broker never minted it and never
// branches on mode at the punch (the mode-blind punch reads
// ownedSession.binding.ExchangeInbound verbatim, Tier 1).
type SessionStartCmd struct {
	AccountID       AccountID
	SessionID       SessionID
	Mode            lifecycle.SessionType
	ExchangeID      ExchangeID
	ExchangeInbound chan<- contract.InboundMessage
	Participant     ParticipantID
}

// StartSession is BROKER_PHASE2 §3 Point 3 — the off-shard orchestrator
// that turns a durable account into a live in-memory session on its
// owning shard. Two stages, with a hard split at the funnel boundary:
//
//  1. OFF-SHARD, on the caller's goroutine:
//     - Validate the command (nil binding / empty participant) before any
//       I/O so a malformed trigger wastes zero resources.
//     - Load the account via the Store's lifecycle.Service (slow I/O —
//       1 Postgres read + 1 Redis snapshot write). Branches by Mode: the
//       two loaders are the ONLY divergence between canonical and
//       on-demand flows inside the broker. Snapshot-fail policy lives
//       inside the L5 loaders (canonical hard-fails, on-demand tolerates)
//       — StartSession propagates the loader's error uniformly, no
//       broker branch.
//
//  2. ON-SHARD, via the funnel command path:
//     - Build the SessionBinding and route an InstallSession command.
//       The install itself (XOR guard + map write + reply) is pure
//       in-memory on the owner goroutine — the single-writer rule
//       protects s.sessions, so install MUST be a command even though
//       the load does not have to be.
//     - Wait for the shard's reply (nil on install, ErrSessionAlreadyLive
//       on XOR refusal), or ctx deadline → ErrBrokerTimeout.
//
// Why the load runs off-shard: a shard owns many accounts across many
// markets. Running SessionLoad's Postgres+Redis round-trips on the shard
// would freeze every other account on that partition — including live
// order flow. Loads also arrive in bulk at market open and A15 agent
// population, so on-shard I/O would saturate shards. Single-writer
// protects s.sessions (which is why install must be a command); it does
// not protect the not-yet-existent account (which is why the build can
// run anywhere).
//
// Error contract (traces to BROKER_PHASE2 §3 error table):
//
//   - nil                             → session installed, caller may
//                                       proceed to intake orders
//   - ErrBadBinding                   → nil ExchangeInbound; off-shard
//                                       reject, no I/O
//   - ErrBadParticipant               → empty Participant; off-shard
//                                       reject, no I/O
//   - lifecycle.ErrNoAccount          → account row missing (bug: caller
//                                       must have run EnsureAccount
//                                       first); nothing installed
//   - lifecycle.ErrLoadFailed         → Postgres unreachable mid-load;
//                                       nothing installed
//   - lifecycle.ErrSnapshotWriteFailed → canonical hard-failed on Redis
//                                       seed; nothing installed (durable
//                                       row untouched). On-demand never
//                                       surfaces this — the loader
//                                       tolerates and returns nil.
//   - ErrSessionAlreadyLive           → XOR refused; the live session for
//                                       this account is NOT the one just
//                                       loaded (teardown clears the slot)
//   - ErrBrokerTimeout                → shard did not reply before ctx
//                                       fired; the install may still have
//                                       landed (caller reconciles via a
//                                       pull-based read)
//   - error from fmt.Errorf           → unknown Mode value (programming
//                                       error at the caller — Mode must
//                                       be Canonical or OnDemand)
func (broker *Broker) StartSession(ctx context.Context, cmd SessionStartCmd) error {
	// Stage 1a — validate off-shard. These guards must fire BEFORE any
	// loader call so a malformed command does not even cost a Postgres
	// read.
	if cmd.ExchangeInbound == nil {
		slog.Error("funnel: StartSession ErrBadBinding",
			"account_id", cmd.AccountID,
			"session_id", cmd.SessionID,
		)
		return ErrBadBinding
	}
	if cmd.Participant == "" {
		slog.Error("funnel: StartSession ErrBadParticipant",
			"account_id", cmd.AccountID,
			"session_id", cmd.SessionID,
		)
		return ErrBadParticipant
	}

	slog.Info("funnel: StartSession begin",
		"account_id", cmd.AccountID,
		"session_id", cmd.SessionID,
		"mode", cmd.Mode,
		"exchange_id", cmd.ExchangeID,
	)

	// Stage 1b — load the account via the Store's lifecycle service.
	// Mode is the ONLY divergence. Snapshot-fail policy lives in the
	// loaders; StartSession propagates whatever they return.
	var (
		loaded     lifecycle.LoadedAccount
		loaderName string
		loaderErr  error
	)
	switch cmd.Mode {
	case lifecycle.Canonical:
		loaderName = "SessionLoad"
		loaded, loaderErr = broker.store.Lifecycle().SessionLoad(ctx, cmd.SessionID, cmd.AccountID)
	case lifecycle.OnDemand:
		loaderName = "OndemandSessionLoad"
		loaded, loaderErr = broker.store.Lifecycle().OndemandSessionLoad(ctx, cmd.SessionID, cmd.AccountID)
	default:
		// Programming error at the caller; a Mode outside the enum is a
		// bug upstream, not a runtime condition to tolerate.
		slog.Error("funnel: StartSession unknown Mode",
			"account_id", cmd.AccountID,
			"session_id", cmd.SessionID,
			"mode", int(cmd.Mode),
		)
		return fmt.Errorf("funnel: StartSession: unknown Mode %v (want Canonical|OnDemand)", cmd.Mode)
	}
	if loaderErr != nil {
		slog.Error("funnel: StartSession loader failed",
			"account_id", cmd.AccountID,
			"session_id", cmd.SessionID,
			"loader", loaderName,
			"err", loaderErr,
		)
		return loaderErr
	}

	// Stage 2 — build the per-session binding and route the install
	// command. The shard handler's XOR guard is the final authority on
	// the one-live-session rule.
	binding := SessionBinding{
		SessionID:       cmd.SessionID,
		ExchangeID:      cmd.ExchangeID,
		ExchangeInbound: cmd.ExchangeInbound,
	}
	reply := make(chan error, 1) // buffered-1 per Tier-0 §7
	broker.route(InstallSession{
		AccountID:   cmd.AccountID,
		Loaded:      loaded,
		Binding:     binding,
		Participant: cmd.Participant,
		Reply:       reply,
	})

	// Stage 2 cont. — wait for the shard reply, or ctx deadline. On
	// timeout the shard still processes and sends on reply; the value
	// drops into the buffer and is GC'd when we walk away.
	installErr := broker.waitReply(ctx, reply)
	if installErr != nil {
		slog.Error("funnel: StartSession install failed",
			"account_id", cmd.AccountID,
			"session_id", cmd.SessionID,
			"err", installErr,
		)
		return installErr
	}

	slog.Info("funnel: StartSession ok",
		"account_id", cmd.AccountID,
		"session_id", cmd.SessionID,
		"mode", cmd.Mode,
	)
	return nil
}
