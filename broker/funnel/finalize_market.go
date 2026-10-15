package funnel

import (
	"context"
	"fmt"
	"log/slog"

	"simulator/economy/lifecycle"
)

// FinalizeMarketOnShard is the shard-internal command the broker's
// FinalizeMarket method fans out to EVERY shard — accounts for a
// closing market are distributed across all N shards by hash(AccountID),
// so finalize is not routable by Account(). Each shard runs its own
// sweep over loadedByExchange[ExchangeID] and replies with per-shard
// aggregates when done.
//
// Carries a context.Context deliberately (the shipped shard commands do
// not): handleFinalizeMarketOnShard calls lifecycle.SessionSave which
// does Postgres + Redis I/O, and the conductor's close barrier owns a
// bounded deadline that must plumb through to those stores. The
// embedded ctx is the only input the handler reads from it.
//
// Fields:
//
//   - Ctx         — bounded by the conductor's close deadline; passed
//                   verbatim into every lifecycle.SessionSave call.
//   - ExchangeID  — the market being closed; the sweep walks
//                   loadedByExchange[ExchangeID].
//   - Reply       — buffered(1), caller-owned. The handler sends exactly
//                   one FinalizeMarketReply.
//
// Account() returns an empty AccountID — this command does NOT go
// through the hash router. Broker.FinalizeMarket uses routeToShard to
// push it directly onto each shard's inbox.
type FinalizeMarketOnShard struct {
	Ctx        context.Context
	ExchangeID ExchangeID
	Reply      chan FinalizeMarketReply
}

// Account returns the empty routing key; FinalizeMarketOnShard is
// delivered via routeToShard, not route(). The method exists only to
// satisfy the Command interface.
func (cmd FinalizeMarketOnShard) Account() AccountID { return "" }

// FinalizeMarketReply is the per-shard aggregate the sweep returns.
//
//   - AccountsProcessed — count of LoadedAccounts the sweep touched.
//   - SaveErrors        — count of SessionSave calls that returned a
//                         non-nil error.
//   - FirstErr          — the first SessionSave error observed, wrapped
//                         with the AccountID. nil if the sweep had zero
//                         save failures.
type FinalizeMarketReply struct {
	AccountsProcessed int
	SaveErrors        int
	FirstErr          error
}

// handleFinalizeMarketOnShard is the shard-side sweep. For every account
// indexed under ExchangeID (whether live in sessions or parked in
// detached): persist via lifecycle.SessionSave (canonical only — a
// non-canonical account in a canonical market's index is an invariant
// violation and panics), then unload the account from the shard.
//
// Forward progress: a per-account SessionSave failure is LOGGED and the
// sweep continues; the LoadedAccount's maps STAY populated for that
// account so the Redis snapshot survives (same don't-discard-on-fail
// contract as lifecycle.SessionSave). A later retry can pick it up.
//
// Blocks the shard for the sweep duration. For canonical EOD this is
// acceptable: the market is already Stopping in the conductor so no new
// orders are coming in for it, and the pre-close auto-square ran
// earlier so SessionSave's assert-flat precondition holds. Accounts for
// OTHER markets that happen to hash to this shard DO wait for the sweep
// to finish — flagged, bounded by the exchangeID's account count.
func (ownerShard *shard) handleFinalizeMarketOnShard(cmd FinalizeMarketOnShard) {
	accountsForExchange := ownerShard.loadedByExchange[cmd.ExchangeID]
	reply := FinalizeMarketReply{}

	if len(accountsForExchange) == 0 {
		slog.Debug("funnel: handleFinalizeMarketOnShard nothing to sweep",
			"exchange_id", cmd.ExchangeID,
		)
		cmd.Reply <- reply
		return
	}

	slog.Info("funnel: handleFinalizeMarketOnShard begin",
		"exchange_id", cmd.ExchangeID,
		"account_count", len(accountsForExchange),
	)

	// Snapshot the AccountID set before iterating — the sweep mutates
	// the same map we are reading, which would otherwise be a range-
	// over-mutating-map hazard.
	accountIDs := make([]AccountID, 0, len(accountsForExchange))
	for accountID := range accountsForExchange {
		accountIDs = append(accountIDs, accountID)
	}

	for _, accountID := range accountIDs {
		// Prefer the live session (user never pressed Leave) over the
		// detached record (user left before close); if the account
		// appears in neither, the index is desynchronized — invariant
		// violation per the project constitution.
		if live, present := ownerShard.sessions[accountID]; present {
			if ownerShard.finalizeOneAccount(cmd.Ctx, accountID, live.account, live.binding.SessionID, &reply) {
				delete(ownerShard.sessions, accountID)
				delete(accountsForExchange, accountID)
			}
			continue
		}
		if parked, present := ownerShard.detached[accountID]; present {
			if ownerShard.finalizeOneAccount(cmd.Ctx, accountID, parked.loaded, parked.lastSessionID, &reply) {
				delete(ownerShard.detached, accountID)
				delete(accountsForExchange, accountID)
			}
			continue
		}
		panic(fmt.Sprintf(
			"funnel: handleFinalizeMarketOnShard: index has account %q under exchange %q but neither sessions nor detached carries it (invariant violation)",
			accountID, cmd.ExchangeID,
		))
	}

	// Drop the index entry if every account for this exchangeID was
	// unloaded. If some SessionSave calls failed we leave entries
	// behind so a retry can walk them again.
	if len(accountsForExchange) == 0 {
		delete(ownerShard.loadedByExchange, cmd.ExchangeID)
	}

	slog.Info("funnel: handleFinalizeMarketOnShard done",
		"exchange_id", cmd.ExchangeID,
		"accounts_processed", reply.AccountsProcessed,
		"save_errors", reply.SaveErrors,
	)
	cmd.Reply <- reply
}

// finalizeOneAccount persists one LoadedAccount via lifecycle.SessionSave
// and updates the per-sweep aggregates. Returns true if the save
// succeeded (caller should unload the account from maps + index); false
// if it failed (caller leaves the maps alone so the Redis snapshot
// survives for a retry).
//
// Panics on an on-demand account (PersistOnClose=false) in a canonical
// market's sweep — invariant violation per the project constitution.
// SessionSave itself would also refuse via ErrNotCanonical, but the
// panic surfaces the wiring bug earlier and louder.
func (ownerShard *shard) finalizeOneAccount(
	ctx context.Context,
	accountID AccountID,
	loaded lifecycle.LoadedAccount,
	sessionID SessionID,
	aggregate *FinalizeMarketReply,
) bool {
	aggregate.AccountsProcessed++

	if !loaded.Account.PersistOnClose {
		panic(fmt.Sprintf(
			"funnel: finalizeOneAccount: account %q has PersistOnClose=false in a canonical FinalizeMarket sweep (want ErrNonCanonicalDuringFinalize category — see errors.go)",
			accountID,
		))
	}

	// saver is nil only when the shard was built with a nil (or
	// nil-Lifecycle) bundle — a Phase-0 wiring where SessionSave was
	// never meant to run. Refuse rather than panic deep inside a nil
	// method call, and record the miss on the aggregate so the caller
	// learns about it.
	if ownerShard.saver == nil {
		slog.Error("funnel: finalizeOneAccount session saver missing (shard not wired for finalize)",
			"account_id", accountID,
			"session_id", sessionID,
		)
		aggregate.SaveErrors++
		if aggregate.FirstErr == nil {
			aggregate.FirstErr = fmt.Errorf("funnel: FinalizeMarket: account %q: session saver missing on shard", accountID)
		}
		return false
	}

	if saveErr := ownerShard.saver.SessionSave(ctx, sessionID, loaded); saveErr != nil {
		slog.Error("funnel: finalizeOneAccount SessionSave failed; leaving account loaded for retry",
			"account_id", accountID,
			"session_id", sessionID,
			"err", saveErr,
		)
		aggregate.SaveErrors++
		if aggregate.FirstErr == nil {
			aggregate.FirstErr = fmt.Errorf("funnel: FinalizeMarket: account %q: %w", accountID, saveErr)
		}
		return false
	}

	slog.Info("funnel: finalizeOneAccount saved + unloading",
		"account_id", accountID,
		"session_id", sessionID,
	)
	return true
}

// FinalizeMarket is the single per-market close barrier the conductor
// waits on at closeAt. Fans out to every shard, each sweeping its own
// accounts for the given ExchangeID (persist via lifecycle.SessionSave
// then unload). Synchronous: returns only after every shard replied OR
// the caller's ctx fired.
//
// Why fan out to ALL shards: accounts hash to shards by AccountID, so a
// closing market's accounts are distributed across every shard. There
// is no single "owning shard" to visit.
//
// Error contract:
//
//   - nil                       — every shard swept cleanly, every
//                                 SessionSave returned nil.
//   - ErrBrokerTimeout          — ctx fired before all shards replied;
//                                 some sweeps may have landed, some may
//                                 still be in flight. The conductor
//                                 proceeds-on-error (forward progress).
//   - ErrFinalizeIncomplete     — every shard replied, but one or more
//                                 reported SessionSave errors. Wraps
//                                 the first per-shard FirstErr via %w
//                                 so errors.Is dispatch sees it.
//
// Even on timeout, the shards continue draining their inboxes; the
// per-shard reply chans are buffered-1 so the pending FinalizeMarketReply
// sends never block.
func (broker *Broker) FinalizeMarket(ctx context.Context, exchangeID ExchangeID) error {
	slog.Info("funnel: FinalizeMarket begin",
		"exchange_id", exchangeID,
		"shard_count", broker.shardCount,
	)

	// One buffered-1 reply chan per shard, so a late-arriving reply
	// never blocks the shard goroutine (same Tier-0 §7 contract as
	// every other request/response command).
	replyChans := make([]chan FinalizeMarketReply, broker.shardCount)
	for shardIndex := range broker.shards {
		replyChans[shardIndex] = make(chan FinalizeMarketReply, 1)
		broker.routeToShard(shardIndex, FinalizeMarketOnShard{
			Ctx:        ctx,
			ExchangeID: exchangeID,
			Reply:      replyChans[shardIndex],
		})
	}

	totalProcessed := 0
	totalSaveErrors := 0
	var firstShardErr error
	for shardIndex, replyChan := range replyChans {
		select {
		case reply := <-replyChan:
			totalProcessed += reply.AccountsProcessed
			totalSaveErrors += reply.SaveErrors
			if reply.FirstErr != nil && firstShardErr == nil {
				firstShardErr = reply.FirstErr
			}
		case <-ctx.Done():
			slog.Error("funnel: FinalizeMarket ctx fired mid-wait",
				"exchange_id", exchangeID,
				"shards_replied", shardIndex,
				"shards_pending", broker.shardCount-shardIndex,
			)
			return ErrBrokerTimeout
		}
	}

	if firstShardErr != nil {
		slog.Error("funnel: FinalizeMarket completed with per-shard save errors",
			"exchange_id", exchangeID,
			"accounts_processed", totalProcessed,
			"save_errors", totalSaveErrors,
			"first_err", firstShardErr,
		)
		return fmt.Errorf("%w: %w", ErrFinalizeIncomplete, firstShardErr)
	}

	slog.Info("funnel: FinalizeMarket ok",
		"exchange_id", exchangeID,
		"accounts_processed", totalProcessed,
	)
	return nil
}

// routeToShard pushes cmd onto the given shard's inbox directly,
// bypassing the hash router. The sibling of route() for fan-out
// commands whose destination is picked by the broker (FinalizeMarket
// fans to all shards) rather than by hash(AccountID). Unexported — the
// hash router is still the only ingress for account-keyed traffic.
//
// Panics on an out-of-range shardIndex — a caller bug per the project
// constitution's invariant-violation rule.
func (broker *Broker) routeToShard(shardIndex int, cmd Command) {
	if shardIndex < 0 || shardIndex >= broker.shardCount {
		panic(fmt.Sprintf("funnel: routeToShard: shardIndex %d out of range [0, %d)", shardIndex, broker.shardCount))
	}
	broker.shards[shardIndex].inbox <- cmd
}
