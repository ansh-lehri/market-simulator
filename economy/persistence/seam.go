package persistence

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"simulator/economy/account"
	"simulator/economy/realize"
)

// Compile-time guard: Component satisfies realize.Persistence in production.
// The reserve.Persistence guard lives in seam_guard_test.go (external test
// package) so this file does not import reserve — the doc §9 rule.
var _ realize.Persistence = (*Component)(nil)

// MarkSnapshotDirty flags one account's live state as dirty. Zero I/O — the
// dirty set is a plain in-memory map (D6). The owner drains the set via
// DrainDirty and calls WriteSnapshot on its own cadence (Phase 2: fully
// synchronous, owner-driven per LAYER_5_PERSISTENCE §5).
//
// Called from reserve (Reserve/Convert/Release/ReleaseExcess terminal steps)
// and, indirectly through realize's Persistence field, on a session-flat
// flush by realize. Never blocks the hot path.
func (component *Component) MarkSnapshotDirty(sessionID realize.SessionID, accountID account.AccountID) {
	component.dirty[dirtyKey{SessionID: sessionID, AccountID: accountID}] = struct{}{}
	slog.Debug("persistence: MarkSnapshotDirty",
		"session_id", sessionID,
		"account_id", accountID,
		"dirty_count", len(component.dirty),
	)
}

// EnqueueClosedTrade translates one closed round-trip and pushes it onto
// the Redis write-behind buffer. The drainer worker (Stage 4) inserts it
// into Postgres asynchronously.
//
// Validation happens up-front via the country adapter's FlattenCostColumns:
// an unknown cost line (D1) is a programming bug in cost/realize and PANICS
// dev-fatally here rather than poisoning the buffer. A Redis LPUSH failure
// panics too — the seam contract (reserve.Persistence / realize.Persistence)
// has no error return, and a dead Redis breaks the money engine.
//
// Bounded by cfg.EnqueueTimeout so a stuck Redis cannot wedge the owner
// goroutine indefinitely.
func (component *Component) EnqueueClosedTrade(closed realize.ClosedTradeRow) {
	insert := translateClosedTrade(closed)

	// Validate BEFORE push — never let a bad cost line reach the buffer.
	if _, err := component.store.costAdapter.FlattenCostColumns(insert.CostLines, insert.CostTotal); err != nil {
		if errors.Is(err, ErrUnknownCostLine) {
			panic(fmt.Sprintf("persistence: EnqueueClosedTrade: unknown cost line for trade %s: %v",
				insert.TradeID, err))
		}
		panic(fmt.Sprintf("persistence: EnqueueClosedTrade: cost validation for trade %s: %v",
			insert.TradeID, err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), component.cfg.EnqueueTimeout)
	defer cancel()
	if err := component.store.pushClosedTrade(ctx, insert); err != nil {
		panic(fmt.Sprintf("persistence: EnqueueClosedTrade: buffer push for trade %s: %v",
			insert.TradeID, err))
	}

	slog.Info("persistence: EnqueueClosedTrade ok",
		"trade_id", insert.TradeID,
		"account_id", insert.AccountID,
	)
}
