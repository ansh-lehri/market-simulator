package persistence

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
)

// insertParticipantTradeSQL is the fixed INSERT for one participant_trades
// row (schema §2.2). Every column universal — country-agnostic. Idempotent:
// ON CONFLICT (trade_id) DO NOTHING so drainer replay is safe (§8 D5).
const insertParticipantTradeSQL = `INSERT INTO participant_trades (
	trade_id, account_id, session_id, instrument_id,
	direction, quantity, entry_price, exit_price,
	entry_at, exit_at,
	gross_pnl, total_costs, net_pnl, pnl_pct,
	exit_reason
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
ON CONFLICT (trade_id) DO NOTHING`

// insertClosedTrade writes ONE closed-trade row atomically: participant_trades
// (universal columns) and trade_costs (country-specific columns via the
// injected adapter) in a single transaction. Both inserts are idempotent —
// replay is safe.
//
// Any error (translate rejection, driver failure, adapter unknown-key,
// constraint violation) rolls the transaction back with no partial write.
func (component *store) insertClosedTrade(ctx context.Context, insert TradeInsert) error {
	slog.Debug("persistence: insertClosedTrade",
		"trade_id", insert.TradeID,
		"account_id", insert.AccountID,
		"session_id", insert.SessionID,
	)

	execErr := component.withTx(ctx, func(tx pgx.Tx) error {
		if err := insertParticipantTradeRow(ctx, tx, insert); err != nil {
			return err
		}
		return component.costAdapter.InsertCostRow(ctx, tx, insert.TradeID, insert.CostLines, insert.CostTotal)
	})
	if execErr != nil {
		return execErr
	}

	slog.Info("persistence: insertClosedTrade ok",
		"trade_id", insert.TradeID,
		"account_id", insert.AccountID,
		"net_pnl", insert.NetPnl,
	)
	return nil
}

// insertParticipantTradeRow is a small helper used inside insertClosedTrade's
// transaction. Unexported and free-function (not a method) so it does not
// leak onto the store surface.
func insertParticipantTradeRow(ctx context.Context, executor CostRowExecutor, insert TradeInsert) error {
	_, err := executor.Exec(ctx, insertParticipantTradeSQL,
		insert.TradeID,
		insert.AccountID,
		insert.SessionID,
		insert.InstrumentID,
		insert.Direction,
		insert.Quantity,
		insert.EntryPrice,
		insert.ExitPrice,
		insert.EntryAt,
		insert.ExitAt,
		insert.GrossPnl,
		insert.TotalCosts,
		insert.NetPnl,
		insert.PnlPct,
		insert.ExitReason,
	)
	if err != nil {
		return fmt.Errorf("persistence: insertParticipantTradeRow %s: %w", insert.TradeID, err)
	}
	return nil
}
