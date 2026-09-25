-- +goose Up
CREATE TABLE participant_trades (
    trade_id       TEXT        NOT NULL,
    account_id     TEXT        NOT NULL,
    session_id     TEXT        NOT NULL,              -- which session; separates within- vs cross-session
    instrument_id  TEXT        NOT NULL,              -- plain text for now (FK when instrument store exists)
    direction      TEXT        NOT NULL,              -- 'long' | 'short' (prototype long-only; stored)
    quantity       BIGINT      NOT NULL,
    entry_price    BIGINT      NOT NULL,              -- integer ticks (never float)
    exit_price     BIGINT      NOT NULL,              -- integer ticks
    entry_at       TIMESTAMPTZ NOT NULL,
    exit_at        TIMESTAMPTZ NOT NULL,              -- the "last N" ordering key
    gross_pnl      BIGINT      NOT NULL,              -- SIGNED: (exit-entry)*qty, pre-cost
    total_costs    BIGINT      NOT NULL,              -- >= 0, sum of the 5.3 stack for this trade
    net_pnl        BIGINT      NOT NULL,              -- SIGNED: gross_pnl - total_costs (what hit the account)
    pnl_pct        REAL        NOT NULL,              -- SIGNED, on net
    exit_reason    TEXT        NOT NULL,              -- 'voluntary' | 'auto_square' | 'panic' | ...

    CONSTRAINT pk_participant_trades         PRIMARY KEY (trade_id),
    CONSTRAINT fk_participant_trades_account FOREIGN KEY (account_id) REFERENCES accounts(account_id),
    CONSTRAINT ck_costs_nonneg               CHECK (total_costs >= 0),
    CONSTRAINT ck_qty_pos                    CHECK (quantity > 0)
);

-- "last N trades for this account" (recent_streak + trade history)
CREATE INDEX ix_trades_account_recent ON participant_trades (account_id, exit_at DESC);
-- "this session's trades" (session P&L / session-scoped views)
CREATE INDEX ix_trades_session        ON participant_trades (session_id);

-- +goose Down
DROP TABLE participant_trades;
