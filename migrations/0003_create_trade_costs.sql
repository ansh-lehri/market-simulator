-- +goose Up
CREATE TABLE trade_costs (
    trade_id       TEXT   NOT NULL,
    brokerage      BIGINT NOT NULL DEFAULT 0,
    stt            BIGINT NOT NULL DEFAULT 0,   -- sell-side only
    exchange       BIGINT NOT NULL DEFAULT 0,   -- both sides
    sebi           BIGINT NOT NULL DEFAULT 0,   -- both sides
    stamp          BIGINT NOT NULL DEFAULT 0,   -- buy-side only
    gst            BIGINT NOT NULL DEFAULT 0,   -- on (brokerage+exchange+sebi)
    autosquare_fee BIGINT NOT NULL DEFAULT 0,   -- CST-5, only when exit_reason = 'auto_square'
    total          BIGINT NOT NULL,             -- == participant_trades.total_costs (denormalized for audit)

    CONSTRAINT pk_trade_costs       PRIMARY KEY (trade_id),
    CONSTRAINT fk_trade_costs_trade FOREIGN KEY (trade_id) REFERENCES participant_trades(trade_id)
);

-- +goose Down
DROP TABLE trade_costs;
