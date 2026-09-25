-- +goose Up
CREATE TABLE accounts (
    account_id     TEXT        NOT NULL,
    owner_type     TEXT        NOT NULL,              -- 'agent' | 'human'
    owner_id       TEXT        NOT NULL,              -- agent_id or user_id
    trading_cash   BIGINT      NOT NULL,              -- SIGNED (may be negative: MON-7 / Option 4), coins
    bank_cash      BIGINT      NOT NULL DEFAULT 0,    -- >= 0, banked reserve
    drip_active    BOOLEAN     NOT NULL DEFAULT FALSE,-- account currently refilling (both pools empty)
    drip_progress  BIGINT,                            -- session-denominated progress to next tap; NULL when inactive
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_accounts       PRIMARY KEY (account_id),
    CONSTRAINT uq_accounts_owner UNIQUE (owner_type, owner_id),
    CONSTRAINT ck_bank_nonneg    CHECK (bank_cash >= 0)
);

CREATE INDEX ix_accounts_owner ON accounts (owner_type, owner_id);

-- +goose Down
DROP TABLE accounts;
