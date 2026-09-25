# Layer 5 — Persistence **Schema & Migrations** (Phase 2, Stage 2)

<!-- SOURCE OF TRUTH: this doc is an extract of LAYER_5_PERSISTENCE.md §2 (identity spine),
     §3.1–3.3 (the three CREATE TABLE blocks, VERBATIM), and §6 (locked decisions). The
     table definitions are transcribed, not designed here — if this doc and
     LAYER_5_PERSISTENCE.md ever disagree on a column/constraint, LAYER_5_PERSISTENCE.md
     wins and this doc is corrected. What this doc ADDS beyond the source is only
     migration mechanics (goose structure, down order, test-DB provisioning) and the
     forward-seam flags — none of which change the schema. -->

## 0. Scope

Stage 2 makes the durable tables **real** in Postgres, as versioned migrations. It is the
first DB task in the economy build and the artifact every later persistence stage binds to:
`store` (Stage 3) writes against exactly these columns/constraints; `lifecycle` (Stage 5)
loads/saves against `accounts`; the drainer (Stage 4) inserts into
`participant_trades` + `trade_costs`.

**Canonical-only.** These tables hold durable, cross-session, **canonical** state. On-demand
sessions never write here (that suppression is a lifecycle concern, Stage 5 — not a schema
concern). Redis (snapshot + write-behind buffer) has **no schema** and is out of scope for
this stage.

**Three tables, in FK order:** `accounts` → `participant_trades` → `trade_costs`.

## 1. Identity spine (from §2 — context, not a schema change)

L5 owns the `accounts` table. Every participant — agent or human — has exactly **one**
account. **All money data FKs to `account_id`**, never to `agent_id` / `user_id`; agents and
humans differ only in `owner_type`. `accounts.owner_id` references the human/platform `users`
layer, which is **not** L5's and is not created by these migrations.

## 2. Tables (VERBATIM from LAYER_5_PERSISTENCE.md §3.1–3.3)

### 2.1 `accounts` — durable three-pool capital (one row per participant)

```sql
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
```

Load-bearing points (§3.1): `trading_cash` is **SIGNED** — debt is a negative balance, no
separate debt column, **no `CHECK (>= 0)` on it, deliberately**. **No available/reserved/settled
split here** — that is live session state (Redis/in-memory); the durable row holds only the
resting figure the split reconciles to. `UNIQUE (owner_type, owner_id)` = one account per
participant. `bank_cash >= 0` — only the trading account absorbs debt.

### 2.2 `participant_trades` — durable activity record (append-only)

```sql
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

    CONSTRAINT pk_participant_trades          PRIMARY KEY (trade_id),
    CONSTRAINT fk_participant_trades_account  FOREIGN KEY (account_id) REFERENCES accounts(account_id),
    CONSTRAINT ck_costs_nonneg                CHECK (total_costs >= 0),
    CONSTRAINT ck_qty_pos                     CHECK (quantity > 0)
);

-- the load-bearing index: "last N trades for this account" (recent_streak + trade history)
CREATE INDEX ix_trades_account_recent ON participant_trades (account_id, exit_at DESC);
-- "this session's trades" (session P&L / session-scoped views)
CREATE INDEX ix_trades_session        ON participant_trades (session_id);
```

Load-bearing points (§3.2): **three P&L columns** (`gross_pnl`/`total_costs`/`net_pnl`) keep the
gross→costs→net view without a join; `pnl_pct` signed, on net. **Round-trip summary rows** — one
row per logical closed trade (matched/weighted FIFO prices), **not per fill**. `(account_id,
exit_at DESC)` is the single most important index (serves `recent_streak`). `fk_participant_trades_account` FKs `account_id` to `accounts`; `instrument_id` is plain TEXT for now.

### 2.3 `trade_costs` — itemized cost stack (contract-note child, 1:1)

```sql
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

    CONSTRAINT pk_trade_costs          PRIMARY KEY (trade_id),
    CONSTRAINT fk_trade_costs_trade    FOREIGN KEY (trade_id) REFERENCES participant_trades(trade_id)
);
```

Load-bearing points (§3.3): **PK = `trade_id`** (1:1 with `participant_trades`). Line items are the
locked India stack (5.3). `total` is denormalized to equal `participant_trades.total_costs` for
standalone auditability.

## 3. Migration mechanics (added by this doc — mechanics, not schema)

- **Tool: goose.** Versioned SQL migrations, single binary, no runtime dependency in the app.
  Migrations are plain `.sql` with goose `-- +goose Up` / `-- +goose Down` annotations.
- **Location:** a `migrations/` dir at the module root (final path an implementation detail).
- **Ordering — up:** `accounts` first, then `participant_trades` (FKs → accounts), then
  `trade_costs` (FKs → participant_trades). A table's indexes and constraints are created **in the
  same migration** as the table.
- **Ordering — down:** exact reverse, to respect FKs — `DROP TABLE trade_costs;` →
  `DROP TABLE participant_trades;` → `DROP TABLE accounts;`. Down migrations are plain `DROP TABLE`
  (indexes drop with their table).
- **One migration or three:** either the three tables as three sequential migrations (one per
  table, cleanest history) or a single `0001` creating all three — implementer's call; the up/down
  ordering rule above holds regardless.
- **Connectivity:** goose runs against whatever the Stage-1 env config points at (`DATABASE_URL`).
  **Nothing hardcoded** — local Postgres now, remote/cloud Postgres later is purely a change of that
  env value + `sslmode`. No code or migration change to switch environments.
- **Test DB provisioning:** the **same** migrations build the dedicated integration-test database
  (`l5_test`, or a test schema), so `store`/`lifecycle` integration tests (tx-rollback per test) run
  against an identical structure. The test DB is provisioned by running these migrations against the
  test `DATABASE_URL`; it is never hand-maintained.

## 4. Forward seams (flagged, NOT actioned in this stage)

- **Country cost-table split.** `trade_costs` carries the **India** stack inline (per §3.3). When US
  is built, the columns differ (SEC/TAF/FINRA) and this becomes country-parametric — the A16 read
  view maps `trade_costs → india_trade_costs` (+ future `us_trade_costs`) via a registry. **Not now:**
  the table stays named `trade_costs` with the India columns; the rename/partition is a later
  migration when US arrives.
- **`instrument_id` FK.** Plain `TEXT` now; a real FK when an instrument-config store exists (a later
  migration).
- **Fill-level audit.** `participant_trades` holds round-trip summary rows only; a separate `fills`
  table is a future addition if fill-level audit is ever wanted — not in scope.

## 5. Not in these migrations (boundary confirmations, from §3.4)

- **`users` / human identity + subscription tier** — human/platform layer; `accounts.owner_id`
  references it but this stage does not create it.
- **Live available/reserved/settled splits, open positions, reservation records** — session-scoped;
  **Redis snapshot only**, never Postgres.
- **Unrealized P&L / marks** — derived at read-time; never stored.

## 6. Definition of done (Stage 2)

- goose up applies cleanly against the env-configured Postgres; the three tables, all constraints
  (`pk_accounts`, `pk_participant_trades`, `pk_trade_costs`, `uq_accounts_owner`, `ck_bank_nonneg`,
  `fk_participant_trades_account`, `ck_costs_nonneg`, `ck_qty_pos`, `fk_trade_costs_trade`) and all
  three indexes exist and match §2 exactly.
- goose down reverses cleanly in FK order, leaving no tables.
- The same migrations provision `l5_test` from the test `DATABASE_URL`.
- Switching to a remote/cloud Postgres requires **only** an env change — verified by running up/down
  against a second `DATABASE_URL` with `sslmode=require`.