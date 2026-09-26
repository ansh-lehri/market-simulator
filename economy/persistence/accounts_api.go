package persistence

import "context"

// LoadAccount is the boundary read: session-start seed of the durable
// account row from Postgres. Called by lifecycle at session load — the
// caller uses the DurableAccount to build an in-memory Account (5.1) and
// seed the Redis snapshot mirror.
//
// Returns ErrNoAccount when the row does not exist. Postgres is read ONLY
// at this boundary (per economy/CLAUDE.md "hot path: in-memory only;
// session snapshot: Redis; boundary: Postgres").
func (component *Component) LoadAccount(ctx context.Context, accountID string) (DurableAccount, error) {
	return component.store.loadAccount(ctx, accountID)
}

// SaveAccount is the boundary write: canonical session-close write-back of
// the reconciled trading_cash figure to Postgres. The caller (lifecycle,
// D7) has already reconciled the live split (Available/Reserved/Settled)
// to one signed value — persistence just writes it.
//
// On-demand sessions never call this (PersistOnClose gate lives in
// realize/lifecycle, not here — persistence has no on/off knob).
func (component *Component) SaveAccount(ctx context.Context, durable DurableAccount) error {
	return component.store.saveAccount(ctx, durable)
}

// OpenAccount is the durable-birth boundary write (LAYER_5_2_LIFECYCLE_FLOWS
// item 18). Called ONCE per participant lifetime, at agent CREATE / human
// signup. Inserts one new accounts row and returns the AccountID used.
//
// AccountID: caller MAY supply DurableAccount.AccountID (any unique TEXT —
// scheme-prefixed, deterministic test id, or an upstream-generated id).
// When empty, persistence mints a 128-bit hex fallback. Uniqueness is
// enforced by the DB (accounts_pkey + uq_accounts_owner), not by policy
// here.
//
// The caller passes the identity fields (OwnerType, OwnerID) and the
// config-sourced starting figures (TradingCash, BankCash, DripActive=false,
// DripProgress=nil).
//
// Error mapping:
//   - uq_accounts_owner UNIQUE violation → ErrAccountExists (caller looks
//     up the existing handle for agent re-spawn / human re-login)
//   - any other pg / driver error (incl. accounts_pkey collision on a
//     caller-supplied id) → wrapped
//
// No Redis, no session — creation is durable and session-less.
func (component *Component) OpenAccount(ctx context.Context, durable DurableAccount) (string, error) {
	return component.store.openAccount(ctx, durable)
}
