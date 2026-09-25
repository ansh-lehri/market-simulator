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
