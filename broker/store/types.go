package store

import "simulator/economy/account"

// AccountID re-exports economy/account.AccountID (a string alias) so
// Phase 2/3 callers of this package don't need to import economy/account
// directly. The underlying type is still `string` — the broker/store
// seam speaks the DB's language (BROKER_PHASE1 §2.4 / P1-D5).
type AccountID = account.AccountID

// maxRecent caps n / limit on the recent-trades and trade-page accessors
// so no caller can request LIMIT 1_000_000. Chosen conservatively; a
// human UI page is typically 20–100 rows, the agent snapshot (A14)
// clamps itself at 20 upstream, and crash-recovery reads a bounded set.
// Belt-and-suspenders defensive cap — Stage B surfaces it.
const maxRecent = 500
