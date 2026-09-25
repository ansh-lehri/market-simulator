package persistence

// Key builders — one place, matching LAYER_5_PERSISTENCE §7 verbatim. Every
// builder honors Config.KeyNamespace: an empty namespace (production) yields
// the raw "l5:..." keys; a non-empty namespace prepends "{ns}:" so integration
// tests can flush their own keyspace without touching another test's data.
//
// Keys built here:
//   {ns:}l5:snap:{session_id}:acct:{account_id}:cash
//   {ns:}l5:snap:{session_id}:acct:{account_id}:pos:{instrument_id}
//   {ns:}l5:snap:{session_id}:acct:{account_id}:resv:{order_id}
//   {ns:}l5:buf:trades

const (
	rootPrefix     = "l5"
	snapSubtree    = "snap"
	accountSubtree = "acct"
	cashLeaf       = "cash"
	positionLeaf   = "pos"
	reservLeaf     = "resv"
	bufferPath     = "buf:trades"
)

// nsPrefix returns the leading segment applied to every key. Empty when
// namespace is unset (production default).
func nsPrefix(namespace string) string {
	if namespace == "" {
		return ""
	}
	return namespace + ":"
}

// accountBase returns the shared prefix for every per-account snapshot key.
// Shape: {ns:}l5:snap:{session_id}:acct:{account_id}
func accountBase(namespace, sessionID, accountID string) string {
	return nsPrefix(namespace) +
		rootPrefix + ":" + snapSubtree + ":" + sessionID + ":" +
		accountSubtree + ":" + accountID
}

// cashKey returns the Redis key for one account's cash snapshot.
func cashKey(namespace, sessionID, accountID string) string {
	return accountBase(namespace, sessionID, accountID) + ":" + cashLeaf
}

// posKey returns the Redis key for one instrument's position snapshot.
func posKey(namespace, sessionID, accountID, instrumentID string) string {
	return accountBase(namespace, sessionID, accountID) + ":" + positionLeaf + ":" + instrumentID
}

// posPrefix returns the glob pattern that matches every position key for one
// account (for SCAN during readSnapshot / stale-key cleanup in writeSnapshot).
func posPrefix(namespace, sessionID, accountID string) string {
	return accountBase(namespace, sessionID, accountID) + ":" + positionLeaf + ":*"
}

// resvKey returns the Redis key for one reservation snapshot.
func resvKey(namespace, sessionID, accountID, orderID string) string {
	return accountBase(namespace, sessionID, accountID) + ":" + reservLeaf + ":" + orderID
}

// resvPrefix returns the glob pattern that matches every reservation key for
// one account.
func resvPrefix(namespace, sessionID, accountID string) string {
	return accountBase(namespace, sessionID, accountID) + ":" + reservLeaf + ":*"
}

// bufKey returns the closed-trade write-behind buffer key. Global per
// namespace — one buffer feeds one drainer.
func bufKey(namespace string) string {
	return nsPrefix(namespace) + rootPrefix + ":" + bufferPath
}
