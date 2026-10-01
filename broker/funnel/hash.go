package funnel

// hashAccount maps an AccountID to a shard index in [0, shardCount).
//
// Algorithm: FNV-1a computed inline over the string bytes.
//   - deterministic within a run — the same AccountID always yields the
//     same index, so an account's state never splits across shards
//     (BROKER_TIER0 §Point-1);
//   - well-distributed for arbitrary opaque string ids;
//   - zero-alloc — the router runs on every command, so the hot path
//     must not allocate. An inline loop over the string avoids the
//     hash/fnv package's []byte-taking Write, which forces a heap escape
//     for the string.
//
// The source doc (BROKER_CONCURRENCY_AND_ROUTING §Point-1) leaves the
// hash abstract; FNV-1a inline is the implementation choice locked in
// BROKER_TIER0_IMPLEMENTATION_DESIGN §Point-1.
//
// Callers must pass shardCount > 0; a zero shardCount would divide by
// zero and is a programming error (the router builds shardCount from
// len(bundles), which the wiring root guarantees is positive).
func hashAccount(accountID AccountID, shardCount int) int {
	const (
		fnvOffset32 uint32 = 2166136261
		fnvPrime32  uint32 = 16777619
	)
	hash := fnvOffset32
	for byteIndex := 0; byteIndex < len(accountID); byteIndex++ {
		hash ^= uint32(accountID[byteIndex])
		hash *= fnvPrime32
	}
	return int(hash % uint32(shardCount))
}
