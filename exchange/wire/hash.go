package wire

import (
	"hash/fnv"

	"simulator/contract"
)

// hashInstrument is the stable hash the Ingress uses to route by
// hash(Instrument) % numShards (EXCHANGE_10 §5). Stability matters so an
// instrument always lands on the same shard — its InstrumentState lives
// there. FNV-32a is deterministic and dep-free from stdlib.
func hashInstrument(instrument contract.InstrumentID) uint32 {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(instrument))
	return hasher.Sum32()
}
