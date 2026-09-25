package persistence_test

// External test package (persistence_test) so this file can import reserve
// WITHOUT making production persistence code import it. LAYER_5_PERSISTENCE
// §9 rule: "Imports no reserve (satisfied structurally)". The guard below
// is a compile-time-only check — if Component ever stops satisfying
// reserve.Persistence, this file fails to compile and CI catches it before
// wiring breaks in the broker funnel.

import (
	"simulator/economy/persistence"
	"simulator/economy/reserve"
)

var _ reserve.Persistence = (*persistence.Component)(nil)
