package bringup

import (
	"errors"
	"fmt"

	"simulator/economy/cost"
	"simulator/economy/persistence"
)

// ErrCountryUnavailable is returned when a BrokerConfig.Country names a
// country whose stack is not available in this build — either because
// it is a defined-but-empty slot ("US") or because the string is not
// in the allowlist. Category sentinel; caller dispatches via errors.Is.
var ErrCountryUnavailable = errors.New("bringup: country not available")

// countryStack bundles the two country-specific values a shard's
// capstone bundle needs: the cost module (fed to cost math in 5.3) and
// the persistence adapter (fed to the drainer's SQL layer).
//
// Correlation invariant (capstone/bundle.go documents it as "caller-
// enforced, not checked"): CostAdapter must match Cost's country.
// Mismatched values route one country's line names into another's
// column layout and fail at the first EnqueueClosedTrade.
// buildCountryStack is the ONE place both are chosen together — the
// correlation is upheld by construction.
type countryStack struct {
	Cost    cost.CostModule
	Adapter persistence.CountryCostAdapter
}

// buildCountryStack maps a BrokerConfig.Country to its (cost module,
// persistence adapter) pair. This is the SOLE place a country string
// binds to concrete Phase-1 wiring — adding a country means adding a
// case here and nothing else at this layer. The country-table dispatch
// Phase 6 does for the read handlers (BROKER_READ_SURFACES §8.1) is a
// separate concern — same allowlist pattern, different mapping.
//
// Allowlist (P1-D7, confirmed with the user):
//   - "IN" → full India stack (cost.NewIndiaCostModule + IndiaCostAdapter)
//   - "US" → ErrCountryUnavailable (slot defined, body not built — same
//     honest-501 posture as the A16 read handler for US).
//   - any other → ErrCountryUnavailable (not in allowlist).
func buildCountryStack(country string) (countryStack, error) {
	switch country {
	case "IN":
		return countryStack{
			Cost:    cost.NewIndiaCostModule(phase1IndiaRates),
			Adapter: persistence.IndiaCostAdapter{},
		}, nil
	case "US":
		return countryStack{}, fmt.Errorf("US (slot defined, not built): %w", ErrCountryUnavailable)
	default:
		return countryStack{}, fmt.Errorf("%q (not in allowlist [IN, US]): %w", country, ErrCountryUnavailable)
	}
}

// phase1IndiaRates is the pinned India rulebook used by every Phase-1
// shard until rulebook injection lands (project_rulebook_injection_tbd).
// Values match the project-locked India rates
// (project_india_rate_locks):
//
//	STT           0.025%   (sell-side only)
//	Exchange      0.00297% (NSE, both sides)
//	SEBI          0.0001%  (both sides)
//	Stamp         0.003%   (buy-side only)
//	GST           18%      (on exchange+SEBI per fill; on brokerage per order)
//	Brokerage     ₹20      (2000 paise, flat per executed order)
//	AutosquareFee ₹50      (5000 paise, CST-5)
//
// This literal is Phase 1's workaround; when the rulebook-injection
// story is decided, this is the single site to re-point.
var phase1IndiaRates = cost.IndiaRates{
	STT:           cost.Rate{Num: 25, Den: 100_000},
	Exchange:      cost.Rate{Num: 297, Den: 10_000_000},
	SEBI:          cost.Rate{Num: 1, Den: 1_000_000},
	Stamp:         cost.Rate{Num: 3, Den: 100_000},
	GST:           cost.Rate{Num: 18, Den: 100},
	Brokerage:     2000,
	AutosquareFee: 5000,
}
