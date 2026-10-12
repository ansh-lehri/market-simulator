package conductor

import (
	"context"
	"fmt"
	"time"

	"simulator/exchange/clock"
	"simulator/exchange/gate"
	"simulator/exchange/wire"
)

// Session boundary phase labels in the on-disk config, mapped to the
// exchange clock's typed phase constants by phaseFromConfigLabel. The
// three labels mirror the three Phase values the clock accepts; any
// other string is a config error.
const (
	phaseLabelContinuous      = "continuous"
	phaseLabelClosingAuction  = "closing_auction"
	phaseLabelClosed          = "closed"
	sessionBoundaryTimeFormat = "15:04" // "HH:MM" in the market timezone
)

// defaultNumShards is the fallback when a market config leaves NumShards
// at zero. A canonical market that genuinely needs many shards sets
// NumShards explicitly in its config.
const defaultNumShards = 1

// phaseFromConfigLabel maps the three accepted on-disk phase labels to
// the exchange clock's typed Phase values. Any other label is a config
// error that aborts the bring-up before the exchange clock sees it (the
// clock itself panics on an invalid terminal phase, so we fail fast
// here).
func phaseFromConfigLabel(label string) (clock.Phase, error) {
	switch label {
	case phaseLabelContinuous:
		return clock.PhaseContinuous, nil
	case phaseLabelClosingAuction:
		return clock.PhaseClosingAuction, nil
	case phaseLabelClosed:
		return clock.PhaseClosed, nil
	default:
		return 0, fmt.Errorf("unknown phase label %q (want %q, %q, or %q)",
			label, phaseLabelContinuous, phaseLabelClosingAuction, phaseLabelClosed)
	}
}

// validateSchedule rejects schedules the exchange clock would otherwise
// panic on, plus one additional business rule (first boundary must open
// the session — a schedule that starts in PhaseClosed never opens the
// market).
//
// Three rules enforced here:
//
//  1. At least two boundaries: a session start plus a terminal close.
//     Zero or one boundary is a config bug — the exchange clock would
//     panic on terminal != PhaseClosed, but we abort cleanly here.
//  2. First phase is not PhaseClosed. The clock would accept a
//     closed-first schedule (it just means the market never opens
//     today), but shipping that silently masks a config mistake.
//  3. Strictly ascending At. A duplicate or descending timestamp panics
//     the clock; aborting with an L7-side error keeps the Provisioning
//     record out of the registry.
//  4. Terminal phase is PhaseClosed. The clock requires this.
func validateSchedule(boundaries []clock.PhaseBoundary) error {
	if len(boundaries) < 2 {
		return fmt.Errorf("schedule has %d boundaries, want at least 2 (session start + terminal close)", len(boundaries))
	}
	if boundaries[0].Phase == clock.PhaseClosed {
		return fmt.Errorf("first boundary phase is PhaseClosed — session never opens today")
	}
	for boundaryIndex := 1; boundaryIndex < len(boundaries); boundaryIndex++ {
		previous := boundaries[boundaryIndex-1]
		current := boundaries[boundaryIndex]
		if !current.At.After(previous.At) {
			return fmt.Errorf("schedule not strictly ascending: boundary[%d].At=%s, boundary[%d].At=%s",
				boundaryIndex-1, previous.At, boundaryIndex, current.At)
		}
	}
	terminal := boundaries[len(boundaries)-1]
	if terminal.Phase != clock.PhaseClosed {
		return fmt.Errorf("terminal boundary phase = %v, want PhaseClosed", terminal.Phase)
	}
	return nil
}

// buildExchangeArgs assembles everything wire.NewExchange needs for one
// canonical bring-up: the shard count, the session schedule anchored to
// today in the market timezone, the per-instrument specs with reference
// prices resolved from the candles store, and the derived open/close
// instants the caller stores on the Market record.
//
// All validation (schedule shape, phase labels, HH:MM parsing) runs
// before any instrument reference lookup, so a malformed schedule aborts
// without issuing any database query. Instrument references are then
// resolved in config order; the first lookup error aborts the whole
// build.
//
// Returned values:
//
//   - numShards: marketConfig.NumShards, or defaultNumShards if zero.
//   - schedule: boundaries sorted-as-authored (strict ascending enforced),
//     anchored to today in the market timezone, terminal PhaseClosed.
//   - specs: one wire.InstrumentSpec per configured instrument, with
//     Reference resolved via readReference, OHLCVStart = openInstant.
//   - openInstant: the first boundary's absolute instant (used as the
//     exchange clock's session start and as every instrument's
//     OHLCVStart).
//   - closeInstant: the terminal (PhaseClosed) boundary's absolute
//     instant. The spin-up caller stores this on the Market record so
//     teardown knows when the market ends.
//
// Pure in the sense that it reads from the injected candleReader but
// writes nothing anywhere. Safe to call before any registry reservation.
func buildExchangeArgs(
	ctx context.Context,
	reader candleReader,
	nowFn func() time.Time,
	marketConfig MarketConfig,
	location *time.Location,
) (
	numShards int,
	schedule clock.SessionSchedule,
	specs []wire.InstrumentSpec,
	openInstant time.Time,
	closeInstant time.Time,
	err error,
) {
	numShards = marketConfig.NumShards
	if numShards == 0 {
		numShards = defaultNumShards
	}

	today := nowFn().In(location)
	year, month, day := today.Date()

	boundaries := make([]clock.PhaseBoundary, 0, len(marketConfig.Session))
	for boundaryIndex, boundaryConfig := range marketConfig.Session {
		parsedWallClock, parseErr := time.ParseInLocation(sessionBoundaryTimeFormat, boundaryConfig.At, location)
		if parseErr != nil {
			return 0, clock.SessionSchedule{}, nil, time.Time{}, time.Time{},
				fmt.Errorf("conductor: buildExchangeArgs: session[%d].at %q: %w", boundaryIndex, boundaryConfig.At, parseErr)
		}
		boundaryInstant := time.Date(
			year, month, day,
			parsedWallClock.Hour(), parsedWallClock.Minute(), 0, 0,
			location,
		)
		phase, phaseErr := phaseFromConfigLabel(boundaryConfig.Phase)
		if phaseErr != nil {
			return 0, clock.SessionSchedule{}, nil, time.Time{}, time.Time{},
				fmt.Errorf("conductor: buildExchangeArgs: session[%d].phase: %w", boundaryIndex, phaseErr)
		}
		boundaries = append(boundaries, clock.PhaseBoundary{At: boundaryInstant, Phase: phase})
	}

	if scheduleErr := validateSchedule(boundaries); scheduleErr != nil {
		return 0, clock.SessionSchedule{}, nil, time.Time{}, time.Time{},
			fmt.Errorf("conductor: buildExchangeArgs: %w", scheduleErr)
	}

	openInstant = boundaries[0].At
	closeInstant = boundaries[len(boundaries)-1].At

	specs = make([]wire.InstrumentSpec, 0, len(marketConfig.Instruments))
	for _, instrumentConfig := range marketConfig.Instruments {
		reference, referenceErr := readReference(ctx, reader, marketConfig.Market, instrumentConfig)
		if referenceErr != nil {
			return 0, clock.SessionSchedule{}, nil, time.Time{}, time.Time{},
				fmt.Errorf("conductor: buildExchangeArgs: reference for %s: %w", instrumentConfig.ID, referenceErr)
		}
		specs = append(specs, wire.InstrumentSpec{
			Instrument:    instrumentConfig.ID,
			Reference:     reference,
			BandBps:       instrumentConfig.BandBps,
			Rules:         gate.Rulebook{TickSize: instrumentConfig.TickSize},
			OHLCVInterval: marketConfig.OHLCVInterval,
			OHLCVStart:    openInstant,
		})
	}

	return numShards, clock.SessionSchedule{Boundaries: boundaries}, specs, openInstant, closeInstant, nil
}
