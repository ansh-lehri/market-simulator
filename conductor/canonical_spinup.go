package conductor

import (
	"context"
	"fmt"
	"time"

	"simulator/broker/marketdata"
	"simulator/contract"
	"simulator/exchange/wire"
)

// tickInterval drives both the exchange monitor's phase-transition
// check and each shard's OHLCV roll cadence. Spec-locked at one second
// for canonical markets — not a config dial.
const tickInterval = time.Second

// sessionDateFormat is the YYYY-MM-DD date suffix on a canonical market
// id "<tag>:<sessionDate>". Computed in the market's own timezone so a
// market's id matches its local session day.
const sessionDateFormat = "2006-01-02"

// spinUpCanonical brings one canonical market to Live state inline on
// the caller's goroutine (the per-market timer, once that ships). Flow:
//
//   1. Load config, resolve the market timezone, compute MarketID
//      "<tag>:<today-in-market-tz>".
//   2. Reserve the registry record under the one mutex. A duplicate
//      MarketID (an existing Provisioning or Live record) returns
//      ErrAlreadyUp without any other action.
//   3. Build exchange construction args off-lock: schedule anchored to
//      today, per-instrument specs with reference prices resolved from
//      the candles store. Any error → abortProvisioning.
//   4. Construct the exchange (no goroutines started).
//   5. Seed: AgentOrchestrator.BringUp (synchronous, bounded by
//      bringUpTimeout) then LiquidityOperator.SeedLiquidity (fire-and-
//      trust). BringUp failure → abortProvisioning.
//   6. Wire both axes — broker settlement consumer first, then the
//      market-data ingestor. Edge channels are unbuffered, so wiring
//      must sit immediately before Start.
//   7. Start the exchange — monitor + shards + ingress goroutines live;
//      the clock is still PhaseClosed, the market is not yet open.
//   8. Commit the handles onto the record under the mutex; State flips
//      Provisioning → Live.
//
// The one mutex is taken exactly twice (reserve + commit) and never
// held across construct, seed, wire, or Start. abortProvisioning takes
// the mutex once to delete the record on any pre-Start failure.
//
// Returns ("", err) on failure; returns (marketID, ErrAlreadyUp) when a
// second trigger finds an existing record — the caller treats this as
// "nothing to do," not a failure.
func (conductor *Conductor) spinUpCanonical(ctx context.Context, marketTag MarketTag) (MarketID, error) {
	marketConfig, loadErr := loadConfig(conductor.cfgDir, marketTag)
	if loadErr != nil {
		return "", fmt.Errorf("conductor: spinUpCanonical: load config for %q: %w", marketTag, loadErr)
	}
	location, locationErr := time.LoadLocation(marketConfig.Timezone)
	if locationErr != nil {
		return "", fmt.Errorf("conductor: spinUpCanonical: timezone %q: %w", marketConfig.Timezone, locationErr)
	}
	sessionDate := conductor.now().In(location).Format(sessionDateFormat)
	marketID := MarketID(string(marketTag) + ":" + sessionDate)

	// ---- RESERVE (lock take #1 — microseconds) ----
	conductor.reg.mu.Lock()
	if _, alreadyPresent := conductor.reg.markets[marketID]; alreadyPresent {
		conductor.reg.mu.Unlock()
		conductor.log.Info("conductor: spinUpCanonical duplicate trigger — already provisioning or live",
			"market_id", marketID,
			"tag", marketTag,
		)
		return marketID, ErrAlreadyUp
	}
	reservedRecord := &Market{
		ID:      marketID,
		Mode:    Canonical,
		Tag:     marketTag,
		State:   Provisioning,
		members: map[SessionID]struct{}{},
	}
	conductor.reg.markets[marketID] = reservedRecord
	conductor.reg.mu.Unlock()
	conductor.log.Info("conductor: spinUpCanonical reserved Provisioning record",
		"market_id", marketID,
		"tag", marketTag,
	)

	// ---- Build args off-lock (any error → abort) ----
	numShards, schedule, specs, openInstant, closeInstant, buildErr := buildExchangeArgs(
		ctx, conductor.referenceReader, conductor.now, marketConfig, location,
	)
	if buildErr != nil {
		conductor.abortProvisioning(marketID, buildErr)
		return "", buildErr
	}

	// ---- Construct exchange (no goroutines yet) ----
	exchangeInstance := conductor.exch.NewExchange(numShards, schedule, specs)
	conductor.log.Info("conductor: spinUpCanonical exchange constructed",
		"market_id", marketID,
		"num_shards", numShards,
		"instrument_count", len(specs),
	)

	// ---- Seed BEFORE wire: a failure here has nothing to tear down ----
	bringUpCtx, cancelBringUp := context.WithTimeout(ctx, conductor.bringUpTimeout)
	defer cancelBringUp()
	bringUpErr := conductor.agents.BringUp(bringUpCtx, AgentBringUpRequest{
		Market:       marketTag,
		ExchangeID:   marketID,
		Mode:         Canonical,
		SessionStart: openInstant,
		SessionEnd:   closeInstant,
		Instruments:  instrumentIDsFrom(specs),
	})
	if bringUpErr != nil {
		conductor.abortProvisioning(marketID, bringUpErr)
		return "", fmt.Errorf("conductor: spinUpCanonical: agent bring-up: %w", bringUpErr)
	}
	conductor.liq.SeedLiquidity(ctx, marketID, liquiditySpecsFrom(marketConfig))

	// ---- Wire both axes — unbuffered edges, must precede Start ----
	conductor.broker.WireInstance(string(marketID), exchangeInstance.Private)
	fanout := conductor.market.WireInstance(
		marketdata.ExchangeID(marketID),
		exchangeInstance.Public,
		string(marketTag),
		marketdata.ModeCanonical,
	)

	// ---- Start the exchange — goroutines live ----
	exchangeInstance.Start(tickInterval)

	// ---- COMMIT (lock take #2 — microseconds) ----
	conductor.reg.mu.Lock()
	reservedRecord.exch = exchangeInstance
	reservedRecord.inbound = exchangeInstance.Inbound
	reservedRecord.fanout = fanout
	reservedRecord.closeAt = closeInstant
	reservedRecord.State = Live
	conductor.reg.mu.Unlock()
	conductor.log.Info("conductor: spinUpCanonical committed Live",
		"market_id", marketID,
		"close_at", closeInstant,
	)
	return marketID, nil
}

// abortProvisioning removes a reserved record under the one mutex. The
// record is expected to be in Provisioning state with no consumers wired
// and no started exchange — this is why spin-up orders every fallible
// step before wiring and Start, so abort is a pure map delete with
// nothing to tear down.
func (conductor *Conductor) abortProvisioning(marketID MarketID, cause error) {
	conductor.reg.mu.Lock()
	delete(conductor.reg.markets, marketID)
	conductor.reg.mu.Unlock()
	conductor.log.Error("conductor: spinUpCanonical aborted; Provisioning record deleted",
		"market_id", marketID,
		"cause", cause,
	)
}

// instrumentIDsFrom extracts the plain id list from the built specs —
// the shape AgentOrchestrator.BringUp expects.
func instrumentIDsFrom(specs []wire.InstrumentSpec) []contract.InstrumentID {
	ids := make([]contract.InstrumentID, 0, len(specs))
	for _, spec := range specs {
		ids = append(ids, spec.Instrument)
	}
	return ids
}

// liquiditySpecsFrom projects the market config's per-instrument
// liquidity section into the operator-port's LiquiditySpec shape. The
// projection is placeholder-shape: the real operator spec will replace
// it when the operator lands.
func liquiditySpecsFrom(marketConfig MarketConfig) []LiquiditySpec {
	specs := make([]LiquiditySpec, 0, len(marketConfig.Instruments))
	for _, instrumentConfig := range marketConfig.Instruments {
		specs = append(specs, LiquiditySpec{
			Instrument: instrumentConfig.ID,
			Levels:     instrumentConfig.Liquidity.Levels,
			SpacingPct: instrumentConfig.Liquidity.SpacingPct,
			NearQty:    instrumentConfig.Liquidity.NearQty,
			Taper:      instrumentConfig.Liquidity.Taper,
		})
	}
	return specs
}
