package conductor

import (
	"context"
	"fmt"
	"time"

	"simulator/broker/funnel"
	"simulator/broker/marketdata"
	"simulator/contract"
	"simulator/economy/lifecycle"
	"simulator/exchange/clock"
	"simulator/exchange/wire"
)

// onDemandNumShards is the fixed shard count for every on-demand
// instance. Spec-locked at one — on-demand sessions carry a single
// user whose order flow never justifies the complexity (or cost) of
// cross-shard routing. Separate from canonical's defaultNumShards
// (which is a config-time fallback) because on-demand ignores any
// shard count the config supplies.
const onDemandNumShards = 1

// onDemandMarketIDPrefix tags the market id of an on-demand instance.
// Format: "od:<sessionID>" — the per-user session id doubles as the
// instance key, which guarantees no collision across users and gives
// the WebSocket dispatcher a trivial lookup path (sessionID → Session
// → Market → fanout).
const onDemandMarketIDPrefix = "od:"

// OnDemandResultKind picks between the two happy-path shapes the
// trigger returns. The failure case does NOT appear here — any
// surfaced error (wrapped ErrOnDemandSetupFailed, wrapped lifecycle
// sentinel, context deadline) is returned via the error channel, not
// via a Kind.
type OnDemandResultKind int

const (
	// OnDemandWarmingUp means the held setup completed and the exchange
	// instance is Live; the UI should count down to WarmupEndsAt before
	// connecting the WebSocket.
	OnDemandWarmingUp OnDemandResultKind = iota

	// OnDemandInsufficientFunds means the account exists but the money
	// gate refused: trading_cash is at or below the configured minimum,
	// or the account is in debt. Not an error — the trigger returned
	// cleanly; the user needs to transfer in before retrying.
	OnDemandInsufficientFunds
)

// OnDemandResult is the trigger's success/soft-reject payload. The
// Kind field discriminates the two shapes:
//
//   - OnDemandWarmingUp populates SessionID, WarmupEndsAt,
//     SessionEndsAt. Message stays empty.
//   - OnDemandInsufficientFunds populates Message with a user-facing
//     hint; the three id/time fields stay zero.
//
// Both shapes map to HTTP 200. Hard failures (setup errors, timeouts,
// XOR conflicts) use the error return instead and never produce an
// OnDemandResult the caller inspects.
type OnDemandResult struct {
	Kind          OnDemandResultKind
	SessionID     SessionID
	WarmupEndsAt  time.Time
	SessionEndsAt time.Time
	Message       string
}

// StartOnDemand brings up one on-demand exchange instance for one
// user. The whole held-setup path runs under the caller's context —
// the HTTP handler sets setupTimeout as the outer bound so a wedged
// orchestrator or shard reply surfaces to the client as a 503 rather
// than a hung request.
//
// Four phases, three registry lock-takes, zero locks held across any
// broker, agent, or exchange call-out:
//
//  1. reserve (locked, microseconds): per-user XOR front-gate +
//     Starting Session record. The Starting record IS the
//     reservation — a second concurrent trigger (canonical or
//     on-demand) is refused here without any I/O.
//
//  2. gate (off-lock): EnsureAccount resolves the durable account
//     handle, then QualifyOnDemand checks the money floor. A gate
//     "false" (cash at or below minimum, debt, or zero cash with a
//     zero floor) returns InsufficientFunds cleanly — no setup
//     attempted. Any error from either call rolls the reservation
//     back without a Market record ever existing.
//
//  3. held setup (two more lock-takes bracket slow off-lock work):
//     create the Market record (lock take #2), BringUp agents BEFORE
//     capturing the construct instant so T0 is tight to the Start
//     moment, build the two-boundary schedule + per-instrument specs
//     under T0, construct the exchange, wire both axes (broker
//     settlement + market-data ingestor), Start, then seal with
//     broker.StartSession. A failure anywhere here goes through
//     failOnDemand — if the exchange was Started, exch.Stop fires
//     before the registry record is removed, so the ingestor's
//     CacheRegistry.Remove and the settlement consumer both exit.
//
//  4. finalize (locked, microseconds): stamp the handles, times, and
//     Active/Live states onto the two records atomically.
//
// Returns an OnDemandResult + nil on either happy path (warming-up or
// insufficient-funds). Returns a wrapped error on every failure path;
// setup-phase errors wrap ErrOnDemandSetupFailed in addition to the
// underlying cause so the HTTP mapper can route them to 503 with a
// {status:"failed", reason:"..."} envelope. Pre-market-record errors
// (bad config, EnsureAccount failure, Qualify transport error) wrap
// only the underlying cause — the mapper routes those by base
// sentinel.
func (conductor *Conductor) StartOnDemand(
	ctx context.Context,
	userID UserID,
	tag MarketTag,
	durationMinutes int,
) (OnDemandResult, error) {
	conductor.log.Info("conductor: StartOnDemand begin",
		"user", userID,
		"tag", tag,
		"duration_minutes", durationMinutes,
	)

	// Load config BEFORE reserve: a bad tag must never pollute byUser
	// with a dangling Starting record that no later step will clear.
	marketConfig, loadErr := loadConfig(conductor.cfgDir, tag)
	if loadErr != nil {
		conductor.log.Error("conductor: StartOnDemand load-config failed",
			"tag", tag,
			"err", loadErr,
		)
		return OnDemandResult{}, fmt.Errorf("conductor: StartOnDemand: load config for %q: %w", tag, loadErr)
	}

	// ---- RESERVE (lock take #1 — microseconds) ----
	conductor.reg.mu.Lock()
	if _, alreadyPresent := conductor.reg.byUser[userID]; alreadyPresent {
		conductor.reg.mu.Unlock()
		conductor.log.Info("conductor: StartOnDemand rejected — user already in session",
			"user", userID,
			"tag", tag,
		)
		return OnDemandResult{}, ErrAlreadyInSession
	}
	sessionID := SessionID(newUUIDv4())
	reservedSession := &Session{
		ID:          sessionID,
		User:        userID,
		AccountType: Ephemeral,
		State:       Starting,
	}
	conductor.reg.sessions[sessionID] = reservedSession
	conductor.reg.byUser[userID] = reservedSession
	conductor.reg.mu.Unlock()
	conductor.log.Info("conductor: StartOnDemand reserved Starting session",
		"session_id", sessionID,
		"user", userID,
		"tag", tag,
	)

	// ---- OFF-LOCK: EnsureAccount ----
	accountID, ensureErr := conductor.broker.EnsureAccount(ctx, lifecycle.OwnerHuman, string(userID))
	if ensureErr != nil {
		conductor.rollbackDeleteOnDemand(sessionID, userID)
		conductor.log.Error("conductor: StartOnDemand ensure-account failed",
			"session_id", sessionID,
			"user", userID,
			"err", ensureErr,
		)
		return OnDemandResult{}, fmt.Errorf("conductor: StartOnDemand: ensure account: %w", ensureErr)
	}

	// ---- OFF-LOCK: QualifyOnDemand (money gate) ----
	qualified, qualifyErr := conductor.broker.QualifyOnDemand(ctx, accountID)
	if qualifyErr != nil {
		conductor.rollbackDeleteOnDemand(sessionID, userID)
		conductor.log.Error("conductor: StartOnDemand qualify-on-demand failed",
			"session_id", sessionID,
			"account_id", accountID,
			"err", qualifyErr,
		)
		return OnDemandResult{}, fmt.Errorf("conductor: StartOnDemand: qualify on-demand: %w", qualifyErr)
	}
	if !qualified {
		conductor.rollbackDeleteOnDemand(sessionID, userID)
		conductor.log.Info("conductor: StartOnDemand gate rejected — insufficient funds",
			"session_id", sessionID,
			"account_id", accountID,
		)
		return OnDemandResult{
			Kind:    OnDemandInsufficientFunds,
			Message: "insufficient funds for on-demand session; move money and retry",
		}, nil
	}

	// ---- CREATE MARKET RECORD (lock take #2 — microseconds) ----
	//
	// The Market goes in as Provisioning with no handles. The session's
	// Market field is stamped under the same lock so a concurrent
	// dispatcher resolve walks a consistent sess → market pair (either
	// the market exists and the session points at it, or neither).
	marketID := MarketID(onDemandMarketIDPrefix + string(sessionID))
	conductor.reg.mu.Lock()
	reservedMarket := &Market{
		ID:      marketID,
		Mode:    OnDemand,
		Tag:     tag,
		State:   Provisioning,
		members: map[SessionID]struct{}{sessionID: {}},
	}
	conductor.reg.markets[marketID] = reservedMarket
	reservedSession.Market = marketID
	conductor.reg.mu.Unlock()
	conductor.log.Info("conductor: StartOnDemand reserved Provisioning market",
		"session_id", sessionID,
		"market_id", marketID,
	)

	// ---- OFF-LOCK: agent populate ----
	//
	// Runs BEFORE construct so the construct-time instant (T0) is tight
	// to the Start moment. A long orchestrator round-trip here does
	// NOT eat into the user's visible warm-up window — the UI starts
	// its countdown from the response's warmup_ends_at, which is
	// anchored to T0 (= post-BringUp).
	instrumentIDs := make([]contract.InstrumentID, 0, len(marketConfig.Instruments))
	for _, instrumentConfig := range marketConfig.Instruments {
		instrumentIDs = append(instrumentIDs, instrumentConfig.ID)
	}
	bringUpCtx, cancelBringUp := context.WithTimeout(ctx, conductor.bringUpTimeout)
	bringUpErr := conductor.agents.BringUp(bringUpCtx, tag, OnDemand, instrumentIDs)
	cancelBringUp()
	if bringUpErr != nil {
		conductor.failOnDemand(sessionID, marketID, "populate_failed: "+bringUpErr.Error(), nil)
		return OnDemandResult{}, fmt.Errorf("conductor: StartOnDemand: agent bring-up: %w: %w", ErrOnDemandSetupFailed, bringUpErr)
	}

	// ---- CONSTRUCT: T0 = now, two-boundary schedule, per-instrument specs ----
	constructInstant := conductor.now()
	warmupDuration := marketConfig.OnDemand.Warmup
	sessionDuration := time.Duration(durationMinutes) * time.Minute

	schedule, scheduleErr := onDemandSchedule(constructInstant, warmupDuration, sessionDuration)
	if scheduleErr != nil {
		conductor.failOnDemand(sessionID, marketID, "schedule_invalid: "+scheduleErr.Error(), nil)
		return OnDemandResult{}, fmt.Errorf("conductor: StartOnDemand: schedule: %w: %w", ErrOnDemandSetupFailed, scheduleErr)
	}

	specs, specsErr := buildInstrumentSpecs(
		ctx, conductor.referenceReader,
		marketConfig.Instruments, tag, marketConfig.OHLCVInterval, constructInstant,
	)
	if specsErr != nil {
		conductor.failOnDemand(sessionID, marketID, "specs_failed: "+specsErr.Error(), nil)
		return OnDemandResult{}, fmt.Errorf("conductor: StartOnDemand: build specs: %w: %w", ErrOnDemandSetupFailed, specsErr)
	}

	exchangeInstance := conductor.exch.NewExchange(onDemandNumShards, schedule, specs)
	conductor.log.Info("conductor: StartOnDemand exchange constructed",
		"session_id", sessionID,
		"market_id", marketID,
		"construct_instant", constructInstant,
		"instrument_count", len(specs),
	)

	// ---- WIRE both axes (unbuffered edges, must precede Start) ----
	conductor.broker.WireInstance(string(marketID), exchangeInstance.Private)
	fanout := conductor.market.WireInstance(
		marketdata.ExchangeID(marketID),
		exchangeInstance.Public,
		string(tag),
		marketdata.ModeOnDemand,
	)

	// ---- START the exchange (goroutines live from here) ----
	exchangeInstance.Start(tickInterval)

	// ---- SEAL: broker.StartSession installs the ephemeral session ----
	//
	// This runs AFTER Start, so a failure here must call exch.Stop to
	// release the monitor/shards/ingress goroutines AND let the
	// settlement consumer + market-data ingestor exit cleanly (they
	// terminate on Private/Public close; the ingestor also calls
	// CacheRegistry.Remove on exit).
	participantID := ParticipantID(newUUIDv4())
	startErr := conductor.broker.StartSession(ctx, funnel.SessionStartCmd{
		AccountID:       accountID,
		SessionID:       funnel.SessionID(sessionID),
		Mode:            lifecycle.OnDemand,
		ExchangeID:      funnel.ExchangeID(marketID),
		ExchangeInbound: exchangeInstance.Inbound,
		Participant:     contract.ParticipantID(participantID),
	})
	if startErr != nil {
		conductor.failOnDemand(sessionID, marketID, "install_failed: "+startErr.Error(), exchangeInstance)
		return OnDemandResult{}, fmt.Errorf("conductor: StartOnDemand: start session: %w: %w", ErrOnDemandSetupFailed, startErr)
	}

	// ---- FINALIZE (lock take #3 — microseconds) ----
	warmupEndsAt := constructInstant.Add(warmupDuration)
	sessionEndsAt := warmupEndsAt.Add(sessionDuration)
	conductor.reg.mu.Lock()
	reservedMarket.exch = exchangeInstance
	reservedMarket.inbound = exchangeInstance.Inbound
	reservedMarket.fanout = fanout
	reservedMarket.warmupEndsAt = warmupEndsAt
	reservedMarket.closeAt = sessionEndsAt
	reservedMarket.State = Live
	reservedSession.Account = AccountID(string(accountID))
	reservedSession.Participant = participantID
	reservedSession.State = Active
	conductor.reg.mu.Unlock()
	conductor.log.Info("conductor: StartOnDemand committed Live",
		"session_id", sessionID,
		"participant_id", participantID,
		"account_id", accountID,
		"market_id", marketID,
		"warmup_ends_at", warmupEndsAt,
		"session_ends_at", sessionEndsAt,
	)
	return OnDemandResult{
		Kind:          OnDemandWarmingUp,
		SessionID:     sessionID,
		WarmupEndsAt:  warmupEndsAt,
		SessionEndsAt: sessionEndsAt,
	}, nil
}

// onDemandSchedule builds the two-boundary continuous-then-closed
// schedule the on-demand exchange runs under. The whole span is one
// uninterrupted continuous phase; the warm-up window is just its
// first warmupDuration slice (agents trade during it, the user is
// not yet admitted). "Warm-up" is a conductor-side concept — the
// exchange itself sees one continuous session that ends at the
// configured close instant.
//
// Returns an error if the resulting schedule would fail
// validateSchedule — concretely, if warmupDuration + sessionDuration
// is non-positive, the two boundaries land at the same instant and
// the clock's strictly-ascending guard would panic. Catching it here
// surfaces a wrapped ErrOnDemandSetupFailed to the HTTP mapper
// instead of a construct-time panic.
func onDemandSchedule(constructInstant time.Time, warmupDuration, sessionDuration time.Duration) (clock.SessionSchedule, error) {
	closeInstant := constructInstant.Add(warmupDuration + sessionDuration)
	boundaries := []clock.PhaseBoundary{
		{At: constructInstant, Phase: clock.PhaseContinuous},
		{At: closeInstant, Phase: clock.PhaseClosed},
	}
	if validateErr := validateSchedule(boundaries); validateErr != nil {
		return clock.SessionSchedule{}, validateErr
	}
	return clock.SessionSchedule{Boundaries: boundaries}, nil
}

// rollbackDeleteOnDemand undoes the reservation step on a pre-setup
// failure (EnsureAccount error, QualifyOnDemand error, or gate
// "false"). Removes the Starting session record and frees the per-
// user XOR slot. The byUser guard matters only if some future path
// moves on from this user between reserve and rollback (today none
// does; the guard is a correctness margin, not a current need).
//
// Does NOT touch broker-side state: EnsureAccount is idempotent and
// durable, so a successful EnsureAccount followed by a failed Qualify
// leaves a reusable account handle that a retry picks up without
// re-opening.
func (conductor *Conductor) rollbackDeleteOnDemand(sessionID SessionID, userID UserID) {
	conductor.reg.mu.Lock()
	reservedSession := conductor.reg.sessions[sessionID]
	delete(conductor.reg.sessions, sessionID)
	if reservedSession != nil && conductor.reg.byUser[userID] == reservedSession {
		delete(conductor.reg.byUser, userID)
	}
	conductor.reg.mu.Unlock()
	conductor.log.Info("conductor: StartOnDemand rollback complete",
		"session_id", sessionID,
		"user", userID,
	)
}

// failOnDemand runs the post-setup failure path: an error landed
// between Market-create and finalize-Live. Two shapes:
//
//   - exchangeHandle == nil: nothing was started (BringUp failed, or
//     schedule/specs build failed between agent populate and
//     NewExchange). Just mark the Session Failed and delete the
//     Market record.
//
//   - exchangeHandle != nil: the exchange was Started (post-Start
//     failure in StartSession). Call exch.Stop first — this closes
//     Private + Public, which lets the settlement consumer and the
//     market-data ingestor drain and exit; the ingestor also calls
//     CacheRegistry.Remove during its exit so the price snapshot is
//     pruned.
//
// byUser is deliberately NOT cleared: the Failed Session stays
// reachable so a fallback status read surfaces "failed" + reason,
// and a future recovery pass has something to reconcile. Clearing
// the Failed session + freeing the per-user slot is a teardown/reap
// concern that lives outside the spin-up flow.
func (conductor *Conductor) failOnDemand(sessionID SessionID, marketID MarketID, reason string, exchangeHandle *wire.Exchange) {
	if exchangeHandle != nil {
		exchangeHandle.Stop()
		conductor.log.Info("conductor: StartOnDemand stopped exchange after post-start failure",
			"session_id", sessionID,
			"market_id", marketID,
		)
	}
	conductor.reg.mu.Lock()
	reservedSession := conductor.reg.sessions[sessionID]
	if reservedSession != nil {
		reservedSession.State = Failed
		reservedSession.FailReason = reason
	}
	delete(conductor.reg.markets, marketID)
	conductor.reg.mu.Unlock()
	conductor.log.Error("conductor: StartOnDemand setup failed",
		"session_id", sessionID,
		"market_id", marketID,
		"reason", reason,
		"exchange_stopped", exchangeHandle != nil,
	)
}
