package conductor

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"simulator/broker/funnel"
	"simulator/broker/marketdata"
	"simulator/broker/store"
	"simulator/contract"
	"simulator/economy/lifecycle"
	"simulator/exchange/emit"
)

// --- On-demand-specific port fakes ------------------------------------

// fakeOnDemandBroker extends fakeJoinBroker with the two capabilities
// the on-demand flow needs that the canonical join flow does not:
//
//   - a programmable QualifyOnDemand hook (canonical join never calls
//     the money gate; on-demand's whole purpose is to call it);
//   - a draining WireInstance that spawns a consumer goroutine so a
//     Started exchange's unbuffered Private channel does not block on
//     the first emit.
//
// EnsureAccount and StartSession come from the embedded fakeJoinBroker
// unchanged — the on-demand flow drives them with the same hook shape
// the canonical join flow uses.
type fakeOnDemandBroker struct {
	*fakeJoinBroker

	qualifyLock               sync.Mutex
	qualifyOnDemandInvocations []funnel.AccountID
	qualifyOnDemandFunc       func(ctx context.Context, accountID funnel.AccountID) (bool, error)

	wireLock        sync.Mutex
	wireInvocations []string
	wireDrainWG     sync.WaitGroup
}

func (fake *fakeOnDemandBroker) QualifyOnDemand(ctx context.Context, accountID funnel.AccountID) (bool, error) {
	fake.qualifyLock.Lock()
	fake.qualifyOnDemandInvocations = append(fake.qualifyOnDemandInvocations, accountID)
	fake.qualifyLock.Unlock()
	if fake.qualifyOnDemandFunc == nil {
		return true, nil
	}
	return fake.qualifyOnDemandFunc(ctx, accountID)
}

func (fake *fakeOnDemandBroker) qualifyOnDemandCount() int {
	fake.qualifyLock.Lock()
	defer fake.qualifyLock.Unlock()
	return len(fake.qualifyOnDemandInvocations)
}

func (fake *fakeOnDemandBroker) WireInstance(exchangeID string, privateStream <-chan contract.EngineResult) {
	fake.wireLock.Lock()
	fake.wireInvocations = append(fake.wireInvocations, exchangeID)
	fake.wireLock.Unlock()
	fake.wireDrainWG.Add(1)
	go func() {
		defer fake.wireDrainWG.Done()
		for range privateStream {
		}
	}()
}

func (fake *fakeOnDemandBroker) wireCount() int {
	fake.wireLock.Lock()
	defer fake.wireLock.Unlock()
	return len(fake.wireInvocations)
}

// fakeOnDemandMarket is the market-data port fake. Drains the tape so
// a Started exchange is not blocked, records the mode + tag passed to
// WireInstance (both of which the on-demand path pins to specific
// values), and returns a real *marketdata.FanOut built once per fake
// so dispatcher-resolution tests can match on pointer identity.
type fakeOnDemandMarket struct {
	stubMarketDataPort

	wireLock         sync.Mutex
	wireInvocations  []fakeOnDemandMarketWireCall
	wireDrainWG      sync.WaitGroup
	returnFanout     *marketdata.FanOut
	returnFanoutOnce sync.Once
	fanoutMarketTag  MarketTag
}

type fakeOnDemandMarketWireCall struct {
	ExchangeID marketdata.ExchangeID
	MarketTag  string
	Mode       marketdata.Mode
}

func (fake *fakeOnDemandMarket) WireInstance(
	exchangeID marketdata.ExchangeID,
	tape <-chan emit.TapeEvent,
	marketTag string,
	mode marketdata.Mode,
) *marketdata.FanOut {
	fake.wireLock.Lock()
	fake.wireInvocations = append(fake.wireInvocations, fakeOnDemandMarketWireCall{
		ExchangeID: exchangeID,
		MarketTag:  marketTag,
		Mode:       mode,
	})
	fake.wireLock.Unlock()
	fake.wireDrainWG.Add(1)
	go func() {
		defer fake.wireDrainWG.Done()
		for range tape {
		}
	}()
	// Build the FanOut once per fake; subsequent WireInstance calls (not
	// expected in normal flow — one WireInstance per instance) return
	// the same handle.
	fake.returnFanoutOnce.Do(func() {
		snapshot := marketdata.NewMarketSnapshot(exchangeID)
		location, _ := time.LoadLocation("Asia/Kolkata")
		fake.returnFanout = marketdata.NewFanOut(snapshot, string(fake.fanoutMarketTag), location)
	})
	return fake.returnFanout
}

func (fake *fakeOnDemandMarket) wireCount() int {
	fake.wireLock.Lock()
	defer fake.wireLock.Unlock()
	return len(fake.wireInvocations)
}

func (fake *fakeOnDemandMarket) lastWireCall() fakeOnDemandMarketWireCall {
	fake.wireLock.Lock()
	defer fake.wireLock.Unlock()
	if len(fake.wireInvocations) == 0 {
		return fakeOnDemandMarketWireCall{}
	}
	return fake.wireInvocations[len(fake.wireInvocations)-1]
}

// Compile-time: fakes satisfy their interfaces.
var (
	_ BrokerPort     = (*fakeOnDemandBroker)(nil)
	_ MarketDataPort = (*fakeOnDemandMarket)(nil)
)

// --- Harness ----------------------------------------------------------

// onDemandHarness bundles everything an on-demand test needs: a
// Conductor wired to recording fakes, a real wire.NewExchange via
// recordingExchangeFactory, deterministic T0 via injected now, and
// cleanup that drains every spawned goroutine.
type onDemandHarness struct {
	conductor   *Conductor
	broker      *fakeOnDemandBroker
	market      *fakeOnDemandMarket
	agents      *recordingAgents
	liquidity   *recordingLiquidity
	exchFactory *recordingExchangeFactory
	fixedNow    time.Time
	marketTag   MarketTag
}

// onDemandFixedNow is the deterministic construct instant for every
// on-demand test. Picked as a precise instant (not round-minute) so
// off-by-one bugs on T0 arithmetic surface clearly in time comparisons.
var onDemandFixedNow = time.Date(2026, 10, 12, 11, 23, 47, 500_000_000, time.UTC)

// configuredWarmupLength is the warm-up window declared in the test
// market config (writeTestMarketConfig writes `"warmup": "15m"`).
const configuredWarmupLength = 15 * time.Minute

func newOnDemandHarness(t *testing.T, marketTag MarketTag) *onDemandHarness {
	t.Helper()
	cfgDir := t.TempDir()
	writeTestMarketConfig(t, cfgDir, marketTag)

	sequencer := &callSequencer{}
	broker := &fakeOnDemandBroker{fakeJoinBroker: &fakeJoinBroker{}}
	market := &fakeOnDemandMarket{fanoutMarketTag: marketTag}
	agents := &recordingAgents{sequencer: sequencer}
	liquidity := &recordingLiquidity{sequencer: sequencer}
	exchFactory := &recordingExchangeFactory{sequencer: sequencer}

	pool := newUnconnectedPool(t)
	conductor, err := NewConductor(Config{
		CfgDir:    cfgDir,
		OHLCVPool: pool,
		Broker:    broker,
		Market:    market,
		Exchange:  exchFactory,
		Agents:    agents,
		Liquidity: liquidity,
		Now:       func() time.Time { return onDemandFixedNow },
	})
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}
	// Reference reader for buildInstrumentSpecs — a known close so
	// specs resolve without hitting the never-connected pgx pool.
	conductor.referenceReader = readerReturningClose(251575)

	t.Cleanup(func() {
		exchFactory.stopAll()
		broker.wireDrainWG.Wait()
		market.wireDrainWG.Wait()
		conductor.Stop()
	})

	return &onDemandHarness{
		conductor:   conductor,
		broker:      broker,
		market:      market,
		agents:      agents,
		liquidity:   liquidity,
		exchFactory: exchFactory,
		fixedNow:    onDemandFixedNow,
		marketTag:   marketTag,
	}
}

// --- Happy path --------------------------------------------------------

func TestStartOnDemand_HappyPath(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	userID := UserID("human-happy-ondemand")
	durationMinutes := 35

	result, err := harness.conductor.StartOnDemand(context.Background(), userID, "IN", durationMinutes)
	if err != nil {
		t.Fatalf("StartOnDemand: %v", err)
	}

	// Result shape.
	if result.Kind != OnDemandWarmingUp {
		t.Errorf("Kind = %v, want OnDemandWarmingUp", result.Kind)
	}
	if !isUUIDv4(string(result.SessionID)) {
		t.Errorf("SessionID %q is not a UUID v4", result.SessionID)
	}
	expectedWarmupEnds := harness.fixedNow.Add(configuredWarmupLength)
	expectedSessionEnds := expectedWarmupEnds.Add(time.Duration(durationMinutes) * time.Minute)
	if !result.WarmupEndsAt.Equal(expectedWarmupEnds) {
		t.Errorf("WarmupEndsAt = %s, want %s", result.WarmupEndsAt, expectedWarmupEnds)
	}
	if !result.SessionEndsAt.Equal(expectedSessionEnds) {
		t.Errorf("SessionEndsAt = %s, want %s", result.SessionEndsAt, expectedSessionEnds)
	}

	// Registry state: session Active + Ephemeral; market Live + OnDemand + stamped handles.
	expectedMarketID := MarketID("od:" + string(result.SessionID))
	harness.conductor.reg.mu.Lock()
	session, sessionPresent := harness.conductor.reg.sessions[result.SessionID]
	market, marketPresent := harness.conductor.reg.markets[expectedMarketID]
	byUser := harness.conductor.reg.byUser[userID]
	harness.conductor.reg.mu.Unlock()

	if !sessionPresent {
		t.Fatal("session record missing from registry")
	}
	if session.State != Active {
		t.Errorf("session.State = %v, want Active", session.State)
	}
	if session.AccountType != Ephemeral {
		t.Errorf("session.AccountType = %v, want Ephemeral", session.AccountType)
	}
	if session.User != userID {
		t.Errorf("session.User = %q, want %q", session.User, userID)
	}
	if session.Market != expectedMarketID {
		t.Errorf("session.Market = %q, want %q", session.Market, expectedMarketID)
	}
	if session.Account == "" {
		t.Error("session.Account not stamped at finalize")
	}
	if session.Participant == "" {
		t.Error("session.Participant not stamped at finalize")
	}
	if byUser != session {
		t.Error("byUser slot does not point at the installed session")
	}

	if !marketPresent {
		t.Fatal("market record missing from registry")
	}
	if market.State != Live {
		t.Errorf("market.State = %v, want Live", market.State)
	}
	if market.Mode != OnDemand {
		t.Errorf("market.Mode = %v, want OnDemand", market.Mode)
	}
	if market.Tag != "IN" {
		t.Errorf("market.Tag = %q, want IN", market.Tag)
	}
	if market.exch == nil {
		t.Error("market.exch not stamped")
	}
	if market.inbound == nil {
		t.Error("market.inbound not stamped")
	}
	if market.fanout == nil {
		t.Error("market.fanout not stamped")
	}
	if !market.warmupEndsAt.Equal(expectedWarmupEnds) {
		t.Errorf("market.warmupEndsAt = %s, want %s", market.warmupEndsAt, expectedWarmupEnds)
	}
	if !market.closeAt.Equal(expectedSessionEnds) {
		t.Errorf("market.closeAt = %s, want %s", market.closeAt, expectedSessionEnds)
	}
	if _, memberPresent := market.members[result.SessionID]; !memberPresent {
		t.Error("session not in market.members")
	}

	// Broker call shape.
	if harness.broker.ensureAccountCount() != 1 {
		t.Errorf("EnsureAccount calls = %d, want 1", harness.broker.ensureAccountCount())
	}
	ensureCall := harness.broker.ensureAccountInvocations[0]
	if ensureCall.OwnerType != lifecycle.OwnerHuman {
		t.Errorf("EnsureAccount OwnerType = %v, want OwnerHuman", ensureCall.OwnerType)
	}
	if ensureCall.OwnerID != string(userID) {
		t.Errorf("EnsureAccount OwnerID = %q, want %q", ensureCall.OwnerID, userID)
	}
	if harness.broker.qualifyOnDemandCount() != 1 {
		t.Errorf("QualifyOnDemand calls = %d, want 1", harness.broker.qualifyOnDemandCount())
	}
	if harness.broker.wireCount() != 1 {
		t.Errorf("broker.WireInstance calls = %d, want 1", harness.broker.wireCount())
	}
	if harness.broker.wireInvocations[0] != string(expectedMarketID) {
		t.Errorf("broker.WireInstance exchangeID = %q, want %q",
			harness.broker.wireInvocations[0], expectedMarketID)
	}
	if harness.broker.startSessionCount() != 1 {
		t.Errorf("StartSession calls = %d, want 1", harness.broker.startSessionCount())
	}
	startCmd := harness.broker.startSessionInvocations[0]
	if startCmd.Mode != lifecycle.OnDemand {
		t.Errorf("StartSession Mode = %v, want OnDemand", startCmd.Mode)
	}
	if string(startCmd.SessionID) != string(result.SessionID) {
		t.Errorf("StartSession SessionID = %q, want %q", startCmd.SessionID, result.SessionID)
	}
	if string(startCmd.ExchangeID) != string(expectedMarketID) {
		t.Errorf("StartSession ExchangeID = %q, want %q", startCmd.ExchangeID, expectedMarketID)
	}
	if startCmd.ExchangeInbound == nil {
		t.Error("StartSession ExchangeInbound nil")
	}
	if string(startCmd.Participant) != string(session.Participant) {
		t.Errorf("StartSession Participant = %q, want %q", startCmd.Participant, session.Participant)
	}

	// Market-data WireInstance pinned to on-demand mode.
	if harness.market.wireCount() != 1 {
		t.Errorf("market.WireInstance calls = %d, want 1", harness.market.wireCount())
	}
	marketWire := harness.market.lastWireCall()
	if marketWire.Mode != marketdata.ModeOnDemand {
		t.Errorf("market.WireInstance Mode = %v, want ModeOnDemand", marketWire.Mode)
	}
	if marketWire.MarketTag != string(harness.marketTag) {
		t.Errorf("market.WireInstance tag = %q, want %q", marketWire.MarketTag, harness.marketTag)
	}
	if string(marketWire.ExchangeID) != string(expectedMarketID) {
		t.Errorf("market.WireInstance exchangeID = %q, want %q", marketWire.ExchangeID, expectedMarketID)
	}

	// Agents got the on-demand mode (not canonical).
	if harness.agents.callCount != 1 {
		t.Errorf("agents.BringUp calls = %d, want 1", harness.agents.callCount)
	}
}

// --- Gate failure: insufficient funds (soft reject, not an error) -----

func TestStartOnDemand_GateNO_InsufficientFunds(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	harness.broker.qualifyOnDemandFunc = func(context.Context, funnel.AccountID) (bool, error) {
		return false, nil
	}
	userID := UserID("human-poor")

	result, err := harness.conductor.StartOnDemand(context.Background(), userID, "IN", 60)
	if err != nil {
		t.Fatalf("StartOnDemand: want nil error on gate NO, got %v", err)
	}
	if result.Kind != OnDemandInsufficientFunds {
		t.Errorf("Kind = %v, want OnDemandInsufficientFunds", result.Kind)
	}
	if result.Message == "" {
		t.Error("Message empty on insufficient_funds result")
	}
	// No Market record, no session residue, no held-setup work ever started.
	harness.conductor.reg.mu.Lock()
	sessionsCount := len(harness.conductor.reg.sessions)
	marketsCount := len(harness.conductor.reg.markets)
	byUserCount := len(harness.conductor.reg.byUser)
	harness.conductor.reg.mu.Unlock()
	if sessionsCount != 0 || marketsCount != 0 || byUserCount != 0 {
		t.Errorf("registry residue on gate NO: sessions=%d markets=%d byUser=%d",
			sessionsCount, marketsCount, byUserCount)
	}
	if harness.agents.callCount != 0 {
		t.Errorf("agents.BringUp calls = %d, want 0 on gate NO", harness.agents.callCount)
	}
	if harness.broker.startSessionCount() != 0 {
		t.Errorf("StartSession calls = %d, want 0 on gate NO", harness.broker.startSessionCount())
	}
	if harness.broker.wireCount() != 0 {
		t.Errorf("broker.WireInstance calls = %d, want 0 on gate NO", harness.broker.wireCount())
	}
	if harness.market.wireCount() != 0 {
		t.Errorf("market.WireInstance calls = %d, want 0 on gate NO", harness.market.wireCount())
	}
	// Qualify is the one broker call that did run.
	if harness.broker.qualifyOnDemandCount() != 1 {
		t.Errorf("QualifyOnDemand calls = %d, want 1 (gate fires)", harness.broker.qualifyOnDemandCount())
	}
}

// --- XOR front-gate: second trigger for same user -------------------

func TestStartOnDemand_XORFrontGate(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	userID := UserID("human-dup")

	if _, firstErr := harness.conductor.StartOnDemand(context.Background(), userID, "IN", 30); firstErr != nil {
		t.Fatalf("first StartOnDemand: %v", firstErr)
	}
	_, secondErr := harness.conductor.StartOnDemand(context.Background(), userID, "IN", 30)
	if !errors.Is(secondErr, ErrAlreadyInSession) {
		t.Fatalf("second StartOnDemand err = %v, want ErrAlreadyInSession", secondErr)
	}

	// Front-gate short-circuited before any new broker I/O.
	if harness.broker.ensureAccountCount() != 1 {
		t.Errorf("EnsureAccount calls = %d, want 1", harness.broker.ensureAccountCount())
	}
	if harness.broker.qualifyOnDemandCount() != 1 {
		t.Errorf("QualifyOnDemand calls = %d, want 1", harness.broker.qualifyOnDemandCount())
	}
	if harness.broker.startSessionCount() != 1 {
		t.Errorf("StartSession calls = %d, want 1", harness.broker.startSessionCount())
	}
}

// --- Cross-mode XOR: canonical session blocks on-demand trigger -----

func TestStartOnDemand_CrossModeXORBlocksSecondTrigger(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	userID := UserID("human-xmode")

	// Pre-install an Active canonical session for this user — the XOR
	// front-gate must refuse any new trigger (on-demand or otherwise)
	// without any broker I/O.
	existingSession := &Session{
		ID:          SessionID(newUUIDv4()),
		User:        userID,
		Market:      MarketID("IN:preexisting"),
		Account:     AccountID("existing-account"),
		Participant: ParticipantID(newUUIDv4()),
		AccountType: Durable,
		State:       Active,
	}
	harness.conductor.reg.mu.Lock()
	harness.conductor.reg.sessions[existingSession.ID] = existingSession
	harness.conductor.reg.byUser[userID] = existingSession
	harness.conductor.reg.mu.Unlock()

	_, err := harness.conductor.StartOnDemand(context.Background(), userID, "IN", 30)
	if !errors.Is(err, ErrAlreadyInSession) {
		t.Fatalf("err = %v, want ErrAlreadyInSession", err)
	}
	// Zero broker calls — reserve short-circuited.
	if harness.broker.ensureAccountCount() != 0 || harness.broker.qualifyOnDemandCount() != 0 ||
		harness.broker.startSessionCount() != 0 {
		t.Errorf("broker should have received zero calls; got ensure=%d qualify=%d start=%d",
			harness.broker.ensureAccountCount(),
			harness.broker.qualifyOnDemandCount(),
			harness.broker.startSessionCount())
	}
}

// --- EnsureAccount failure rolls back; Qualify never called ----------

func TestStartOnDemand_EnsureAccountErrorRollsBack(t *testing.T) {
	cases := []error{
		lifecycle.ErrBadOwnerType,
		lifecycle.ErrBadOwnerID,
		lifecycle.ErrCreateFailed,
		store.ErrNoAccount,
		store.ErrLoadFailed,
	}
	for _, forcedErr := range cases {
		t.Run(forcedErr.Error(), func(t *testing.T) {
			harness := newOnDemandHarness(t, "IN")
			harness.broker.ensureAccountFunc = func(context.Context, lifecycle.OwnerType, string) (funnel.AccountID, error) {
				return "", forcedErr
			}

			_, err := harness.conductor.StartOnDemand(context.Background(), UserID("human-e"), "IN", 30)
			if !errors.Is(err, forcedErr) {
				t.Fatalf("err = %v, want wrap of %v", err, forcedErr)
			}
			if errors.Is(err, ErrOnDemandSetupFailed) {
				t.Error("err must NOT wrap ErrOnDemandSetupFailed — pre-market-record failures route by base sentinel")
			}
			if harness.broker.qualifyOnDemandCount() != 0 {
				t.Errorf("QualifyOnDemand called %d times after EnsureAccount failure; want 0",
					harness.broker.qualifyOnDemandCount())
			}
			// Rollback: no session, no byUser, no market.
			harness.conductor.reg.mu.Lock()
			defer harness.conductor.reg.mu.Unlock()
			if len(harness.conductor.reg.sessions) != 0 || len(harness.conductor.reg.byUser) != 0 ||
				len(harness.conductor.reg.markets) != 0 {
				t.Errorf("registry residue after rollback: sessions=%d byUser=%d markets=%d",
					len(harness.conductor.reg.sessions),
					len(harness.conductor.reg.byUser),
					len(harness.conductor.reg.markets))
			}
		})
	}
}

// --- Qualify transport error rolls back ------------------------------

func TestStartOnDemand_QualifyErrorRollsBack(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	driverFailure := store.ErrLoadFailed
	harness.broker.qualifyOnDemandFunc = func(context.Context, funnel.AccountID) (bool, error) {
		return false, driverFailure
	}

	_, err := harness.conductor.StartOnDemand(context.Background(), UserID("human-q"), "IN", 30)
	if !errors.Is(err, driverFailure) {
		t.Fatalf("err = %v, want wrap of %v", err, driverFailure)
	}
	if errors.Is(err, ErrOnDemandSetupFailed) {
		t.Error("err must NOT wrap ErrOnDemandSetupFailed — pre-market-record failures route by base sentinel")
	}

	// Setup not entered: no market, no agents, no WireInstance, no Start.
	harness.conductor.reg.mu.Lock()
	marketsCount := len(harness.conductor.reg.markets)
	sessionsCount := len(harness.conductor.reg.sessions)
	byUserCount := len(harness.conductor.reg.byUser)
	harness.conductor.reg.mu.Unlock()
	if marketsCount != 0 || sessionsCount != 0 || byUserCount != 0 {
		t.Errorf("registry residue: markets=%d sessions=%d byUser=%d",
			marketsCount, sessionsCount, byUserCount)
	}
	if harness.agents.callCount != 0 {
		t.Errorf("agents.BringUp calls = %d, want 0", harness.agents.callCount)
	}
	if harness.broker.wireCount() != 0 || harness.market.wireCount() != 0 {
		t.Errorf("WireInstance calls on failure: broker=%d market=%d",
			harness.broker.wireCount(), harness.market.wireCount())
	}
}

// --- Populate failure BEFORE construct: Session=Failed, no exch.Stop -

func TestStartOnDemand_PopulateFailedBeforeConstruct(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	harness.agents.returnErr = ErrOrchestratorUnavailable
	userID := UserID("human-populate-fail")

	_, err := harness.conductor.StartOnDemand(context.Background(), userID, "IN", 30)
	if err == nil {
		t.Fatal("expected error on forced BringUp failure")
	}
	if !errors.Is(err, ErrOrchestratorUnavailable) {
		t.Errorf("err does not wrap ErrOrchestratorUnavailable: %v", err)
	}
	if !errors.Is(err, ErrOnDemandSetupFailed) {
		t.Errorf("setup-phase failure must wrap ErrOnDemandSetupFailed: %v", err)
	}
	// Pre-construct failure: nothing built, nothing to Stop.
	if len(harness.exchFactory.builtExchanges) != 0 {
		t.Errorf("exchanges constructed = %d, want 0 (populate failed before construct)",
			len(harness.exchFactory.builtExchanges))
	}
	if harness.broker.wireCount() != 0 || harness.market.wireCount() != 0 {
		t.Errorf("WireInstance called before populate: broker=%d market=%d",
			harness.broker.wireCount(), harness.market.wireCount())
	}
	if harness.broker.startSessionCount() != 0 {
		t.Errorf("StartSession called = %d, want 0", harness.broker.startSessionCount())
	}
	// Session retained as Failed with reason; market record gone; byUser retained.
	harness.conductor.reg.mu.Lock()
	defer harness.conductor.reg.mu.Unlock()
	if len(harness.conductor.reg.sessions) != 1 {
		t.Fatalf("sessions = %d, want 1 (Failed session retained)", len(harness.conductor.reg.sessions))
	}
	var failedSession *Session
	for _, s := range harness.conductor.reg.sessions {
		failedSession = s
	}
	if failedSession.State != Failed {
		t.Errorf("session.State = %v, want Failed", failedSession.State)
	}
	if !strings.Contains(failedSession.FailReason, "populate_failed") {
		t.Errorf("session.FailReason = %q, want prefix \"populate_failed\"", failedSession.FailReason)
	}
	if harness.conductor.reg.byUser[userID] != failedSession {
		t.Error("byUser slot must still point at Failed session (fallback status read)")
	}
	if len(harness.conductor.reg.markets) != 0 {
		t.Errorf("markets = %d, want 0 (market deleted on fail)", len(harness.conductor.reg.markets))
	}
}

// --- Install failure AFTER Start: exch.Stop fires, market gone, Session=Failed.

func TestStartOnDemand_InstallFailedAfterStart(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	harness.broker.startSessionFunc = func(context.Context, funnel.SessionStartCmd) error {
		return funnel.ErrBrokerTimeout
	}
	userID := UserID("human-install-fail")

	_, err := harness.conductor.StartOnDemand(context.Background(), userID, "IN", 30)
	if err == nil {
		t.Fatal("expected error on forced StartSession failure")
	}
	if !errors.Is(err, funnel.ErrBrokerTimeout) {
		t.Errorf("err does not wrap funnel.ErrBrokerTimeout: %v", err)
	}
	if !errors.Is(err, ErrOnDemandSetupFailed) {
		t.Errorf("post-start failure must wrap ErrOnDemandSetupFailed: %v", err)
	}
	// Exchange was built (BringUp succeeded, construct ran) AND Started
	// (WireInstance both axes ran before Start).
	if len(harness.exchFactory.builtExchanges) != 1 {
		t.Fatalf("built exchanges = %d, want 1 (StartSession failed after construct+start)",
			len(harness.exchFactory.builtExchanges))
	}
	if harness.broker.wireCount() != 1 || harness.market.wireCount() != 1 {
		t.Errorf("wire counts broker=%d market=%d, want 1 each (both axes wired before Start)",
			harness.broker.wireCount(), harness.market.wireCount())
	}
	// Session Failed, FailReason prefix install_failed, byUser retained, market deleted.
	harness.conductor.reg.mu.Lock()
	defer harness.conductor.reg.mu.Unlock()
	if len(harness.conductor.reg.sessions) != 1 {
		t.Fatalf("sessions = %d, want 1 (Failed session retained)", len(harness.conductor.reg.sessions))
	}
	var failedSession *Session
	for _, s := range harness.conductor.reg.sessions {
		failedSession = s
	}
	if failedSession.State != Failed {
		t.Errorf("session.State = %v, want Failed", failedSession.State)
	}
	if !strings.Contains(failedSession.FailReason, "install_failed") {
		t.Errorf("session.FailReason = %q, want prefix \"install_failed\"", failedSession.FailReason)
	}
	if harness.conductor.reg.byUser[userID] != failedSession {
		t.Error("byUser slot must still point at Failed session")
	}
	if len(harness.conductor.reg.markets) != 0 {
		t.Errorf("markets = %d, want 0 (market record deleted on install_failed)", len(harness.conductor.reg.markets))
	}
}

// --- No liquidity seeding ever happens -------------------------------

// TestStartOnDemand_SeedLiquidityNeverCalled pins the OD-locked
// invariant that on-demand instances do NOT seed operator liquidity —
// the book is formed by agent flow plus the warm-up window alone.
// Called out as its own test so a future regression (someone adds a
// liq.SeedLiquidity call to the on-demand path by analogy with
// canonical) surfaces here with a specific failure message rather
// than hiding inside the happy-path broader assertions.
func TestStartOnDemand_SeedLiquidityNeverCalled(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	userID := UserID("human-noseed")

	_, err := harness.conductor.StartOnDemand(context.Background(), userID, "IN", 30)
	if err != nil {
		t.Fatalf("StartOnDemand: %v", err)
	}

	// The recordingLiquidity records every SeedLiquidity call to the
	// shared sequencer. On-demand must emit zero.
	sequence := harness.exchFactory.sequencer.snapshot()
	for _, label := range sequence {
		if strings.HasPrefix(label, "liq.SeedLiquidity") {
			t.Errorf("on-demand bring-up called liquidity operator (%q); on-demand must never seed liquidity", label)
		}
	}
}

// --- Dispatcher resolution: od:<sid> walks to the right FanOut -----

// TestStartOnDemand_DispatcherResolvesOnDemand verifies the WebSocket
// dispatcher resolves an on-demand sessionID → Session.Market → fanout
// correctly WITHOUT any dispatcher code change. The on-demand
// SessionID happens to be the Market's suffix, but the dispatcher is
// never coded against that structure — it only walks the registry
// edges. This test proves the shipped dispatcher works for on-demand
// instances by construction.
func TestStartOnDemand_DispatcherResolvesOnDemand(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")
	userID := UserID("human-dispatch")

	result, err := harness.conductor.StartOnDemand(context.Background(), userID, "IN", 30)
	if err != nil {
		t.Fatalf("StartOnDemand: %v", err)
	}

	resolvedFanout := harness.conductor.fanoutForSession(result.SessionID)
	if resolvedFanout == nil {
		t.Fatal("fanoutForSession returned nil for an Active on-demand session")
	}
	if resolvedFanout != harness.market.returnFanout {
		t.Errorf("fanoutForSession returned %p, want %p (the one WireInstance built)",
			resolvedFanout, harness.market.returnFanout)
	}
}

// --- Lock-release invariant: reg.mu free across every call-out ------

// TestStartOnDemand_LockReleasedAcrossCallOuts is the on-demand mirror
// of the dispatcher's lock-released-before-delegate probe. The one
// registry mutex MUST be released before every broker/agent/exchange
// call-out — holding it across any of them would freeze every
// concurrent trigger and WebSocket resolve for the duration of the
// slow call.
//
// Each fake call records a TryLock attempt. A successful acquire
// proves the mutex was free at that moment; a failure proves the
// StartOnDemand body held it across the call-out, which is the bug
// this test exists to catch. Explicit multi-line failure message so a
// future regression surfaces WHY the invariant matters, not just that
// TryLock returned false.
func TestStartOnDemand_LockReleasedAcrossCallOuts(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")

	type probe struct {
		name     string
		acquired bool
	}
	var probeLock sync.Mutex
	var probes []probe
	recordProbe := func(name string) {
		acquired := harness.conductor.reg.mu.TryLock()
		if acquired {
			harness.conductor.reg.mu.Unlock()
		}
		probeLock.Lock()
		probes = append(probes, probe{name: name, acquired: acquired})
		probeLock.Unlock()
	}

	harness.broker.ensureAccountFunc = func(ctx context.Context, ownerType lifecycle.OwnerType, ownerID string) (funnel.AccountID, error) {
		recordProbe("EnsureAccount")
		return funnel.AccountID("acct-probe:" + ownerID), nil
	}
	harness.broker.qualifyOnDemandFunc = func(ctx context.Context, accountID funnel.AccountID) (bool, error) {
		recordProbe("QualifyOnDemand")
		return true, nil
	}
	harness.broker.startSessionFunc = func(ctx context.Context, cmd funnel.SessionStartCmd) error {
		recordProbe("StartSession")
		return nil
	}
	// Agents and exchange factory need the probe too; those fakes don't
	// have programmable hooks the way the broker does, so instrument via
	// shim-ports installed just for this test.
	harness.conductor.agents = probingAgents{inner: harness.agents, probe: func() { recordProbe("agents.BringUp") }}

	_, err := harness.conductor.StartOnDemand(context.Background(), UserID("human-probe"), "IN", 30)
	if err != nil {
		t.Fatalf("StartOnDemand: %v", err)
	}

	// Every probed call must have acquired the lock.
	for _, pr := range probes {
		if !pr.acquired {
			t.Errorf(`registry mutex HELD during %q — StartOnDemand must release reg.mu before every call-out.
A lock held across a broker/agent/exchange call-out freezes every concurrent trigger
and WebSocket resolve for the duration of that call. The call sequence that failed
this invariant: %+v`, pr.name, probes)
		}
	}
	// Sanity: we actually ran the probe at every expected call-out.
	expected := map[string]bool{
		"EnsureAccount":    false,
		"QualifyOnDemand":  false,
		"agents.BringUp":   false,
		"StartSession":     false,
	}
	for _, pr := range probes {
		expected[pr.name] = true
	}
	for name, seen := range expected {
		if !seen {
			t.Errorf("probe %q never fired — the flow did not reach that call-out", name)
		}
	}
}

// probingAgents wraps a recordingAgents so a lock-probe fires on each
// BringUp call without disturbing the recording behavior the harness
// otherwise relies on. Used only by the lock-release test.
type probingAgents struct {
	inner *recordingAgents
	probe func()
}

func (p probingAgents) BringUp(ctx context.Context, market MarketTag, mode Mode, instruments []contract.InstrumentID) error {
	p.probe()
	return p.inner.BringUp(ctx, market, mode, instruments)
}

func (p probingAgents) WindDown(market MarketTag, mode Mode) {
	p.inner.WindDown(market, mode)
}

// --- Config load failure before any reserve --------------------------

func TestStartOnDemand_UnknownTag_FailsBeforeReserve(t *testing.T) {
	harness := newOnDemandHarness(t, "IN")

	_, err := harness.conductor.StartOnDemand(context.Background(), UserID("human-badtag"), "ZZZ", 30)
	if err == nil {
		t.Fatal("expected error for unknown tag")
	}
	// Nothing reserved — reserve was never reached.
	harness.conductor.reg.mu.Lock()
	defer harness.conductor.reg.mu.Unlock()
	if len(harness.conductor.reg.sessions) != 0 || len(harness.conductor.reg.byUser) != 0 ||
		len(harness.conductor.reg.markets) != 0 {
		t.Errorf("registry mutated on config-load failure: sessions=%d byUser=%d markets=%d",
			len(harness.conductor.reg.sessions),
			len(harness.conductor.reg.byUser),
			len(harness.conductor.reg.markets))
	}
}

// --- OnDemandResultKind.String not needed yet; keep this file in sync.

func TestOnDemandResult_WarmupBeforeSessionEnds(t *testing.T) {
	// Pure assertion against the arithmetic: WarmupEndsAt must precede
	// SessionEndsAt by exactly the configured session duration. Guard
	// against sign errors / unit confusion in future edits to the
	// finalize math.
	harness := newOnDemandHarness(t, "IN")
	durationMinutes := 42

	result, err := harness.conductor.StartOnDemand(context.Background(), UserID("human-math"), "IN", durationMinutes)
	if err != nil {
		t.Fatalf("StartOnDemand: %v", err)
	}
	delta := result.SessionEndsAt.Sub(result.WarmupEndsAt)
	wantDelta := time.Duration(durationMinutes) * time.Minute
	if delta != wantDelta {
		t.Errorf("SessionEndsAt - WarmupEndsAt = %v, want %v", delta, wantDelta)
	}
	if result.WarmupEndsAt.Sub(harness.fixedNow) != configuredWarmupLength {
		t.Errorf("WarmupEndsAt - T0 = %v, want %v",
			result.WarmupEndsAt.Sub(harness.fixedNow), configuredWarmupLength)
	}
}

