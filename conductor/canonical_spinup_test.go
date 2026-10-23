package conductor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"simulator/broker/marketdata"
	"simulator/contract"
	"simulator/exchange/clock"
	"simulator/exchange/emit"
	"simulator/exchange/wire"
)

// --- Test doubles -----------------------------------------------------

// callSequencer threads a shared, ordered record of labelled calls
// through every recording double. Ordering assertions read a snapshot;
// a call's label uniquely identifies which double emitted it (and, when
// useful, which market it ran against).
type callSequencer struct {
	sequencerLock sync.Mutex
	calls         []string
}

func (sequencer *callSequencer) record(label string) {
	sequencer.sequencerLock.Lock()
	sequencer.calls = append(sequencer.calls, label)
	sequencer.sequencerLock.Unlock()
}

func (sequencer *callSequencer) snapshot() []string {
	sequencer.sequencerLock.Lock()
	defer sequencer.sequencerLock.Unlock()
	copied := make([]string, len(sequencer.calls))
	copy(copied, sequencer.calls)
	return copied
}

// recordingAgents is an AgentOrchestrator that logs each call to the
// sequencer and returns a configurable canned response (nil for
// success, any error for forced-failure).
type recordingAgents struct {
	sequencer   *callSequencer
	returnErr   error
	callLock    sync.Mutex
	callCount   int
	lastRequest AgentBringUpRequest
}

func (recorder *recordingAgents) BringUp(ctx context.Context, request AgentBringUpRequest) error {
	recorder.sequencer.record("agents.BringUp:" + string(request.Market))
	recorder.callLock.Lock()
	recorder.callCount++
	recorder.lastRequest = request
	recorder.callLock.Unlock()
	return recorder.returnErr
}

// lastRequestSnapshot returns the most recent BringUp request this
// recorder observed, for tests asserting on fields beyond call count
// (e.g. Scenario).
func (recorder *recordingAgents) lastRequestSnapshot() AgentBringUpRequest {
	recorder.callLock.Lock()
	defer recorder.callLock.Unlock()
	return recorder.lastRequest
}

func (recorder *recordingAgents) WindDown(ctx context.Context, exchangeID MarketID, market MarketTag, mode Mode) error {
	recorder.sequencer.record("agents.WindDown:" + string(market))
	return nil
}

// recordingLiquidity is a fire-and-trust LiquidityOperator that only
// records its call.
type recordingLiquidity struct {
	sequencer *callSequencer
}

func (recorder *recordingLiquidity) SeedLiquidity(ctx context.Context, marketID MarketID, specs []LiquiditySpec) {
	recorder.sequencer.record("liq.SeedLiquidity:" + string(marketID))
}

// recordingBroker satisfies BrokerPort — inherits EnsureAccount /
// StartSession / QualifyOnDemand from the embedded stub, overrides
// WireInstance to record ordering and spawn a drain goroutine that
// discards results so a started exchange is not blocked emitting them.
type recordingBroker struct {
	stubBrokerPort
	sequencer *callSequencer
	drainWG   sync.WaitGroup
}

func (recorder *recordingBroker) WireInstance(exchangeID string, privateStream <-chan contract.EngineResult) {
	recorder.sequencer.record("broker.WireInstance:" + exchangeID)
	recorder.drainWG.Add(1)
	go func() {
		defer recorder.drainWG.Done()
		for range privateStream {
		}
	}()
}

// recordingMarket satisfies MarketDataPort — inherits WSConfig from
// the embedded stub, overrides WireInstance to record ordering and
// drain the public tape.
type recordingMarket struct {
	stubMarketDataPort
	sequencer *callSequencer
	drainWG   sync.WaitGroup
}

func (recorder *recordingMarket) WireInstance(
	exchangeID marketdata.ExchangeID,
	tape <-chan emit.TapeEvent,
	market string,
	mode marketdata.Mode,
) *marketdata.FanOut {
	recorder.sequencer.record("market.WireInstance:" + string(exchangeID))
	recorder.drainWG.Add(1)
	go func() {
		defer recorder.drainWG.Done()
		for range tape {
		}
	}()
	// Returning nil is fine for spin-up tests: the Market record simply
	// stores whatever is returned; nothing in this phase dereferences it.
	return nil
}

// recordingExchangeFactory wraps the real wire.NewExchange so happy-path
// tests get a working exchange, records every construction, and keeps
// handles so cleanup can Stop every built exchange (releasing monitor
// / shards / ingress goroutines).
type recordingExchangeFactory struct {
	sequencer        *callSequencer
	constructionLock sync.Mutex
	builtExchanges   []*wire.Exchange
}

func (factory *recordingExchangeFactory) NewExchange(numShards int, schedule clock.SessionSchedule, specs []wire.InstrumentSpec) *wire.Exchange {
	factory.sequencer.record("exch.NewExchange")
	exch := wire.NewExchange(numShards, schedule, specs)
	factory.constructionLock.Lock()
	factory.builtExchanges = append(factory.builtExchanges, exch)
	factory.constructionLock.Unlock()
	return exch
}

func (factory *recordingExchangeFactory) stopAll() {
	factory.constructionLock.Lock()
	defer factory.constructionLock.Unlock()
	for _, exch := range factory.builtExchanges {
		exch.Stop()
	}
}

// Compile-time: doubles satisfy their interfaces.
var (
	_ AgentOrchestrator = (*recordingAgents)(nil)
	_ LiquidityOperator = (*recordingLiquidity)(nil)
	_ BrokerPort        = (*recordingBroker)(nil)
	_ MarketDataPort    = (*recordingMarket)(nil)
	_ ExchangeFactory   = (*recordingExchangeFactory)(nil)
)

// --- Harness -----------------------------------------------------------

type spinUpHarness struct {
	conductor *Conductor
	sequencer *callSequencer
	agents    *recordingAgents
	liq       *recordingLiquidity
	broker    *recordingBroker
	market    *recordingMarket
	exch      *recordingExchangeFactory
	reader    *fakeCandleReader
	cfgDir    string
}

// writeTestMarketConfig drops one <tag>.json into cfgDir with a
// three-boundary schedule (continuous open at 09:15, closing_auction at
// 15:15, closed at 15:30, Asia/Kolkata) and one instrument. Mirrors the
// shape loadConfig expects.
func writeTestMarketConfig(t *testing.T, cfgDir string, marketTag MarketTag) {
	t.Helper()
	content := fmt.Sprintf(`{
  "market": %q,
  "timezone": "Asia/Kolkata",
  "ohlcv_interval": "1m",
  "num_shards": 1,
  "preopen_lead": "25m",
  "session": [
    {"at": "09:15", "phase": "continuous"},
    {"at": "15:15", "phase": "closing_auction"},
    {"at": "15:30", "phase": "closed"}
  ],
  "ondemand": {"duration": "1h", "warmup": "15m"},
  "instruments": [
    {
      "id": "RELIANCE",
      "tick_size": 5,
      "band_bps": 1000,
      "seed_price": 250000,
      "liquidity": {"levels": 10, "spacing_pct": 0.1, "near_qty": 100, "taper": 0.5}
    }
  ]
}`, string(marketTag))
	path := filepath.Join(cfgDir, string(marketTag)+".json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writeTestMarketConfig %q: %v", marketTag, err)
	}
}

func newSpinUpHarness(t *testing.T, marketTag MarketTag) *spinUpHarness {
	t.Helper()
	cfgDir := t.TempDir()
	writeTestMarketConfig(t, cfgDir, marketTag)
	sequencer := &callSequencer{}
	harness := &spinUpHarness{
		sequencer: sequencer,
		agents:    &recordingAgents{sequencer: sequencer},
		liq:       &recordingLiquidity{sequencer: sequencer},
		broker:    &recordingBroker{sequencer: sequencer},
		market:    &recordingMarket{sequencer: sequencer},
		exch:      &recordingExchangeFactory{sequencer: sequencer},
		reader:    readerReturningClose(251575),
		cfgDir:    cfgDir,
	}
	pool := newUnconnectedPool(t)
	cd, err := NewConductor(Config{
		CfgDir:    cfgDir,
		OHLCVPool: pool,
		Broker:    harness.broker,
		Market:    harness.market,
		Exchange:  harness.exch,
		Agents:    harness.agents,
		Liquidity: harness.liq,
		Now:       func() time.Time { return scheduleTestNow },
	})
	if err != nil {
		t.Fatalf("NewConductor: %v", err)
	}
	cd.referenceReader = harness.reader
	harness.conductor = cd
	t.Cleanup(func() {
		harness.exch.stopAll()
		harness.broker.drainWG.Wait()
		harness.market.drainWG.Wait()
		cd.Stop()
	})
	return harness
}

// configureAnotherMarket writes a second tag.json so a harness can run
// concurrent different-tag spin-ups out of one cfgDir.
func configureAnotherMarket(t *testing.T, harness *spinUpHarness, extraTag MarketTag) {
	t.Helper()
	writeTestMarketConfig(t, harness.cfgDir, extraTag)
}

// --- Tests -------------------------------------------------------------

func TestSpinUpCanonical_HappyPathCommitsLive(t *testing.T) {
	harness := newSpinUpHarness(t, "IN")
	marketID, err := harness.conductor.spinUpCanonical(context.Background(), "IN")
	if err != nil {
		t.Fatalf("spinUpCanonical: %v", err)
	}
	if marketID != "IN:2026-10-12" {
		t.Errorf("marketID = %q, want %q", marketID, "IN:2026-10-12")
	}

	harness.conductor.reg.mu.Lock()
	record, present := harness.conductor.reg.markets[marketID]
	harness.conductor.reg.mu.Unlock()
	if !present {
		t.Fatalf("registry has no record for %q", marketID)
	}
	if record.State != Live {
		t.Errorf("State = %v, want Live", record.State)
	}
	if record.exch == nil {
		t.Error("exch not set on committed record")
	}
	if record.inbound == nil {
		t.Error("inbound not set on committed record")
	}
	// fanout is intentionally nil in these tests (recordingMarket returns nil);
	// the field is present and assignable, which is what the commit path needs.
	istLoc := istLocation(t)
	expectedClose := time.Date(2026, 10, 12, 15, 30, 0, 0, istLoc)
	if !record.closeAt.Equal(expectedClose) {
		t.Errorf("closeAt = %s, want %s", record.closeAt, expectedClose)
	}
	if record.Mode != Canonical {
		t.Errorf("Mode = %v, want Canonical", record.Mode)
	}
	if record.Tag != "IN" {
		t.Errorf("Tag = %q, want %q", record.Tag, "IN")
	}
	if record.members == nil {
		t.Error("members map not initialized")
	}
}

func TestSpinUpCanonical_OrderingSeedBeforeWireBeforeStart(t *testing.T) {
	harness := newSpinUpHarness(t, "IN")
	_, err := harness.conductor.spinUpCanonical(context.Background(), "IN")
	if err != nil {
		t.Fatalf("spinUpCanonical: %v", err)
	}

	calls := harness.sequencer.snapshot()
	// Required ordering per the locked design:
	//   exch.NewExchange
	//   agents.BringUp
	//   liq.SeedLiquidity
	//   broker.WireInstance
	//   market.WireInstance
	// (exchange.Start is not recorded directly; State=Live, which the
	// happy-path test asserts, is the signal that Start ran after the
	// two WireInstance calls.)
	required := []string{
		"exch.NewExchange",
		"agents.BringUp:IN",
		"liq.SeedLiquidity:IN:2026-10-12",
		"broker.WireInstance:IN:2026-10-12",
		"market.WireInstance:IN:2026-10-12",
	}
	positions := make([]int, len(required))
	for requiredIndex, requiredLabel := range required {
		foundAt := -1
		for callIndex, callLabel := range calls {
			if callLabel == requiredLabel {
				foundAt = callIndex
				break
			}
		}
		if foundAt < 0 {
			t.Fatalf("required call %q missing from sequence %v", requiredLabel, calls)
		}
		positions[requiredIndex] = foundAt
	}
	for pairIndex := 1; pairIndex < len(positions); pairIndex++ {
		if positions[pairIndex] <= positions[pairIndex-1] {
			t.Errorf("ordering violated: %q (pos %d) must come after %q (pos %d) in %v",
				required[pairIndex], positions[pairIndex],
				required[pairIndex-1], positions[pairIndex-1],
				calls)
		}
	}
}

func TestSpinUpCanonical_ReserveGateSequentialDuplicateReturnsErrAlreadyUp(t *testing.T) {
	harness := newSpinUpHarness(t, "IN")

	firstID, firstErr := harness.conductor.spinUpCanonical(context.Background(), "IN")
	if firstErr != nil {
		t.Fatalf("first spin-up: %v", firstErr)
	}

	secondID, secondErr := harness.conductor.spinUpCanonical(context.Background(), "IN")
	if !errors.Is(secondErr, ErrAlreadyUp) {
		t.Fatalf("second spin-up error = %v, want ErrAlreadyUp", secondErr)
	}
	if secondID != firstID {
		t.Errorf("duplicate return id = %q, want same as first %q", secondID, firstID)
	}
	// The second attempt must not have reached the exchange factory or
	// the agents.
	if countOf(harness.sequencer.snapshot(), "exch.NewExchange") != 1 {
		t.Errorf("exch.NewExchange called more than once — the reserve gate did not block the duplicate")
	}
	if harness.agents.callCount != 1 {
		t.Errorf("agents.BringUp calls = %d, want 1 (duplicate must not reach seeding)", harness.agents.callCount)
	}
}

func TestSpinUpCanonical_ReserveGateConcurrentDuplicateYieldsExactlyOneWinner(t *testing.T) {
	harness := newSpinUpHarness(t, "IN")

	var waitForBoth sync.WaitGroup
	var raceResults [2]error
	var raceIDs [2]MarketID
	start := make(chan struct{})
	for goroutineIndex := 0; goroutineIndex < 2; goroutineIndex++ {
		goroutineIndex := goroutineIndex
		waitForBoth.Add(1)
		go func() {
			defer waitForBoth.Done()
			<-start
			raceIDs[goroutineIndex], raceResults[goroutineIndex] = harness.conductor.spinUpCanonical(context.Background(), "IN")
		}()
	}
	close(start)
	waitForBoth.Wait()

	successCount := 0
	alreadyUpCount := 0
	for _, raceErr := range raceResults {
		switch {
		case raceErr == nil:
			successCount++
		case errors.Is(raceErr, ErrAlreadyUp):
			alreadyUpCount++
		default:
			t.Errorf("unexpected error on concurrent spin-up: %v", raceErr)
		}
	}
	if successCount != 1 {
		t.Errorf("concurrent spin-ups produced %d successes, want exactly 1", successCount)
	}
	if alreadyUpCount != 1 {
		t.Errorf("concurrent spin-ups produced %d ErrAlreadyUp, want exactly 1", alreadyUpCount)
	}
}

func TestSpinUpCanonical_AbortOnBringUpFailureCleansRegistry(t *testing.T) {
	harness := newSpinUpHarness(t, "IN")
	bringUpFailure := ErrOrchestratorUnavailable
	harness.agents.returnErr = bringUpFailure

	marketID, err := harness.conductor.spinUpCanonical(context.Background(), "IN")
	if err == nil {
		t.Fatal("expected error on forced BringUp failure")
	}
	if !errors.Is(err, bringUpFailure) {
		t.Errorf("error does not wrap %v: %v", bringUpFailure, err)
	}
	if marketID != "" {
		t.Errorf("marketID on failure = %q, want empty string", marketID)
	}

	harness.conductor.reg.mu.Lock()
	_, stillPresent := harness.conductor.reg.markets["IN:2026-10-12"]
	harness.conductor.reg.mu.Unlock()
	if stillPresent {
		t.Error("Provisioning record not deleted after BringUp failure")
	}

	// No wiring, no Start — the sequencer must not have recorded any
	// of those calls.
	calls := harness.sequencer.snapshot()
	for _, forbiddenLabel := range []string{
		"liq.SeedLiquidity:IN:2026-10-12",
		"broker.WireInstance:IN:2026-10-12",
		"market.WireInstance:IN:2026-10-12",
	} {
		if containsString(calls, forbiddenLabel) {
			t.Errorf("abort path invoked %q — expected zero calls to seed/wire after BringUp failure", forbiddenLabel)
		}
	}
}

func TestSpinUpCanonical_AbortOnBuildExchangeArgsFailure_NeverConstructsExchange(t *testing.T) {
	harness := newSpinUpHarness(t, "IN")
	// Force buildExchangeArgs to fail by injecting a reader that returns
	// a generic driver error on the reference lookup — schedule is fine,
	// but the per-instrument reference read fails.
	driverFailure := errors.New("reader forced failure")
	harness.conductor.referenceReader = &fakeCandleReader{returnRow: fakeCandleRow{scanErr: driverFailure}}

	marketID, err := harness.conductor.spinUpCanonical(context.Background(), "IN")
	if err == nil {
		t.Fatal("expected error when the reference reader fails")
	}
	if !errors.Is(err, driverFailure) {
		t.Errorf("error does not wrap driver failure: %v", err)
	}
	if marketID != "" {
		t.Errorf("marketID on failure = %q, want empty string", marketID)
	}

	calls := harness.sequencer.snapshot()
	if containsString(calls, "exch.NewExchange") {
		t.Errorf("exchange constructed before build args failure was detected: %v", calls)
	}
	if harness.agents.callCount != 0 {
		t.Errorf("agents.BringUp was called %d times after build-args failure; want 0", harness.agents.callCount)
	}

	harness.conductor.reg.mu.Lock()
	_, stillPresent := harness.conductor.reg.markets["IN:2026-10-12"]
	harness.conductor.reg.mu.Unlock()
	if stillPresent {
		t.Error("Provisioning record not deleted after buildExchangeArgs failure")
	}
}

func TestSpinUpCanonical_MissingConfigFile_FailsBeforeReserve(t *testing.T) {
	harness := newSpinUpHarness(t, "IN")
	// Spin up a tag that has no config file in cfgDir.
	marketID, err := harness.conductor.spinUpCanonical(context.Background(), "US")
	if err == nil {
		t.Fatal("expected error for unknown market tag")
	}
	if marketID != "" {
		t.Errorf("marketID = %q, want empty on config load failure", marketID)
	}

	harness.conductor.reg.mu.Lock()
	markets := len(harness.conductor.reg.markets)
	harness.conductor.reg.mu.Unlock()
	if markets != 0 {
		t.Errorf("registry has %d records after a load-config failure; want 0 (reserve not reached)", markets)
	}
}

func TestSpinUpCanonical_ConcurrentDifferentTagsBothCommitLive(t *testing.T) {
	harness := newSpinUpHarness(t, "IN")
	configureAnotherMarket(t, harness, "US")

	var waitForBoth sync.WaitGroup
	var results [2]error
	tags := []MarketTag{"IN", "US"}
	for goroutineIndex, tag := range tags {
		goroutineIndex := goroutineIndex
		tag := tag
		waitForBoth.Add(1)
		go func() {
			defer waitForBoth.Done()
			_, results[goroutineIndex] = harness.conductor.spinUpCanonical(context.Background(), tag)
		}()
	}
	waitForBoth.Wait()

	for resultIndex, resultErr := range results {
		if resultErr != nil {
			t.Errorf("tag %q spin-up: %v", tags[resultIndex], resultErr)
		}
	}

	harness.conductor.reg.mu.Lock()
	defer harness.conductor.reg.mu.Unlock()
	if _, ok := harness.conductor.reg.markets["IN:2026-10-12"]; !ok {
		t.Error("IN:2026-10-12 record missing after concurrent spin-up")
	}
	if _, ok := harness.conductor.reg.markets["US:2026-10-12"]; !ok {
		t.Error("US:2026-10-12 record missing after concurrent spin-up")
	}
	for marketID, record := range harness.conductor.reg.markets {
		if record.State != Live {
			t.Errorf("record %q State = %v, want Live", marketID, record.State)
		}
	}
}

func TestSpinUpCanonical_MarketIDUsesSessionDateInMarketTimezone(t *testing.T) {
	// scheduleTestNow is 07:00 UTC on Mon 2026-10-12; in IST that is
	// 12:30 the same date. The market id must carry the IST date.
	harness := newSpinUpHarness(t, "IN")
	marketID, err := harness.conductor.spinUpCanonical(context.Background(), "IN")
	if err != nil {
		t.Fatalf("spinUpCanonical: %v", err)
	}
	if !strings.HasPrefix(string(marketID), "IN:") {
		t.Fatalf("marketID = %q, want prefix \"IN:\"", marketID)
	}
	dateSuffix := strings.TrimPrefix(string(marketID), "IN:")
	if dateSuffix != "2026-10-12" {
		t.Errorf("market id date suffix = %q, want %q", dateSuffix, "2026-10-12")
	}
}

// --- small helpers -----------------------------------------------------

func countOf(slice []string, needle string) int {
	count := 0
	for _, element := range slice {
		if element == needle {
			count++
		}
	}
	return count
}

func containsString(slice []string, needle string) bool {
	return countOf(slice, needle) > 0
}
