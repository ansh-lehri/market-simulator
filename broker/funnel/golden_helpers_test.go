package funnel

import (
	"testing"
	"time"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/capstone"
	"simulator/economy/cost"
	"simulator/economy/lifecycle"
	"simulator/economy/persistence"
	"simulator/economy/realize"
	"simulator/economy/reserve"
)

// -----------------------------------------------------------------------------
// Stage-2 golden test harness.
//
// Goldens pin the EXACT reservation-call sequence + OrderTrack.Status
// transition + finalize behaviour that the (not-yet-written) Phase-5
// settle adapter and handlePlaceOrder must produce, with expected values
// COPIED FROM THE DOCS (Exchange_Broker_Contract.md §9 and
// BROKER_PHASE5_IMPLEMENTATION_DESIGN.md §3.4) — not derived from this
// session's reading of the formula.
//
// RESERVATION CALL LOG: we read the ordered sequence of
// reserve.ReserveIfAvailable / Convert / Release / ReleaseExcess calls off
// the SPY EventSink wired into the ReservationService. Each method emits
// exactly one event (except Convert's shared tail, which may emit a
// second ConvertEvent for terminal brokerage). Asserting on
// events.emitted = direct, order-preserving proof of what the reserve
// package did, no interface wrapping needed.
//
// SETTLE GOLDENS pre-seed a reservation via ReserveIfAvailable, reset the
// spies (so the pre-seed noise is cleared), THEN exercise handleApplyResult
// — assertions trace to the doc-pinned expected post-reserve state.
//
// MARKET-BUY PLACE GOLDENS exercise handlePlaceOrder against a hot
// mktCache stub, assert on the FundedOrder punched + the reservation
// shape left in the reserve book.
//
// CURRENT STATE (Stage 2 — RED): handleApplyResult and handlePlaceOrder are
// empty-stub seams; every golden fails its assertions. Stage 5 (settle) and
// Stage 3 (place) implement to green.
// -----------------------------------------------------------------------------

// goldenIndiaRates — mirror of economy/reserve/convert_test.go indiaRatesFixture,
// locked by [[project_india_rate_locks]]. Used so goldens can hand-compute
// per-line cost totals.
var goldenIndiaRates = cost.IndiaRates{
	STT:           cost.Rate{Num: 25, Den: 100_000},     // 0.025%
	Exchange:      cost.Rate{Num: 297, Den: 10_000_000}, // 0.00297%
	SEBI:          cost.Rate{Num: 1, Den: 1_000_000},    // 0.0001%
	Stamp:         cost.Rate{Num: 3, Den: 100_000},      // 0.003%
	GST:           cost.Rate{Num: 18, Den: 100},         // 18%
	Brokerage:     2000,
	AutosquareFee: 5000,
}

// goldenExitAt — fixed timestamp fed into Convert so lot EntryAt and
// RealizationInput.ExitAt are bit-identical across replays.
var goldenExitAt = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

const (
	goldenBrokerID      BrokerID              = "B-golden"
	goldenAccountID     AccountID             = "acct-G"
	goldenSessionID                           = "sess-G"
	goldenExchangeID                          = "X-golden"
	goldenInstrument    contract.InstrumentID = "ACME"
	goldenParticipantID                       = "P-golden"
)

// -----------------------------------------------------------------------------
// Spies — scoped to the goldens, mirror of economy/reserve/spy_test.go
// shapes (not exported, so duplicated here).
// -----------------------------------------------------------------------------

type goldenLedger struct {
	accounts map[account.AccountID]*account.Account
}

func newGoldenLedger() *goldenLedger {
	return &goldenLedger{accounts: map[account.AccountID]*account.Account{}}
}

func (ledger *goldenLedger) Get(id account.AccountID) *account.Account {
	return ledger.accounts[id]
}

func (ledger *goldenLedger) put(acct *account.Account) {
	ledger.accounts[acct.ID] = acct
}

type goldenDirtyCall struct {
	SessionID string
	AccountID account.AccountID
}

type goldenPersistence struct {
	dirty []goldenDirtyCall
}

func newGoldenPersistence() *goldenPersistence { return &goldenPersistence{} }

func (persist *goldenPersistence) MarkSnapshotDirty(sessionID reserve.SessionID, accountID account.AccountID) {
	persist.dirty = append(persist.dirty, goldenDirtyCall{SessionID: sessionID, AccountID: accountID})
}

type goldenEvents struct {
	emitted []reserve.Event
}

func newGoldenEvents() *goldenEvents { return &goldenEvents{} }

func (events *goldenEvents) Emit(event reserve.Event) {
	events.emitted = append(events.emitted, event)
}

// reset clears the recorded events — called after pre-seed setup so the
// golden only sees events the exercised handler emitted.
func (events *goldenEvents) reset() {
	events.emitted = events.emitted[:0]
}

type goldenRealizer struct {
	fills []realize.RealizationInput
}

func newGoldenRealizer() *goldenRealizer { return &goldenRealizer{} }

func (realiz *goldenRealizer) OnSellFill(input realize.RealizationInput) {
	realiz.fills = append(realiz.fills, input)
}

// -----------------------------------------------------------------------------
// goldenHarness — one assembled shard + its spies + the exchange edge.
// Each golden builds its own fresh harness.
// -----------------------------------------------------------------------------

type goldenHarness struct {
	shard     *shard
	ledger    *goldenLedger
	persist   *goldenPersistence
	events    *goldenEvents
	realiz    *goldenRealizer
	exchange  chan contract.InboundMessage
	accountID account.AccountID
}

func newGoldenHarness(_ *testing.T) *goldenHarness {
	ledger := newGoldenLedger()
	persist := newGoldenPersistence()
	events := newGoldenEvents()
	realiz := newGoldenRealizer()
	costs := cost.NewIndiaCostModule(goldenIndiaRates)
	resvSvc := reserve.NewReservationService(ledger, costs, realiz, events, persist)

	exchange := make(chan contract.InboundMessage, 16)

	// A capstone.Bundle with ONLY Persist wired. The *persistence.Component
	// is built with nil pool / nil redis / nil adapter — tolerated by
	// NewComponent (its doc says "nils are tolerated only for tests that
	// exercise a subset"). MarkSnapshotDirty is pure in-memory map
	// manipulation and never touches storage, so it works. Any other
	// Component method would nil-deref — we don't call them from the funnel
	// handlers under test here. DrainDirty is the non-destructive read tests
	// use to assert that the dirty flag landed.
	bundle := &capstone.Bundle{
		Persist: persistence.NewComponent(nil, nil, nil, persistence.Config{}),
	}

	assembledShard := &shard{
		brokerID: goldenBrokerID,
		sessions: map[AccountID]*ownedSession{},
		tracks:   map[OrderID]*OrderTrack{},
		mktCache: nopMarketCache{},
		resv:     resvSvc,
		bundle:   bundle,
	}
	return &goldenHarness{
		shard:     assembledShard,
		ledger:    ledger,
		persist:   persist,
		events:    events,
		realiz:    realiz,
		exchange:  exchange,
		accountID: goldenAccountID,
	}
}

// seedAccount installs an account into the ledger AND installs a matching
// live session on the shard so handlers that look up sessions[aid] succeed.
// Session binding points at the harness's capture channel so punches are
// observable.
func (harness *goldenHarness) seedAccount(cash account.Cash, positions map[contract.InstrumentID]*account.Position) *account.Account {
	if positions == nil {
		positions = map[contract.InstrumentID]*account.Position{}
	}
	acct := &account.Account{
		ID:        harness.accountID,
		Cash:      cash,
		Positions: positions,
	}
	harness.ledger.put(acct)
	harness.shard.sessions[harness.accountID] = &ownedSession{
		account: lifecycle.LoadedAccount{
			Account:   acct,
			OwnerType: lifecycle.OwnerHuman,
			OwnerID:   "owner-golden",
		},
		binding: SessionBinding{
			SessionID:       goldenSessionID,
			ExchangeID:      goldenExchangeID,
			ExchangeInbound: harness.exchange,
		},
		participant: goldenParticipantID,
		instruments: permissiveInstruments{},
	}
	return acct
}

// seedReservation runs ReserveIfAvailable against the harness's resv so a
// record exists in the book + Account state reflects a reservation. Called
// as the pre-seed step of every settle golden; after this, events.reset()
// clears the ReserveEvent so the golden's assertions only see settle-side
// emissions.
func (harness *goldenHarness) seedReservation(order reserve.Order) {
	if err := harness.shard.resv.ReserveIfAvailable(reserve.Participant{ID: goldenParticipantID}, order); err != nil {
		panic("golden seedReservation failed: " + err.Error())
	}
}

// seedTrack installs an OrderTrack with the given status + flags. Status
// defaults to OrderStatusWorking; IsMarketBuy must be set at place time so
// goldens that exercise settleMatch's market-buy branch must pass true.
func (harness *goldenHarness) seedTrack(orderID OrderID, status OrderStatus, isMarketBuy bool) {
	harness.shard.tracks[orderID] = &OrderTrack{
		OrderID:     orderID,
		AccountID:   harness.accountID,
		Instrument:  goldenInstrument,
		Status:      status,
		IsMarketBuy: isMarketBuy,
	}
}

// hotMktCache swaps in a MarketPriceCache stub that returns the given
// BestAsk tick for (goldenExchangeID, goldenInstrument) and misses for
// anything else. Phase-6 LastPrice always misses here — these goldens
// exercise the Phase-5 market-buy path, which never consults LastPrice;
// Phase-6 position-mark goldens inject their own stub.
func (harness *goldenHarness) hotMktCache(bestAsk contract.Ticks) {
	harness.shard.mktCache = &hotMktCacheStub{
		exchange:   goldenExchangeID,
		instrument: goldenInstrument,
		bestAsk:    bestAsk,
	}
}

type hotMktCacheStub struct {
	exchange   ExchangeID
	instrument contract.InstrumentID
	bestAsk    contract.Ticks
}

func (stub *hotMktCacheStub) BestAsk(exchange ExchangeID, instrument contract.InstrumentID) (contract.Ticks, bool) {
	if exchange != stub.exchange || instrument != stub.instrument {
		return 0, false
	}
	return stub.bestAsk, true
}

func (stub *hotMktCacheStub) LastPrice(_ ExchangeID, _ contract.InstrumentID) (contract.Ticks, bool) {
	return 0, false
}

// -----------------------------------------------------------------------------
// Event-log helpers — the goldens assert on events.emitted as the ordered
// reservation-call log. Shapes mirror economy/reserve/seams.go event types.
// -----------------------------------------------------------------------------

func mustConvertEvent(t *testing.T, event reserve.Event) reserve.ConvertEvent {
	t.Helper()
	ce, ok := event.(reserve.ConvertEvent)
	if !ok {
		t.Fatalf("expected ConvertEvent, got %T: %+v", event, event)
	}
	return ce
}

func mustReleaseEvent(t *testing.T, event reserve.Event) reserve.ReleaseEvent {
	t.Helper()
	re, ok := event.(reserve.ReleaseEvent)
	if !ok {
		t.Fatalf("expected ReleaseEvent, got %T: %+v", event, event)
	}
	return re
}
