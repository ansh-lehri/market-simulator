package reserve

import (
	"fmt"
	"log/slog"

	"simulator/economy/account"
	"simulator/economy/cost"
)

// ReservationService is the 5.2 reservation brain. It holds NO balances —
// its only owned state is the ReservationBook (created internally by the
// constructor; hydration from a Redis snapshot at session load lands as a
// separate internal seam in a later phase). Every mutation to Account
// fields goes through the Tier-1 hands via the injected AccountLedger.
//
// Every collaborator is injected by interface (structural typing lets a
// single concrete satisfy both this package's seams and realize's, where
// the shapes overlap):
//
//   - ledger  — resolves AccountID → *Account (read handle).
//   - costs   — 5.3 CostModule; called from Convert (per-fill) and
//               Convert/Release (per-order terminal brokerage).
//   - realiz  — 5.4 RealizerHandoff; called from Convert's sell branch.
//   - events  — money-event emission at every reserve/convert/release
//               call site (event system deferred, see seams.go).
//   - persist — async MarkSnapshotDirty at the end of every method.
//               NEVER a synchronous store write on the hot path.
type ReservationService struct {
	book    *ReservationBook
	ledger  AccountLedger
	costs   cost.CostModule
	realiz  RealizerHandoff
	events  EventSink
	persist Persistence

	// logger is a boundary logger — used at ingress (ReserveIfAvailable
	// admit/reject) and lifecycle boundaries (Convert / Release /
	// ReleaseExcess in later stages). NEVER used inside the read-compare
	// or the hand call itself: those must stay pure/replayable so logs
	// cannot become control flow.
	logger *slog.Logger
}

// NewReservationService wires the service with all injected collaborators
// and creates an EMPTY internal reservation book (Option (a) per stage
// planning — the broker service will speak only OrderID at the API
// surface; hydration is a private internal seam added later).
//
// The logger defaults to slog.Default(); callers who want a scoped logger
// can wrap the returned service and swap the logger field (kept exported-
// by-package-only via SetLogger below) or pass a pre-built base logger
// through slog's global once at process init.
func NewReservationService(
	ledger AccountLedger,
	costs cost.CostModule,
	realiz RealizerHandoff,
	events EventSink,
	persist Persistence,
) *ReservationService {
	return &ReservationService{
		book:    newReservationBook(),
		ledger:  ledger,
		costs:   costs,
		realiz:  realiz,
		events:  events,
		persist: persist,
		logger:  slog.Default(),
	}
}

// SetLogger overrides the boundary logger (test-only ergonomics; the
// production wiring uses slog.Default). Kept exported so an integration
// test or a broker-side wrapper can inject a scoped logger without a
// hidden package-level global.
func (service *ReservationService) SetLogger(logger *slog.Logger) {
	service.logger = logger
}

// mustGetAccount resolves an AccountID through the ledger seam. A nil
// return is an ordering invariant violation — Gate 4 would not admit an
// order for an account that was never loaded — so it is dev-fatal (panic
// per root policy), not a returned error. Every hot-path method
// (ReserveIfAvailable / Convert / Release / ReleaseExcess) funnels
// account resolution through this helper so the invariant lives in ONE
// place.
func (service *ReservationService) mustGetAccount(accountID account.AccountID) *account.Account {
	resolvedAccount := service.ledger.Get(accountID)
	if resolvedAccount == nil {
		panic(fmt.Sprintf("reserve: account %q not loaded (ordering invariant broken)", accountID))
	}
	return resolvedAccount
}
