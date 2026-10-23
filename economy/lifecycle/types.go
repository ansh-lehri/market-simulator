package lifecycle

import (
	"simulator/economy/account"
)

// SessionType picks between the two session flavours at load time. The
// only storage-level difference between them is the boolean PersistOnClose
// on the resulting Account (LAYER_5_2_LIFECYCLE_FLOWS §0):
//   - Canonical → PersistOnClose=true  (session_save writes back to Postgres)
//   - OnDemand  → PersistOnClose=false (session_discard drops it entirely)
type SessionType int

const (
	Canonical SessionType = iota
	OnDemand
)

func (sessionType SessionType) String() string {
	switch sessionType {
	case Canonical:
		return "canonical"
	case OnDemand:
		return "ondemand"
	default:
		return "unknown"
	}
}

// OwnerType matches the accounts.owner_type column values (schema §2.1:
// TEXT NOT NULL, CHECK'd downstream to {'agent','human'}). Used by
// open_account (item 18) to write the durable identity row.
type OwnerType int

const (
	OwnerAgent OwnerType = iota
	OwnerHuman
)

// String returns the on-wire / on-DB column value ("agent" / "human"). The
// accounts.owner_type column stores this exact text — never the enum ordinal.
func (ownerType OwnerType) String() string {
	switch ownerType {
	case OwnerAgent:
		return "agent"
	case OwnerHuman:
		return "human"
	default:
		return "unknown"
	}
}

// isValid reports whether the enum value is one of the two legal
// constants (rejects OwnerType(2), OwnerType(-1), etc). Used by
// open_account's input validation.
func (ownerType OwnerType) isValid() bool {
	return ownerType == OwnerAgent || ownerType == OwnerHuman
}

// ParseOwnerType maps the wire/column string ("agent" / "human") back to
// the typed enum — the inverse of String(). Any other value, including
// "", is ErrBadOwnerType; a caller that wants a default for an omitted
// field resolves it before calling this, not here, so this stays a
// strict parse with no implicit fallback.
func ParseOwnerType(wireValue string) (OwnerType, error) {
	switch wireValue {
	case "agent":
		return OwnerAgent, nil
	case "human":
		return OwnerHuman, nil
	default:
		return OwnerAgent, ErrBadOwnerType
	}
}

// LoadedAccount is what session_load / ondemand_session_load return. It
// bundles the freshly hydrated *account.Account with the durable identity
// fields (OwnerType, OwnerID) that session_save needs at teardown.
//
// Why bundled: DurableAccount (persistence's DTO) requires OwnerType /
// OwnerID on write (they're NOT NULL, immutable identity), but
// account.Account is the live money-state model and deliberately carries
// no identity beyond the account_id. The doc's session_save(a *Account)
// signature has no other channel to carry them, and adding identity fields
// to account.Account would violate its "money-state only" scope. So we
// carry them here — the caller (broker / session mgr) holds the whole
// LoadedAccount for the session's life and hands it back at close.
//
// crash_recovery_reload returns a RestoredSession (distinct type in F1)
// that includes the raw ReservationSnaps for reserve/funnel to hydrate.
type LoadedAccount struct {
	Account   *account.Account
	OwnerType OwnerType
	OwnerID   string
}
