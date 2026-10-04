package transfer

import (
	"simulator/broker/store"
	"simulator/economy/account"
)

// The two legal direction strings on the request. Lower-case, underscore;
// unknown values 400 at the handler (not here — this file is pure).
const (
	directionToBank    = "to_bank"
	directionToTrading = "to_trading"
)

// bucketsFor maps the one request direction into the (From, To) L5 bucket
// pair the live-path command carries. This is the live half of the two-
// vocabulary seam (BROKER_PHASE3 P3-D2): the live Transfer command speaks
// buckets, so MoveCash takes exactly this pair with no translation.
//
//	to_bank    → (Available, Bank)      push: trading cash → bank
//	to_trading → (Bank, Available)      pull: bank → trading cash
//
// Returns ok=false for any other direction string; the handler maps
// ok=false to HTTP 400 before the funnel.
func bucketsFor(direction string) (fromBucket account.Bucket, toBucket account.Bucket, ok bool) {
	switch direction {
	case directionToBank:
		return account.Available, account.Bank, true
	case directionToTrading:
		return account.Bank, account.Available, true
	default:
		return 0, 0, false
	}
}

// directionFor maps the one request direction into the Store's
// TransferDirection enum — the durable half of the two-vocabulary seam.
// store.DurableTransfer takes a direction (not buckets) because it emits
// one guarded UPDATE per direction, and the SQL is asymmetric.
//
//	to_bank    → store.ToBank      (trading_cash → bank_cash)
//	to_trading → store.ToTrading   (bank_cash → trading_cash)
//
// Returns ok=false for any other direction string. The handler validates
// direction ONCE (via bucketsFor), so a false here would be a programming
// error; the ok return is retained for symmetry and defensive callers.
func directionFor(direction string) (store.TransferDirection, bool) {
	switch direction {
	case directionToBank:
		return store.ToBank, true
	case directionToTrading:
		return store.ToTrading, true
	default:
		return 0, false
	}
}
