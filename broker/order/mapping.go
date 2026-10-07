package order

import (
	"simulator/contract"
)

// The wire-level enum strings. Lower-case, no hyphens; the Phase-5 doc
// §4.2 pins these exactly. Unknown values at parse time → the handler
// writes 400 before touching the funnel.
//
// Also exposed as public map keys where useful (sideFor etc.) — the
// per-endpoint handlers only call the parsers below, which hide the
// map, but the constants themselves are the single source of truth
// for the wire spelling.
const (
	wireSideBuy  = "buy"
	wireSideSell = "sell"

	wireTypeLimit  = "limit"
	wireTypeMarket = "market"

	wireTIFGTC = "gtc"
	wireTIFIOC = "ioc"
	wireTIFFOK = "fok"
)

// parseSide maps the wire-level side string to contract.Side.
//
// Returns (zero, false) on an unknown value so the handler can map
// ok=false to a 400 with the specific "side must be buy or sell"
// message. Case-sensitive on purpose: the spec pins lower-case and a
// forgiving match would hide client bugs where the caller is sending
// capitalised strings by mistake.
func parseSide(wire string) (contract.Side, bool) {
	switch wire {
	case wireSideBuy:
		return contract.Buy, true
	case wireSideSell:
		return contract.Sell, true
	default:
		return 0, false
	}
}

// parseType maps the wire-level order-type string to contract.OrdType.
// Only "limit" and "market" are legal on the place endpoint (per
// Phase-5 §4.2); the modify endpoint uses an is_market bool instead
// (§9.1) and so does not call this.
func parseType(wire string) (contract.OrdType, bool) {
	switch wire {
	case wireTypeLimit:
		return contract.Limit, true
	case wireTypeMarket:
		return contract.Market, true
	default:
		return 0, false
	}
}

// parseTIF maps the wire-level TIF string to contract.TIF. All three
// values (gtc / ioc / fok) are accepted on the wire even though FOK is
// not yet implemented downstream (contract §3.2 note); the exchange is
// where that gets rejected, not here.
func parseTIF(wire string) (contract.TIF, bool) {
	switch wire {
	case wireTIFGTC:
		return contract.GTC, true
	case wireTIFIOC:
		return contract.IOC, true
	case wireTIFFOK:
		return contract.FOK, true
	default:
		return 0, false
	}
}

// tifForModify derives contract.TIF from the modify endpoint's
// is_market boolean (BROKER_READ_SURFACES §9.1): true → IOC (market
// never rests), false → GTC (limit rests). Matches tifFor in
// broker/funnel/place.go for the Type→TIF default on standalone place —
// modify's wire shape just encodes the Type decision as a bool instead
// of a string.
//
// Pure: no I/O, no state. Called inline from the modify handler when
// building the replacement OrderSpec.
func tifForModify(isMarket bool) contract.TIF {
	if isMarket {
		return contract.IOC
	}
	return contract.GTC
}

// typeForModify is the sibling of tifForModify for the Type field:
// true → Market, false → Limit. Together they translate the modify
// endpoint's is_market bool into the (Type, TIF) pair the OrderSpec
// carries.
func typeForModify(isMarket bool) contract.OrdType {
	if isMarket {
		return contract.Market
	}
	return contract.Limit
}
