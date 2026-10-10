// Package marketdata is the broker's public, instrument-keyed, account-free
// market-data axis — the sibling of the private settlement consumer. It
// consumes the exchange's public tape, persists 1-min OHLCV, serves chart
// history (GET /candles) and live price/candle streams (GET /ws), and
// satisfies broker/funnel's MarketPriceCache (best-ask for market-buy,
// last-price for position marks).
//
// Spec: docs/marketdata/BROKER_MARKETDATA_DESIGN.md (+ the three impl docs
// and the WS wire-protocol doc). This file ships only the Stage-1 surface
// (CandleStore + the row types + the migration). Higher stages — rollup,
// HTTP handler, MarketSnapshot, FanOut, WS transport, ingestor, holder /
// WireInstance — land stage-by-stage.
package marketdata
