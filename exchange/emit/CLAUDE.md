# emit — event emitter, sequence & OHLCV (1.9)

## Spec
`docs/exchange/EXCHANGE_00` + `EXCHANGE_19` (this component).
This component is the exchange's whole outbound voice — it implements the sequence
everyone stubs and shapes every other component's output into wire events. Read the
docs whose records it consumes so the mappings are faithful:
- `docs/exchange/EXCHANGE_12_MATCHING_CORE.md` and
  `docs/exchange/EXCHANGE_13_AUCTION_MATCHER.md` — both stamp `Trade.Seq` via
  `SeqSource.Next()`; this component is the REAL `SeqSource` (replaces their stubs).
- `EXCHANGE_11` (`QuoteState`), `EXCHANGE_14` (`EngineResult` — the private stream
  reuses it), `EXCHANGE_13` (`AuctionResult`), `EXCHANGE_18` (phase transitions →
  session events). It turns each neutral record into a tagged wire event.

## What this package owns
The exchange's outbound voice: one `Emitter` per instrument owning the per-instrument
event `Seq`, the two shared channel handles, and the `Emit*` methods; the real
`SeqSource` (`Next()`); the `OHLCVAggregator` (continuous 1-minute candles); and the
public tape types (`TapeEvent`/`TapeKind`/`PublicTrade`/`DepthSnapshot`/`Candle`/
`SessionEvent`). It is the only component that writes the outbound channels.

## Invariants (EXCHANGE_19 §8)
1. Public events carry no participant identity — `PublicTrade` has price/qty only.
2. Private events always echo `OrderID` — via `EngineResult`.
3. Every event carries a per-instrument `Seq`; per-instrument event order equals
   emission order.
4. `Trade.Seq` is stamped once, at trade creation, and reused on the public print —
   trade order and tape order agree.
5. OHLCV is continuous — a candle every interval per instrument, flat at last price
   when no trades.
6. Quote/depth emitted only on actual change — no no-op snapshots.
7. The private stream reuses `EngineResult` — no parallel private-event type.
8. The emitter is the only component that writes the outbound channels — every other
   component returns neutral records.

## Deps
- `contract` — `QuoteState`, `Trade`, `EngineResult`, value types.
- `match` / `auction` — the `SeqSource` interface this implements (the real one).
- `auction` — `AuctionResult` (mapped into `EngineResult` for the private stream).

## Do NOT
- Re-stamp `Trade.Seq` in `EmitTrades` — reuse the `Seq` set at trade creation.
- Put any participant identity on a public event.
- Emit a quote/depth snapshot that didn't change.
- Introduce a parallel private-event type — reuse `EngineResult`.

## Testing (table + OHLCV golden-ish)
OHLCV folding (open/high/low/close/volume, flat candle on an empty bucket, roll
resets) — golden-style; `Next()` monotonic and per-instrument; `EmitTrades` reuses
`Trade.Seq`; a public print carries no participant id. Emit to buffered test channels
and assert exactly what lands. Mostly table.