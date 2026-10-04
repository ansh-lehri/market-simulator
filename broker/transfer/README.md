# broker/transfer — Wiring Handoff for Phase 4 (Tier-4 HTTP Mount)

Phase 3 shipped the `POST /v1/account/transfer` handler, its ports, its
timeout dial, and the full status-table test coverage. It did **not**
wire the handler into a running HTTP server. That mount step was
deliberately carved out of Phase 3 (orientation flag) because it lives on
the Tier-4 HTTP axis, which also owns the balance / positions / orders /
trades / P&L / costs endpoints — one mux and one `*http.Server`
lifecycle serves them all.

This README is the handoff: everything the next phase needs to mount the
handler without re-reading Phase 3.

## What is already in place

- `broker/transfer.NewHandler(broker, durableStore, cfg) http.Handler` —
  the one factory. Satisfies `http.Handler`; register under
  `broker/transfer.Path` (`"/v1/account/transfer"`).
- `broker/transfer.Config{TransferTimeout time.Duration}` — the handler's
  one dial. `Validate()` rejects zero / negative at construction.
- `broker/transfer.ErrBadConfig` — category sentinel for `Validate`
  failures.
- Nil-safe construction: `NewHandler` panics on nil broker, nil store, or
  invalid `cfg`. These are bringup-time programming errors, not runtime
  conditions.
- Panic recovery inside the handler: an un-caught panic in a request
  goroutine logs a stack at ERROR and writes a 500. The shard loop runs
  on its own goroutine and is unaffected.

## What Phase 4 (or whoever owns the mount) must do

1. **Add `TransferTimeout` to `bringup.BrokerConfig`.** Validate it
   `> 0` alongside the other dials. Thread it through `bringUp` into a
   `transfer.Config` at the mount point.

2. **Add an `*http.Server` to `bringup.Runtime`.** One server, one mux,
   covering transfer today and the Tier-4 read endpoints next.
   Suggested surface:

   ```go
   type Runtime struct {
       Broker *funnel.Broker
       Store  *store.Store
       HTTP   *http.Server   // new

       bundles      []*capstone.Bundle
       shutdownOnce sync.Once
   }
   ```

3. **Register the transfer handler at `transfer.Path`.** Inside `bringUp`,
   after `broker.Start()`:

   ```go
   mux := http.NewServeMux()
   mux.Handle(transfer.Path, transfer.NewHandler(brokerInstance, brokerStore, transfer.Config{
       TransferTimeout: brokerCfg.TransferTimeout,
   }))

   server := &http.Server{
       Addr:    brokerCfg.HTTPAddr,       // new BrokerConfig field
       Handler: mux,
       // ReadHeaderTimeout, ReadTimeout, etc. are server-level dials;
       // TransferTimeout is per-request and lives on the handler.
   }
   go func() {
       if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
           slog.Error("bringup: HTTP server exited", "err", err)
       }
   }()
   ```

4. **Update `bringup.Shutdown` ordering.** The HTTP server must stop
   **first** so no new request can begin; then the broker; then the
   store. In-flight requests drain under their own ctx deadlines.

   ```go
   func (runtime *Runtime) Shutdown() {
       runtime.shutdownOnce.Do(func() {
           // 1. Stop accepting new requests; let in-flight finish under
           //    their own TransferTimeout.
           shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
           defer cancel()
           if err := runtime.HTTP.Shutdown(shutdownCtx); err != nil {
               slog.Warn("bringup: HTTP shutdown error", "err", err)
           }

           // 2. Broker.Stop — owners drain inboxes.
           runtime.Broker.Stop()

           // 3. Per-shard bundles (LIFO) — existing code.
           for i := len(runtime.bundles) - 1; i >= 0; i-- {
               runtime.bundles[i].Close()
           }

           // 4. Store (its own close method handles redis then pool).
           runtime.Store.Close()
       })
   }
   ```

   The ordering is load-bearing: a late request landing after `Broker.Stop`
   would route onto a closed inbox and panic on `send-to-closed`.

5. **Add `HTTPAddr` (or similar) to `BrokerConfig`.** Validate non-empty
   if the HTTP axis is intended to run. For tests, `":0"` is the usual
   pattern — the server binds to a random port and `server.Addr` reports it.

## Contract the handler expects at mount time

- `broker` argument satisfies `brokerPort` — one method,
  `Transfer(ctx, aid, from, to, amount) (TransferResult, error)`.
  `*funnel.Broker` already satisfies it; no interface declaration
  change on the funnel side is needed.
- `durableStore` argument satisfies `storePort` — one method,
  `DurableTransfer(ctx, aid, dir, amount) (available, bank, err)`.
  `*store.Store` already satisfies it.
- `cfg.TransferTimeout > 0`.

The handler is otherwise self-contained: no middleware required, no
request-id injection assumed, no auth layer (per the Tier-4 convention —
`account_id` is in the body, not an auth principal).

## Where the Tier-4 read endpoints will plug in later

The same mux will host read endpoints (`/v1/account/balance`, `/v1/account/positions`,
`/v1/account/trades`, `/v1/account/pnl`, `/v1/account/costs`). Each
follows the same shape as `broker/transfer`: a `NewHandler` factory +
its own `readTimeout` dial, mounted under its route. The Phase-3 handler
sets the pattern; Phase 4+ should keep the shape so adding each read is
a one-file addition.

## Tests to add at the mount phase

- Bringup smoke test: `BringUp` returns a Runtime with a running HTTP
  server, a `POST /v1/account/transfer` with a valid body returns 200
  end-to-end, `Shutdown` closes the server cleanly.
- Shutdown ordering: trigger a Transfer in-flight (artificial delay via
  a long request), call `Shutdown`, assert the in-flight request
  completes (not aborted mid-write) and subsequent requests get a dial
  error (server closed).

The existing `broker/transfer/http_test.go` live-runtime cases already
cover the handler-level end-to-end; they just use `httptest.NewServer`
instead of the bringup HTTP server. Promoting them to the bringup
HTTP server at Phase 4 is one swap.

## Deferred Phase-3 items (not required for Phase 4, but tracked)

- **`unit` field vocabulary.** The request body carries `unit`; the
  handler parses it and ignores the value. The Tier-4 unit convention
  will pin the vocabulary; the handler already accepts the field so no
  wire break is needed at that point.
- **F5 "mid-position allowed" funnel test.** Deferred until
  `PlaceOrder`/`reserve` are wired through the funnel's handler bodies —
  without that, there is no honest path to `Reserved > 0` in a test. The
  structural claim ("`MoveCash` never writes `Reserved`") is already
  covered one layer down in `economy/account/cash_test.go`.
