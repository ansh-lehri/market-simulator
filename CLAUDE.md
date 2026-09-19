# Simulator Engine — Project Constitution
<!-- Root CLAUDE.md: loaded every session. Universal rules only; component detail
     lives in each package's own CLAUDE.md. Target < 200 lines. -->

Go implementation of the market-simulator engine core: an **exchange** and a
**broker** running as two goroutine-clusters in one process (split into services
later, no rewrite). The `docs/` design docs are the spec; `Exchange_Broker_Contract.docx`
is the authority for the exchange↔broker wire shapes.

## Stack
- Go — version per `go.mod`. Logging: standard `log/slog`.
- Property tests: `pgregory.net/rapid`. Format/lint: `gofmt`, `go vet`, `golangci-lint`.
- Target infra (persistence, mostly deferred in the prototype): PostgreSQL (durable
  truth) + Redis (crash-recovery snapshot mirror).

## Commands
- Build: `go build ./...`
- Test (standing gate): `go test ./... -race`
- Lint: `go vet ./...` && `golangci-lint run`

## The two domains and the seam
- **Exchange** — sole writer of *market truth* (order book, price, trades, depth,
  session phase). Sharded by `Instrument`. Never touches money.
- **Broker** — sole writer of *account/money truth* (balances, positions,
  reservations, costs, P&L). Hosts L5, Gate 4, the stop subsystem, settlement, and
  the tape relay. Sharded by `AccountID`. Never touches market truth.
- They meet at exactly one seam. **Only values cross it — never a pointer into the
  other side's state.** Shared seam types live in `contract/`. This message-boundary
  discipline is what makes the future process split a transport swap, not a rewrite.

## Concurrency — single writer per owner
- Each unit of state has one owner goroutine: exchange owner = one `Instrument`;
  broker owner = one `AccountID`. The owner is the **sole** code path that touches
  its state.
- Everyone else does **message-in / copy-out**: send the owner a message, get a value
  copy back. Never hold or hand out a pointer into another owner's state.
- Therefore components carry **no mutexes** — single-writer is the lock.
  `go test ./... -race` must always stay green; a race is a bug in the ownership
  model, not something to lock around.
- The one deliberately shared value: global session phase, an `atomic.Int32`
  (one writer, many readers).

## Value discipline (correctness, not style)
- **All money is integer coins (`int64`); all prices integer ticks (`int64`). Never
  float** — float money/price is a bug (breaks price-keyed structures and exact
  settlement).
- Amount/quantity arguments are **strictly positive**; direction is carried by *which*
  function and *which* buckets, never by a negative argument. Signedness lives in
  balances and results, never in an argument.

## Errors vs panics
- **Invariant violation → panic** (dev-fatal): a broken coupling invariant, a hand
  failing its guard after the caller already proved the precondition, missing hot-path
  state. Programming errors — surface them, never swallow.
- **Expected/business failure → typed error** to the caller: insufficient
  funds/position, order rejected, not-found on cancel.
- **Never partially mutate:** validate fully before the first field write; on any
  error, state is untouched.

## Logging
- Structured only (`log/slog`). Log at **boundaries** — ingress, the shard/funnel
  loop, order punch, settlement, session lifecycle, the emitter, admission rejects,
  the seam.
- **Never log inside pure code** — the Tier-1 hands, the matching walk, cost math.
  They stay I/O-free so replay is deterministic; a log line there is a bug.
- Logging is never control flow, and **no test asserts on log output.** Never
  log-and-swallow an error.
- Boundary logs carry correlation ids where they apply: `SessionID`, `AccountID`,
  `OrderID`, `Instrument`.

## Persistence
- Happens at **boundaries, asynchronously** — `MarkSnapshotDirty` → Redis snapshot
  mirror; write-behind buffer → Postgres. **No synchronous store write on the hot
  path.**
- No live state is in-memory-only by default: each flow's design already carries its
  snapshot + durable form. Follow the doc; don't invent a new persistence path.

## Coding principles (specific to this codebase)
- **Hands vs brain:** Tier-1 hands are dumb single-guard mutators (move / mint / burn /
  read one thing). Tier-2 (reservation, realization, matching) decides *what* to do,
  then calls the hands in order. A hand holds no policy.
- **Decide vs execute:** the matcher decides fills; the book executes them via its
  mutators. The decider never reaches into the executed structure directly.
- **Interface-first, defer concrete:** depend on an interface, stub it, build the
  concrete later (the `SideBook` / `CircuitClamp` / `SeqSource` pattern). Never block a
  component on an unbuilt one.
- **One writer per domain:** the owner of a structure is its only writer; cross-domain
  work goes through that owner.
- **Parameter Names:** use informational context fit parameter names not alphabets etc. Goal is to convey meaning of what each parameter is at any line of code to whoever is reading
<!-- Deliberately NOT here: generic "clean code / DRY / SOLID / good names". gofmt +
     golangci-lint enforce the mechanical half; aspirational rules dilute attention. -->

## The spec is the authority
- The component's `docs/` file **is** the specification. Implement what it says;
  **do not invent behavior it doesn't state.**
- The doc defines "correct" — as a worked example or as a rule you compute the expected
  value from.
- Decisions marked **LOCKED** are not reopened. Anything crossing a defined interface
  (the seam, L5 signatures, persistence) is **verified against the doc/contract, never
  reconstructed.** If something looks wrong or missing, **stop and ask** — don't
  silently fix it.

## Testing policy
- Tests ship **in the same stage** as the code they cover, never a later pass.
- **A property test for every invariant** the doc lists; **a golden scenario test** for
  each deterministic behavior; table-driven units for pure functions. Benchmarks only
  where a real decision rides on them.
- **Assertions trace to the spec, never to the code's output.** For subtle components
  (the matcher, realization), write the golden scenarios **first** (expected values
  pinned from the doc, test red), then implement to green.
- `go test ./... -race` is a standing gate; add a determinism check (run a scenario
  twice, assert identical output) for stateful parts.

## Workflow (every session)
1. **Orientation first, no code:** read the component's doc(s) + its `CLAUDE.md`,
   summarize what it owns, propose a file layout + test plan (invariants → property
   tests, behaviors → golden scenarios, expected values worked out for review).
   **Make no changes.**
2. Implement **one stage, then stop and report.** No unrequested refactors; never reach
   into another package.
3. Human reviews the `git diff` (code *and* tests) before every commit.

## Structure
- One package per component. Seam types → `contract/`. Specs → `docs/` (not inside
  packages). Code under `exchange/` and `broker/`.