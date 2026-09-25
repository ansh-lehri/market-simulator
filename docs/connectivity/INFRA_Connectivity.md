#**Config & Connectivity**

<!-- Standalone infra stage. No design ancestor to extract from (unlike the schema doc,
     which transcribes LAYER_5_PERSISTENCE.md). This IS the spec — it is written here, not
     derived. Scope is deliberately small: env-driven configuration + the two DB
     connection constructors, nothing else. -->

## 0. Scope & standing

It produces the module's configuration loader and the constructors that open **live,
verified** Postgres and Redis handles, so Stage 3 (`store`) has a `*pgxpool.Pool` and a
`*redis.Client` to run against.

**Standalone — depends on nothing in the platform or the other stages:**
- Imports only `pgx`/`pgxpool`, `go-redis`, and stdlib. **No** economy package, **no**
  `contract`, **no** `store`/`persist`/`lifecycle`, **no** compute types.
- **Not** the platform's Python `config_loader` — this Go module gets its own.
- **Module-level infra, not economy-scoped.** It lives at the module root (any package —
  exchange or economy — may use it later), not under `economy/`.

## 1. Deliverables

1. A typed **`Config`** (and its sub-structs `PostgresConfig`, `RedisConfig`).
2. A **loader** — `Load() (Config, error)` — that reads env into `Config`, applies
   defaults, and **fails fast** on any missing required var or malformed value, before any
   connection is attempted.
3. **`NewPostgres(ctx, PostgresConfig) (*pgxpool.Pool, error)`** — build the pool, **ping**
   within a timeout, return the live handle or a wrapped error.
4. **`NewRedis(ctx, RedisConfig) (*redis.Client, error)`** — build the client, **ping**
   within a timeout, return the live handle or a wrapped error.

That is the entire stage. No queries, no schema, no domain logic.

## 2. Environment variable schema (the config surface — the one decision)

**DSN-primary.** Connection details travel as a single URL per store, because that is what
makes local→cloud a config change, not a code change: every cloud knob (TLS, host, port,
credentials) lives inside the URL string.

| Var | Required | Meaning / example |
|-----|----------|-------------------|
| `DATABASE_URL` | **yes** | Full pgx DSN. `postgres://user:pass@host:5432/simdb?sslmode=disable` (local) → `...@cloud-host:5432/simdb?sslmode=require` (cloud). May also carry pgx pool params as query args (`pool_max_conns`, `pool_min_conns`, `pool_max_conn_lifetime`, …). |
| `REDIS_URL` | **yes** | go-redis URL. `redis://:pass@host:6379/0` (local) → `rediss://...` (TLS, cloud). Carries host/port/password/db. |
| `DB_PING_TIMEOUT` | no (default `5s`) | Startup ping deadline, applied to both Postgres and Redis. Fail-fast bound. |
| `PG_MAX_CONNS` / `PG_MIN_CONNS` | no | Optional explicit pool overrides **if** not carried in the DSN. Prefer DSN query params; expose these only if cleaner. |
| `REDIS_POOL_SIZE` | no | Optional Redis pool override; else the client default. |

- **Nothing hardcoded.** No host, port, credential, sslmode, or timeout literal appears in
  code — all of it is env, with only non-secret operational defaults (e.g. ping timeout)
  baked in.
- **Discrete-vars alternative (`PG_HOST/PORT/USER/...`) is deliberately NOT used** — it
  duplicates what the DSN already expresses and splits the cloud toggle across many vars.
  DSN-primary keeps one authority per store.
- Exact pool/TLS query-param support of pgx and go-redis is confirmed in orientation before
  coding — the intent (all connection state in the URL) is fixed; the precise param names
  are a library detail.

## 3. Loader contract

- `Load()` reads env once, returns a fully-populated `Config` value or an error naming the
  **first** missing/invalid required var.
- Validation is **fail-fast and total**: a missing `DATABASE_URL`/`REDIS_URL`, an
  unparseable URL, or a non-duration `DB_PING_TIMEOUT` → error, no partial `Config`.
- The loader is the **only** place env is read. Everything downstream receives values.

## 4. Constructor contract

- **Value-in.** `NewPostgres`/`NewRedis` take a `PostgresConfig`/`RedisConfig` **value** —
  they do **not** read env themselves. This keeps them testable and keeps env-reading
  isolated in the loader at the wiring root.
- **Ping on startup.** Each constructor opens the pool/client, then issues a `Ping` bounded
  by `ctx` + `DB_PING_TIMEOUT`. **An unreachable or misconfigured DB fails here**, at
  startup, not on the first real query.
- **Return the handle; caller owns lifecycle.** The constructor returns the live
  `*pgxpool.Pool` / `*redis.Client`; the wiring root holds it and is responsible for
  `Close()` on shutdown. **No package-level global handle, no singleton** — handles are
  injected, not reached for.
- **Errors are wrapped** with context (which store, what failed) so a bad DSN or a down DB
  gives an actionable message.

## 5. Cloud-anytime (a first-class requirement, not an afterthought)

Switching from local Postgres/Redis to a remote/cloud instance must be an **env change
only**, with **zero code change**:
- Postgres: change host + `sslmode=disable` → `require`/`verify-full` in `DATABASE_URL`.
- Redis: `redis://` → `rediss://` (TLS) in `REDIS_URL`.
This is a **done-criterion** (§8), verified by running the constructors against a second
`DATABASE_URL`/`REDIS_URL` with TLS enabled.

## 6. Logging & secrets

- Structured `slog` at the connect **boundary** only: "postgres connected" / "redis
  connected" with non-secret facts (host, db name, pool size).
- **Never log the DSN, URL, or password** — redact. Credentials come from env, live in no
  source file, and appear in no log line.

## 7. What this stage does NOT do

- No schema, no migrations (Stage 2).
- No queries, no snapshot/buffer logic (Stage 3 `store`).
- No `Persistence` interface, no workers (Stage 4 `persist`).
- No economy/compute/`contract` imports; no domain types.
- No global/singleton connection; no reconnect/health-loop policy (a later concern if
  needed — startup ping is the Phase-2 bar).

## 8. Definition of done

- `Load()` returns a valid `Config` from a correct env; missing/invalid required vars fail
  fast with a clear message; defaults apply.
- `NewPostgres`/`NewRedis` return live handles that ping successfully against the local
  instances; a bad DSN or down instance returns a wrapped error at construct time.
- Constructors take config **values**; env is read only in `Load()`; no hardcoded
  connection literals anywhere; no secret is logged.
- **Cloud check:** the same binary connects to a TLS/remote Postgres and Redis with only
  `DATABASE_URL`/`REDIS_URL` changed — no recompile of connection logic.

## 9. Testing

- **Loader — unit tests** (no DB): set env → assert `Config`; unset a required var → assert
  fail-fast error; assert defaults (ping timeout) applied; malformed URL → error.
- **Constructors — light integration** against the local Postgres/Redis: ping succeeds and a
  handle is returned; a deliberately bad DSN → wrapped error (no hang — the ping timeout
  bounds it). No schema is needed to ping.
- `go test ./... -race` stays green (standing gate); nothing here is concurrent.

## 10. Open decisions to confirm at orientation

1. **Package shape:** one `config` package holding `Config` + `Load` + both constructors, or
   split `config` (env→`Config`, no driver imports) from a `db`/`conn` package (constructors,
   imports drivers)? Recommendation: **split** — keeps `config` driver-free and the
   constructors in one small infra package.
2. **Module-root path** for these package(s) (e.g. `config/` and `db/`), confirmed as
   module-level infra, not under `economy/`.
3. Exact pgx/go-redis URL param support for pool/TLS tuning (library detail, confirmed before
   coding).