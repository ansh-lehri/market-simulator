// Command server is a DRAFT single-process composition root.
//
// It is NOT wired into any build target, NOT tested, and NOT reviewed
// against the project's usual bar (golden scenarios, property tests,
// `go vet`/`golangci-lint`). Treat every line below as a sketch to
// discuss, not production code — per CLAUDE.md's workflow, a real
// version of this file would need its own orientation + test plan
// before being treated as "done."
//
// Scope, deliberately: bring up the broker (broker/bringup) and the
// conductor (conductor) in one process, on one HTTP mux, with a clean
// shutdown order. Corrected note (the original draft's comment here was
// stale): broker/order and broker/transfer are NOT separately mounted
// by this file because bringup.BringUp already mounts them
// unconditionally, inside itself, since commit dd66a73 — there is no
// "leave them unmounted" option with the current bringup API. See
// docs/api-contract-v2/cross-cutting.md §7 for the full trace.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"simulator/broker/bringup"
	"simulator/conductor"
	"simulator/connectivity/config"
	"simulator/connectivity/db"
)

func main() {
	// Stage 0 — process-level setup. ctx is cancelled on SIGINT/SIGTERM;
	// everything below either respects ctx directly or respects the
	// derived shutdownCtx built from it at the bottom of this function.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// main's own operational knobs. Nothing upstream reads these from
	// env (confirmed: os.Getenv appears nowhere outside
	// connectivity/config/load.go) — BrokerConfig and conductor.Config
	// are both plain Go struct literals that something has to fill in.
	// Hardcoded here for the draft; a real version would likely read
	// these from flags or main's own small env block, kept separate
	// from connectivity's DATABASE_URL/REDIS_URL concern.
	const (
		httpAddr          = ":8000"
		marketCfgDir      = "./configs/markets" // needs at least one <tag>.json before anything can trade
		conductorPoolSize = int32(4)            // separate from the broker Store's own pool sizing

		// defaultAgentServerURL is the fallback when AGENT_SERVER_URL is
		// unset — matches the simulation-agents process's own default
		// local dev listen address. AGENT_SERVER_URL is the ONE place
		// this address is configured in this whole process; everything
		// downstream (conductor.NewHTTPAgentOrchestrator, both spin-up
		// flows) receives it as an already-constructed dependency and
		// never re-reads the env var or hardcodes the address a second
		// time.
		defaultAgentServerURL = "http://localhost:8090"
	)
	agentServerURL := os.Getenv("AGENT_SERVER_URL")
	if agentServerURL == "" {
		agentServerURL = defaultAgentServerURL
	}
	brokerCfg := bringup.BrokerConfig{
		BrokerID:        "B-1",
		ShardCount:      4,
		InboxDepth:      64,
		Country:         "IN",
		StartingCapital: 10_00_000, // 10 lakh coins, illustrative
		StartingBank:    0,
		OnDemandMinimum: 0,
		ReadTimeout:     2 * time.Second,
		// CandleReadTimeout / WS* left zero → bringup's own defaults.
	}

	// Stage 1 — bring up the broker. This is fully self-contained: it
	// reads DATABASE_URL / REDIS_URL from env itself (via
	// connectivity/config.Load, called inside BringUp), opens every
	// pool, builds every shard, builds the market-data holder, and
	// starts it. rt.Mux comes back pre-populated with all 14
	// broker-owned routes: the 9 broker/reads routes, the 3 broker/order
	// routes, broker/transfer's route, and GET /v1/candles — bringup
	// mounts all of these unconditionally, inside BringUp itself.
	runtime, err := bringup.BringUp(ctx, brokerCfg)
	if err != nil {
		slog.Error("server: bringup.BringUp failed", "err", err)
		return
	}
	// Shutdown is idempotent and safe to defer even if later stages
	// fail — see Stage 5 for the full, ordered teardown this defer
	// chain builds toward.
	defer runtime.Shutdown()

	// Stage 2 — open the conductor's OWN Postgres pool.
	//
	// WHY this exists at all: conductor.Config.OHLCVPool is explicitly
	// documented (conductor/conductor.go's own comment on the field) as
	// a SEPARATE pool from the broker's Store pool — "so chart-history
	// reads at market-open don't compete with session-load traffic on
	// the broker's pool." It is not optional and not satisfiable by
	// reusing runtime.Store's pool; conductor.NewConductor requires its
	// own *pgxpool.Pool value in this field (nil → construction error).
	//
	// WHY main has to build it, not reuse something from Stage 1:
	// bringup.BringUp loaded a connectivity.Config (the DSN) internally
	// to build its OWN pools, but Runtime has no field exposing that
	// loaded config back out — there is no "give me the DSN you already
	// parsed" handle on *bringup.Runtime. So the only way for main to
	// get a DSN to build this second pool is to ask connectivity/config
	// for it directly, itself:
	infraCfg, err := config.Load()
	if err != nil {
		slog.Error("server: config.Load failed", "err", err)
		return
	}
	// This is the SAME env vars BringUp already read a moment ago
	// (DATABASE_URL / REDIS_URL) — config.Load() is pure parsing with
	// no connection side effect, so reading it twice is redundant but
	// harmless, not a bug. The two resulting *pgxpool.Pool objects
	// (runtime.Store's, and this one) are legitimately two separate
	// pools by design, both pointed at the same physical database, each
	// sized independently — that duplication is intentional, only the
	// env *read* is incidentally doubled.
	//
	// db.NewPostgres is the one function connectivity/db exposes for
	// turning a PostgresConfig into a live, ping-tested pool — the same
	// function bringup.BringUp uses internally for its own pools.
	ohlcvPool, err := db.NewPostgres(ctx, config.PostgresConfig{
		DSN:         infraCfg.Postgres.DSN,
		MaxConns:    conductorPoolSize,
		PingTimeout: infraCfg.Postgres.PingTimeout,
	})
	if err != nil {
		slog.Error("server: conductor OHLCV pool failed", "err", err)
		return
	}
	// main opened this pool itself, so main — not any existing
	// component — is responsible for closing it. Nothing else owns it.
	defer ohlcvPool.Close()

	// Stage 3 — build and start the conductor ON TOP OF the already-live
	// broker. Confirmed by direct source read: conductor never imports
	// broker/bringup and NewConductor requires a non-nil Broker/Market
	// in its Config (errors at construction otherwise) — it has no path
	// to build a broker itself. *funnel.Broker and *marketdata.MarketData
	// already satisfy conductor's BrokerPort / MarketDataPort interfaces
	// structurally (see the compile-time assertions in
	// conductor/ports.go), so runtime.Broker / runtime.Market pass
	// straight through with no adapter.
	//
	// Exchange left nil deliberately — defaults to the real
	// wire.NewExchange factory. Exchange instances are constructed
	// dynamically, per market, by conductor's own canonical/on-demand
	// spin-up flows — never by main directly (this is why
	// exchange/example/main.go is not a template for this file).
	//
	// Agents is the real, HTTP-backed orchestrator client
	// (conductor.NewHTTPAgentOrchestrator), pointed at agentServerURL —
	// see that const's comment above for why this is the one place the
	// address is configured. brokerCfg.BrokerID/Country are reused
	// verbatim rather than re-declared, so the two configs can never
	// silently drift apart.
	//
	// Liquidity is left nil — still the randomized-outcome stub
	// (LiquidityOperator has no real implementation yet; separate,
	// unbuilt market-infra concern, not addressed by this change).
	cond, err := conductor.NewConductor(conductor.Config{
		CfgDir:    marketCfgDir,
		OHLCVPool: ohlcvPool,
		Broker:    runtime.Broker,
		Market:    runtime.Market,
		Agents:    conductor.NewHTTPAgentOrchestrator(agentServerURL, brokerCfg.BrokerID, brokerCfg.Country, nil),
	})
	if err != nil {
		slog.Error("server: conductor.NewConductor failed", "err", err)
		return
	}
	// Start spawns one per-market timer goroutine per <tag>.json found
	// under CfgDir. Zero configured markets is legal — the process
	// boots and serves, just has nothing to join yet.
	if err := cond.Start(); err != nil {
		slog.Error("server: conductor.Start failed", "err", err)
		return
	}
	defer cond.Stop()

	// MountRoutes adds all 10 conductor routes (including the liveness
	// probe and GET /v1/ws) onto the SAME mux Stage 1 already populated
	// with the 14 broker-owned routes. runtime.Mux now has 24 distinct
	// paths registered on it, with no overlap between the two sets.
	cond.MountRoutes(runtime.Mux)

	// Stage 4 — serve.
	httpServer := &http.Server{
		Addr:    httpAddr,
		Handler: runtime.Mux,
	}
	serveErrCh := make(chan error, 1)
	go func() {
		slog.Info("server: listening", "addr", httpAddr)
		serveErrCh <- httpServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		slog.Info("server: shutdown signal received")
	case serveErr := <-serveErrCh:
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			slog.Error("server: ListenAndServe failed", "err", serveErr)
		}
	}

	// Stage 5 — shutdown, strict reverse order. The three deferred
	// calls above (cond.Stop, runtime.Shutdown, ohlcvPool.Close) fire
	// in LIFO order as main returns — i.e. exactly: httpServer.Shutdown
	// here, then cond.Stop(), then runtime.Shutdown(), then
	// ohlcvPool.Close(). That ordering is deliberate: stop accepting
	// new HTTP work first, then stop conductor's background timers
	// (which may themselves call into the broker), then stop the
	// broker's shard owners (which must exit before their DB/Redis
	// handles close), then finally close the one pool main itself
	// opened.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelShutdown()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("server: httpServer.Shutdown failed", "err", err)
	}
}
