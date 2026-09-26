// Package capstone is the wiring root for the economy money engine — it
// composes the concrete persistence.Component, cost module, ledger, and
// the reserve / realize / lifecycle services against the real Postgres +
// Redis handles opened by connectivity/db.
//
// It is deliberately NOT a cmd/ main. The broker funnel that owns the
// per-session-owner goroutine does not exist yet; without an owner
// goroutine there is nothing to run standalone. Once the funnel lands, a
// real cmd/broker main will assemble the same pieces via a Wire()-shaped
// call.
//
// Stage 6 uses capstone as (a) the wiring seat where the Phase-1
// no-op/spy Persistence gets swapped for the real component, and (b) the
// harness under which the end-to-end capstone integration test drives
// the full money loop against the live l5_test database.
package capstone
