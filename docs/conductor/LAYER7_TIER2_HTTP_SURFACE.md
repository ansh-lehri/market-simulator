Layer 7 — Tier 2, Stage 3: HTTP Surface

What this doc covers. L7's external door — the HTTP endpoints the frontend calls to trigger session operations, what each carries in and returns, and the three principles that shape the surface (auth boundary, pull-based readiness, session-identity-only responses). Everything designed in the seeding and admission stages is machinery that runs behind these endpoints; this stage defines the doors.

Status. The endpoint set and semantics are locked. Two items are noted as not-closed at the end: a market-listing read endpoint (held, undecided) and identity-on-reconnect (still open — likely a defer-for-beta call).

A note on the paths below. The locked decisions are the endpoint set and their semantics (what each does, blocks vs returns-immediately, what it returns). The literal path strings shown (/session/...) are the natural REST shape of those decisions, not separately ratified naming — treat them as illustrative of the semantics, not as hard-locked strings.

Builds on.

LAYER7_TIER2_ADMISSION.md — the canonical + on-demand admit flows these endpoints trigger, and the Session states (Starting/Active) the status endpoint reads.
LAYER7_TIER2_SEEDING.md — the per-user spin-up goroutine that start-on-demand kicks off (whose progress the status poll tracks).
1. Three principles under the surface
1.1 Auth is not L7's job (LOCKED)

L7 (and the broker, and the rest) do no authentication. The frontend sends the user identity packet on every call: user email + a unique user id (+ a token in the packet if one is needed). An auth layer above L7 has already verified the person, so by the time a request reaches L7 the userID is trusted — L7 takes it as a given input, uses it as the key for EnsureAccount / the XOR check / the registry, and never validates it.

Because identity rides in the request packet, the frontend can call services directly (this is why the on-demand fund-move is a direct user→broker HTTP call, with L7 not in the path — see the admission doc §3.3). Every service trusts the same verified packet.

(In the endpoint sections below, "takes the userID packet" is implicit on every endpoint and not repeated.)

1.2 Readiness is pull-based, not pushed (LOCKED)

Canonical join is fast (the market is already up), so it blocks and returns directly. Start-on-demand is slow — it includes the sync BRING_UP wait (the 3–4 min "preparing your simulation" time) plus the formation window. An HTTP request can't be held open that long, so:

Start-on-demand returns immediately with a session id + status preparing.
The frontend polls a session-status endpoint every few seconds (~2–3s is fine over a 3–4 min wait) until the status flips to ready (or failed).

Chosen pull-based over a websocket push — a WS was judged too much work for this, and pull fits well because:

The status endpoint is a pure registry read — the session's Starting → Active progression is the status; L7 holds no push machinery, no long-lived connections.
It reuses a surface needed anyway ("what's my current session").
The one downside (a few seconds' latency after actually-ready, plus some "still preparing" traffic) is invisible on a multi-minute prep screen.

Failure surfaces naturally as a third status. The parked "sync-BRING_UP failure/timeout" item from the seeding stage resolves here: the status endpoint returns failed (+ a reason) instead of ready, and the frontend shows the error + a retry. So status has three values: preparing / ready / failed.

1.3 L7 returns session identity, never market data (LOCKED)

A successful join / ready returns session identity only — session id, participant id, status. It does not return market data (current price, the book, a chart snapshot). The live market view comes from the market-data side once the person is wired (the model-(b) handoff from admission §2.3), not from L7's response. This keeps L7 out of market data entirely, consistent with the whole design: the frontend gets "you're in, here's your session handle," then renders the market from the market-data feed.

2. The endpoints
2.1 Join canonical — blocking
POST /session/canonical
  in:  { userID packet, market }
  out: { sessionID, participantID, status: "active" }

Runs the canonical admit flow (admission §2): EnsureAccount → XOR → record + mint → wire. This is fast (the market is already up — just ensure-account + wiring), so it blocks and returns the result directly. No polling for canonical. On a rollback failure (admission §2.4), it returns an error instead.

2.2 Start on-demand — returns immediately, then polled
POST /session/ondemand
  in:  { userID packet, market }
  out (gate fail): { status: "insufficient_funds", message: "move money, then retry" }
  out (gate pass): { sessionID, status: "preparing" }

Splits by speed (admission §3):

Fast decisions run inline and return immediately: EnsureAccount → XOR → gate (ask broker "qualify?"). If the gate says NO, it returns right there with the insufficient-funds message — the user never sees a "preparing" screen for a gate fail.
If the gate says YES: seal the ephemeral account → kick off the spin-up goroutine → return immediately with sessionID + preparing.
The slow part (BRING_UP wait, formation window) runs behind the poll; the frontend watches it via the status endpoint.
2.3 Session status — the poll target
GET /session/{sessionID}/status
  out: { status: "preparing" | "ready" | "failed", reason? }

A pure registry read — reports the session's current state (Starting → preparing, Active → ready; failed + reason if spin-up failed). This is what the frontend polls after start-on-demand, and it doubles as the "what's my current session" read.

2.4 Exit — one endpoint, both modes
POST /session/{sessionID}/exit
  out: { ok }

One endpoint for both modes. L7 knows the session's mode from its record, so it does the right thing without the caller specifying:

Canonical → leave. The shared market keeps running for everyone else; the person is just removed from it.
On-demand → end / teardown. The market is theirs, so exiting tears it down.

Only the endpoint shape is defined here. The actual teardown / reap mechanics (cleaning up the ephemeral account, freeing the session, tearing down the on-demand instance) are Tier 3.

3. Surface summary
Endpoint	Blocks?	Returns	Triggers
POST /session/canonical	yes (fast)	sessionID, participantID, active	canonical admit flow
POST /session/ondemand	no (returns preparing, or gate-fail msg)	sessionID + preparing / insufficient-funds	gate → seal → spin-up goroutine
GET /session/{id}/status	n/a (read)	preparing/ready/failed(+reason)	registry read
POST /session/{id}/exit	—	ok	leave (canonical) / teardown→T3 (on-demand)
4. Decision log (Tier 2 — HTTP Surface)
#	Decision	Status
H.1	L7/broker do NO auth; frontend sends email + unique user id (+ token if needed) on every call; L7 trusts the userID, never validates	LOCKED
H.2	Async on-demand start = pull-based (not WS); returns immediately with preparing, frontend polls status until ready/failed	LOCKED
H.3	Status has 3 values preparing/ready/failed(+reason); failed absorbs the parked sync-BRING_UP failure/timeout item	LOCKED
H.4	POST join-canonical blocks and returns session id + participant id + active (fast)	LOCKED
H.5	POST start-on-demand runs ensure/XOR/gate inline; gate-fail returns the insufficient-funds message; gate-pass seals, kicks off spin-up, returns preparing	LOCKED
H.6	GET session-status = pure registry read; doubles as "what's my current session"	LOCKED
H.7	POST exit = one endpoint both modes; L7 picks leave (canonical) vs teardown (on-demand) from the session mode; mechanics → Tier 3	LOCKED
H.8	Join / ready returns session identity only (sessionID, participantID, status) — NO market data; live view comes from the market-data feed	LOCKED
H.9	Market-listing endpoint ("what canonical markets can I join")	HELD (undecided)
5. Still open
Market-listing read. Whether L7 owns a "what canonical markets can I join" endpoint is held — L7's reads so far are session-only (things in its own registry); listing what markets exist may be config or another service. Revisit.
Identity-on-reconnect. A reconnecting browser presenting a token so L7 re-finds its membership and re-binds (reconnect ≈ a lightweight re-admit). Still open — likely a defer-for-beta call. This is the one remaining loose thread in the whole admission + HTTP arc.