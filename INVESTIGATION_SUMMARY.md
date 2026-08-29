# The `io::poll::poll` / `io::poll::pollable::ready` replay trap — full investigation summary

**Status: RESOLVED (two structural fixes).** Root cause #1 (Eighth capture — `pollable_seq`
counter scope mismatch across snapshot boundaries) confirmed and fixed; full detail in
`ROUND_TWELVE_THIRTEEN_FIX.md`. Root cause #2 (Finding B below — a batched `poll()` call's replay
crashing on a stray same-batch `ready()` entry recorded out of the guest's replayed structural
check order) confirmed via the Tenth capture and fixed; full detail in
`FINDING_B_FIX_DESIGN.md` (root cause, fix design, and implementation notes) and
`ROUND_FIFTEEN_FINDINGS.md`/`ROUND_FOURTEEN_FINDINGS.md` (the synthetic-reproduction rounds that
preceded it). Both fixes are empirically falsified (reverting each reproduces its exact trigger
condition; restoring passes) and regression-tested. This document is left as-is below (the
narrative up to and including the open findings that led to the Eighth capture, plus Finding B's
own section further down) as the historical record of how the investigation got there; read the
two fix docs above first for the resolutions themselves. This document consolidates everything
known through Round Eleven — read it for the chronology and the falsified hypotheses if picking
the work up cold, then read the fix docs for what actually happened next.

## The symptom

A `WorkerAgent` (video-harness's Golem agent that runs an LLM tool-calling loop, dispatching to
`WorkflowAgent`/`SandboxAgent` via cross-agent RPC) periodically enters permanent `Failed` status
after being idle for minutes, with:

```
error: Component trapped: Unexpected oplog entry during replay:
       expected io::poll::poll, got io::poll::pollable::ready
```

The trap is **deterministic** — re-invoking the same `Failed` worker re-traps identically every
time, at the same `retry from:` index, forever. It is not a transient race from the caller's
perspective; whatever went wrong was baked into the recorded oplog the first time it happened.

## What's proven true (high confidence, code-read and/or empirically confirmed)

1. **Not caused by stale/rebuilt app code.** Multiple captures (the "Fourth" and "Fifth" oplogs,
   see below) come from a container where component revision 0 is the *only* revision ever
   deployed — the exact binary that recorded the history later fails to replay it. Rules out "old
   binary replaying new code's oplog" as an explanation.
2. **Not caused by concurrent/overlapping `atomicRpcCall()` regions.** The video-harness
   app-level fix (commit `bdd966f`, "serialize suspends starts") is confirmed working in every
   captured oplog since it shipped: `BEGIN ATOMIC REGION`/`END ATOMIC REGION` pairs are always
   strictly sequential, never overlapping. This *did* fix a real, different bug (see "Fixed
   sub-bugs" below) but the trap under investigation here persists after that fix.
3. **A single, solitary, already-cleanly-closed atomic region is sufficient to reproduce it** —
   the "Fifth oplog" has exactly one `atomically()`-wrapped RPC call in the entire history, no
   sibling regions anywhere nearby, and still traps.
4. **The exact mechanism is a `ready()`→`poll()`→`ready()` guest-side polling loop, not
   `poll()` alone.** Confirmed via live-instrumented capture ("Sixth capture"): the guest's real
   polling pattern for an RPC-completion future is an optimistic non-blocking `ready()` check
   first (usually `false`), then a blocking `poll()`, then a **second** `ready()` call to confirm
   before reading the result — not the single bare `poll()` call `pollable.block()` performs
   (that distinction was actually discovered mid-investigation and is why several early synthetic
   reproduction attempts, using `.block()`, failed to reproduce anything).
5. **The break is specifically the SECOND `ready()` call.** Both live and replay agree on the
   first `ready()` (false, correctly never persisted — see "Fixed sub-bugs" #1) and on `poll()`
   (correctly consumes the recorded `io::poll::poll` entry). The second `ready()` call is where
   replay diverges: it reports "no match, synthesizing false" instead of matching the recorded
   `true` entry, leaving the oplog cursor un-advanced; the *next* `poll()` call then unconditionally
   consumes that un-consumed entry and traps on the type mismatch.
6. **`pollable_seq` identity resolution is provably correct at the point of divergence** (Seventh
   capture, `d15590420` instrumentation): the second `ready()` call resolves the *same* `seq` for
   the *same* `rep` as the first call (`is_new: false`), on both live and replay independently.
   Whatever's wrong, it is not a naive "different pollable identity" mixup at this observation
   point — though see "Open questions" below for a subtler version of this that isn't yet ruled
   out.
7. **In-process worker eviction/reload is code-identical to a fresh process restart** for the
   specific state instrumented here. Traced `RunningWorker::create_instance` →
   `DurableWorkerCtx::create()` → `PrivateDurableWorkerState::new()`
   (`golem-worker-executor/src/durable_host/mod.rs:287,457`): this path runs unconditionally,
   every time, whether the worker is loading for the first time, reloading after LRU
   eviction-under-memory-pressure, or reloading after a full process restart — always
   constructing a genuinely fresh `pollable_seq`/`next_pollable_seq`/`active_atomic_regions`/
   `current_retry_point`. Nothing is cached or reused across an eviction boundary. Ruled out as
   the divergent mechanism (Round Eleven).

## Fixed sub-bugs along the way (all already merged/shipped, all still correct)

These are documented in full in `GOLEM_IO_POLL_BUG.md` in the main golem checkout (untracked
there, so not present in worktrees — read it directly at
`/Users/hannes/work/golem/GOLEM_IO_POLL_BUG.md` if you have access to that checkout). Summary,
oldest to newest:

1. **Skip-`false` optimization**: `ready() == false` is never persisted to the oplog (informationally
   redundant — replay synthesizes it whenever the next real entry isn't an `IoPollReady`). This
   is *load-bearing* for the whole investigation: it's *why* the guest's real
   `ready()`→`poll()`→`ready()` pattern only ever leaves ONE `io::poll::pollable::ready` entry per
   round-trip in the oplog (the confirming `true`), not two — which is what made the pattern easy
   to misread as `poll()`-then-`ready()` from the raw oplog dump alone, delaying its discovery.
2. **Rep-tagged matching, then found unreliable across a restore** (`5b61472ff`): wasmtime's raw
   resource-table `rep` isn't stable across a snapshot-based restore or a rebuild, so tagging
   `IoPollReady` entries by raw rep let a wrong pollable steal another's entry.
3. **Seq-based matching** (`f89c8b85d`, the fix currently in place): replaced raw `rep` with a
   logical, call-order-derived `pollable_seq` (`PrivateDurableWorkerState::pollable_seq()`,
   `durable_host/mod.rs`) — assigned the first time a given `rep` is observed by `ready()` in the
   current process lifetime, live or replay alike, cleared on `drop()` so a reused `rep` gets a
   fresh seq. Six wasmtime-instance-level verification rounds passed against concurrent-pollable
   and adversarial-completion-order hypotheses. **This fix is real, tested, and working** — see
   fact 6 above — it just isn't sufficient to prevent the trap under investigation here.
4. **`WriteRemoteBatched` replay cursor stall after snapshot restore** (`d604e6e0a`): unrelated
   HTTP-batch replay-cursor bug, fixed separately, not implicated in this trap.

The cherry-pick list actually deployed to the fat-image release binary (video-harness
`release/release.sh`, `GOLEM_RELEASE_CHERRY_PICKS`) includes all of the above as of this
investigation.

## Chronology of captures

Full detail in `/Users/hannes/work/golem/oplog-backups/README.md` (in the golem fork's main
checkout — not this worktree). Short version, in the order they were captured (not necessarily
the order presented in that file):

| # | What | Key fact |
|---|---|---|
| 3rd oplog | Correctly round-decomposed `run()` | Confirmed NOT the same signature as later captures — a red herring, ruled out early |
| 4th oplog | `scene_plates`, post-serialize-starts-fix | First capture proving the trap survives the concurrent-region fix; 4 sequential regions |
| 5th oplog | `character_sheet`, single region | Sharpest early repro: ONE atomic region, no siblings, still traps |
| 6th capture | LIVE-instrumented (branch `3a1c0dc97`) | First direct observation of the ready→poll→ready mechanism and exactly where it breaks |
| 7th capture | + `DurableFunctionType` tag (`d15590420`) + `clear_pollable_seq` logging | `pollable_seq` resolution confirmed correct at divergence; two new open findings (below) — captured against a busy multi-worker process, later found to lack per-worker attribution |

## Synthetic reproduction attempts — all negative (this session, "Round 8" through "Round 11")

Every attempt below is a real wasmtime-instance-level test (not a description) in
`golem-worker-executor/tests/{durability,rpc}.rs` and companion test-component code, on branch
`hrapp/scene-plates-sequential-poll-trap`. All pass — meaning none of them reproduce the trap,
despite deliberately matching production's confirmed shape ever more closely each round:

| Round | File(s) | What it tested | Result |
|---|---|---|---|
| 8 | `ROUND_EIGHT_FINDINGS.md` | Generic `atomically()` region (plain HTTP) + suspend + cold restart, no snapshot | PASS (no trap) |
| 9 | `ROUND_NINE_FINDINGS.md` | Real cross-agent RPC (generated proxy) instead of plain HTTP; single-invocation vs split-invocation shape; exact production region count (4) | PASS on all 3 sub-variants |
| 10 | `ROUND_TEN_FINDINGS.md` | The *exact* `ready()`→`poll()`→`ready()` shape (round 8/9 had used `.block()`, a single bare `poll()` — this was the actual missing ingredient identified from the Sixth capture), on both plain HTTP and a **real `future-invoke-result` pollable** via raw `WasmRpc` API. Required deliberate callee-side latency (a real timer block) so the first `ready()` check genuinely observes `false` — same-process calls otherwise complete too fast. Own instrumentation confirmed the reproduction's trace matches production field-for-field | PASS — replays correctly even with a byte-for-byte matching trace shape |
| 11 | `ROUND_ELEVEN_FINDINGS.md` | Real in-process memory-pressure eviction (not `drop(executor)`) of the worker mid-suspend, then reload | Inconclusive — code-level proof (see fact 7 above) obtained instead; could not get the target worker to actually evict under the memory-pressure test harness in the time available; the non-working test was reverted rather than committed |

**The pattern across all of this**: every dimension constructible in an isolated, same-process,
same-compiled-component test — atomic region content (HTTP vs. real RPC), invocation-boundary
shape (split vs. single), region count (1 vs. 4), the exact polling call sequence (verified via
own instrumentation to match production field-for-field), and (partially) the reload trigger
(eviction vs. restart) — has been matched to production and still replays cleanly. The
synthetic tests are not wrong to keep failing to reproduce it; they are narrowing down what it
depends on.

## Two new open findings (Round 12, from the Seventh capture)

**Caveat that applies to both**: the Seventh capture was taken against a *busy, multi-worker*
process (4-5 workers running concurrently, all logging to one trace file) with instrumentation
that, at the time, carried no per-worker identity — so a LIVE-side trace line and a REPLAY-side
trace line could not be reliably attributed to the *same* worker instance by grep/proximity alone.
**This has now been fixed** (commit `ca32dfc04`, this round): every `ATOMIC_TRACE`/
`POLLSEQ_TRACE`/`POLLREADY_TRACE`/`POLLCALL_TRACE`/`RETRY_TRACE` line now carries
`agent_id = <the worker's OwnedAgentId>`. Both findings below need a fresh capture with this fix
active before either can be trusted as a real (vs. capture-artifact) observation.

### Finding A: `clear_pollable_seq` fires immediately after a pollable's confirming `ready()=true`

Confirms the `f89c8b85d` design ("cleared on `drop()` so a reused rep gets a fresh seq") is
active and firing routinely — not itself surprising. The open question is whether **replay's
drop/clear timing can diverge from live's** relative to *other* pollables' `ready()` calls, in a
way that shifts which logical pollable a given `seq` value maps to, without ever violating the
"same rep observed this session → same current seq" invariant the existing instrumentation
checks (that invariant only proves internal self-consistency at each snapshot in time, not that
live and replay agree on *which* logical pollable owns a given seq across the whole session).

**Code-level reasoning** (not yet empirically confirmed): `pollable_seq()` assigns strictly by
call order — "the Nth distinct rep first observed via `ready()` this session" — and does not
itself reference wall-clock time or any host-side scheduling decision. For live and replay to
diverge in clear/reassign timing *without* the guest's own bytecode-level control flow having
already diverged (which would just be a restatement of the original bug, not an independent
cause), there would need to be host-side or async-runtime-level non-determinism in *when* a
`Drop` actually runs relative to sibling `ready()`/`poll()` calls — e.g. if the guest's compiled
async executor (whatever the TypeScript SDK compiles down to for driving concurrent futures)
schedules pollable polling/dropping based on *real completion order* of concurrent operations
rather than a fixed bytecode order. This would tie directly into Finding B below (concurrently
polled pollables) — if that's real, Finding A is very plausibly the same phenomenon from a
different angle, not an independent cause.

### Finding B: `poll()` observed with multiple reps in one call — not the single-pollable model assumed earlier

`reps: [6, 5]` (replay) vs. `reps: [6, 7]` (live, elsewhere in the same log) — both reps in the
replay's batch got "no match, synthesizing false" *before* `poll()` ran, i.e. genuinely
simultaneous multi-pollable polling, not sequential calls that happened to log adjacently. This
does not match the single-RPC-pollable-at-a-time model every prior round's analysis (and every
synthetic reproduction attempt) assumed.

**Important capture-quality caveat**: rep numbers are per-worker-instance, not global. Without
agent-ID attribution (fixed this round, not yet re-captured with the fix active), "`reps: [6,7]`
live vs. `[6,5]` replay" could easily be two *different workers'* unrelated batched polls that
happen to share rep 6 by coincidence, not the same logical operation observed twice. **Do not
treat this as confirmed until re-captured with `ca32dfc04` active.**

**Code-level reasoning on what a genuine multi-pollable batch here would mean**: two known,
legitimate code paths in video-harness *do* batch multiple pollables together intentionally and
are already covered by existing regression tests (`concurrent_pollables_survive_worker_replay`
and its adversarial-completion-order variant, both pre-dating this investigation, both still
passing): the `create_promise()`/`get_promise_result`-backed render-promise polling loop, which
legitimately awaits N concurrently-dispatched render promises together. If Finding B's batch is
*this* pattern, it would mean the underlying `ready→poll→ready` divergence isn't specific to
`golem::rpc::future-invoke-result` pollables at all — it can also strike
`GetPromiseResultEntry`-backed ones, which is a *plain, promise-backed* pollable, not an
RPC-future. That would actually be a clarifying finding, not a second distinct bug: it would mean
the divergence is generic to the ready→poll→ready shape for *any* pollable kind under some as-yet
unidentified condition, not something specific to cross-agent RPC's particular host-call
sequence (`get_agent_type`/spans/`future-invoke-result::get`) — which would in turn suggest
round nine and ten's RPC-specific reproduction attempts, and round eight's plain-HTTP attempt,
were both on the right track shape-wise but still missing some other necessary ingredient (most
plausibly: genuine *concurrent* polling of multiple in-flight pollables, which none of rounds
8-11 exercised — every synthetic ready→poll→ready reproduction so far polled exactly one pollable
at a time).

**Also newly observed**: `get_current_retry_point` hit the "no active atomic regions,
`current_retry_point: 1214`" branch — the first time any capture has hit this branch rather than
"innermost active atomic region, `begin_index: N`". Traced why this branch reports what it does
(`begin_function()`, `durable_host/mod.rs` around line 1290-1324): for any `ReadLocal`/
`ReadLocalPollable`-tagged call (which is what *every* `io::poll::poll`/`io::poll::pollable::ready`
call is, whether or not it's wrapped in an `atomically()` region) — as opposed to
`WriteRemote`/`WriteRemoteBatched(None)` calls — `current_retry_point` is continuously updated to
"the last written non-hint oplog entry" on **every single poll/ready call**, not just at
transaction boundaries. This is a generic fallback retry-point mechanism, not evidence of a
structurally different bug: it simply means *this* particular trap instance happened outside any
`atomically()`-wrapped region — consistent with, and mildly supporting, the Finding B hypothesis
above that this capture caught the divergence on the render-promise polling pattern (which is
never atomically-wrapped) rather than on the RPC-future's own poll/ready pair (which always is).

## Recommended next steps, in priority order

1. **Re-capture with `ca32dfc04` (agent-ID tagging) active** to resolve Finding B's "same worker
   or different workers" ambiguity definitively. This is the single highest-value next action —
   everything downstream depends on it.
2. **If Finding B is confirmed as a genuine same-worker concurrent batch**: build a synthetic
   reproduction combining round ten's exact `ready()`→`poll()`→`ready()` shape with genuine
   *concurrency* — multiple pollables (RPC-future and/or promise-backed) in flight and polled
   together, not sequentially one-at-a-time as every round 8-11 test did. This is the one
   structural dimension no synthetic test has varied yet.
3. **If Finding B turns out to be a capture artifact** (different workers, not a real batch):
   the investigation reverts to being stuck at the end of Round 11 — eviction/reload ruled out,
   every single-pollable synthetic shape ruled out, and the most likely remaining explanation is
   something about the live container environment (real multi-worker concurrent load sharing the
   executor process, or genuine cross-process/cross-container timing against a truly separate
   `WorkflowAgent`/`SandboxAgent`) that hasn't been isolated yet.
4. **Independently of 1-3**: if a clean, unambiguous capture ever shows the confirming second
   `ready()=true` call's LIVE-recorded `DurableFunctionType` (now logged directly by `d15590420`)
   with a *different* seq value than what replay's second `ready()` computes for the same logical
   pollable, that is definitive proof of a recording-side bug (hypothesis "a" from the Round
   Twelve ask) and should be pursued directly — inspect what's read/mutates `pollable_seq` between
   the two `ready()` calls (i.e. inside `poll()`'s own `Durability<IoPollPoll>::new()`/`.replay()`
   path) for anything that could shift the seq counter unexpectedly.

## Where the branch and instrumentation live

- Branch: `hrapp/scene-plates-sequential-poll-trap`, worktree at
  `/Users/hannes/work/golem-worktrees/scene-plates-sequential-poll-trap` (based on
  `1.5.x-cloud-benchmarks`). Nothing on this branch has been pushed or merged.
- All instrumentation (`ATOMIC_TRACE`/`POLLSEQ_TRACE`/`POLLREADY_TRACE`/`POLLCALL_TRACE`/
  `RETRY_TRACE`, all now agent-ID-tagged) lives in `golem-worker-executor/src/durable_host/{mod.rs,
  golem/v1x.rs, io/poll.rs}` and is gated at `debug!`/`trace!` level — harmless to leave active,
  `-vvvv` (or `RUST_LOG=golem_worker_executor=trace` for the test binary) is required to see the
  `trace!`-level per-poll-call lines; `-vvv`/`debug` shows the lower-frequency atomic-region and
  pollable-seq lines only.
- Raw captures (redacted oplogs, trace logs) live in `/Users/hannes/work/golem/oplog-backups/`
  (main checkout, not this worktree) with a running `README.md` narrative — the canonical source
  for capture-level detail this document summarizes.
- Test-component additions (`CustomDurability`, `RpcCaller`/`RpcCounter`) are in
  `test-components/host-api-tests/src/custom_durability.rs` and
  `test-components/agent-rpc/golem-it-agent-rpc-rust/src/lib.rs` — reusable building blocks for
  the next round's synthetic reproduction attempt (e.g. a genuine-concurrency variant per
  recommendation 2 above).
