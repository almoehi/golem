# Round Eleven: in-process eviction/reload vs. fresh-process restart

Continuation of `ROUND_TEN_FINDINGS.md`'s open question: does reloading a worker via genuine
in-process eviction (LRU/memory-pressure driven, unloaded and later reloaded within a single
still-running executor process — the actual mechanism behind `golem agent invoke` against a
worker that isn't currently resident) skip initialization a fresh process restart performs, or
leak process-lifetime state across the boundary, in a way that could explain why production's
second `ready()` call fails to match while round ten's `drop(executor); start()`-based
reproduction does not?

## Code-level finding: the reload path is unconditionally fresh, regardless of trigger

Traced the actual code path from `golem agent invoke` down to instance construction:

- `ActiveWorkers::acquire_memory` (`services/active_workers/mod.rs:250`) blocks in a loop calling
  `admission.admit(memory, &self.eviction_source())`, which internally calls
  `evict_at_most_memory` (`mod.rs:542`) — first evicting `LoadedIdle` workers, then
  `WarmRunnable` ones if still under pressure — via `Worker::stop_if_evictable`
  (`worker/mod.rs:1082`), which calls `stop_internal_locked(..., FinalWorkerState::Unloaded)`.
  This tears down the worker's `instance`/`Store` (setting `WorkerInstance` to `Unloaded`) but
  the `Arc<Worker<Ctx>>` itself **stays resident** in `ActiveWorkers`'s map — eviction does not
  remove the worker from the process, only its loaded instance.
- The next invocation against that worker calls `RunningWorker::create_instance`
  (`worker/mod.rs:2814`), which — **on every call, whether this is the worker's first-ever load,
  a reload after eviction, or a reload after a fresh process restart** — calls
  `Ctx::create(...)` (`durable_host/mod.rs:287`), which unconditionally constructs a brand new
  `PrivateDurableWorkerState::new(...)` (`mod.rs:457`) — the struct holding `pollable_seq:
  HashMap::new()`, `next_pollable_seq: 0`, `active_atomic_regions: Vec::new()`,
  `current_retry_point: OplogIndex::INITIAL` (confirmed from the constructor at `mod.rs:4252-4259`
  read in round eight). There is no cache, memoization, or reuse of this state anywhere in
  `create_instance` or `Ctx::create` — every reload replays from the durable oplog into a
  genuinely fresh `DurableWorkerCtx`.
- `RunningWorker::create_instance` does read several fields off the surviving `parent:
  Arc<Worker<Ctx>>` (`worker_metadata`, `component_service`, `engine`, `snapshot_recovery_disabled`
  flag) — these are legitimate process/worker-level singletons (shared wasmtime `Engine`,
  service handles) unrelated to replay bookkeeping, not caches of the specific state under
  investigation.

**Conclusion**: for the exact mechanism instrumented and hypothesized about in round ten
(`pollable_seq`/`next_pollable_seq`/`active_atomic_regions`/`current_retry_point`), in-process
eviction+reload and a full process restart are **structurally identical** — both go through the
identical `RunningWorker::create_instance` → `Ctx::create()` → fresh `PrivateDurableWorkerState::new()`
path, with nothing cached or reused across the boundary. This rules out a state leak in this
specific machinery as the explanation for the eviction-vs-restart distinction.

## Empirical attempt: could not reliably trigger real in-process eviction of a promise-suspended worker

Attempted to build the test the coordinator asked for, modeled on the proven
`scalability.rs::eviction_prefers_idle_workers_over_warm_runnable` pattern (tight memory pool +
`large_initial_memory` filler worker forcing real eviction, already used successfully elsewhere
in this suite): worker A runs round ten's exact `atomic_double_ready_rpc_call_then_promise_init`
+ fire-and-forget `sequential_atomic_rpc_await` (suspending on an incomplete promise), then a
single `large_initial_memory` filler worker is started under a memory pool too tight for both to
coexist, with no other `LoadedIdle` candidate present to prefer over A.

Required building the `large_initial_memory`/`large_dynamic_memory` test components in this
worktree first (never built here; `golem-cli build` under `test-components/scalability/`, wasm
copied to `test-components/scalability_large_initial_memory_release.wasm` — build artifacts, not
committed as source changes).

Swept the memory pool size across seven runs (520MB, 550MB, 610MB, 630MB, 650MB, 700MB — plus the
original 768MB precedent): **at no pool size did admitting the filler visibly evict worker A.**
Below the filler's own actual admission charge (~591783526 bytes ≈ 564MB, notably higher than the
536870912-byte value the agent's `run` method returns as its self-reported allocation — the
admission charge includes additional overhead) the filler request backs off and retries forever,
with **zero eviction attempts ever logged** (`record_worker_eviction`,
"Collecting storage eviction candidates", "Freed ... bytes by evicting" — none of active_workers's
own eviction log lines ever fired, at any pool size tested). Above roughly 650MB the filler
admits immediately without any backoff at all. There was no pool size in between where admission
failed *and* eviction of A was attempted — worker A never appears to be treated as an eviction
candidate in this scenario at all.

Investigated why: `Worker::eviction_class()`/`stop_if_evictable()` require `waiting_for_command`
true (worker between commands, permit released) and no `has_queued_internal_work` /
`has_resume_replay` / `has_interrupt`. Confirmed from `invocation_loop.rs` that
`waiting_for_command` does correctly become `true` after a suspend (the same
"Releasing/Re-acquiring concurrent-agent permit" cycle observed repeatedly in this test's own
trace output), and confirmed `has_interrupt` (`interrupt_signal`, set only by
`Worker::set_interrupting` — an *external* interrupt request, e.g. for `unload_environment`) is
unrelated to and unaffected by `poll()`'s internal `InterruptKind::Suspend` return path — so
neither of those explains it outright. Did not get to definitively identify which condition
(most likely `has_queued_internal_work`, i.e. `running.queue`) keeps a promise-suspended worker
non-evictable, or whether it's something else entirely (e.g. a race between the admission
gate's retry cadence and the worker's own suspend/re-check cycle) — ran out of time budget on
this specific empirical thread after seven calibration attempts.

**Did not commit the non-working integration test.** A test that doesn't actually verify eviction
occurred (no assertion, no log-based check, just "did the second invoke happen to succeed") would
misrepresent what it demonstrates — every run happened to pass, but for reasons unrelated to
eviction (either the filler fit without evicting anything, at large pool sizes, or the test never
reached the reload step at all, timing out during admission at small pool sizes). Reverted that
change; only this findings file is committed for round eleven.

## Where this leaves the investigation

Combining the code-level finding (reload path is identical regardless of trigger, for the state
under investigation) with the empirical difficulty (this specific worker shape — suspended
mid-invocation on an incomplete promise, as opposed to the proven eviction test's "queued a second
invocation on top of a completed one" WarmRunnable shape — may not even be a real-world eviction
candidate the way I set it up, or may need eviction-mechanism instrumentation of its own to
pin down reliably) — I did not find evidence that in-process eviction is the missing ingredient.

Per the coordinator's own fallback: this points toward the real trigger being something in the
live container environment that none of rounds eight through eleven's synthetic tests have
exercised. The most likely untested candidate, per the coordinator's own framing: **genuine
cross-process RPC timing** — every synthetic RPC reproduction so far (rounds nine, ten, and this
round's abandoned attempt) called an `RpcCounter` agent living in the *same compiled component*,
invoked through the *same test executor process*. Production's actual call is
`WorkerAgent` → `WorkflowAgent`, a **different agent type, likely running as durably separate
worker state with real dispatch/scheduling latency** (not necessarily a different OS process, but
a genuinely independent, differently-timed invocation path) — something no test in this
investigation has constructed. That is the next round's most promising lead if the coordinator
wants to continue down the synthetic-test path; alternatively, since the coordinator now has a
live, reliable natural reproduction in the real container, further empirical narrowing may be
faster done there directly (e.g., checking whether the trap's callee — the real `WorkflowAgent` —
itself does anything unusual around the time of the second `ready()` call, via the same
instrumentation already committed on this branch).

## What's committed this round

- This file only. No code changes (the attempted `rpc.rs` test was reverted after failing to
  reliably demonstrate real eviction).
