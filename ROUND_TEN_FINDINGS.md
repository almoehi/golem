# Round Ten: exact ready→poll→ready shape reproduced via instrumentation — still doesn't trap

Continuation of `ROUND_NINE_FINDINGS.md`, following the coordinator's live-instrumented
production capture ("Sixth capture" in `/Users/hannes/work/golem/oplog-backups/README.md`),
which pinpointed the failure to the guest's real per-iteration polling shape: `ready()`
(optimistic, non-blocking) → `poll()` (blocking) → a **second** `ready()` call to confirm before
reading — not `pollable.block()`'s single bare `poll()` call (round eight/nine's shape).

## What was built

1. **Plain HTTP variant** — `CustomDurabilityImpl::atomic_double_ready_call_then_promise_init`
   (`test-components/host-api-tests/src/custom_durability.rs`): one `atomically()`-wrapped HTTP
   call, driven manually as `first_ready = pollable.ready()`; if false, blocking
   `wasi::io::poll::poll(&[&pollable])` then a **second** `pollable.ready()` to confirm, before
   reading the body. Test: `atomic_double_ready_call_survives_cold_replay_after_suspend`
   (`durability.rs`).

   First attempt used a local axum server responding instantly — **the first `ready()` check
   always observed `true` immediately**, so the `poll()` + second-`ready()` branch never
   executed at all (confirmed via the branch's own instrumentation: only one `POLLREADY_TRACE`
   line, `result: Ok(true)`, ever appeared). Added a deliberate 300ms server-side delay
   (`tokio::time::sleep` before responding) to force the first check to genuinely observe
   `false` — after which the branch executed as intended and the test still passed.

2. **Real RPC variant** — `RpcCallerImpl::atomic_double_ready_rpc_call_then_promise_init`
   (`test-components/agent-rpc/golem-it-agent-rpc-rust/src/lib.rs`): the same manual
   ready→poll→ready shape, but on a REAL `golem::rpc::future-invoke-result` pollable — the raw
   `WasmRpc::new(...).async_invoke_and_await(...)` API (bypassing the `agent_implementation`-
   generated `RpcCounterClient` proxy that round nine used, whose internal polling shape is
   opaque/unverified), matching the production trace's actual failing resource type exactly.
   Added `RpcCounter::inc_by_slow` (a real ~150ms `monotonic_clock::subscribe_duration` block on
   the callee side) since a same-process `inc_by` RPC call completes too fast locally for the
   caller's first `ready()` check to ever observe anything but `true` (same problem as the HTTP
   variant's first attempt, confirmed by checking the trace before adding the delay — omitted
   from this file for brevity, same pattern). Test:
   `atomic_double_ready_rpc_call_survives_cold_replay_after_suspend` (`rpc.rs`).

## Result: both PASSED — and the RPC variant's own instrumentation confirms the shape is exactly right

```
test result: ok; 2 passed; 0 failed; ...
```

Reading the RPC variant's trace lines directly (own instrumentation from commit `3a1c0dc97`,
`RUST_LOG=golem_worker_executor=trace`):

```
LIVE:
  pollable_seq(rep=2) -> seq=0, is_new=true
  ready(rep=2) -> LIVE, result: Ok(false)                    <- first check, genuinely false
  pollable_seq(rep=2) -> seq=0, is_new=false                 <- same logical pollable, correctly re-identified
  ready(rep=2) -> LIVE, result: Ok(true)                     <- second (confirming) check, true, recorded
  mark_end_operation: popped=true                            <- atomic region closes cleanly

REPLAY (after drop(executor) + start(), promise still incomplete, no snapshot):
  mark_begin_operation: REPLAY pushed region begin_index=10
  pollable_seq(rep=2) -> seq=0, is_new=true
  ready(rep=2) -> REPLAY no match, synthesizing false          <- matches live's un-recorded false, correct
  pollable_seq(rep=2) -> seq=0, is_new=false                   <- same logical pollable, correctly re-identified
  ready(rep=2) -> REPLAY matched entry, matched_oplog_index=15  <- *** matched correctly this time ***
  mark_end_operation: popped=true                              <- atomic region closes cleanly, no trap
```

This is the **exact** live/replay shape from the coordinator's production trace (`is_new:
true→false`, `result: false→true` on live; `no match, synthesizing false` → then a second
`pollable_seq`/`ready()` pair on replay) — down to the same field values in the same order. The
only difference: in production, the second replay `ready()` call reported "no match, synthesizing
false" a **second** time (never matching), causing the subsequent `poll()` to trap on the
un-consumed entry. In this reproduction, the second replay `ready()` call **does** match
(`matched_oplog_index: 15`) and the atomic region closes and replays cleanly.

## What this rules out

Combined with rounds eight and nine, every dimension I can construct in an isolated
wasmtime-instance-level test has now been matched to production and still doesn't trap:
atomic-region content (plain HTTP and real RPC), invocation shape (split and single), region
count (1 and 4), and now the exact `ready→poll→ready` polling shape on the real failing resource
type (`future-invoke-result`), with instrumentation-verified live/replay values matching
production's own trace field-for-field.

## What's still different — the likely remaining gap

The one structural difference I have not been able to test: **how the cold resume is actually
triggered**. My tests use `drop(executor); start(deps, &context).await` — a full Rust-level
teardown and recreation of the entire `TestWorkerExecutor` (fresh wasmtime `Engine`, fresh
component cache, fresh everything). The coordinator's capture notes the trap-log-capturing
invocation was done via `golem agent invoke ... inspect` against an **already-`Failed` worker in
a still-running `golem -vvvv server run` process** — and the original natural trap happened after
the workspace was merely idle for ~15 minutes, not after a container/process restart. That points
to **in-process worker eviction and reload** (an LRU/idle-timeout-driven unload-then-reload-from-
oplog within one long-lived server process) as the actual trigger, not a full process/engine
restart. These are plausibly different code paths in `golem-worker-executor`'s worker lifecycle
management, and I could not find an explicit "evict this worker without restarting the whole
executor" API in the test framework (`golem-test-framework::dsl::TestDsl`,
`golem-worker-executor-test-utils`) to construct a test for it — worth checking directly against
the live container (which the coordinator has and I don't from this worktree) whether triggering
the trap via a genuine idle-eviction-then-reinvoke (no process restart at all) versus a full
container restart changes anything, and whether `golem-worker-executor`'s source has a distinct
eviction/reload code path worth instrumenting next (a plausible location:
`golem-worker-executor/src/services/worker_activator.rs` or the LRU-cache eviction logic wherever
`worker.rs`/`invocation_loop.rs` unloads an idle worker from memory — not yet located/read this
round given time spent on the above).

## What's committed this round

- `test-components/host-api-tests/src/custom_durability.rs`:
  `atomic_double_ready_call_then_promise_init`.
- `test-components/agent-rpc/golem-it-agent-rpc-rust/src/lib.rs`: `RpcCounter::inc_by_slow`,
  `RpcCaller::atomic_double_ready_rpc_call_then_promise_init`.
- `golem-worker-executor/tests/durability.rs`:
  `atomic_double_ready_call_survives_cold_replay_after_suspend` (with a deliberately delayed
  local HTTP server, see above).
- `golem-worker-executor/tests/rpc.rs`:
  `atomic_double_ready_rpc_call_survives_cold_replay_after_suspend`.
- This file.

Not run: full test suite. No existing code was modified in these two test-component files beyond
additive new methods (`RpcCounter::inc_by`, the generated `RpcCounterClient`, and all prior tests
are untouched).
