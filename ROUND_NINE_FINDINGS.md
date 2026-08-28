# Round Nine: real cross-agent RPC + single-invocation shape — also negative

Continuation of `ROUND_EIGHT_FINDINGS.md`. Both of that round's untested follow-up hypotheses
(and a third, more exact variant) have now been tested. All three passed clean — no trap.

## Hypothesis A: RPC-specific host-call sequence is required (not plain HTTP)

Built genuine two-agent RPC test scaffolding in `test-components/agent-rpc/golem-it-agent-rpc-rust/src/lib.rs`
(`RpcCaller` calling a real `RpcCounter` callee agent — an existing test-component pair already
used by `golem-worker-executor/tests/rpc.rs`'s `counter_resource_test_*` suite). Added:

- `sequential_atomic_rpc_calls_then_promise_init(n)` — `n` sequential `atomically_async()`-wrapped
  calls, each constructing a **fresh** `RpcCounterClient::get(...)` proxy and calling `.inc_by(1)`
  — the real cross-agent RPC host-call sequence (`get_agent_type` ×2, `START SPAN`
  rpc-connection, `START SPAN` rpc-invocation, `io::poll::poll`, `io::poll::pollable::ready`,
  `golem::rpc::future-invoke-result::get`, `FINISH SPAN`), not round eight's plain WASI HTTP
  fetch. Then `create_promise()` (not awaited).
- `sequential_atomic_rpc_await(promise_id)` — `blocking_await_promise`, same as round eight.

Test `sequential_atomic_rpc_call_region_survives_cold_replay_after_suspend` (`rpc.rs`): same
two-invocation/suspend/cold-restart/complete-promise shape as round eight's test, `n=1`.

**Result: PASSED.** `test result: ok; 1 passed; 0 failed`. The real cross-agent RPC content inside
the atomic region does not, by itself, reproduce the trap either.

## Hypothesis B: the atomic region and the suspend must be in the SAME invocation

Round eight and hypothesis A above both split the atomic region (invocation 1) from the
suspend-inducing `blocking_await_promise` (invocation 2, fire-and-forget) — production has both
inside one invocation (`advanceReasoningLoop`'s single round). Added
`sequential_atomic_rpc_calls_then_await(n, promise_id)` — `n` atomic RPC calls immediately
followed by `blocking_await_promise` in one method, so one external invocation does both.
(`promise_id` is still created via a separate, non-suspending, zero-atomic-call
`sequential_atomic_rpc_calls_then_promise_init(0)` beforehand, purely so the test can learn the id
to complete it externally — promise *creation* is not part of either hypothesis under test.)

Test `sequential_atomic_rpc_call_and_suspend_single_invocation_survives_cold_replay` (`rpc.rs`),
`n=1`: same suspend/cold-restart/complete-promise shape, but the atomic call and the suspend are
now genuinely one invocation. Success verified via `wait_for_status(Idle)` + a fresh independent
follow-up RPC call succeeding (not just an oplog-queryable check), to catch a "reports Idle while
still wedged" false negative.

**Result: PASSED.** `test result: ok; 1 passed; 0 failed`.

## Hypothesis C (this round's own addition): exact production N=4, not N=1

The "1 region is sufficient" simplification used throughout rounds eight and nine so far was
inferred from a *summary* of the separate `character_sheet` oplog in
`/Users/hannes/work/golem/oplog-backups/README.md` — which on closer reading is actually a
**different trap shape entirely** (a `monotonic_clock::subscribe_duration` timer racing an HTTP
stream pollable — "Bug 2", not an atomic RPC region at all). It was never personally re-traced
from `scene_plates`, the oplog this whole investigation actually started from and which was
byte-traced in full (see the original session's `ROUND_EIGHT_FINDINGS.md` intro and the archived
oplog itself) — and `scene_plates` has **4** sequential atomic regions, not 1.

Test `sequential_atomic_rpc_call_and_suspend_single_invocation_n4_survives_cold_replay` (`rpc.rs`):
identical to hypothesis B's test but `n=4`, matching the confirmed production shape exactly (4
sequential `atomically_async()`-wrapped RPC calls, single invocation, suspend, cold restart, no
intervening snapshot).

**Result: PASSED.** `test result: ok; 1 passed; 0 failed`.

## Where this leaves the investigation

Four independent hypotheses tested across rounds eight and nine, all negative:

| Atomic region content | Invocation shape | Region count | Result |
|---|---|---|---|
| Plain WASI HTTP | split (2 invocations) | 1 | PASS (round 8) |
| Real cross-agent RPC | split (2 invocations) | 1 | PASS (round 9, hyp. A) |
| Real cross-agent RPC | single invocation | 1 | PASS (round 9, hyp. B) |
| Real cross-agent RPC | single invocation | 4 (exact production count) | PASS (round 9, hyp. C) |

Every structural dimension I can construct in an isolated two-agent wasmtime-instance-level test —
atomic-region content, invocation-boundary shape, and region count — has now been varied
independently and in combination, with zero reproduction. This is strong evidence the actual
trigger requires something a minimal synthetic repro structurally cannot capture. Candidates,
none testable from this worktree:

1. **Real production concurrency/load**: the fat-image container runs many agents (`WorkerAgent`,
   `WorkflowAgent`, `SandboxAgent`, `PolicyAgent`, etc.) concurrently in the same executor process;
   my test's executor has exactly two agent instances alive. If the bug depends on contention for
   some process-global or executor-level resource-table/scheduling state, it would never surface
   in isolation.
2. **The real `WorkflowAgent.run()`'s actual complexity**: my `RpcCounter.inc_by()` callee is a
   single synchronous field mutation. The real `WorkflowAgent.run()` does its own nested work
   (dispatching to `serverless-comfy` backends, its own `atomically()`/snapshot calls, etc.) — if
   the bug depends on host-call patterns *inside* the callee's own execution (not just the
   caller's atomic-region wrapper), a toy callee can't trigger it.
3. **Real idle duration**: production had roughly a 2-minute gap between `SUSPEND` and the cold
   resume attempt; this round's tests use a 500ms `tokio::time::sleep`. If the trap depends on
   crossing some time-based boundary (e.g. periodic snapshot timing interacting badly with *just
   barely* not firing, or an executor-side idle-eviction path with its own timing-sensitive
   bookkeeping distinct from the plain cold-restart path `drop(executor)` exercises), that's
   untested.
4. **The specific fat-image container/deployment path**: `drop(executor); start(deps, &context)`
   in this test suite recreates a fresh `WorkerExecutorTestDependencies`-backed executor talking to
   a `Testcontainers`-managed Postgres; production runs inside the `harness` Docker container via
   `golem -E release`. Possible the trigger is specific to that deployment path (executor restart
   mechanics, timing, or configuration differences) rather than to a raw cold-replay-from-oplog
   codepath at all.

**This needs something I don't have from an isolated worktree**: either real access to the fat-image
container to reproduce with the actual production code path and timing (per the coordinator's
offer), or a targeted stress/concurrency test harness deliberately running many agent types
simultaneously — a much larger investment than this round's scope.

## What's committed this round

- `test-components/agent-rpc/golem-it-agent-rpc-rust/src/lib.rs`: `sequential_atomic_rpc_calls_then_promise_init`,
  `sequential_atomic_rpc_await`, `sequential_atomic_rpc_calls_then_await` on `RpcCaller`.
- `golem-worker-executor/tests/rpc.rs`: three new passing tests (hypotheses A, B, C above).
- This file.

Not run: full `golem-worker-executor` test suite — only the new tests were compiled/run, same
caveat as round eight. No existing code was modified.
