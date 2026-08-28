# Round Eight: `atomically()` + cold replay-after-suspend — negative result, narrowed hypothesis

## Context

Two production `WorkerAgent` traps (`workspace-example-hitl@1.0`, ids `03adbf53...@scene_plates`
and `d00cf6d0...@character_sheet`) hit `expected io::poll::poll, got io::poll::pollable::ready`
on a cold worker resume, `retry from:` pointing at a `BeginAtomicRegion` index. Both traps
happened on a binary that already includes every fix from `GOLEM_IO_POLL_BUG.md` (skip-`false`
optimization, seq-based `IoPollReady` matching, positional-`poll()` replay, `replaying_http_batch`)
and the video-harness app-level "serialize suspends starts" fix (commit `bdd966f` on
`video-harness`, confirmed working — no concurrent/overlapping `atomically()` regions in either
oplog). See `/Users/hannes/work/golem/oplog-backups/README.md` ("Fourth oplog" and "Fifth oplog"
sections) for the full byte-level trace this investigation started from.

The `d00cf6d0...@character_sheet` oplog is the minimal shape: exactly ONE `atomically()`-wrapped
region (a single `atomicRpcCall()`-wrapped `WorkflowAgent.run()` dispatch — `2x get_agent_type`,
`START SPAN` (rpc-connection), `START SPAN` (rpc-invocation), `io::poll::poll{count:1}`,
`io::poll::pollable::ready`, `golem::rpc::future-invoke-result::get`, `FINISH SPAN`), which closes
cleanly (`END ATOMIC REGION`), followed by a `create_promise`/`await_promise`-style wait that also
closes cleanly, followed by a bare `SUSPEND` with no completing `io::poll::poll` entry (matching
`poll()`'s live-mode early-suspend fast path for promise-backed pollables, which never persists an
`IoPollPoll` entry when it fires). ~7 minutes idle, no intervening `SNAPSHOT`. Cold resume → full
replay from the last real snapshot → diverges at the `BeginAtomicRegion` index.

## Hypothesis tested

`get_current_retry_point()` (`golem-worker-executor/src/durable_host/mod.rs:2615`) returns
`active_atomic_regions.last().begin_index` whenever the stack is non-empty at the moment of the
trap. For the retry point to land on the atomic region's own begin index, that region must still
be considered "open" by the *replay* attempt's bookkeeping, even though the *original recording*
shows it cleanly closed (`BEGIN`/`END ATOMIC REGION` both present, no dangling entries). `mark_end_operation()` (`golem-worker-executor/src/durable_host/golem/v1x.rs:403`) pops a
region via `active_atomic_regions.retain(|r| r.begin_index != begin)` only after successfully
reading an `EndAtomicRegion` oplog entry (replay mode) — and `mark_begin_operation()`'s replay path
first checks `lookup_oplog_entry(begin_index, is_end_atomic_region)`; if that search fails to find
the matching end, it calls `switch_to_live()` and discards the region's recorded content entirely,
re-executing it live. **Hypothesis**: something about a cold full-history replay across a clean
`SUSPEND` boundary (as opposed to the concurrent/overlapping-region and adversarial-completion-order
scenarios the existing six verification rounds already cover) causes this end-of-region lookup, or
the region's internal `ready()`/`poll()` seq-matching, to fail — purely from replaying an
`atomically()`-wrapped operation, with no concurrency involved at all.

## Test built

Added to `test-components/host-api-tests/src/custom_durability.rs` (new `CustomDurability` agent
methods) and `golem-worker-executor/tests/durability.rs`
(`sequential_atomic_rpc_region_survives_cold_replay_after_suspend`):

1. `sequential_atomic_calls_then_promise_init(n)` — `n` sequential `atomically()`-wrapped real
   WASI HTTP GET calls (self-contained: request → subscribe → `.block()` → read → drop, all inside
   the closure), then `create_promise()` (not awaited yet). Matches the production oplog's
   `BEGIN ATOMIC REGION → ... → io::poll::poll → io::poll::pollable::ready → ... → END ATOMIC
   REGION` shape for one region.
2. `sequential_atomic_await(promise_id)` — `golem_rust::blocking_await_promise(&promise_id)`, the
   same primitive `WorkerAgent.awaitPromise()` uses. Invoked fire-and-forget (`invoke_agent`, not
   `invoke_and_await_agent`) against a not-yet-complete promise, so it genuinely suspends —
   matching production's bare `SUSPEND` with no completing `IoPollPoll`.
3. Test flow: call (1) with `n=1`, fire-and-forget call (2), sleep 500ms to let the suspend land,
   `drop(executor)` + start a fresh one (forces full cold oplog replay, no snapshot exists for this
   component), `complete_promise()` externally via the test-framework API, then call (2) again and
   assert the payload comes back correctly.

## Result: PASSED — hypothesis as stated is FALSIFIED

```
test result: ok; 1 passed; 0 failed; ...
```

No trap. A single `atomically()`-wrapped **plain WASI HTTP** call, followed by suspend-for-promise
and a cold full-history replay with no intervening snapshot, replays correctly on this binary. The
generic "atomic region + cold replay-after-suspend" shape is **not**, by itself, sufficient to
reproduce the trap.

## What this narrows the search to

The test's atomic region body differs from the production trap's in one structural way: it does a
plain `wasi:http` fetch (`check_write`/`write`/body-stream `io::poll` calls only) instead of a
cross-agent RPC dispatch. The production atomic region specifically contains, in order:
`golem::agent::get_agent_type` (x2) → `START SPAN` (`rpc-connection`) → `START SPAN`
(`rpc-invocation`) → `io::poll::poll` → `io::poll::pollable::ready` →
`golem::rpc::future-invoke-result::get` → `FINISH SPAN`. None of that RPC-specific machinery
(`AgentClass.get()`'s `get_agent_type` lookup, the nested tracing spans, or
`future-invoke-result::get`'s own durability handling — a different code path from the WASI HTTP
`check_write`/`write` one) is exercised by this test.

Two follow-up hypotheses, not yet tested (out of scope for this round given time spent bootstrapping
the build — this worktree had no prior build artifacts, so getting a first passing test required a
from-scratch `golem-cli` build + a full `golem-worker-executor` test-binary compile before any
actual investigation could run):

1. **RPC-specific replay gap**: build a genuine two-agent RPC test (a caller wrapping
   `SomeAgent.get(id).method()` in `atomically()`, a real callee agent) and repeat the same
   suspend + cold-replay sequence. This is the structurally faithful repro and the most likely next
   step to actually trigger the trap — it requires new WIT-level test scaffolding (a second agent
   type), which is a larger lift than reusing the existing `CustomDurability` HTTP/promise
   primitives this round did.
2. **Single-invocation vs split-invocation shape**: this test's atomic region and its suspend are
   two *separate* external invocations (`sequential_atomic_calls_then_promise_init` then
   `sequential_atomic_await`); production has both inside *one* invocation
   (`advanceReasoningLoop`/`advanceChatLoop`). `active_atomic_regions` and `pollable_seq` are
   worker-level (not per-invocation) state reconstructed by replaying the full oplog regardless of
   invocation boundaries, so this is a lower-probability explanation than (1), but not ruled out —
   worth collapsing into one invocation if (1) also passes clean.

## What's committed

- `test-components/host-api-tests/src/custom_durability.rs`: `sequential_atomic_calls_then_promise_init`
  and `sequential_atomic_await` methods on `CustomDurability` (trait + impl + doc comments).
- `golem-worker-executor/tests/durability.rs`:
  `sequential_atomic_rpc_region_survives_cold_replay_after_suspend` test (passing — a real
  regression guard for the plain-`atomically()`-HTTP-call + cold-replay shape, even though it
  didn't reproduce the original trap).
- This file.

Not run: the full `golem-worker-executor --lib`/`--test integration` suite (461+ tests) — only the
new test was compiled and run, given the time already spent on cold-build bootstrapping. No
existing code was modified, only new test methods/functions were added, so regression risk to
existing tests is low but not verified in this round.
