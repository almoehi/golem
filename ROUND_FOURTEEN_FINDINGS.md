# Round Fourteen: same-target double-ready N=4, interleaved yield-promise, and genuine
# concurrent multi-pollable poll() — all three still negative

Continuation of `INVESTIGATION_SUMMARY.md` (Round 12's "Two new open findings") and
`ROUND_TWELVE_THIRTEEN_FIX.md` (the shipped `pollable_seq` counter-recovery fix, confirmed in a
separate investigation thread to NOT cover this trap — our only snapshot is at construction,
before that fix's snapshot-resume-boundary mechanism is even reachable). Closes the two highest-
priority untested dimensions from `INVESTIGATION_SUMMARY.md`'s "Recommended next steps": Finding
B's literal ask ("build a synthetic reproduction combining round ten's exact
`ready()`->`poll()`->`ready()` shape with genuine concurrency") plus a new dimension neither
finding covered (same target agent reused across N sequential atomic regions, with production's
real interleaved `yieldForSnapshot()` cycle between each).

## Motivation

A live production capture (this session's own trace, `WorkerAgent(...,"...@scene_plates")`,
2026-08-29) empirically confirmed `io::poll::poll` registers a growing pollable count starting at
the second of four sequential `WorkflowAgent.run()` dispatches (`count: 1` -> `count: 2`,
persisting for every subsequent poll through `SUSPEND`) — structurally matching Finding B's
`reps: [6, 5]` observation, this time from a clean single-agent oplog dump (not the ambiguous
multi-worker trace log Finding B's caveat warned about), independently corroborating that Finding
B is real, not a capture artifact. This round builds the synthetic reproduction Finding B calls
for, in three sub-variants of increasing structural fidelity.

## Round 14a: same-target sequential double-ready, N=4, no interleaved yield

New agent method `sequential_atomic_double_ready_rpc_calls_then_await` (`RpcCaller`,
`test-components/agent-rpc/golem-it-agent-rpc-rust/src/lib.rs`): combines round ten's manual
`ready()`->`poll()`->`ready()` shape (the confirmed exact production polling pattern, on a real
`future-invoke-result` pollable) with round nine's N=4-sequential-regions/single-invocation
structure — but unlike round nine's `sequential_atomic_rpc_calls_then_await` (fresh, per-iteration
-unique target agent), every iteration targets the SAME `RpcCounter` instance, matching production
exactly (all 4 `scene_plates` dispatches go to the SAME `WorkflowAgent(krea2_base_realism@main,...)`).

Test: `sequential_atomic_double_ready_rpc_calls_same_target_n4_survives_cold_replay` (`rpc.rs`).
No snapshot policy configured — cold restart replays from genesis, matching production's actual
shape (a single `SNAPSHOT` at construction, none since).

**Result: PASSED.** `test result: ok; 1 passed; 0 failed`. Own `POLLCALL_TRACE` instrumentation
confirms every `poll()` call in this variant registers exactly one pollable (`reps: [1]` or
`reps: [2]`, never a multi-element array) across all 4 dispatches, live and replay alike — same-
target reuse alone does not produce a lingering/leaked second pollable in a purely synchronous
Rust reproduction.

## Round 14b: + production's real interleaved yield-promise cycle

Production's `atomicRpcCall()` (`video-harness/src/util/golem.ts:81-108`) is `atomically(fn);
await yieldForSnapshot();` **per dispatch**, not N atomic calls batched before one final await —
confirmed against the production oplog, whose 4 dispatch atomic regions (`#890-899`, `#905-915`,
`#921-931`, `#937-947`) are each immediately followed by their own `yieldForSnapshot()`-shaped
`create_promise`/`complete_promise`/`poll`/`ready`/`get_promise_result` cycle (`#900-904`,
`#916-920`, `#932-936`, `#948-952`) before the next dispatch begins. Round 14a's loop lacked this
interleaving. Updated the same method to insert a `create_promise()` + `complete_promise()` +
`blocking_await_promise()` round-trip (the Rust equivalent of `yieldForSnapshot()`) immediately
after each atomic region closes, matching production's oplog shape exactly.

**Result: PASSED again.** Same test, rebuilt component. `POLLCALL_TRACE` still shows only
single-element `reps` arrays throughout — the interleaved promise-yield cycle does not introduce a
second concurrently-registered pollable either.

## Round 14c: genuine concurrent dual-pollable poll() (Finding B's literal ask)

New agent method `concurrent_double_ready_rpc_calls_then_await`: two independent `WasmRpc`
proxies, both targeting the SAME `RpcCounter` instance, both `async_invoke_and_await()`-fired with
NO await between them (genuinely simultaneously in-flight, unlike every prior round which always
fully resolved one pollable before creating the next), subscribed, and polled TOGETHER —
`poll(&[&pollable_a, &pollable_b])`, one call, two pollables — looping the double-ready
confirm-per-index logic until both resolve, all inside one `atomically()` region.

Test: `concurrent_double_ready_rpc_calls_survives_cold_replay_after_suspend` (`rpc.rs`).

**Result: PASSED.** `test result: ok; 1 passed; 0 failed`. Critically, `POLLCALL_TRACE` confirms
this **is** the genuine multi-pollable shape Finding B described — `reps: [4, 5]` repeated across
many busy-poll iterations while both `inc_by_slow` calls are in flight — structurally matching
production's own `reps: [6, 5]` observation almost exactly. Even with this confirmed, isolated,
single-atomic-region genuine concurrent 2-pollable `poll()` present in the oplog, cold restart +
full replay from genesis succeeds cleanly.

## Where this leaves the investigation

Three independent, structurally distinct hypotheses tested this round, all negative:

| Variant | Same target | Interleaved yield | Genuine concurrent poll | Result |
|---|---|---|---|---|
| 14a | yes | no | no | PASS |
| 14b | yes | yes | no | PASS |
| 14c | yes | n/a (single region) | **yes** (`reps: [4,5]` confirmed) | PASS |

Finding B's own literal recommendation — "build a synthetic reproduction combining round ten's
exact shape with genuine concurrency" — is now DONE, cleanly isolated, instrumentation-verified to
match production's `reps` shape, and still does not trap. This rules out "genuine multi-pollable
polling, by itself, in an isolated single-agent test" as a sufficient condition. Combined with
rounds 8-13's exhaustive coverage of every other single-region/multi-region/split-invocation/
same-invocation/snapshot-boundary dimension, the set of structurally-constructible-in-isolation
hypotheses is now effectively exhausted.

**What remains untested, in priority order:**

1. **TypeScript SDK / QuickJS async-runtime-level scheduling.** Every round in this investigation
   (8 through 14) drives the raw Rust `WasmRpc`/`io::poll` WIT API directly and synchronously —
   there is no async executor multiplexing multiple pending JS `Promise`s the way the actual
   `@golemcloud/golem-ts-sdk`-compiled QuickJS runtime does for production's real TypeScript
   `WorkerAgent`. Production's `yieldForSnapshot()` (`video-harness/src/util/golem.ts:21-25`) is
   itself a TS-level app helper, not a raw SDK primitive — confirmed via a separate research
   thread this session that its doc comment's core claim ("forces snapshotting: {every:1} to fire
   at this exact point") is factually wrong at the engine level (`on_external_invocation_completed`
   only fires at top-level invocation return, never at an internal `awaitPromise` yield) — so
   whatever scheduling machinery the QuickJS-compiled `async`/`await` code actually uses to drive
   concurrent `Promise`-backed operations has never been exercised by any Rust-only reproduction
   in this investigation. A genuine TS-SDK-based reproduction (bringing up `agent-sdk-ts`, per
   Round 13's noted gap: "needs infrastructure — npm/node deps... outside this fix's scope to set
   up") is the one dimension no round has attempted.
2. **Real production-scale concurrency/load** (Round 9's candidate #1, still untested): the
   fat-image container runs many agent types concurrently in one executor process; every round's
   test executor has at most two or three agent instances alive.
3. **The real `WorkflowAgent.run()`'s actual complexity** (Round 9's candidate #2, still
   untested): the toy `RpcCounter.inc_by_slow` callee is a single field mutation plus a timer
   block — the real callee does its own nested `atomically()`/snapshot/fetch work.

## What's committed this round

- `test-components/agent-rpc/golem-it-agent-rpc-rust/src/lib.rs`:
  `sequential_atomic_double_ready_rpc_calls_then_await`,
  `concurrent_double_ready_rpc_calls_then_await` on `RpcCaller`.
- `golem-worker-executor/tests/rpc.rs`:
  `sequential_atomic_double_ready_rpc_calls_same_target_n4_survives_cold_replay`,
  `concurrent_double_ready_rpc_calls_survives_cold_replay_after_suspend` — both passing.
- This file.

Not run: full `golem-worker-executor` test suite (only the new tests were compiled/run, same
caveat as every prior round). No existing code was modified — only additive new methods/tests.
