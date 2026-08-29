# Round Fifteen: replay-mismatch instrumentation, and a long-lived-timer + fresh-pollable
# batched-poll reproduction — still negative in isolation

Continuation of `ROUND_FOURTEEN_FINDINGS.md`. New directive: focus purely on Finding B (the
batched `poll()` replay failure), now confirmed live and post-fix by a separate investigation
thread's Ninth capture (`/Users/hannes/work/golem/oplog-backups/README.md`) — a `poll()` call
batching `reps: [20, 23]` where rep 20/seq 179 is a **long-lived, many-times-polled-but-never-
resolved, timer-shaped** pollable and rep 23/seq 181 is fresh; both fail to match during replay
("no match, synthesizing false"), crash follows. That capture also established the real
production shape: this recurs on video-harness `main` (the old, non-round-decomposed single-
invocation reasoning loop), not the `hrapp/reasoning-loop-refactor` branch's
`advanceReasoningLoop()` round-based shape.

Two lines of work this round, both committed:

## 1. Deeper replay-mismatch instrumentation (commit `203380c85`)

Round 14's traces (`POLLCALL_TRACE`/`POLLREADY_TRACE`) only ever showed "no match, synthesizing
false" — they never revealed *what entry was actually sitting at the replay cursor* when a match
failed, or *why* a strict (non-search) replay mismatch occurred. Two additions, both structural
and low-risk (no behavior change, verified by the pre-existing unit test suite):

- **`ReplayState::try_get_oplog_entry`** (`replay_state.rs`): now traces every peeked entry —
  oplog index, the entry's full `Debug` (bounded: `OplogPayload`'s `Debug` impl only prints
  `bytes_len` for `SerializedInline`, never raw bytes — safe to leave unconditional at `trace!`
  level), and whether the caller's predicate matched. This is the single choke point for *every*
  caller (poll `ready()`, RPC, HTTP, ...), not just poll.rs's own call-site tracing.
- **`Durability::validate_oplog_entry`** (`durability.rs`): the exact site that produces
  production's crash message ("expected io::poll::poll, got io::poll::pollable::ready") now also
  logs the actual entry's `durable_function_type` (e.g. `ReadLocalPollable(179)`) and the
  `begin_index` this replay attempt started from — turning the bare function-name mismatch into
  an actionable "which specific pollable's entry got read out of order here."

Both changes compile clean and the four existing `try_get_oplog_entry_*` unit tests plus the full
`durable_host::` unit suite (64 tests) still pass unmodified. This is what the coordinator's next
live capture (against video-harness `main`, per the `LIVE_ITERATION_PLAYBOOK_AND_FINDINGS.md`
pipeline) should build against — it should now show, at the exact crash point, the actual
`ReadLocalPollable(seq)` (or other) entry that was found instead of what was expected.

## 2. Round 14d: long-lived timer, 4 prior regions of history, batched with a fresh pollable

Round 14c confirmed the genuine-batched-poll shape (`reps: [4,5]`) but with **two freshly-created**
pollables — neither had prior polling history from earlier regions. The Ninth capture's rep 20 is
categorically different: a near-infinite timer-shaped pollable that survived and was repeatedly
`ready()`-checked across several *earlier* sequential dispatches before finally being batched with
a fresh one. No prior round tested this combination.

**New agent method** `RpcCaller::long_lived_timer_batched_with_fresh_rpc_after_n_regions`
(`test-components/agent-rpc/golem-it-agent-rpc-rust/src/lib.rs`): a near-infinite
`monotonic_clock::subscribe_duration` timer pollable is created ONCE (unlike
`timer_race_multi_step`, which creates+drops a fresh timer every cycle) and kept alive across `n`
sequential same-target atomic RPC regions (each also checking the timer's `ready()`, always
`false`, accumulating real replay-relevant call history for it), interleaved with production's
`yieldForSnapshot()`-shaped promise round-trip between regions (matching 14a/b). A final region
then polls the long-lived timer together with a FRESH RPC pollable in one `poll()` call — the
`reps: [long, fresh]` shape. Everything happens inside ONE external invocation, matching `main`'s
actual (pre-round-decomposition) shape.

**Test**: `long_lived_timer_batched_with_fresh_rpc_survives_cold_replay` (`rpc.rs`), n=4 prior
regions matching production's 4 dispatches. No snapshot policy — cold restart replays from
genesis, matching production's actual shape.

**Getting the test to actually exercise cold replay of the batched poll required a fix to the
test itself**, not just new agent code: the original 500ms pre-restart sleep (copied from
14a/b's pattern) was not long enough for the ~750ms of real `inc_by_slow` blocking work (5 calls
× ~150ms) plus promise round-trips to complete, so the batched `poll()` call only ever ran live,
*after* the cold restart, and replay only ever exercised the four single-pollable regions —
never the batched one. Confirmed via `POLLCALL_TRACE`: with a 500ms sleep, `reps: [1, 4]` only
ever appeared with `is_live: true`; bumped the sleep to 3000ms (~4x margin), and confirmed
`reps: [1, 4]` now appears with `is_live: false` too — i.e. replay genuinely reconstructs the
batched-poll oplog entry from scratch, the actual condition this test needs to exercise.

**Result: PASSED.** `test result: ok; 1 passed; 0 failed`, both alone and alongside 14b/the
pollable_seq regression test. Cold replay of a long-lived (4-region-history) timer pollable
batched with a fresh RPC pollable — the closest isolated synthetic reproduction of the Ninth
capture's `reps: [20, 23]` shape yet attempted — still does not trap.

## Where this leaves Finding B

Every structurally-constructible-in-isolation-via-raw-Rust-`WasmRpc`/`io::poll` hypothesis through
round 14 was negative; round 15 adds "long-lived timer pollable with real multi-region history,
batched with a fresh pollable" to that list — also negative. Round 14's "what remains untested"
list (`ROUND_FOURTEEN_FINDINGS.md`) is unchanged and still stands, #1 in priority order: the
TypeScript SDK / QuickJS async-runtime-level scheduling that every round (8 through 15) has
bypassed by driving the raw Rust WIT API directly. Given the confirmed real occurrence is on
`main`'s single-long-invocation shape (not requiring any round-decomposition machinery this
investigation hasn't modeled), the live capture against `main` — now instrumented per section 1 —
is the more promising near-term path to a decisive answer than further raw-Rust synthetic
variants.

## Aside: pre-existing test-infrastructure flakiness (unrelated, not investigated further)

While validating this round, Round 14a's own **unmodified** test
(`sequential_atomic_double_ready_rpc_calls_same_target_n4_survives_cold_replay`) failed
reproducibly (3/3 attempts) when run **alone**, with `Runtime error: WorkerActivator is disabled,
not creating instance` (`golem-worker-executor/src/services/worker_activator.rs`'s
`LazyWorkerActivator` — its `Weak` upgrade fails). It passes reliably when run alongside sibling
tests (test-r then forces single-threaded execution: "Cannot run tests in parallel when tests have
shared dependencies..."). Confirmed via `git checkout HEAD~1 -- durability.rs replay_state.rs`
(reverting this round's instrumentation entirely) that the failure is 100% unrelated to any change
in this round — it reproduces identically against the pre-instrumentation code. Not investigated
further as out of scope for Finding B; flagging in case it affects the coordinator's own test runs.

## What's committed this round

- `golem-worker-executor/src/durable_host/replay_state.rs`,
  `golem-worker-executor/src/durable_host/durability.rs`: instrumentation (commit `203380c85`).
- `test-components/agent-rpc/golem-it-agent-rpc-rust/src/lib.rs`:
  `long_lived_timer_batched_with_fresh_rpc_after_n_regions` on `RpcCaller`.
- `golem-worker-executor/tests/rpc.rs`:
  `long_lived_timer_batched_with_fresh_rpc_survives_cold_replay` — passing.
- This file.

Not run: full `golem-worker-executor` test suite (only new/related tests were compiled/run, same
caveat as every prior round). No existing test-component code was modified — only additive new
methods/tests (aside from the sleep-duration fix inside this round's own new test, needed for the
test to be structurally correct in the first place — never landed as "passing" with the wrong
shape).
