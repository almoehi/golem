# Root cause and fix: `pollable_seq` counter scope mismatch across snapshot boundaries

Continuation of `INVESTIGATION_SUMMARY.md`. This document records the confirmed root cause (the
"Eighth capture" in `/Users/hannes/work/golem/oplog-backups/README.md`), the fix, its empirical
falsification, and the regression sweep — the entries `INVESTIGATION_SUMMARY.md` predates.

## Root cause

`pollable_seq`/`next_pollable_seq` (`PrivateDurableWorkerState`, `durable_host/mod.rs`) is a
monotonic counter that assigns each distinct pollable observed via `ready()` a logical identity
(`ReadLocalPollable(N)`), used to tag persisted `IoPollReady` oplog entries so replay can match the
right entry to the right pollable (`f89c8b85d`, fixing the earlier rep-based scheme's instability
across restores — see `GOLEM_IO_POLL_BUG.md`). The counter resets to empty exactly when
`PrivateDurableWorkerState::new()` runs — confirmed (Round Eleven) to fire identically whether the
worker is loading for the first time, reloading after in-process LRU eviction, or reloading after a
full process restart, with nothing cached across any of those boundaries.

That reset is correct **only when replay will start from oplog genesis** — nothing could have been
observed yet, so 0 is the true value. It is wrong whenever the instance being constructed will
instead resume from a **snapshot**: periodic (or automatic) snapshotting can fire after the live
counter has already climbed past 0, and resuming from that snapshot — by design — never re-executes
the host calls before it (that is the entire point of snapshotting). Every `ReadLocalPollable(N)`
entry persisted *after* the snapshot still carries the seq value live's uninterrupted count assigned
it, which a freshly-reset-to-0 counter can never independently reconstruct. Live-captured evidence
(Eighth capture, agent-ID-isolated from a busy multi-worker log): the same logical pollable resolved
to `seq: 160` on the live session that recorded it, and `seq: 0` on the replay attempt that failed
to match it — `0 != 160`, replay correctly refuses to consume the entry, synthesizes `false`
instead, and the next `poll()` call unconditionally consumes that un-consumed entry and traps with
`expected io::poll::poll, got io::poll::pollable::ready`.

This also explains every earlier capture in this investigation, both the ones that trapped and the
one ("Sixth capture", `character_sheet`) that appeared to replay clean: purely a function of whether
a periodic snapshot happened to land between the worker's true start and the region under replay.
Zero such snapshots (worker recently constructed, or the region is early enough in its life) → both
counters coincidentally start from the same true 0 → matches fine. One or more snapshots landing
before the region → counters diverge by exactly the number of pollables observed in that gap →
traps, deterministically, every time.

## Why none of rounds 8-11's synthetic reproductions ever caught this

Every synthetic test across rounds 8 through 11 used `drop(executor); start(deps, &context)` with
no periodic snapshotting configured. Without a snapshot, cold resume always replays from oplog
genesis — where a fresh 0 counter is, coincidentally, always correct. The missing ingredient the
whole time was not the atomic region's content, the invocation shape, the region count, or even the
exact `ready()`→`poll()`→`ready()` polling pattern (all already matched to production by round ten)
— it was a snapshot landing between two `pollable_seq`-consuming rounds.

## Fix

**File**: `golem-worker-executor/src/durable_host/mod.rs`

The counter cannot be reset to 0 unconditionally. Two directions were considered:

- **(a) Persist the counter as part of the snapshot itself.** Awkward: `pollable_seq` is an
  engine-internal `PrivateDurableWorkerState` field, not application-level state the existing
  `saveSnapshot`/`loadSnapshot` TypeScript mechanism touches, so this would need an engine-level
  snapshot format extension — a bigger, more invasive change, and the only approach fully robust to
  an edge case where a pollable is *assigned* a seq (via a `ready()` call, even one that returns
  `false`) but never *confirmed* (never returns `true`, hence never persisted — see "Known
  limitation" below) before a snapshot boundary.
- **(b) Derive the correct starting value at reconstruction time**, by scanning the durably
  persisted oplog for the highest `ReadLocalPollable(N)` recorded at or before the snapshot's oplog
  index, and initializing the fresh counter to `N + 1` instead of `0`. No snapshot format change
  required — computed once, at worker load time, from data that's already durably persisted.

**Chose (b)** — implemented as `PrivateDurableWorkerState::recover_next_pollable_seq()`, called from
`new()` whenever `last_snapshot_index` is `Some(_)`:

```rust
async fn recover_next_pollable_seq(oplog: &Arc<dyn Oplog>, snapshot_idx: OplogIndex) -> u32 {
    const CHUNK_SIZE: u64 = 1024;
    let mut max_seq: Option<u32> = None;
    let mut idx = OplogIndex::INITIAL;
    while idx.as_u64() <= snapshot_idx.as_u64() {
        let remaining = snapshot_idx.as_u64() - idx.as_u64() + 1;
        let n = remaining.min(CHUNK_SIZE);
        let entries = oplog.read_many(idx, n).await;
        if entries.is_empty() { break; }
        for entry in entries.values() {
            if let OplogEntry::HostCall {
                function_name: HostFunctionName::IoPollReady,
                durable_function_type: DurableFunctionType::ReadLocalPollable(seq),
                ..
            } = entry {
                max_seq = Some(max_seq.map_or(*seq, |m| m.max(*seq)));
            }
        }
        idx = idx.range_end(n).next();
    }
    max_seq.map_or(0, |m| m + 1)
}
```

Called from `new()`:

```rust
let next_pollable_seq = match last_snapshot_index {
    Some(snapshot_idx) => Self::recover_next_pollable_seq(&oplog, snapshot_idx).await,
    None => 0,
};
```

Uses the same chunked-read pattern (`oplog.read_many`, `1024`-entry chunks) `ReplayState`'s own
oplog scans already use elsewhere in this file — consistent with existing engine conventions, not a
new cost model. `pollable_seq` (the `HashMap<rep, seq>` itself, as opposed to the counter that
generates new values into it) legitimately still starts empty on every construction — it is a
per-instance cache rebuilt lazily as `ready()` re-observes each pollable after resume, not something
that needs pre-seeding; only `next_pollable_seq` needed the fix.

### Known limitation (deliberately accepted, not closed by this fix)

`recover_next_pollable_seq` can only recover what's actually recorded in the oplog. A pollable whose
`ready()` was called (spending a counter slot, since `pollable_seq()` runs unconditionally at the
top of `ready()` before checking the result) but never returned `true` before the snapshot boundary
leaves **no trace at all** in the persisted oplog (Bug #1's skip-`false` optimization) — the scan
would silently under-count by one for each such pollable. This is a genuine gap in direction (b);
only direction (a) closes it fully.

Analyzed whether this gap is reachable by any current code path in this codebase and concluded it is
not, for the two families of pollables that actually call `ready()`:

- **RPC-future pollables** (`golem::rpc::future-invoke-result`, the mechanism this whole
  investigation is about): always live and die entirely within one `atomically()` region, which is
  entirely within one invocation. Periodic/automatic snapshots only ever fire *between* fully
  completed external invocations (`on_external_invocation_completed`/the `Periodic` check in
  `invocation_loop.rs`) — so an RPC-future pollable's assignment and confirmation are always on the
  same side of any snapshot boundary, never straddling one.
- **Promise-backed pollables** (the render-promise-await pattern, `create_promise`/
  `get_promise_result`): these route through `poll()`'s early-suspend fast path
  (`promise_backed_pollables` map, checked directly via `is_ready()`), which is a separate mechanism
  that never calls `ready()`/`pollable_seq()` at all — confirmed by reading `Host::poll()`'s
  live-mode fast path in `io/poll.rs`. A long-pending render that spans many snapshots across many
  rounds never touches `pollable_seq` in the first place.

If a future pollable usage pattern (e.g. a raw `.ready()` call on a pollable that legitimately
persists un-confirmed across an invocation/snapshot boundary — nothing in this codebase does that
today) needs this gap closed, direction (a) is the correct follow-up; it was not pursued here to
keep this fix contained to the confirmed, reproducible bug.

## Documentation updated to match

- `pollable_seq`/`next_pollable_seq` field doc comments (`durable_host/mod.rs`): previously claimed
  the counter "never needs to survive a snapshot-based restore" — this claim was the bug. Rewritten
  to state the actual invariant and point at the fix.
- `pollable_seq()` method doc comment: updated to note correctness now depends on `new()` seeding
  the counter correctly, not on an assumption that a fresh 0 is always safe.
- Two other doc comments in `io/poll.rs` referencing "stable... across a snapshot-based restore" and
  "a pollable can never have a poll loop in flight across a snapshot-based restore" were reviewed
  and left unchanged — both describe a different, still-true property (in-flight-identity safety at
  the instant a snapshot is taken), not the counter-baseline issue this fix addresses.

## Empirical falsification

Built `atomic_rpc_call_across_periodic_snapshot_survives_cold_replay`
(`golem-worker-executor/tests/rpc.rs`) — the first synthetic test in this entire investigation
(rounds 8 through 11 all failed to reproduce the trap) to actually combine the missing ingredient:
real snapshotting (`SnapshotPolicy::EveryNInvocation { count: 2 }`, deterministic — no wall-clock
timing dependency) landing between two atomic-region RPC calls, followed by a cold restart that
forces resume from that snapshot rather than genesis.

Required adding snapshot support to the `RpcCaller` test agent
(`test-components/agent-rpc/golem-it-agent-rpc-rust/src/lib.rs`) — `#[agent_definition(snapshotting
= "enabled")]` + `#[derive(Serialize, Deserialize)]` on `RpcCallerImpl` (matching
`JsonSnapshotCounter`'s existing pattern in `test-components/agent-counters/src/snapshot_test.rs`),
since periodic/every-N snapshotting silently never fires at all for an agent that doesn't opt in —
confirmed by first observing zero `Snapshot` oplog entries ever appear for an unmodified `RpcCaller`
regardless of snapshot policy or wait time, with no error or warning logged either.

**Result, verified in both directions (temporarily reverting the fix and restoring it, not just
reading the diff):**

- **With the fix reverted** (`next_pollable_seq` hardcoded back to `0`): the test fails with the
  *exact* production signature — `Unexpected imported function call entry in oplog: expected
  io::poll::poll, got io::poll::pollable::ready`, `Component trapped`. This is the first time this
  investigation reproduced the real bug synthetically, not just a plausible-looking near-miss.
- **With the fix restored**: the test passes.

## Regression sweep

Full `--lib` unit test suite: **459 passed, 0 failed**.

Integration tests re-run against this fix (all components pre-existing in the worktree except
`agent-counters`, newly built this round for the JSON-snapshot tests):

| Test | Result |
|---|---|
| `lazy_pollable` | PASS |
| `snapshot_based_recovery` (+ `_preserves_state_across_multiple_restarts`) | PASS |
| `automatic_snapshot_disabled` / `_periodic` / `_every_2nd_invocation` | PASS |
| `rust_default_json_snapshot_recovery` (+ `_across_multiple_restarts`) | PASS |
| `periodic_snapshot_recovery_survives_a_second_snapshot_generation` | PASS |
| `concurrent_pollables_survive_worker_replay` | PASS |
| `concurrent_pollables_adversarial_completion_order_survives_replay` | PASS |
| `timer_races_real_http_and_survives_worker_replay` | PASS |
| `timer_race_rep_reuse_across_many_sequential_cycles` | PASS |
| `rpc::counter_resource_test_{1,2,3,5}` (+ `_with_restart` variants) | PASS |
| All round 8-10 synthetic tests (`atomic_double_ready_{call,rpc_call}_survives_cold_replay_after_suspend`, `sequential_atomic_rpc_{region,call_and_suspend_single_invocation{,_n4}}_survives_cold_replay_after_suspend`) | PASS |
| `atomic_rpc_call_across_periodic_snapshot_survives_cold_replay` (this round's new test) | PASS |

**Not run** (pre-existing environment gaps in this worktree, unrelated to this fix — the components
were never built here): `ts_default_json_snapshot_recovery` and its multi-restart variant, and
`wasi::snapshot_replay_handles_io_poll_ready_rep_mismatch` (needs the TS SDK's `agent-sdk-ts`
component and the `http-tests` component respectively; both require infrastructure — npm/node
deps in one case — outside this fix's scope to set up). Given this fix is entirely in engine-level,
language-agnostic Rust code (`durable_host/mod.rs`), the Rust-side coverage above (which exercises
the identical `PrivateDurableWorkerState`/`pollable_seq` machinery) is sufficient evidence of
correctness; the TS-specific tests exercise a different snapshot *content* mechanism, not the
counter-recovery logic this fix touches.

## Files changed

| File | Change |
|---|---|
| `golem-worker-executor/src/durable_host/mod.rs` | `recover_next_pollable_seq()`; `new()` calls it when `last_snapshot_index` is `Some`; updated `pollable_seq`/`next_pollable_seq`/`pollable_seq()` doc comments |
| `golem-worker-executor/tests/rpc.rs` | New test `atomic_rpc_call_across_periodic_snapshot_survives_cold_replay` |
| `test-components/agent-rpc/golem-it-agent-rpc-rust/src/lib.rs` | `RpcCaller` opts into snapshotting (`snapshotting = "enabled"` + `Serialize`/`Deserialize` on `RpcCallerImpl`) so the new test can exercise a real snapshot boundary |
