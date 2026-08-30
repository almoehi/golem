# Finding B fix design (v2) — poll() replay must defer to N stray same-batch ready() entries

Status: **IMPLEMENTED, approved, and committed.** See "Implementation notes" at the end of this
document for what actually shipped, exact diffs from this design, and the falsification/
regression-sweep results. v2 superseded v1 (single-stray-entry design) per explicit direction:
the fix must generalize to an arbitrary number of batched pollables, any number of which may have
resolved out of the guest's structural check order — not be bounded to or validated only for
2-pollable batches. v2 also resolved/bounded the three open questions raised on v1 — see §5.

Seed: Tenth capture (`/Users/hannes/work/golem/oplog-backups/README.md`), live trap on `main`,
`WorkerAgent(...,"1f93ff64-...@scene_backdrops")`, retry from oplog index 834, `reps: [20, 24]`.

## 1. Root cause (unchanged from v1, restated)

`ready()`'s replay (`try_get_oplog_entry`, seq-predicate) correctly rejects an entry that doesn't
belong to the pollable being asked about (seq mismatch) and synthesizes `false`, leaving the
entry un-consumed. `poll()`'s replay path (`Durability::replay()` → strict, positional,
unconditional `HostCall` consumption) has no equivalent identity check — it treats whatever
`HostCall` entry is next as if it must be its own `IoPollPoll` entry. When a batch member's
confirmed-`true` entry gets recorded (live) before the guest's replayed structural check order
reaches it, this crashes: `expected io::poll::poll, got io::poll::pollable::ready`.

## 2. Question 3 resolved: the guest-level trigger, with source-level confirmation

**Finding, with high confidence, grounded in actual source (not speculation):** the mechanism is
`std::collections::HashMap`'s per-process-randomized iteration order, inside the WASI async
reactor that bridges "await a promise representing an in-flight WASI operation" to real
`poll()`/`ready()` host calls.

Confirmed directly from `wstd` 0.6.5's reactor (vendored at
`~/.cargo/registry/src/.../wstd-0.6.5/src/runtime/reactor.rs`) — and confirmed that `wstd` is the
actual runtime in use, via a real WASM backtrace from this investigation's own round-15 test
failure (`<wstd[...]::runtime::reactor::Reactor>::spawn_unchecked` appears in the trap backtrace):

```rust
struct InnerReactor {
    pollables: Mutex<Slab<Pollable>>,
    wakers: Mutex<HashMap<Waitee, Waker>>,   // <-- std HashMap, default (randomized) hasher
    ready_list: Mutex<VecDeque<Runnable>>,
}

fn check_pollables<F>(&self, check_ready: F) {
    let wakers = self.inner.wakers.lock().unwrap();
    ...
    for (waitee, waker) in wakers.iter() {           // <-- iteration order is NOT insertion order,
        indexed_wakers.push(waker);                  //     not dispatch order, not deterministic
        targets.push(&pollables[pollable_index.0]);  //     across process instances
    }
    let ready_indexes = check_ready(&targets);        // -> wasip2::io::poll::poll(targets)
    let ready_wakers = ready_indexes.into_iter().map(|index| indexed_wakers[index as usize]);
    for waker in ready_wakers {
        waker.wake_by_ref()                           // <-- wake ORDER is hashmap-order-dependent
    }
}
```

Each pollable an async task is waiting on registers a `(Waitee, Waker)` pair into this `HashMap`
(`Reactor::ready()`, called from each task's own `WaitFor::poll()` the first time it's polled and
finds itself not-yet-ready). When multiple tasks are simultaneously pending and the reactor blocks
(`block_on_pollables` → `check_pollables`), **all currently-pending pollables get batched into one
`wasip2::io::poll::poll()` call together** — this is the direct, textbook mechanism producing a
batched multi-pollable `poll()` from what the JS/Rust-level code expresses as several independent
`await`s. Critically: `wakers.iter()`'s order depends on `HashMap`'s default `RandomState` hasher,
which is seeded from OS entropy fresh per process — the LIVE process instance and any REPLAY
process instance (a fresh wasmtime component instantiation, confirmed as the shape of both the
Ninth and Tenth captures) get **independently randomized seeds**, so the SAME set of pending
`(Waitee, Waker)` pairs iterates in a **different order** across the two runs. This directly drives:
(a) the order of `targets` fed into the shared `poll()` call, and (b) — the part that matters most
— the order `waker.wake_by_ref()` is called for however many pollables `poll()` found ready at
once, which determines the order those tasks get re-queued into the FIFO `ready_list` and
subsequently re-polled, i.e. the order their own individual `ready()` host calls fire. This is a
genuine, real, per-process source of non-determinism in the guest's structural check order,
entirely independent of WASM bytecode determinism (the bytecode IS deterministic; the *value fed
into it* — HashMap iteration order — is not, because it depends on process-local entropy that
Golem's replay model has no mechanism to reproduce).

**Confidence level:** this is confirmed with certainty for `golem-rust` (the Rust SDK used by this
investigation's own test components — the backtrace proves it). It is not directly confirmed for
`@golemcloud/golem-ts-sdk`'s compiled WASM shim (no vendored Rust source was found in
`node_modules/@golemcloud/golem-ts-sdk` — only a prebuilt `wasm/` artifact) — but `wstd` is the
Golem ecosystem's standard "bridge an async Rust runtime to raw WASI 0.2" crate, used for
precisely this problem, and it would be surprising for the TS SDK's own shim to solve the
identical problem differently. Treat this as a strong, checkable, well-reasoned hypothesis, not
100%-certain bytecode-level proof.

**This also explains why rounds 8–15 never reproduced Finding B**: every synthetic round drove
`golem_rust::wasip2::io::poll::poll`/`ready` **directly**, by hand, in fixed Rust structural order
— entirely bypassing `wstd`'s `Reactor`/`HashMap`-keyed waker registry. None of those tests ever
went through an actual Task/Waker-based executor with a HashMap-backed pending set, so there was
no source of reordering for them to hit, no matter how faithfully they reproduced batch shape,
timing, or prior history. A synthetic reproduction that *does* route through `wstd`'s real
`block_on`/`Reactor` (e.g. using `agent_implementation`'s generated async dispatch, or `futures_lite`
combinators like round 15's own `futures_concurrency::future::FutureGroup`, rather than manual
`WasmRpc`/raw `io::poll` calls) is the one remaining, plausible way to get an integration-level
repro — noted as a follow-up in the test plan (§5b), not attempted in this design.

**Confirmed app-level trigger, video-harness source:** `LlmClient.runOneRound()`'s Phase A
(`video-harness/src/llm/client.ts:1293-1312`) fires every `allowBatch`-marked tool call's dispatch
**without awaiting between them** — `this.executeCall(call, dispatch).then((r) => {...})`, no
`await` before the loop's next iteration — genuinely N-way concurrent, N bounded only by how many
`allowBatch` calls the LLM's `tool_calls` response contains in one turn (no fixed cap in the code).
`add_artifact_file` is confirmed `allowBatch: true`
(`video-harness/src/worker/worker-agent.ts:1772`), and its own doc comment
(`worker-agent.ts:1750-1751`) gives a concrete example matching production's naming almost
exactly: *"a single `wfOutputPort` has been produced by more than one `wf_xxx` call... e.g. 4 scene
plates through the same workflow/port"* — i.e. the LLM can legitimately register 3-4+ output files
in one turn, each potentially involving its own cross-agent RPC/lookup. Phase B
(`client.ts:1317-1344`) then collects results via `for (const call of calls) { ... await
batchPending.get(callSig) ... }` — **fixed original-call-order**, not real-completion-order. This
is exactly the app-level shape that feeds N independently-dispatched, concurrently-outstanding
WASI-backed promises into `wstd`'s shared reactor at once, with no guarantee their real completion
order matches the array order Phase B awaits them in — precisely the condition the `HashMap`
mechanism above turns into a live/replay divergence.

**Scope note (not addressed by this fix, flagged for follow-up):** the same `wstd` reactor
mechanism is generic — it isn't specific to `io::poll`/RPC futures, it's used for *any* WASI
pollable-based wait, including HTTP fetch()'s input/output stream pollables. video-harness has at
least one other confirmed N-way-concurrent-fetch pattern:
`download-manager.ts:138` (`Promise.all(batch.map((item) => this.fetchBytes(item.url)))`,
inside one `atomically()` region). The comment there (`download-manager.ts:122-124`) claims this is
safe because *"Golem replays concurrent WASI calls within one atomically() region correctly
(positional IoPollReady matching)"* — that justification predates this investigation's own
seq-tagging fix and may itself be describing the now-superseded pre-Bug-2-fix model; whether
HTTP's own replay path (`HttpTypesOutgoingBodyStreamCheckWrite`/similar) has an equivalent
per-resource identity check or is vulnerable to the same class of divergence as `poll()` was is
**not evaluated here** — flagging as a candidate follow-up investigation, out of scope for this
fix (which is scoped to `io::poll::poll`/`io::poll::pollable::ready` specifically, matching the
Tenth capture).

## 3. Questions 1 & 2 resolved: general-N design, no batch-size assumption

The v1 design (single non-consuming peek, defer to the guest's natural retry) does not extend
cleanly to N>1 simultaneous strays — you cannot "peek ahead" past an already-peeked entry without
either a new peek-at-offset primitive (risky: this investigation does not have full certainty
about `last_replayed_index`'s exact bookkeeping outside the already-proven `try_get_oplog_entry`
path, and hand-rolling new buffer arithmetic risks a subtle, hard-to-verify bug in exactly the
class of code this fix needs to get right) or actually consuming entries as they're found.

**v2 design: consume strays for real, cache their answer, let `ready()` consult the cache.**
Reuses `try_get_oplog_entry` — the existing, proven primitive — as the *only* oplog-manipulation
building block; no new peek/offset primitive.

**New replay-only state** (`PrivateDurableWorkerState`, `durable_host/mod.rs`):

```rust
/// REPLAY-ONLY cache of pollable seqs whose IoPollReady(seq)=<result> entry was consumed by a
/// DIFFERENT poll() call's replay logic, because it appeared in the oplog before the guest's
/// replayed structural check order reached that pollable's own ready() call (see Tenth capture /
/// FINDING_B_FIX_DESIGN.md §2 for why this ordering divergence is real: wstd's Reactor batches
/// concurrently-pending pollables via a HashMap-keyed waker set whose iteration order is
/// per-process-randomized, not reproducible across live vs. a fresh replay instance). Always
/// empty on the live path — only poll()'s replay branch ever populates it.
pre_resolved_pollable_ready: HashMap<u32, bool>,
```

```rust
/// Records that `seq`'s IoPollReady confirmation was consumed early, by a poll() call's replay,
/// on behalf of a pollable whose own ready() call hasn't replayed yet.
pub fn record_pre_resolved_pollable_ready(&mut self, seq: u32, ready: bool) {
    self.pre_resolved_pollable_ready.insert(seq, ready);
}

/// Takes (removes) a pre-resolved answer for `seq`, if poll()'s replay already consumed its
/// entry on this pollable's behalf. Consulted by ready()'s replay BEFORE its own
/// try_get_oplog_entry lookup — if present, the oplog has nothing left to find for this seq
/// (already consumed), so this is the only remaining source of the answer.
pub fn take_pre_resolved_pollable_ready(&mut self, seq: u32) -> Option<bool> {
    self.pre_resolved_pollable_ready.remove(&seq)
}
```

(`pollable_seq_if_assigned` from v1 is retained unchanged — still needed to build the batch's seq
set without perturbing assignment order.)

**`Host::poll`'s replay branch** — loop instead of single peek, using ONLY `try_get_oplog_entry`:

```rust
} else {
    // REPLAY. batch_seqs: only reps that already have an assigned seq can possibly own a
    // stray entry — pollable_seq is assigned unconditionally at the top of every ready() call
    // (live or replay), and per wstd's Reactor (Reactor::ready(), called from every task's
    // first WaitFor::poll()), a pollable cannot become part of a batched poll() at all until
    // its own ready() has been called at least once (registering it in `wakers`) — so by
    // construction every batch member here should already have a seq. (This is also the
    // resolution to "what if a batch member was never ready()-checked yet": per this
    // reactor architecture it's very likely unreachable — see §3 discussion below. If it DOES
    // happen, pollable_seq_if_assigned returns None, the member can't match any stray-entry
    // predicate below, and replay falls through to the existing crash path — no worse than
    // today, just not specifically fixed for that sub-case.)
    let batch_seqs: HashSet<u32> = in_
        .iter()
        .filter_map(|r| self.state.pollable_seq_if_assigned(r.rep()))
        .collect();

    loop {
        let peeked = self
            .state
            .replay_state
            .try_get_oplog_entry(|entry| {
                matches!(
                    entry,
                    OplogEntry::HostCall { function_name: HostFunctionName::IoPollPoll, .. }
                ) || matches!(
                    entry,
                    OplogEntry::HostCall {
                        function_name: HostFunctionName::IoPollReady,
                        durable_function_type: DurableFunctionType::ReadLocalPollable(seq),
                        ..
                    } if batch_seqs.contains(seq)
                )
            })
            .await?;

        match peeked {
            // Genuine poll() entry — decode + return its response. Identical semantics to
            // today's durability.replay(self) happy path (same entry shape, same decode).
            Some((_idx, entry @ OplogEntry::HostCall {
                function_name: HostFunctionName::IoPollPoll, ..
            })) => break Ok(decode_poll_response(entry)?),

            // A stray same-batch ready() confirmation, consumed for real (try_get_oplog_entry
            // has now durably advanced past it — this is NOT recoverable from the oplog a
            // second time) and cached so the owning pollable's own later ready() call still
            // gets the right answer. Keep looping — there may be more strays, or poll()'s own
            // entry, still ahead.
            Some((_idx, OplogEntry::HostCall {
                function_name: HostFunctionName::IoPollReady,
                durable_function_type: DurableFunctionType::ReadLocalPollable(seq),
                response, ..
            })) => {
                let payload = decode_poll_ready_response(response).await?;
                trace!(
                    agent_id = %self.owned_agent_id,
                    seq,
                    result = ?payload.result,
                    "POLLCALL_TRACE poll() REPLAY consumed stray same-batch ready() entry, caching"
                );
                self.state.record_pre_resolved_pollable_ready(
                    seq,
                    payload.result.unwrap_or(false),
                );
                continue;
            }

            // Neither shape matched — existing crash path, unchanged, for genuinely
            // unexpected/unrelated entries.
            _ => break Ok(durability.replay(self).await?),
        }
    }
};
```

(`decode_poll_response`/`decode_poll_ready_response` are factored-out helpers for the existing
payload-download-and-decode steps `Durability::replay_raw()` and `ready()`'s own replay branch
already perform respectively — no new decoding logic, just reused in a new call site. Exact
factoring is an implementation detail for the coding pass, not a design decision.)

**`HostPollable::ready`'s replay branch** — one new check at the top, before the existing
`try_get_oplog_entry` call:

```rust
if let Some(pre_resolved) = self.state.take_pre_resolved_pollable_ready(pollable_seq) {
    trace!(
        agent_id = %self.owned_agent_id, rep = pollable_rep, seq = pollable_seq,
        result = pre_resolved,
        "POLLREADY_TRACE ready() REPLAY using answer pre-resolved by an earlier poll() call"
    );
    return Ok(pre_resolved);
}
// ... existing try_get_oplog_entry-based logic, completely unchanged below this point ...
```

### Why this generalizes correctly to arbitrary N (question 1, resolved)

- The loop's only two "keep going" / "stop" conditions are per-entry, not per-batch-size: "is this
  poll()'s own entry" (stop, success) / "is this a stray for one of my batch members" (consume,
  cache, keep looping) / "neither" (stop, existing crash). N stray entries in a row is just N
  loop iterations — nothing in the mechanism assumes or checks a batch size, 2-pollable or
  otherwise. This directly satisfies "one general mechanism for 0 or more stray entries, in any
  order, for any batch size."
- No entry is ever double-counted or lost: `try_get_oplog_entry`'s matched-path durably advances
  the cursor exactly once per consumed entry (this is the same proven primitive every other
  replay path in this file already relies on); the cache is a strict one-shot hand-off consumed
  exactly once by `take_pre_resolved_pollable_ready`.
- Termination is guaranteed: each loop iteration consumes one real oplog entry via
  `try_get_oplog_entry`, so the loop is bounded by the (finite) distance to the next
  non-matching-or-genuine entry; running off the end of the oplog surfaces as the same
  "missing oplog entry" error `internal_get_next_oplog_entry` already produces today, not an
  infinite loop.

### Why question 2 (unassigned-seq batch member) is very likely unreachable, not a separate case

Per `wstd`'s `Reactor::ready()` (called from every task's `WaitFor::poll()` — the very first time
any task representing a pending WASI wait gets polled, it calls the raw `ready()` host call
directly): a pollable only gets registered into `self.inner.wakers` (and therefore only becomes
eligible to be swept into a *batched* `poll()` call at all) *after* its own `ready()` has already
returned `false` at least once. Since `pollable_seq(rep)` is assigned unconditionally at the top
of every `ready()` call (live or replay) regardless of the boolean result, this means: **by the
time a pollable can appear inside a batched `poll()`'s `in_` argument at all, it must already have
had `ready()` called on it at least once** — so `pollable_seq_if_assigned` should always find an
assigned seq for every genuine batch member, on both live and replay, independent of the
HashMap-reordering issue (that issue affects *which* pollable gets checked *when*, not *whether*
each one gets its initial optimistic check at all before ever entering a shared `poll()`). This
isn't a 100%-certain proof (this investigation has not read `wstd`'s full `block_on` driver loop,
only `reactor.rs`, and has not confirmed golem-ts-sdk's shim follows the identical pattern), but
it's a source-grounded, checkable argument, and — critically — **the design does not depend on
it being true**: if it's wrong and an unassigned-seq stray does occur, this fix's loop simply
falls through to today's existing crash for that one sub-case, which is strictly no worse than
current behavior. Question 2 is therefore not a separate mechanism needing its own design — it's
the same general loop, and its "unmatched → existing crash" fallback already covers this case
safely (if not helpfully) either way.

## 4. Test plan (updated for the general design)

### 4a. Unit tests — N=1, N=2, N=3 strays, out-of-order among themselves, seeded from the Tenth capture's shapes

Same `MutableBatchOplog`-based approach as v1 (`replay_state.rs` test module), extended:

1. **`poll_replay_consumes_single_stray_and_finds_poll_entry`** — oplog: `[IoPollReady(seq=154),
   IoPollPoll]` (Tenth capture's literal 2-entry shape, but now testing the general loop rather
   than a single peek). Batch = `{150, 154}`. Assert: no crash; `pre_resolved_pollable_ready`
   contains `{154: true}` afterward; poll()'s own response is returned from the second (genuine)
   entry unchanged.
2. **`poll_replay_consumes_multiple_strays_before_own_entry`** — oplog:
   `[IoPollReady(seq=154), IoPollReady(seq=161), IoPollPoll]`, batch = `{150, 154, 161, 172}`
   (4-pollable batch, 2 strays, in a specific order). Assert: no crash; cache contains
   `{154: true, 161: true}`; a subsequent `ready(rep-for-161)` call (via a second test step) reads
   `true` from the cache without touching the oplog further; a subsequent `ready(rep-for-150)`
   call (never resolved, no stray recorded for it) falls through to the normal
   `try_get_oplog_entry` path and correctly synthesizes `false`.
3. **`poll_replay_stray_order_independent`** — same as #2 but with the two stray entries in the
   OPPOSITE order (`161` before `154`) — assert identical end state, proving the mechanism doesn't
   depend on which stray arrives first.
4. **`poll_replay_still_crashes_on_genuinely_unrelated_entry`** — oplog:
   `[some unrelated HostCall entry, e.g. HttpTypesOutgoingBodyStreamCheckWrite]`, batch =
   `{150, 154}`. Assert: existing crash path still fires (`durability.replay()`'s
   `validate_oplog_entry` mismatch) — proving the fix's scope is exactly "stray same-batch
   ready() entries," not "swallow any mismatch."
5. **`pollable_seq_if_assigned` unit tests** (as in v1): `None` for unseen rep, `Some(seq)` after
   assignment, and — critically — confirms it never mutates `next_pollable_seq`.

### 4b. Best-effort integration-level probe, revised to route through a real Task/Waker executor

v1's proposed integration probe (reversed structural-order-vs-speed, driven by hand-authored
`WasmRpc`/raw `io::poll` calls) is now understood to be very unlikely to reproduce anything, per
§2's finding: raw `io::poll` calls bypass `wstd`'s `Reactor`/HashMap entirely, so there is no
source of reordering for such a test to hit, regardless of how the pollables' relative speed is
constructed. A test with any chance of exercising the *actual* mechanism needs to route through a
genuine Task/Waker-based concurrent-await pattern (e.g. `futures_concurrency::future::FutureGroup`
or `futures_lite::future::zip`/`race`, as `wstd`'s own reactor.rs test module already does for ITS
OWN unit tests at lines 337-380 and 433-458) — spawning 3+ genuinely concurrent RPC-await tasks
and letting `wstd`'s real reactor batch them. Flagging as a valuable follow-up, not attempted in
this design pass — even if built, since HashMap seeding is randomized per-process, such a test
could only be a *probabilistic* repro (rerun until the seeds happen to produce divergent orders
on a captured live oplog vs. a fresh replay process), which is a fundamentally different
falsification shape than every prior round's deterministic repro — worth a separate design
discussion if pursued, not bundled into this fix's landing criteria. **The unit tests in §4a are
what actually gate correctness of this fix; they are not optional.**

### 4c. Falsification (bidirectional)

- Before the fix: unit tests #1–3 in §4a fail (crash) against current `poll()` replay code.
- After the fix: all of §4a passes; all four pre-existing `try_get_oplog_entry_*` tests pass
  unmodified; full `durable_host::` unit suite (64 tests as of round 15) passes.

### 4d. Full regression sweep

Same as v1 §3d: rounds 8–15's integration tests, the Eighth capture's fix regression test
(`atomic_rpc_call_across_periodic_snapshot_survives_cold_replay`), noting the pre-existing
`WorkerActivator` test-infra flake (`ROUND_FIFTEEN_FINDINGS.md`) is unrelated and may need a rerun.

## 5. Status of the three open questions

1. **3+-pollable batches** — resolved. The design is a loop over the same primitive
   (`try_get_oplog_entry`), with no batch-size assumption anywhere; N strays in any order is N
   loop iterations. Not bounded to or specially-cased for N=2.
2. **Unassigned-seq batch member** — bounded, not fully proven. Same general mechanism (no
   separate design needed); very likely structurally unreachable given `wstd`'s confirmed
   "ready() called before a pollable can join a batch" invariant, but not 100%-verified against
   golem-ts-sdk's actual shim source. If wrong, falls through to today's crash — strictly no
   regression either way.
3. **Guest-level trigger** — resolved to a well-reasoned, source-grounded hypothesis (not
   bytecode-level certainty): `wstd`'s `HashMap`-keyed waker registry, confirmed in use by
   golem-rust via an actual backtrace from this investigation, produces per-process-randomized
   wake/check ordering whenever multiple WASI-backed promises are concurrently pending — and
   video-harness's `LlmClient.runOneRound()` Phase A/B (`client.ts:1279-1344`, specifically
   `allowBatch` tools like `add_artifact_file`) is a confirmed, unbounded-N, real-world trigger
   for exactly that condition. Also surfaced a same-mechanism scope concern
   (`download-manager.ts`'s concurrent-fetch pattern) flagged as a follow-up, not fixed here.

## 6. Recommendation

Proceed to implementation per §3/§4 above. The remaining uncertainty (question 2's "very likely
but not proven" status, and the HTTP-path scope note in §2) are both bounded such that getting
them wrong costs nothing beyond "this specific fix doesn't help that sub-case" — neither can turn
this change into a correctness regression relative to today. Given that, and that questions 1 and
3 are now resolved with concrete, source-grounded reasoning, this feels ready to build — happy to
take one more look at the code itself once written, given the standing bar of falsify
bidirectionally + full regression sweep before calling it done.

## 7. Implementation notes (post-approval)

Approved and implemented essentially as designed in §3, with one refinement made during coding:

- **`pollable_seq_if_assigned`** (`durable_host/mod.rs`, alongside `pollable_seq`/
  `clear_pollable_seq`): implemented exactly as designed — a non-mutating `HashMap::get().copied()`.
- **`pre_resolved_pollable_ready: HashMap<u32, bool>`** field on `PrivateDurableWorkerState`,
  plus `record_pre_resolved_pollable_ready`/`take_pre_resolved_pollable_ready`: implemented
  exactly as designed.
- **`HostPollable::ready`'s replay branch** (`io/poll.rs`): one new check at the top —
  `take_pre_resolved_pollable_ready(pollable_seq)` — exactly as designed.
- **`Host::poll`'s replay branch**: refined from the design's inline loop into a standalone,
  directly-unit-testable free function, `consume_stray_ready_entries(replay_state, oplog,
  batch_seqs, record_stray)`, called from `Host::poll`. This is a pure refactor of the designed
  logic (same predicate, same `try_get_oplog_entry`-only mechanism, same cache hand-off) — the
  motivation was testability: extracting it let the regression tests exercise the exact
  production algorithm directly against a crafted oplog (`MutableBatchOplog`, mirroring
  `replay_state.rs`'s own test-double pattern) without needing a full `DurableWorkerCtx`/wasmtime
  store. `Host::poll` now: builds `batch_seqs` from `in_`, calls the extracted function collecting
  `(seq, ready)` pairs, traces + caches each one via `record_pre_resolved_pollable_ready`, then
  falls through to the unchanged `durability.replay(self).await?` for whatever's left — matching
  the design's control flow exactly, just with the scanning loop factored out.

### Test plan — what was actually built (`io/poll.rs`'s own `#[cfg(test)] mod tests`, not
`replay_state.rs` as §4a first assumed — the extracted function needed a `download_payload`-
capable oplog double, which fits better colocated with `consume_stray_ready_entries` itself):

1. `consumes_single_stray_matching_tenth_capture_shape` — the Tenth capture's exact
   `[IoPollReady(seq=154)=true, IoPollPoll]` shape (154 = rep 24's seq); asserts the stray is
   consumed+reported and poll()'s own entry survives untouched.
2. `old_unconditional_consume_would_have_hit_the_production_mismatch` — the bidirectional
   "before" half, run against the *same* crafted oplog: reproduces the OLD mechanism directly
   (`ReplayState::get_oplog_entry()`, i.e. unconditional consumption — what
   `Durability::replay_raw()` used) and asserts it genuinely finds `IoPollReady` where
   `IoPollPoll` was expected — confirming this crafted scenario is a real reproduction of the
   production trigger, not merely a shape that happens to exercise the new code path. (A literal
   integration-level "revert the engine change, run a component, watch it crash" cycle is not
   achievable for this bug — see §4a/§4b's reasoning, reconfirmed during implementation: hand-
   authored deterministic Rust test-component code cannot produce a live/replay call-order
   divergence, since both runs execute identical instructions given identical replayed answers;
   only `wstd`'s `HashMap`-seeded reactor can, and its seed isn't test-controllable. This
   same-oplog before/after pair is the most direct, honest reproduction achievable.)
3. `consumes_multiple_strays_before_genuine_poll_entry` — N=2 strays (seq 154, then 161) in a
   4-pollable batch, followed by poll()'s own entry — proves generalization past N=1.
4. `consumes_multiple_strays_regardless_of_order` — same as #3 with the two strays swapped —
   proves order-independence.
5. `ignores_ready_entry_for_seq_outside_the_batch` — a `ready()` entry for a seq NOT in the
   batch is left completely untouched — proves the fix's scope is exactly "same-batch strays,"
   not "swallow anything unexpected."
6. `no_op_when_next_entry_is_already_genuine_poll_entry` — the common happy path (no strays at
   all) — the function does nothing, entry stays for the caller.

All 6 pass. Falsification: test #2 stands as the permanent "before" proof (it doesn't depend on
the fix existing or not — it directly demonstrates the OLD mechanism's mismatch on this exact
byte pattern); tests #1/#3–6 are the "after" proofs, and were confirmed to fail appropriately
during development before the implementation was complete (the very first version of test #1,
run before `consume_stray_ready_entries` existed, was a compile error — the strongest possible
"before" signal: the assertion is inexpressible without the fix).

### Regression sweep results

- Full `--lib` suite: **465 passed, 0 failed**.
- `durable_host::` unit suite (includes the pre-existing `try_get_oplog_entry_*` tests,
  unmodified): **64 passed, 0 failed**, both before and after this change (checked at each stage).
- Full `rpc.rs` integration test file (all tests not requiring the TS `agent_rpc` component,
  which is not built in this worktree — a pre-existing, unrelated environment gap present since
  round 8): **24/24 passed** (rounds 8–15's 10 poll/ready-specific tests + 14 general RPC tests
  — resource sharing, cancellation, ephemeral invocation, etc.).
- Full `durability.rs` integration test file (same TS-component caveat): **16/16 passed**,
  including `concurrent_pollables_adversarial_completion_order_survives_replay` — a pre-existing
  test (not part of this investigation's own rounds) whose name suggested direct relevance;
  confirmed still passing.
- The Eighth capture's regression test, `atomic_rpc_call_across_periodic_snapshot_survives_cold_replay`:
  **passed**.
- One pre-existing, unrelated flake: `sequential_atomic_double_ready_rpc_calls_same_target_n4_survives_cold_replay`
  fails when run alone with `Runtime error: WorkerActivator is disabled, not creating instance`
  (a `LazyWorkerActivator` weak-reference race in the test harness, documented in
  `ROUND_FIFTEEN_FINDINGS.md`'s "Aside") — reproduces identically with or without this round's
  changes (re-confirmed); passes reliably when run alongside sibling tests. Not a regression.
- `http.rs`, `wasi.rs`, and other test files requiring components not built in this worktree
  (`golem_it_http_tests_release`, websocket/blobstore/rdbms components) were not run — this
  worktree has never had those built, in any round of this investigation; only
  `agent_rpc_rust`, `host_api_tests`, and `agent_counters` are available locally.

## 8. v3/v4 — generalizing beyond `IoPollReady` (Eleventh capture), fully resolved

Status: **IMPLEMENTED, approved, and committed.** See §11 ("v4 implementation notes") for what
actually shipped, exact diffs from this design, and the falsification/regression-sweep results.
Larger scope than v1/v2: three call sites (`poll()`, `get()`, HTTP incoming body-stream reads), a
new `DurableFunctionType` variant, and a new snapshot-recovered counter. §8.2's first draft
(`begin_index` as the RPC identity) was found unsound during this revision's own re-verification
and replaced (§8.2.1-8.2.2) — see §8.10 for the full resolution summary of all three original
open questions plus the HTTP investigation.

### 8.1 What the Eleventh capture showed

Live-verifying `420d02119` against `main` again: the shipped mechanism worked exactly as
designed —

```
poll(reps=[23,22]) ... peek(961) = IoPollReady(ReadLocalPollable(185))   <- rep 23's, not rep 22's
POLLCALL_TRACE poll() REPLAY consumed stray same-batch ready() entry, caching, seq: 185, ready: true
```

— but the *next* entry was a **different stray entry type** the fix doesn't recognize:

```
peek(962) = HostCall { function_name: GolemRpcFutureInvokeResultGet, durable_function_type: WriteRemote }
CRASH: expected io::poll::poll, got golem::rpc::future-invoke-result::get
```

Entry 962 is the completion-fetch step of a **different, concurrently-dispatched
`WorkflowAgent.run()` call** — a sibling RPC invocation, not a sibling pollable — that happened
to complete and get recorded before the current `poll()` call's own `IoPollPoll` entry. Same root
mechanism as Finding B (real completion order of concurrently in-flight operations doesn't have
to match the guest's replayed structural check order), but this time the racing operations are
RPC calls themselves (`golem::rpc::future-invoke-result::get`), not `io::poll` pollables — and
the shipped fix only recognizes `IoPollReady` strays.

### 8.2 Is there already a shared identity mechanism across entry types? (REVISED — see 8.2.1)

No. `ReadLocalPollable(seq)` is a pollable-specific `DurableFunctionType` tag; RPC's
`future-invoke-result::get()` entries are tagged plain, untagged `DurableFunctionType::WriteRemote`
— **no identity at all**, and its replay path (`HostFutureInvokeResult::get`,
`wasm_rpc/mod.rs:874`) uses the exact same unconditional `get_oplog_entry!(replay_state,
OplogEntry::HostCall)` macro `poll()`'s replay used before the Finding B fix. This needs a new
identity to be introduced, not reused.

#### 8.2.1 REVISED: the `begin_index` idea (as first proposed) is UNSOUND — traced and rejected

The first draft of this section proposed using the RPC call's own `begin_index: OplogIndex`
(already tracked in `FutureInvokeResultState`) as its identity, specifically to avoid
`pollable_seq`'s counter-recovery complexity. Tracing the actual computation to the same depth as
the original `pollable_seq`/Eighth-capture investigation (per explicit request) found a concrete
flaw that rules this out.

**Where `begin_index` actually comes from** (`async_invoke_and_await`, `wasm_rpc/mod.rs:412-414`,
called unconditionally live or replay — no `is_live()` gate above it):

```rust
let begin_index = self.begin_function(&DurableFunctionType::WriteRemote).await?;
```

`begin_function` (`durable_host/mod.rs:1192`) has two branches depending on
`assume_idempotence`. **Confirmed**: `assume_idempotence` is hardcoded `true` at construction
(`durable_host/mod.rs:4292`, `PrivateDurableWorkerState::new()` — not a per-agent config this
engine exposes as configurable to `false` in practice), so `DurableFunctionType::WriteRemote`
calls with `assume_idempotence == true` always take the **second** branch
(`durable_host/mod.rs:1310-1322`), which does NOT write an explicit `BeginRemoteWrite` entry —
it computes a "current position" value directly:

```rust
let begin_index = if self.state.replay_state.is_live() {
    self.state.oplog.current_oplog_index().await          // LIVE
} else {
    self.state.replay_state.last_replayed_non_hint_index() // REPLAY
};
```

**The flaw**: `current_oplog_index()` (live) reflects the position of the **last entry written,
of any kind, including hints**. `last_replayed_non_hint_index()` (replay) — per its own name and
the `begin_function` comment directly above the code that uses it ("hint entries must be ignored
because they are nondeterministic") — **explicitly excludes hint entries**, tracking only the
last *non-hint* entry replay has consumed. `OplogEntry::is_hint()` includes real, routinely-fired
entry kinds — critically, `Log` (confirmed via `golem-common/src/base_model/oplog/mod.rs`'s
`hint: true` markers). video-harness's own dispatch loop calls `console.warn(...)` between tool
dispatches (`client.ts:1144`, `startSuspendingCall`'s `console.warn("tool call (start): ...")`) —
a `Log` entry landing between two sequential RPC dispatches is not a hypothetical edge case, it's
what the actual app code does on essentially every tool call.

**Concretely**: dispatch A's `begin_index` = X. A `Log` hint entry gets written (e.g. the next
tool call's own `console.warn`). Dispatch B's `begin_index`: **live** reads
`current_oplog_index()` = (position after the Log entry, since live counts everything written);
**replay** reads `last_replayed_non_hint_index()` = still X's neighborhood (the Log entry doesn't
advance it, by design). These are **different values for the same logical dispatch** — exactly
the kind of live/replay divergence this whole fix exists to eliminate, not one to introduce.

**Why this doesn't affect `begin_function`'s existing, legitimate use of this same formula**
(for `WriteRemoteBatched`/`WriteRemoteTransaction` retry-point tracking): that usage is **purely
self-referential** — the value is computed fresh, used only as "a reasonable place for *this same
replay run* to retry from," and never compared against an independently-recorded value from a
*different* execution. My use case is the opposite: persist a value during LIVE, recompute it
independently during a **later, separate REPLAY**, and require bitwise equality — a categorically
stronger guarantee the existing formula was never designed to provide, and (per the trace above)
does not provide.

#### 8.2.2 REVISED design: a dedicated counter, mirroring `pollable_seq`'s structure exactly (not `begin_index`)

Given the position-derived approach is unsound, and reconsidering the counter-based approach
this section originally argued against: `pollable_seq`'s *actual* problem (Eighth capture) was
narrower than "counters are unsafe" — it was specifically "a counter that resets to 0 on every
fresh instance construction is wrong when constructing from a snapshot with prior live history."
That failure mode is **already fully solved and proven** (`recover_next_pollable_seq`, the
Option 2 fix, falsified bidirectionally and shipped). Reusing the *identical pattern* — a new,
independent counter for RPC identity, with its own snapshot-embedded recovery mirroring
`recover_next_pollable_seq` exactly — carries no new *class* of risk, just more of the same,
already-validated mechanism. This is the corrected design:

**New fields on `PrivateDurableWorkerState`** (`durable_host/mod.rs`, structurally identical to
`pollable_seq`/`next_pollable_seq`, kept fully separate rather than merged into them — merging
would mean touching the already-shipped, already-proven pollable_seq mechanism for no benefit):

```rust
/// Maps a FutureInvokeResult's wasmtime resource rep to a logical, call-order-derived sequence
/// number — the RPC-call analog of `pollable_seq`. Assigned unconditionally (live or replay) the
/// first time a given rep is observed, at the top of `async_invoke_and_await` (the dispatch
/// call, which creates the resource — see 8.8.1 for why dispatch-time, not first-get()-call
/// time, is the correct assignment point). Kept as an entirely separate map/counter from
/// `pollable_seq` rather than merged into it, to avoid touching that already-shipped, already-
/// proven mechanism.
invoke_result_seq: HashMap<u32, u32>,
next_invoke_result_seq: u32,
```

`recover_next_invoke_result_seq()` mirrors `recover_next_pollable_seq()` exactly — reads
`next_invoke_result_seq` directly from the `Snapshot` oplog entry at construction time when
resuming from a snapshot, falls back to 0 for genesis replay. The `Snapshot` entry's `raw{}`
block gains a **second** new field (`next_invoke_result_seq: u32`, alongside the Option-2 fix's
`next_pollable_seq: u32`) — same mechanism, same file, same falsification bar as that fix.

**New `DurableFunctionType` variant**: `WriteRemoteConcurrent(u32)` — carries the assigned
`invoke_result_seq` value (a counter value, like `ReadLocalPollable(u32)`, **not** an `OplogIndex`
— correcting the earlier draft). `HostFutureInvokeResult::get`'s LIVE path tags its persisted
entry with this instead of plain `WriteRemote`.

**Backward compatible with already-persisted oplogs**, same reasoning as before: `get()`'s
existing REPLAY fallback (`get_oplog_entry!`, unconditional) is left completely unchanged as the
final step after stray-scanning (§8.4) — an old, untagged `WriteRemote` entry never matches the
new stray-recognition predicate, so it falls straight through to the unchanged unconditional
read, exactly as today.

### 8.3 The "my own identity" problem — why `get()` differs from `poll()`/`ready()`

`poll()` and `ready()` are naturally distinguishable by `function_name` (`IoPollPoll` vs.
`IoPollReady`) — `poll()`'s stray-scanner can never accidentally match `poll()`'s own entry,
because `poll()`'s own entries are a different function entirely. **RPC's `get()` doesn't have
this luxury**: every concurrently-dispatched sibling call's `get()` produces the *same*
`GolemRpcFutureInvokeResultGet` function_name — only the `WriteRemoteConcurrent(begin_index)` tag
tells two `get()` entries apart. A stray-scanner for `get()`'s replay must explicitly **exclude
its own `begin_index`** from what it treats as "a stray belonging to someone else," or it would
wrongly consume-and-cache its own genuine entry before ever reaching its own unconditional final
read.

### 8.4 The generalized mechanism

One shared, entry-type-agnostic primitive plus per-kind decode/cache logic at each call site —
NOT two hardcoded branches duplicated across `poll()` and `get()`.

**Shared scanning loop** (`ReplayState`, or a free function taking `&mut ReplayState` — same
`try_get_oplog_entry`-only building block as the shipped fix, generalized to a caller-supplied
predicate + callback, with zero knowledge of payload shapes):

```rust
/// Walks forward consuming (durably) zero or more oplog entries matching `is_stray`, handing
/// each one to `on_stray` for type-specific decode+cache, until an entry doesn't match. Returns
/// as soon as the caller's own genuine entry (or a real mismatch) is next — entirely
/// entry-type-agnostic; callers decide what "stray" means and what to do with one.
pub async fn consume_stray_entries(
    replay_state: &mut ReplayState,
    is_stray: impl Fn(&OplogEntry) -> bool,
    mut on_stray: impl FnMut(OplogIndex, OplogEntry),
) -> Result<(), WorkerExecutorError> {
    loop {
        match replay_state.try_get_oplog_entry(&is_stray).await? {
            Some((idx, entry)) => on_stray(idx, entry),
            None => return Ok(()),
        }
    }
}
```

**One call site, both kinds recognized** — this is where "`IoPollReady` and
`GolemRpcFutureInvokeResultGet` both fall out as instances" happens: both `poll()`'s and `get()`'s
stray predicates recognize *both* known kinds (broadened from v2's batch-scoped check — see
below), each decoded/cached via its own `PrivateDurableWorkerState` map:

```rust
fn is_known_stray(entry: &OplogEntry, state: &PrivateDurableWorkerState, exclude_invoke_result_seq: Option<u32>) -> bool {
    match entry {
        OplogEntry::HostCall {
            function_name: HostFunctionName::IoPollReady,
            durable_function_type: DurableFunctionType::ReadLocalPollable(seq),
            ..
        } => state.pollable_seq.values().any(|v| v == seq),

        OplogEntry::HostCall {
            function_name: HostFunctionName::GolemRpcFutureInvokeResultGet,
            durable_function_type: DurableFunctionType::WriteRemoteConcurrent(seq),
            ..
        } => Some(*seq) != exclude_invoke_result_seq
            && state.invoke_result_seq.values().any(|v| v == seq),

        _ => false,
    }
}
```

- `poll()` calls this with `exclude_invoke_result_seq: None` (poll() has no single RPC-call
  identity of its own to exclude — it isn't itself an RPC call).
- `get()` calls this with `exclude_invoke_result_seq: Some(my_seq)` (its own
  `invoke_result_seq_if_assigned(rep)`, mirroring `pollable_seq_if_assigned`).

**Broadened from v2's "my explicit batch" to "any currently-tracked identity."** v2 scoped
`poll()`'s stray recognition to `in_`'s own batch (`pollable_seq_if_assigned` per pollable in the
batch). Since `get()` has no equivalent "batch" to scope against (a single `this:
Resource<FutureInvokeResult>`, no list of siblings), and since *any* stray entry for *any*
currently-tracked identity (pollable or invoke-result) unambiguously belongs to *some* real,
still-live operation this instance is tracking — deferring+caching it is always correct
regardless of whether it happens to be in the caller's own batch. Dropping the batch-scoping
restriction is a **simplification that is also strictly more correct** (handles the general
case, not just same-batch races), not a loosening of safety: seqs/begin_indexes are never reused,
so a stray consumed-and-cached here can never be mismatched against a later, unrelated operation.

**New state on `PrivateDurableWorkerState`** (`durable_host/mod.rs`, alongside `pollable_seq`/
`pre_resolved_pollable_ready` — `invoke_result_seq`/`next_invoke_result_seq` themselves defined
in §8.2.2; no separate "tracked" set needed, `invoke_result_seq`'s own key/value maps already
serve that role, exactly mirroring how `pollable_seq` itself is both the assignment map and the
"currently tracked" set):

```rust
/// REPLAY-ONLY cache, symmetric with pre_resolved_pollable_ready: a stray
/// GolemRpcFutureInvokeResultGet entry's decoded result, consumed early by a DIFFERENT call's
/// stray-scan, cached here for the owning get() call to consult first. Keyed by
/// invoke_result_seq (not OplogIndex — see §8.2.2's correction).
pre_resolved_invoke_result: HashMap<u32, SerializableInvokeResult>,
```

`invoke_result_seq_if_assigned(rep)` / `clear_invoke_result_seq(rep)` mirror
`pollable_seq_if_assigned`/`clear_pollable_seq` exactly (non-mutating lookup; clear-on-drop so a
reused wasmtime rep gets a fresh seq).

**`HostFutureInvokeResult::get`'s replay branch**, restructured to match `ready()`'s
cache-then-scan-then-fallback shape:

```rust
} else {
    // my_seq: this FutureInvokeResult's own invoke_result_seq (assigned at dispatch time —
    // see §8.2.2 and §8.8.1 — so it's always already assigned by the time get() replays).
    let my_seq = self.state.invoke_result_seq_if_assigned(this.rep())
        .expect("invoke_result_seq assigned at dispatch time, before get() can be called");

    // 1. Check the cache first — a sibling's poll()/get() stray-scan may already have
    //    consumed my own entry on my behalf.
    if let Some(cached) = self.state.take_pre_resolved_invoke_result(my_seq) {
        /* use cached directly, same downstream handling as the decoded oplog case */
    } else {
        // 2. Scan past any stray entries belonging to OTHER tracked operations (excluding
        //    my own seq).
        consume_stray_entries(&mut self.state.replay_state, |e| is_known_stray(e, &self.state, Some(my_seq)), |idx, entry| { /* decode + cache by kind */ }).await?;
        // 3. Unchanged existing fallback — whatever's left must be mine (every other known
        //    identity has been filtered out) or a genuine, still-correctly-crashing mismatch.
        let (_, oplog_entry) = get_oplog_entry!(self.state.replay_state, OplogEntry::HostCall)?;
        /* unchanged decode from here down */
    }
};
```

**`Host::poll`'s replay branch**: unchanged shape from the shipped fix, just widen the predicate
(`is_known_stray` instead of the `IoPollReady`-only check) and add the `WriteRemoteConcurrent`
decode arm alongside the existing `IoPollReady` one in the `on_stray` callback.

### 8.5 `DurableFunctionType::WriteRemoteConcurrent(u32)` — touch points

Mirrors exactly the file list `ReadLocalPollable(u32)` originally touched (confirmed by grep —
same enum, same exhaustiveness-checked match sites):

- `golem-common/src/model/oplog/raw_types.rs`: new variant on the `DurableFunctionType` enum
  (`#[desert(transparent)]`, matching every other variant's attribute).
- `golem-worker-executor/src/durable_host/durability.rs`, `is_eligible_for_internal_retry`: new
  arm `DurableFunctionType::WriteRemoteConcurrent(_) => self.durable_execution_state.assume_idempotence`
  — identical retry classification to plain `WriteRemote` (it's the same kind of call, just
  identity-tagged).
- `golem-common/src/model/oplog/public_types.rs`: maps to `PublicDurableFunctionType::WriteRemote(Empty{})`
  — engine-internal tag stays invisible to the public projection, exactly like
  `ReadLocalPollable(_) => PublicDurableFunctionType::ReadLocal(Empty{})` does today.
- `golem-common/src/model/oplog/protobuf.rs`: maps to `WrappedFunctionType::WriteRemote` with
  `oplog_index: None` — same reasoning (confirmed inert/dead code path for this engine's own
  `Oplog`/`OplogService` implementations, per the Option 2 fix's own investigation of this exact
  file).
- `golem-worker-executor/src/durable_host/mod.rs:100-125` (the persist-nothing-zone /
  region-membership check `should_skip_to` feeds): likely needs no new arm at all — plain
  `WriteRemote` isn't explicitly matched there either (falls to `_ => false`), so
  `WriteRemoteConcurrent(_)` should fall to the same wildcard; confirm via the compiler's
  exhaustiveness checking during implementation, don't assume.
- **`Snapshot` oplog entry** (`golem-common/src/base_model/oplog/mod.rs`'s `raw{}` block for the
  `Snapshot` case, plus the same construction sites the Option 2 fix touched for
  `next_pollable_seq`: `golem-common/src/model/oplog/protobuf.rs`, `golem-worker-executor/src/
  model/public_oplog/mod.rs`, `.../public_oplog/wit.rs`, `golem-worker-executor/src/services/
  oplog/tests.rs`, `golem-worker-executor/src/worker/status.rs`, and the real write site
  `golem-worker-executor/src/worker/invocation_loop.rs`): a **second** new field,
  `next_invoke_result_seq: u32`, alongside `next_pollable_seq: u32` — same mechanism, same
  falsification bar (bidirectional: revert, confirm the analogous Eighth-capture-shaped trap
  reproduces for RPC identity across a periodic snapshot; restore, confirm it doesn't) as the
  Option 2 fix.
- Any other exhaustive `match self.function_type` / `match durable_function_type` site the
  compiler flags after adding the variant — same discovery process used for the Option 2 fix
  (`cargo check`-driven, not manual grep-and-hope).

### 8.6 Bidirectional cross-contamination — traced concretely, not assumed

Four directions to check (`ready()` and `get()` are the two "final, single-identity-match"
consumers; `poll()` and `get()`'s *unconditional* fallback are the two "could blindly swallow a
stray" consumers):

**`ready()` → can it be confused by a stray of any kind (pollable or RPC)? No — safe by
construction, no change needed.** `ready()`'s replay (`io/poll.rs`) uses `try_get_oplog_entry`
with a predicate that requires an *exact* `IoPollReady` function_name **and** exact seq match. Any
non-matching entry — whether a different pollable's `IoPollReady`, a `GolemRpcFutureInvokeResultGet`,
or anything else — simply fails the predicate, `try_get_oplog_entry` leaves it untouched, and
`ready()` synthesizes `false`. This is the exact mechanism that already made the *first* stray
(Tenth capture) not crash `ready()` itself — only `poll()`'s *unconditional* consumption crashed.
`ready()` can never accidentally consume something that isn't its own exact entry, for any entry
type, today or after this fix. No changes needed here, confirmed by re-reading the existing code
rather than assumed from the pollable case generalizing.

**`poll()` → confirmed vulnerable to both directions** (both already established): stray
`IoPollReady` (Tenth capture, shipped fix) and stray `GolemRpcFutureInvokeResultGet` (Eleventh
capture, this design). Both handled by the widened `is_known_stray` predicate (§8.4).

**`get()` → does it need to recognize stray `GolemRpcFutureInvokeResultGet` (sibling get()
calls)? Confirmed yes**, and specifically *why*: `get()`'s current replay
(`get_oplog_entry!(replay_state, OplogEntry::HostCall)`, `wasm_rpc/mod.rs:874`) matches **any**
`HostCall` variant — it does not check `function_name` at all (unlike `validate_oplog_entry`,
which `get()`'s path doesn't use — `get_oplog_entry!` only checks the outer `OplogEntry` variant).
A sibling `get()` call's own entry — same `GolemRpcFutureInvokeResultGet` function_name — would
be silently accepted by the macro, then fail only at the *response payload* decode step
(`match response { HostResponse::GolemRpcInvokeGet(...) => ..., other => Err(unexpected_oplog_entry(...)) }`)
— since both entries share the same response *shape* (`HostResponseGolemRpcInvokeGet`), this
wouldn't even necessarily error — it could **silently consume the wrong RPC call's result**, no
error at all. Confirmed vulnerable, and confirmed the failure mode is worse than a loud crash
when the two entries happen to be the same shape.

**`get()` → does it need to recognize stray `IoPollReady` (the reverse of the Eleventh capture's
direction)? Confirmed yes, traced concretely — not assumed.** Same reasoning: `get_oplog_entry!`
matches any `HostCall`, so a stray `IoPollReady` entry sitting where `get()`'s own entry is
expected would also be accepted by the macro. This time the response *shape* differs
(`HostResponsePollReady` vs. the expected `HostResponseGolemRpcInvokeGet`), so the existing
`match response { ..., other => Err(unexpected_oplog_entry(...)) }` arm *would* catch it and
error — loudly, unlike the sibling-`get()` case above, but still a crash `get()` shouldn't have
today. No live capture has shown this exact direction yet, but the code path is unconditional
regardless of which entry type shows up, so nothing about the mechanism is direction-specific —
confirmed vulnerable by tracing the actual macro and match arms, not inferred from symmetry.
`is_known_stray` already recognizes both kinds at every call site (§8.4), so this is covered by
the same design with no special-casing needed.

### 8.7 HTTP concurrent fetches — investigated, confirmed vulnerable, brought into scope

Traced `io/streams.rs`'s `HostInputStream::read` (representative of the whole family —
`blocking_read`, `skip`, `blocking_skip`, and the output-stream `check_write`/`write`/`flush`
variants all follow the identical shape) to the same depth as `poll()`/`get()`.

**HTTP body-stream calls already carry a per-request identity — unlike RPC's plain `WriteRemote`.**
`io/streams.rs:59-64`: `Durability::<HttpTypesIncomingBodyStreamRead>::new(self,
DurableFunctionType::WriteRemoteBatched(Some(begin_idx)))` — not the bare `WriteRemote` RPC's
`get()` used. `WriteRemoteBatched(Option<OplogIndex>)`'s own doc comment (`raw_types.rs:333-341`)
confirms `begin_idx` here is a **real, explicit `BeginRemoteWrite` oplog entry's own index** (the
first call in a batch passes `None`, which triggers writing that entry; every subsequent call in
the same batch passes `Some(that_entry's_index)`) — not a "current position" approximation, so it
does **not** have the hint-entry-asymmetry flaw §8.2.1 found in the RPC `begin_index` idea: a
`BeginRemoteWrite` entry is an actual, independently-discoverable oplog position
(`get_oplog_entry!(..., OplogEntry::BeginRemoteWrite)` finds it directly on replay), immune to
hint-entry skipping the same way `pollable_seq`/`invoke_result_seq` are.

**Per-request identity is already tracked, resource-rep-keyed, exactly like `pollable_seq`.**
`get_http_request_begin_idx` (`io/streams.rs:1165-1175`) reads `ctx.state.open_http_requests.get(&handle).begin_index`
— `open_http_requests: HashMap<handle, RequestState>` is *already* a rep-keyed per-request
identity map, structurally identical in shape to `pollable_seq`/the new `invoke_result_seq`. No
new identity-assignment mechanism is needed for HTTP at all — the identity already exists and is
already sound; what's missing is *using* it defensively during replay.

**The vulnerability: the identity is recorded but never verified during replay — and the failure
mode is silent data corruption, not a crash.** `HostInputStream::read`'s replay branch
(`durability.replay(self).await`, `io/streams.rs:116`) goes through the same generic
`Durability::replay_raw()` → `get_oplog_entry!(HostCall)` → `validate_oplog_entry` path already
traced for `poll()`/`get()` — which checks **only `function_name`**, never the
`durable_function_type`'s embedded `Some(begin_idx)` parameter. Two concurrently in-flight HTTP
streams' reads share the exact same `function_name`
(`HttpTypesIncomingBodyStreamRead`) — so if their real completion order (driven by the same
`wstd`-reactor `HashMap`-keyed waker mechanism, §2, which is generic across *any* concurrently-
polled WASI resource, not just RPC futures) diverges from replay's structural check order, replay
would not crash at all — `validate_oplog_entry`'s function_name check would pass (same function),
and stream A's replay would **silently consume stream B's bytes** as its own read result. This is
a strictly worse failure mode than `poll()`/`get()`'s loud crashes: silent data corruption in
whatever's downloaded, not a visible trap.

**Confirmed reachable in video-harness's actual code**: `download-manager.ts:138`
(`Promise.all(batch.map((item) => this.fetchBytes(item.url)))`) and `fileshare.ts:81` both fire
genuinely concurrent fetches inside one `atomically()` region — the same "N independently-awaited
operations, real completion order not guest-structural-order-guaranteed" shape already confirmed
for RPC dispatches (§2's `LlmClient` trace) and pollables (Tenth capture).

**Design: a third instance of the same shared mechanism, not a bolt-on.** `is_known_stray` (§8.4)
gains a third arm:

```rust
OplogEntry::HostCall {
    function_name: HostFunctionName::HttpTypesIncomingBodyStreamRead
        | HostFunctionName::HttpTypesIncomingBodyStreamBlockingRead
        | HostFunctionName::HttpTypesIncomingBodyStreamSkip
        | HostFunctionName::HttpTypesIncomingBodyStreamBlockingSkip,
    durable_function_type: DurableFunctionType::WriteRemoteBatched(Some(begin_idx)),
    ..
} => Some(*begin_idx) != exclude_http_begin_idx
    && state.open_http_requests.values().any(|r| r.begin_index == *begin_idx),
```

No new counter, no new snapshot-recovery — `open_http_requests`'s `begin_index` is already sound
(§ above) and already populated at request-open time. The cache is keyed by `OplogIndex`
(`pre_resolved_http_stream_chunk: HashMap<OplogIndex, HostResponseStreamChunk>` or similar —
per-function-kind response shapes differ across the read/skip/write/check_write family, same
"caller decodes, primitive doesn't" split as §8.4's `on_stray` callback already provides for).
`HostInputStream::read`'s (and siblings') replay branch gains the same
cache-then-scan-then-fallback restructure `get()`'s does (§8.4) — `exclude_http_begin_idx` is
*this* call's own `begin_idx` (already available, `get_http_request_begin_idx`), same
own-identity-exclusion reasoning as §8.3 (concurrent HTTP streams share `function_name`, exactly
like sibling `get()` calls do, so exclusion is required here too, not just for RPC).

**What still needs implementation-time verification, flagged honestly rather than assumed
resolved**: `open_http_requests`'s exact population/clearing lifecycle (when a request opens vs.
when `begin_index` actually gets assigned — the `WriteRemoteBatched(None)` → `Some(idx)`
two-phase pattern means the *first* call in a batch doesn't have `begin_idx` yet when it starts,
unlike `pollable_seq`/`invoke_result_seq`'s simpler "assigned unconditionally up front" shape) has
not been traced to the same line-by-line depth §8.8.1 gives the RPC dispatch path below — this is
real, additional tracing to do during implementation, not a gap in the design's soundness (the
identity mechanism itself is confirmed sound; only its precise lifecycle timing remains to
verify). The output-stream side (`check_write`/`write`/`flush`/`splice`) should be checked for
the same `WriteRemoteBatched(Some(begin_idx))` shape before assuming it's identical to the
input-stream side used for this analysis.

### 8.8 `invoke_result_seq` assignment timing — resolved

Traced `async_invoke_and_await` (`wasm_rpc/mod.rs:380-519`, quoted in full in §8.2.1): it runs
**unconditionally, live or replay** (no `is_live()` gate above the dispatch logic — only the
`Pending`-vs-`Deferred` state constructed differs), and it's the method that **creates** the
`FutureInvokeResult` resource (`self.table().push(FutureInvokeResultEntry { ... })`) — meaning a
given resource `rep` does not exist before this call returns. This makes dispatch time the only
correct assignment point, not "first call to `get()`" (the open question's original phrasing):
`invoke_result_seq(rep)` must be called unconditionally inside `async_invoke_and_await`, exactly
where `pollable_seq(rep)` is called unconditionally at the top of `ready()` — same pattern,
different call site because the two resource kinds become "known" to the engine at different
points (a `Pollable` always already exists when `ready()`/`poll()` touch it; a
`FutureInvokeResult` is *born* inside `async_invoke_and_await`, so that's its natural first-touch
point). `clear_invoke_result_seq(rep)` fires on the resource's `drop`, mirroring
`clear_pollable_seq`. This also confirms §8.4's `get()` code sketch is correct as written — `my_seq`
is always already assigned by the time `get()` runs, since dispatch necessarily precedes it.

### 8.9 Test plan

- **Regression, mandatory**: all six existing `io/poll.rs` unit tests (§7) must keep passing
  unmodified against the widened predicate — the Tenth capture's shape (single `IoPollReady`
  stray) must still resolve identically.
- **New unit test, seeded from the Eleventh capture's exact entries**: oplog =
  `[IoPollReady(seq=185)=true, GolemRpcFutureInvokeResultGet(WriteRemoteConcurrent(some_seq))=<result>, IoPollPoll]`
  (`peek(961)`/`peek(962)`/poll()'s own next entry) — confirm the exact rep/seq values (185 for
  the pollable; the RPC call's own assigned `invoke_result_seq`, not literally 185 — a distinct
  counter namespace) from the archived oplog file before writing the test, don't guess. Asserts
  `poll()`'s widened scanner consumes both strays (one of each kind), caches both answers
  correctly via their respective caches, and poll()'s own entry survives untouched.
- **`get()`-side test — the "exclude my own identity" case**: oplog =
  `[GolemRpcFutureInvokeResultGet(WriteRemoteConcurrent(sibling_seq))=<result>, GolemRpcFutureInvokeResultGet(WriteRemoteConcurrent(my_seq))=<result>]`
  — two DIFFERENT concurrent calls' entries in a row, own entry genuinely second. Asserts: (a)
  the scanner correctly identifies the FIRST as a stray (sibling) and consumes+caches it, (b)
  does NOT mistake the SECOND (mine) for a stray even though it matches the same
  `WriteRemoteConcurrent` shape, correctly leaving it for the unconditional fallback read.
- **`get()`-side test — the reverse direction (§8.6)**: oplog = `[IoPollReady(seq=some_pollable)=true, GolemRpcFutureInvokeResultGet(WriteRemoteConcurrent(my_seq))=<result>]`
  — a stray pollable confirmation in front of `get()`'s own entry. Asserts the widened scanner
  consumes+caches it (into `pre_resolved_pollable_ready`, for that pollable's own `ready()` to
  find later) and reaches `get()`'s own entry without error.
- **`get()`-side test — cache hit**: `take_pre_resolved_invoke_result` populated by a prior
  (simulated) stray-consume; assert `get()`'s replay uses it directly without touching the oplog
  at all.
- **`invoke_result_seq` unit tests, mirroring `pollable_seq`'s own**: assigned unconditionally at
  the top of `async_invoke_and_await` (not lazily at `get()` time); `invoke_result_seq_if_assigned`
  never mutates the counter; `recover_next_invoke_result_seq` reads correctly from a `Snapshot`
  entry, mirroring `recover_next_pollable_seq`'s own tests.
- **HTTP unit tests, same shapes as the RPC ones above**: a stray same-`function_name` HTTP
  stream-read entry (different `begin_idx`) in front of the current stream's own entry, consumed
  and cached; the "exclude my own `begin_idx`" case (two concurrent streams' reads, own entry
  genuinely second); a cache-hit case.
- **Bidirectional falsification**: same approach as §7 — a same-oplog "before" test (the OLD
  unconditional `get_oplog_entry!`/`validate_oplog_entry`-equivalent mechanism, run directly
  against the Eleventh capture's crafted entries, and against the HTTP silent-misattribution
  scenario specifically demonstrating stream A would consume stream B's bytes) proving each
  genuinely reproduces its failure mode, paired with "after" tests proving the widened mechanism
  avoids each. A true integration-level live/replay-divergence repro remains not achievable for
  the same reason established in §4b/§7 (hand-authored deterministic Rust can't force the
  underlying `wstd`-reactor-level ordering non-determinism) — unchanged conclusion, now confirmed
  applicable to RPC and HTTP too, not just pollables.
- **Full regression sweep**: everything from §7's sweep, re-run — full `--lib`, `durable_host::`,
  full `rpc.rs`/`durability.rs` integration suites, Eighth capture's fix test. Add `http.rs`'s
  suite to the sweep this time if the relevant test component is buildable in the environment
  doing the implementation (it wasn't in this worktree as of §7 — re-check).

### 8.10 Resolution status

All three original open questions (§8.8 in the prior revision) are now resolved:

1. **`invoke_result_seq` assignment timing** — resolved, §8.8 above (dispatch time, inside
   `async_invoke_and_await`, unconditional).
2. **Does `get()` need to recognize stray `IoPollReady`, not just sibling RPC entries?** —
   resolved via concrete tracing (§8.6): yes, confirmed vulnerable (the mismatch would surface as
   a response-shape decode error, not silently) — not merely "covered for free," genuinely
   necessary.
3. **Is the chosen identity live/replay-identical, traced to `pollable_seq`'s depth?** — resolved,
   and the answer changed the design: `begin_index` (the original proposal) is **not**
   live/replay-identical (§8.2.1, hint-entry asymmetry between `current_oplog_index()` and
   `last_replayed_non_hint_index()`, concretely reachable via `console.warn`/`Log` hints between
   dispatches). Replaced with a dedicated counter (`invoke_result_seq`) mirroring `pollable_seq`'s
   already-proven structure instead (§8.2.2).

**Newly surfaced and resolved in this revision**: HTTP concurrent fetches (§8.7) — investigated
with the same rigor, confirmed vulnerable (silent data misattribution between concurrent streams,
not a crash), brought into scope as a third instance of the same shared mechanism, using its
*already-sound* `open_http_requests`/`begin_index` (a real `BeginRemoteWrite` entry position, not
a `current_oplog_index()`-style approximation — so it does not have RPC's original identity flaw).

**Remaining, explicitly flagged, genuinely open items** (implementation-time verification, not
design uncertainty):
- HTTP's `open_http_requests` population/clearing lifecycle and the `WriteRemoteBatched(None)` →
  `Some(idx)` two-phase assignment timing needs the same line-by-line trace §8.8 gave the RPC
  dispatch path (§8.7's own caveat).
- HTTP's output-stream side (`check_write`/`write`/`flush`/`splice`) should be confirmed to use
  the identical `WriteRemoteBatched(Some(begin_idx))` shape before assuming the input-stream
  analysis transfers directly.
- Every other host-call replay site beyond `poll()`/`get()`/HTTP streams (websocket, blobstore,
  keyvalue, rdbms, clocks, random, `golem/v1x.rs` — 100+ grep hits, §8.6's predecessor scope note)
  remains unevaluated — flagged as before, out of scope unless a live capture or explicit request
  brings a specific one into scope, matching this investigation's standing discipline of fixing
  confirmed traps.

## 11. v4 implementation notes (post-approval)

Approved and implemented as designed in §8, with the HTTP lifecycle trace (§8.7's flagged item)
completed during implementation as directed, confirming and narrowing the design's own scope.

### HTTP lifecycle trace — confirmed, and the scope narrowed

Traced `outgoing_handler::handle()` (`http/outgoing_http.rs:234-237`): `begin_idx =
begin_durable_function(&WriteRemoteBatched(None))` — `WriteRemoteBatched(None)` **unconditionally**
takes `begin_function`'s "write a real `BeginRemoteWrite` entry" branch (the `||
WriteRemoteBatched(None)` arm of the guard, independent of `assume_idempotence` — unlike RPC's
plain `WriteRemote`, which is gated behind `!assume_idempotence` and hardcoded `true`, §8.2.1).
Confirms §8.7's premise exactly: HTTP's `begin_index` is always a real, independently-discoverable
oplog position, immune to the hint-entry asymmetry that ruled out RPC's original `begin_index`
identity — no further correction needed here, unlike RPC's.

**The scope-narrowing finding**: `open_http_requests: HashMap<u32, HttpRequestState>`
(`durable_host/mod.rs`) — the map `tracked_concurrent_op_seqs()` reads for HTTP's tracked
identities — is populated by `handle()`'s own `self.state.open_http_requests.insert(...)`
(`outgoing_http.rs:384`), called **unconditionally**, live or replay (no `is_live()` gate around
it) — so for the common case this investigation's live captures actually exhibit (a full oplog
replay that re-executes `handle()` itself — one long invocation, single snapshot near genesis,
matching both the Tenth and Eleventh captures), `open_http_requests` is repopulated symmetrically
on replay, exactly like `pollable_seq`/`invoke_result_seq`. But a documented comment
(`durable_host/mod.rs`, `replaying_http_batch`'s field doc) states `open_http_requests` is "never
populated during replay" for a **different, narrower** scenario: resuming past a snapshot taken
*mid-HTTP-request* (replay reconstructs application state via snapshot + oplog replay only for
entries *after* the snapshot — it never re-executes `handle()` in that case, since that call
predates the snapshot). That scenario already has its own special-cased, single-value recovery
path (`replaying_http_batch: Option<OplogIndex>` — one in-flight batch, not a set), pre-dating this
fix and not extended to support multiple concurrently-tracked batches during THAT specific
recovery path.

**Decision (implemented as designed, scope stated precisely rather than assumed)**: HTTP
stray-recognition is correct and effective for the full-replay case; it silently doesn't fire for
the narrower mid-request-snapshot case (empty `open_http_requests` → empty tracked set → nothing
recognized as a stray → falls to the existing, unchanged `durability.replay()` path — the exact
same behavior as before this fix, not a regression). This is documented directly in
`tracked_concurrent_op_seqs()`'s doc comment (`durable_host/mod.rs`) so it isn't a silent gap.
Extending coverage to the mid-request-snapshot case would mean generalizing
`replaying_http_batch` from a single value to a set — a materially bigger, separate change to an
existing, working recovery path, not justified without a live capture showing it's actually hit.

**Scoped to `HttpTypesIncomingBodyStreamRead` only** (not `blocking_read`/`skip`/`blocking_skip`/
the output-stream `check_write`/`write`/`flush`/`splice` family), matching `download-manager.ts`'s
actual usage (reading fetched bytes) — the confirmed, live-relevant instance. The output-stream
side's `WriteRemoteBatched(Some(begin_idx))` shape was spot-checked (`check_write`,
`io/streams.rs:330-334`) and confirmed identical, so extending coverage there later is
mechanical — add the `HostFunctionName` variant(s) to `is_stray_concurrent_entry`'s HTTP arm and
the matching `decode_and_cache_stray_entry` arm — not a redesign, but not implemented now absent
live evidence of need.

**Status clarification (post-implementation Q&A, unambiguous, for anyone auditing scope)**:
- **Input side (`HttpTypesIncomingBodyStreamRead`, concurrent fetches/reads) — FIXED.** Wired into
  the shared mechanism end to end; matches `download-manager.ts`'s actual `Promise.all(fetchBytes)`
  usage exactly.
- **Output side (`check_write`/`write`/`flush`/`splice`, concurrent outgoing writes) — NOT FIXED AT
  ALL.** Only the identity shape was spot-checked (confirming the mechanism *would* work if wired
  up); zero code changes to these functions' replay branches — they still use the plain,
  unmodified `durability.replay(self)` path with no stray-recognition whatsoever.
- **Is the output-side gap a confirmed live risk today?** No. A grep sweep of video-harness for
  concurrent-write patterns near upload/write call sites (`fileshare.ts:81`'s `presignPut`/
  `presignGet` — local URL-signing, no outgoing HTTP call at all; `workflow-agent.ts:2911` — reads,
  not writes) found no real concurrent-outgoing-body-write scenario. The output side's risk is
  inferred purely from structural similarity to the (fixed) input side, not from any observed
  trigger — a documented, plausible-but-unconfirmed gap for future investigation, not a live bug
  being knowingly left open.

### What was implemented, file by file

- **`golem-common/src/model/oplog/raw_types.rs`**: new `DurableFunctionType::WriteRemoteConcurrent(u32)`
  variant (a seq value, like `ReadLocalPollable(u32)` — not an `OplogIndex`, correcting v3's first
  draft).
- **`golem-common/src/base_model/oplog/mod.rs`**: `Snapshot`'s `raw{}` block gains
  `next_invoke_result_seq: u32`, alongside `next_pollable_seq`.
- **`golem-common/src/model/oplog/{public_types,protobuf}.rs`,
  `golem-worker-executor/src/model/public_oplog/wit.rs`,
  `golem-worker-executor/src/services/oplog/tests.rs`, `golem-worker-executor/src/worker/status.rs`**:
  exhaustiveness/construction touch points for both the new `DurableFunctionType` variant and the
  new `Snapshot` field — same file list the Option 2 fix touched for `next_pollable_seq`, found via
  `cargo check`'s own exhaustiveness errors, not pre-guessed.
- **`golem-worker-executor/src/durable_host/durability.rs`**: `is_eligible_for_internal_retry` and
  the `durability::DurableFunctionType` bridging `From` impl both gained a
  `WriteRemoteConcurrent(_)` arm, mapped identically to plain `WriteRemote`.
- **`golem-worker-executor/src/durable_host/replay_state.rs`**: `ReplayState::consume_stray_entries`
  — the shared, entry-type-agnostic walking primitive (§8.4), built purely on the existing
  `try_get_oplog_entry`.
- **`golem-worker-executor/src/durable_host/mod.rs`**: `invoke_result_seq`/`next_invoke_result_seq`/
  `recover_next_invoke_result_seq` (mirroring `pollable_seq` exactly, kept fully separate);
  `pre_resolved_invoke_result`/`pre_resolved_http_stream_chunk` caches;
  `TrackedConcurrentOpSeqs`/`tracked_concurrent_op_seqs()`/`is_stray_concurrent_entry` (the shared
  identity predicate, all three kinds) and `decode_and_cache_stray_entry` (the shared decode+cache
  dispatch, all three kinds) — the two functions every call site composes with, so `IoPollReady`,
  `GolemRpcFutureInvokeResultGet`, and `HttpTypesIncomingBodyStreamRead` genuinely fall out as
  instances rather than being duplicated per call site. `pollable_seq_if_assigned` (v1/v2, no
  longer used once `tracked_concurrent_op_seqs()` replaced its one caller) removed rather than
  left dead.
- **`golem-worker-executor/src/durable_host/io/poll.rs`**: `Host::poll`'s replay branch widened —
  same shape as the shipped fix, `is_stray_concurrent_entry`/`decode_and_cache_stray_entry` instead
  of the old, `IoPollReady`-only `consume_stray_ready_entries` (removed).
- **`golem-worker-executor/src/durable_host/wasm_rpc/mod.rs`**: `async_invoke_and_await` assigns
  `invoke_result_seq(rep)` unconditionally right after resource creation (dispatch time, per
  §8.8); `HostFutureInvokeResult::drop` clears it; `get()`'s live path tags its entry with
  `WriteRemoteConcurrent(seq)`; `get()`'s replay path restructured to the
  cache-then-scan-then-fallback shape (§8.4), excluding its own seq.
- **`golem-worker-executor/src/durable_host/io/streams.rs`**: `HostInputStream::read`'s replay
  branch restructured identically, excluding its own `begin_idx`.
- **`golem-worker-executor/src/worker/invocation_loop.rs`**: the periodic-snapshot-save flow reads
  and embeds `next_invoke_result_seq()` alongside `next_pollable_seq()`.

### Test plan — what was actually built

Per §8.9, adjusted for where the logic ended up living (`is_stray_concurrent_entry` is a pure,
free function taking owned `TrackedConcurrentOpSeqs` — directly unit-testable without any heavy
`PrivateDurableWorkerState` construction, unlike `decode_and_cache_stray_entry`, which needed the
full state):

- **`durable_host/mod.rs`'s `stray_entry_tests` module** (new): `is_stray_concurrent_entry`
  seeded directly from the Tenth capture's shape (`recognizes_tracked_pollable_stray`) and the
  Eleventh capture's shape (`recognizes_tracked_invoke_result_stray_from_poll_context`), plus the
  "exclude my own identity" case for both RPC (`excludes_own_invoke_result_identity`) and HTTP
  (`excludes_own_http_request_identity`, the symmetric case §8.6/§8.7 predicted), plus a rejection
  test for genuinely unrelated entries.
- **`durable_host/replay_state.rs`**: `consume_stray_entries_walks_past_n_matches_then_stops` /
  `consume_stray_entries_no_op_when_nothing_matches` — the shared primitive's own N=0/N=2,
  entry-type-agnostic coverage (replacing the narrower, `IoPollReady`-only coverage the deleted
  `io/poll.rs` test module had). `old_unconditional_consume_would_have_hit_eleventh_capture_mismatch`
  — the "before" half of bidirectional falsification, seeded from the Eleventh capture's exact
  entry shape, proving the OLD unconditional mechanism (`get_oplog_entry()`) genuinely mismatches
  on it — same approach as the shipped fix's own falsification, confirmed to still apply here
  (a true integration-level live/replay-divergence repro remains not achievable, unchanged
  conclusion from §4b/§7, now confirmed for RPC and HTTP too).
- `io/poll.rs`'s own test module (round-16, testing the now-deleted `consume_stray_ready_entries`
  directly) was removed rather than left testing dead code — its coverage is now provided by the
  two modules above, which test the same underlying mechanism in its current, shared form.

### Regression sweep results

- Full `--lib` suite (`golem-worker-executor`): **467 passed, 0 failed** (up from 465 pre-this-round:
  net +7 new tests in `replay_state`/`stray_entry_tests`, −9 tests removed with the deleted
  `io/poll.rs` module, +4 elsewhere accounted for by the counted totals).
- Full `rpc.rs` integration suite (23/24 tests not requiring the TS `agent_rpc` component, same
  pre-existing environment gap as round 16): **22 passed**, 1 failure — the same, already-documented
  `WorkerActivator` test-infra flake (`sequential_atomic_double_ready_rpc_calls_same_target_n4_survives_cold_replay`,
  fails in isolation, confirmed unrelated to any change in this investigation across three separate
  rounds now) — not a regression.
- Full `durability.rs` integration suite: **16/16 passed**, including every snapshot-round-trip
  test (`snapshot_based_recovery`, `automatic_snapshot_disabled/every_2nd_invocation/periodic`,
  `periodic_snapshot_recovery_survives_a_second_snapshot_generation`) — these specifically exercise
  the real `Snapshot` entry save/recover path through `invocation_loop.rs`, confirming the new
  `next_invoke_result_seq` field's wiring end-to-end, not just via unit tests.
- `golem-common --lib`: hit a pre-existing, unrelated flake (a random test process crash — exit
  status 1, no panic message, different unrelated test module each run: `cache`, `one_shot`,
  `optional_field_update`) on three consecutive runs. **Confirmed unrelated via `git stash`**: the
  identical crash reproduces with this round's `golem-common` changes fully reverted. Not
  investigated further (out of scope — pre-existing, and none of the crash sites are anywhere
  near the `oplog`/`DurableFunctionType` code this round touched).
- `http.rs`, `wasi.rs`, and other integration test files requiring components not built in this
  worktree were not run — same pre-existing environment gap noted in every prior round.
