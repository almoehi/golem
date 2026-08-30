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

## 12. Twelfth/thirteenth capture confirms the output-stream gap live — `check_write` fix design

Status: **DESIGN ONLY — not implemented, not approved yet.** §11's own "Status clarification"
flagged the HTTP output-stream side (`check_write`/`write`/`flush`/`splice`) as spot-checked but
"NOT FIXED AT ALL... a documented, plausible-but-unconfirmed gap." This section confirms it live,
root-causes it with the same rigor as §8/§11, and proposes the fix. No code has been changed; no
`cargo build` was run for this section (offline analysis only, per explicit direction).

### 12.1 The live capture

Source: `/Users/hannes/work/golem/oplog-backups/2026-08-30_smoketest_findings/3e0f1d89-character_sheet.oplog.fresh`
(3138-line dump, ~608 real entries), agent
`WorkerAgent("workspace-smoketest@1.0","3e0f1d89-bd02-4bff-a96e-3374a03f2399@character_sheet")`,
captured on `main`'s custom golem binary `sha=5135ea4fcff1a550a0aa08dbe339463ccfd1ffe6` (per
`/golem-build-info.txt`, confirmed to have every fix documented above — including the §11 v4
`is_stray_concurrent_entry`/`consume_stray_entries` mechanism — as an ancestor). This is the same
oplog `golem/oplog-backups/README.md`'s "Twelfth/thirteenth capture — 2026-08-30" section already
gave a preliminary read on; this section supersedes that preliminary read with full root-causing
and a fix design. Cross-references the top-level `video-harness/CLAUDE.md`'s "Agents with
long-running `awaitPromise`" rule, which already named this exact signature from an earlier,
oplog-less RCA (2026-08-27) — this is occurrence #3, now fully traced.

**The exact entries** (own oplog dump; `at:` timestamps and `result:` payloads read directly, not
inferred):

```
#00601 CALL golem::api::create_promise        at 05:13:08.116Z   (yieldForSnapshot fence)
#00602 CALL golem::api::complete_promise       at 05:13:08.117Z
#00603 CALL io::poll::poll            {count:1} at 05:13:08.117Z  -> ok([0])
#00604 CALL io::poll::pollable::ready          at 05:13:08.117Z  -> ok(true)
#00605 CALL golem::api::get_promise_result     at 05:13:08.117Z  -> some([49])   ("1", the fence's own payload)
#00606 LOG   "wf_krea2_character_sheet: awaiting render promise (renderId=bd4fe65860c768f2aa0b2fdfe933aa98)"
#00607 SUSPEND                                 at 05:13:08.118Z   (clean — no error)
#00608 ERROR                                   at 05:19:21.886Z   retry from: 605
#00610 ERROR (second forced resume)            at 05:20:53.609Z   retry from: 605
#00613 ERROR (third forced resume)             at 05:43:51.552Z   retry from: 605
```

Server-log-equivalent error text captured in the dump's own `error:` field (three separate
occurrences, byte-identical):
```
Unexpected oplog entry during replay: expected io::poll::poll, got http::types::outgoing_body_stream::check_write
```

`#00606`/`#00607` are `workflowToolFinish()`'s `awaitPromise(promiseId)` call
(`video-harness/src/worker/worker-agent.ts:1467`, log line at `:1427`) — the render dispatched at
`#00590-#00600`'s `atomicRpcCall()` region hadn't completed yet, so the worker genuinely suspended
(clean `SUSPEND`, not a trap) to wait for an external `complete_promise()` call from the render
backend. Three later resume attempts (external `wakeUp`/TUI-reconnect-poll kicks, per the
README's own read) each require reconstructing the whole worker instance via replay from the
single construction-time `SNAPSHOT` (entry `#00006` — the separate, already-tracked snapshot-
frequency gap noted in this repo's `CLAUDE.md`, explicitly out of scope here) through `#00607`,
and each of the three replay attempts traps identically.

### 12.2 Locating the concurrency: a stranded entry from ~500 positions earlier, not a same-vicinity mismatch

A full function-name census of the capture (`grep -oE 'CALL [a-zA-Z:_.-]+' | sort | uniq -c`):

```
126 io::poll::pollable::ready        21 io::poll::poll
106 http::types::outgoing_body_stream::write
106 http::types::outgoing_body_stream::check_write
 55 http::types::incoming_body_stream::blocking_read
 16 random::get_random_u             14 http::types::future_incoming_response::get
 12 golem::agent::get_agent_type      8 golem::api::create_promise
  7 wall_clock::now                   7 monotonic_clock::subscribe_duration
  7 golem::api::get_promise_result    7 golem::api::complete_promise
  6 keyvalue::eventual::set           6 golem::rpc::future-invoke-result::get
```

Every `check_write`/`write` entry in the dump is clustered into four narrow windows — entries
`#00060-00106`, `#00150-00204`, `#00251-00308`, `#00332-...` (confirmed via `awk` scanning for the
nearest preceding `#NNNNN:` marker before each `check_write` line) — each cluster's timestamps span
under 100ms, consistent with one LLM completion request's request-body write loop per round of the
reasoning loop. **None of these clusters are anywhere near `#00605-00608`** — the nearest cluster
ends around `#00308`, roughly 300 entries before the trap surfaces. Since Golem's replay engine
tracks a single linear cursor (`try_get_oplog_entry`/`ReplayState`, walked strictly forward, never
backward), "expected `io::poll::poll`, got `check_write`" at the `#605` neighborhood can only mean
one thing: **the cursor is still parked on a `check_write` entry from one of those far-earlier
clusters, never consumed**, and by the time guest code (in THIS replay attempt's own execution) next
calls `io::poll::poll()` — most plausibly the fence's own `#00603` call, or an equivalent call
inside the render-promise wait's own polling machinery — the "next unconsumed" entry the engine
hands it is that stranded `check_write`, hundreds of positions earlier in wall-clock terms but
still first in cursor order.

This is not a hypothetical replay-vs-original-live divergence (the classic Finding B shape,
requiring two different process instantiations) — **all three ERROR entries are separate replay
attempts of the identical byte-for-byte oplog**, so this is genuinely a case of the SAME divergence
mechanism (§2's `wstd`-reactor-HashMap non-determinism) triggering identically three times,
independently, at 05:19, 05:20, and 05:43 — three different process instantiations (three different
random HashMap seeds), all landing on a stray at the same point. Given the entry density involved
— 106 `check_write` + 106 `write` + 55 incoming reads, all pollable-backed, concentrated across
~250 oplog entries early in the invocation — this reads less like "a rare unlucky ordering" and
more like "with this many concurrently-registerable pollables in play, some stray is close to
guaranteed regardless of the specific random seed." Worth noting as a severity signal, not
re-litigating §2's already-settled mechanism.

**Direct confirmation that `check_write` calls are themselves poll-gated** (own oplog dump,
`#00059-00061`):
```
#00059 CALL io::poll::pollable::ready   at 05:11:12.615Z  -> ok(true)
#00060 CALL http::types::outgoing_body_stream::check_write
            input: {uri: "https://ollama.com/api/chat", method: post, ...}
            result: ok(1048576)
#00061 CALL http::types::outgoing_body_stream::write
            input: {...}  result: ok([...large JSON chunk...])
```
Every `check_write` is preceded by its own `ready()`/(implicitly, batched) `poll()` wait on the
output stream's own pollable — structurally identical to the pollable-batching shape already
root-caused for `IoPollReady` (§2) and RPC `get()` (§8.1). An outgoing-body-stream pollable is
exactly as eligible to be swept into `wstd`'s shared, HashMap-ordered `poll()` batch as any other
concurrently-pending pollable — there is nothing special about it that would exempt it from §2's
mechanism.

### 12.3 App-level trigger: traced, and it's an ordinary single `fetch()` — no double-dispatch bug

The `check_write`/`write` entries' own `uri` field (`https://ollama.com/api/chat`) points to
`OllamaBackend.chat()` (`video-harness/src/llm/ollama-backend.ts:159`):

```typescript
const response = await fetch(`${this.endpoint}/api/chat`, {
  method: "POST",
  headers,
  body: JSON.stringify(body),
});
...
const payload = (await response.json()) as OllamaChatResponse;
```

Traced end to end: a single, plain, sequentially-`await`ed `fetch()` call — no `ReadableStream`
body, no `duplex`, no `ReadableStream`/`Promise.all` fan-out, nothing resembling
`LlmClient.runOneRound()`'s Phase A concurrent tool dispatch (§2) or `download-manager.ts`'s
concurrent-fetch pattern (§8.7). This is a materially different — and more concerning — finding
than §11's own risk assessment: §11's grep sweep (upload/write call sites: `fileshare.ts:81`,
`workflow-agent.ts:2911`) concluded "no real concurrent-outgoing-body-write scenario" and treated
the output-side gap as needing an app-level concurrent-write trigger to matter. **This capture
shows that is not required** — one ordinary, sequentially-awaited `fetch()` POST, with a body large
enough to need multiple `check_write`/`write` cycles (106 of each here — a large system-prompt-plus-
tool-schema payload, per the excerpted body content), is sufficient on its own, given `wstd`'s
generic per-pollable batching (§2) has no notion of "this is just one ordinary fetch." Any
`fetch()` call whose body needs more than a single `write()` cycle is a candidate, not just
call sites with visible concurrent-dispatch code.

### 12.4 Same mechanism? Confirmed by reading the actual code, not assumed

`HostOutputStream::check_write` (`golem-worker-executor/src/durable_host/io/streams.rs:346-461`),
`is_http` branch:

```rust
let result = if durability.is_live() {
    /* ... unchanged live path ... */
} else {
    durability.replay(self).await          // <- line 396: unconditional, no stray-scan
}
.map_err(StreamError::from)?;
```

This is the exact same unconditional-consumption shape `poll()`, `get()`, and `read()` all had
*before* their respective fixes (§1, §8.2, §11) — confirmed by direct comparison, not inferred
from naming. `is_stray_concurrent_entry` (`durable_host/mod.rs:4263-4295`) has exactly three match
arms today — `IoPollReady`, `GolemRpcFutureInvokeResultGet`, `HttpTypesIncomingBodyStreamRead` —
and falls to `_ => false` for everything else, confirmed by reading the full match exhaustively.
`HttpTypesOutgoingBodyStreamCheckWrite` is not among them, so `consume_stray_entries` never
recognizes a stray `check_write` entry, and any call site that reaches it (currently `poll()`'s and
`get()`'s and `read()`'s own stray-scanners, none of which check for it either) has no path but the
unconditional fallback — which is precisely `expected io::poll::poll, got
http::types::outgoing_body_stream::check_write`.

**The identity mechanism this needs already exists and is already sound — confirmed, not assumed.**
`check_write`'s `state.begin_index` comes from `get_http_output_stream_state`
(`io/streams.rs:1127-1139`), which reads `open_http_requests`'s `HttpRequestState.begin_index`
(`durable_host/mod.rs:3913-3917`) — **the exact same field, same map, same request-scoped identity**
the already-fixed `read()` (input side) uses via `get_http_request_begin_idx`
(`io/streams.rs:1165-1175`, per §8.7/§11). One `HttpRequestState` per request covers *both*
directions (`output_stream_rep`, `body_handle` are separate fields on the same struct,
`mod.rs:3913-3937`) — there is no new identity concept to design here, unlike RPC's `begin_index`
(§8.2.1, ruled unsound) or a from-scratch counter (`pollable_seq`/`invoke_result_seq`, §8.2.2).
§11's own HTTP lifecycle trace already confirmed `WriteRemoteBatched(None)` unconditionally writes
a real `BeginRemoteWrite` entry at `handle()` regardless of `assume_idempotence` (`http/
outgoing_http.rs:234-237`) — that finding applies identically to the output-stream side, since it's
the same `begin_index` value, not a separately-computed one.

**Verdict: this is the same mechanism as §1/§8/§11 — a straightforward generalization, not a
structurally different problem.** No new identity, no new counter, no snapshot-recovery
complication beyond what §11 already resolved (and explicitly scoped) for HTTP.

### 12.5 Fix design

Five changes, all additive, none touching the already-shipped poll/get/read paths beyond widening
one shared match statement:

**1. `is_stray_concurrent_entry`** (`durable_host/mod.rs:4263-4295`) — new arm, reusing the
existing `http_begin_indexes`/`exclude_http_begin_idx` (no new tracked set — `open_http_requests`
already covers both directions, §12.4):

```rust
OplogEntry::HostCall {
    function_name: HostFunctionName::HttpTypesOutgoingBodyStreamCheckWrite,
    durable_function_type: DurableFunctionType::WriteRemoteBatched(Some(begin_idx)),
    ..
} => {
    Some(*begin_idx) != exclude_http_begin_idx
        && tracked.http_begin_indexes.contains(begin_idx)
}
```

**2. New cache field + accessors** (`durable_host/mod.rs`, alongside
`pre_resolved_http_stream_chunk`/`record_pre_resolved_http_stream_chunk`/
`take_pre_resolved_http_stream_chunk`, `mod.rs:4189,4826-4855`) — a **separate** cache, not a reuse
of `pre_resolved_http_stream_chunk`, because the response *shape* differs
(`HostResponseStreamCheckWrite` vs. `HostResponseStreamChunk`) — same reasoning that already forced
`pre_resolved_pollable_ready`/`pre_resolved_invoke_result`/`pre_resolved_http_stream_chunk` to stay
three separate maps rather than one shared one (§8.4):

```rust
/// REPLAY-ONLY cache, symmetric with pre_resolved_http_stream_chunk: a stray
/// HttpTypesOutgoingBodyStreamCheckWrite entry's decoded result, consumed early by a DIFFERENT
/// call's stray-scan, cached here for the owning check_write() call to consult first.
pre_resolved_http_stream_check_write: HashMap<OplogIndex, HostResponseStreamCheckWrite>,
```
`record_pre_resolved_http_stream_check_write`/`take_pre_resolved_http_stream_check_write` mirror
the `_chunk` pair exactly (insert/remove, same trace-log shape).

**3. `decode_and_cache_stray_entry`** (`durable_host/mod.rs:4887-4969`) — new match arm, same
download-payload-then-`try_into()` shape as the existing three arms:

```rust
OplogEntry::HostCall {
    function_name: HostFunctionName::HttpTypesOutgoingBodyStreamCheckWrite,
    durable_function_type: DurableFunctionType::WriteRemoteBatched(Some(begin_idx)),
    response,
    ..
} => {
    let host_response: HostResponse = self.oplog.download_payload(response).await
        .map_err(WorkerExecutorError::runtime)?;
    let payload: HostResponseStreamCheckWrite = host_response.try_into()
        .map_err(WorkerExecutorError::runtime)?;
    self.record_pre_resolved_http_stream_check_write(begin_idx, payload);
    Ok(())
}
```

**4. `check_write()`'s `is_http` branch** (`io/streams.rs:368-398`) — restructure the replay half
to the same cache-then-scan-then-fallback shape `read()` already has (`io/streams.rs:115-148`):

```rust
let result = if durability.is_live() {
    /* unchanged */
} else if let Some(cached) =
    self.state.take_pre_resolved_http_stream_check_write(state.begin_index)
{
    Ok(cached)
} else {
    let tracked = self.state.tracked_concurrent_op_seqs();
    let mut strays = Vec::new();
    self.state.replay_state.consume_stray_entries(
        |entry| is_stray_concurrent_entry(entry, &tracked, None, Some(state.begin_index)),
        |idx, entry| strays.push((idx, entry)),
    ).await.map_err(|e| StreamError::Trap(wasmtime::Error::from_anyhow(e.into())))?;
    for (idx, entry) in strays {
        self.state.decode_and_cache_stray_entry(idx, entry).await
            .map_err(|e| StreamError::Trap(wasmtime::Error::from_anyhow(e.into())))?;
    }
    durability.replay(self).await
}
.map_err(StreamError::from)?;
```

**5. No change needed** to `check_write()`'s `replaying_http_batch`/post-snapshot-restore branches
(`io/streams.rs:401-447`) — identical scope-narrowing already accepted for `read()` in §11 (the
narrower mid-request-snapshot recovery path, not exercised by this capture's shape — one long
invocation, single genesis snapshot, matching both this and the Tenth/Eleventh captures).

**Traced explicitly, not assumed: no cross-direction contamination.** `read()`'s own
`exclude_http_begin_idx: Some(my_begin_idx)` parameter is per-*request*, not per-*direction* — so
once the new arm above exists, a `read()` call for request X will correctly also exclude request
X's *own* `check_write` entries from being treated as "someone else's" stray (the condition
`Some(*begin_idx) != exclude_http_begin_idx` evaluates false for X either way, regardless of which
arm/function-name matched), leaving them for `check_write()`'s own replay to consume normally — not
accidentally swallowed by a `read()` call on the same request's other direction. Verified by
re-reading the actual condition, not inferred from the fields' names.

### 12.6 Scope recommendation: fix `check_write` and `write` together, not `check_write` alone

`write()` (`io/streams.rs:463-566`) has the **identical** unconditional-replay gap at line 513
(`let replayed = durability.replay(self).await;`), using the same `state.begin_index` identity.
Six more call sites share the exact same `WriteRemoteBatched(Some(state.begin_index))` shape
(`flush`/`blocking_flush`/`write_zeroes`/`blocking_write_zeroes_and_flush`/`splice`/
`blocking_splice`, grepped at `io/streams.rs:587,675,728,805,895,976,1043`) — confirmed via the
shared-pattern doc comment at `io/streams.rs:1249-1251` ("the standard retry pattern shared by
`check_write`, `write`, `flush`, `blocking_flush`, `write_zeroes`, and
`blocking_write_zeroes_and_flush`").

§11 scoped its own fix to `HttpTypesIncomingBodyStreamRead` only (not `blocking_read`/`skip`/
`blocking_skip`) on the reasoning that `read()` was the one confirmed call shape and the others
were rarer, separately-triggered patterns not worth fixing without their own live evidence. That
reasoning does **not** transfer cleanly to `check_write`/`write`: this capture's own census
(§12.2) shows **106 `check_write` and 106 `write` calls, 1:1, in lockstep** — they are not separate
call shapes exercised independently, they are the two halves of the same per-chunk write protocol
(`check_write` reports capacity, `write` consumes it — confirmed adjacent in the oplog,
`#00060`→`#00061`). Fixing `check_write` alone leaves `write()` exposed to the structurally
identical "expected io::poll::poll, got http::types::outgoing_body_stream::write" trap — not a
new/different bug, the other half of the exact pattern this capture already demonstrates is real
and reachable via one ordinary `fetch()` call. **Recommendation: implement both `check_write` and
`write` in the same change** (each gets its own small cache — response shapes differ,
`HostResponseStreamCheckWrite` vs. `HostResponseStreamWriteWithBytes` — but the restructuring is
mechanical, identical shape, per point 4 above applied twice).

`flush`/`blocking_flush`/`write_zeroes`/`blocking_write_zeroes_and_flush`/`splice`/
`blocking_splice`: **zero occurrences in this capture** (confirmed via the full function-census in
§12.2 — none appear at all). Leave unfixed, same "no live evidence yet" bar §11 already applied to
`blocking_read`/`skip`/`blocking_skip` — flagged as the same latent, structurally-identical,
not-yet-observed gap, mechanical to extend later (add the `HostFunctionName` variant(s) to both
match statements) if a future capture shows one of them is live-relevant.

### 12.7 Test plan (mirrors §8.9/§11's approach)

- **Regression, mandatory**: all existing `stray_entry_tests` (§11) and `read()`/`check_write`
  unit coverage keep passing unmodified against the widened `is_stray_concurrent_entry` match.
- **New unit test, seeded from this capture's actual shape**: once the real `begin_index` values
  are extracted from the archived oplog (don't guess) — a stray `check_write` (and separately, a
  stray `write`) entry preceding a `poll()`'s own `IoPollPoll` entry, with the stray's begin_idx in
  `tracked.http_begin_indexes`; assert consumed+cached, `poll()`'s own entry survives untouched.
- **"Exclude my own identity" test** (mirrors `excludes_own_invoke_result_identity`/
  `excludes_own_http_request_identity`, §11): two concurrent HTTP requests' `check_write` entries,
  own request's entry genuinely second — assert the sibling is recognized+cached, the caller's own
  is left for its normal fallback read.
- **Cross-direction non-interference test** (new, specific to this section's finding in §12.5): a
  `read()` call for request X does NOT consume a `check_write` entry ALSO tagged begin_idx=X (same
  request, other direction) — proving the shared `exclude_http_begin_idx` parameter treats "my own
  request, either direction" as non-stray, correctly deferring to `check_write()`'s own replay.
- **`check_write`/`write` cache-hit tests**: `take_pre_resolved_http_stream_check_write`
  (resp. the `write` equivalent) populated by a prior simulated stray-consume; assert the replay
  path uses it directly without touching the oplog.
- **Bidirectional falsification**: a same-oplog "before" test reproducing this capture's exact
  entry shape against the OLD unconditional `durability.replay()` mechanism (proving it genuinely
  mismatches, matching the literal production error text in §12.1), paired with an "after" test
  proving the widened mechanism avoids it. A true integration-level live/replay-divergence repro
  remains not achievable for the same reason established in §4b/§7/§8.9 (hand-authored
  deterministic Rust cannot force the underlying `wstd`-reactor-level ordering non-determinism) —
  unchanged conclusion, now confirmed applicable to HTTP output streams too.
- **Full regression sweep**: same bar as §7/§11 — full `--lib`, `durable_host::`, full
  `rpc.rs`/`durability.rs` integration suites, Eighth capture's fix test, re-run after this change.

### 12.8 Status

**Design only.** Not implemented, not approved. No `cargo build` run, no container/binary touched,
per this investigation phase's explicit scope (offline analysis working from the already-captured
`3e0f1d89-character_sheet.oplog.fresh` evidence and source reading only). Ready for review.

---

## 13. Thirteenth capture, second agent — `expected HostCall, got FinishSpan`: the stray-scan itself over-consumes when RPC identity drifts

**Capture:** `/Users/hannes/work/golem/oplog-backups/2026-08-30_smoketest_findings/b2d8d571-scene_plates.oplog.fresh`
**Agent:** `WorkerAgent("workspace-smoketest@1.0","b2d8d571-7e0e-4a28-902c-a940f4cc42da@scene_plates")`
**Binary:** the shipped Finding-B fix (`ed4c58fcc` / merge `5135ea4fc`) — this is a *post-fix* trap, not stale
pre-fix evidence.

Investigated independently of, and in parallel with, §12 (`3e0f1d89-character_sheet`'s `check_write`
trap — a different agent, different entry pair, not touched here).

### 13.1 The live capture

```
#01148:
ERROR
          at:                2026-08-30T05:23:35.234Z
          retry from:        1098
          error:             Unexpected oplog entry during replay: expected OplogEntry :: HostCall |,
                             got FinishSpan { timestamp: Timestamp(Timestamp("2026-08-30T05:21:33.116000000Z")),
                                              span_id: SpanId(13566386660567402609) }
```

Repeated verbatim at `#01149`, `#01150`, `#01152`, `#01153`, `#01155`, `#01157`, `#01159` — deterministic
and permanent, identical `retry from: 1098` every time.

`13566386660567402609 == 0xbc457cca3cb7c871`. That resolves *exactly* to an entry in this same oplog:

```
#01107:
FINISH SPAN
          at:                2026-08-30T05:21:33.116Z          <- byte-identical timestamp to the error
          span id:           bc457cca3cb7c871
```

### 13.2 First conclusion: the oplog is well-formed. Nothing is misordered.

The premise this investigation started from — "a span belonging to one `atomicRpcCall` region is being
delivered/replayed at a DIFFERENT region's boundary" — is **falsified by the capture**. The relevant window
(`#01083`–`#01140`) contains four structurally identical, strictly non-overlapping regions, each one a
textbook `atomicRpcCall()` → `WorkflowAgent("krea2_base_realism@main",…).run()` dispatch:

| Region | BeginAtomic | `get_agent_type` ×2 | StartSpan (rpc-connection) | StartSpan (rpc-invocation) | poll | ready | `future-invoke-result::get` | **FinishSpan** | EndAtomic (begin index) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 1083 | 1084–1085 | 1086 `f98d43a9cad6cba1` | 1087 `11d60b8bf0507bc5` | 1088 | 1089 | 1090 | **1091** `11d60b8bf0507bc5` | 1092 (1083) |
| 2 | 1098 | 1099–1100 | 1101 `caf43432b25fbce7` | 1102 `bc457cca3cb7c871` | 1104 | 1105 | 1106 | **1107** `bc457cca3cb7c871` | 1108 (1098) |
| 3 | 1114 | 1115–1116 | 1117 `2d2012cbdcd48d3d` | 1118 `a086f2c124d61844` | 1120 | 1121 | 1122 | **1123** `a086f2c124d61844` | 1124 (1114) |
| 4 | 1130 | 1131–1132 | 1133 `d9f1a13fe17ce111` | 1134 `48d376785b2d6af8` | 1136 | 1137 | 1138 | **1139** `48d376785b2d6af8` | 1140 (1130) |

Every `FinishSpan` sits exactly one entry after its own region's `future-invoke-result::get`, closing its own
region's own `rpc-invocation` span — precisely what
`HostFutureInvokeResult::get` does (`wasm_rpc/mod.rs:985-990`: `end_function(...)` then
`self.finish_span(&span_id)`). `#01107` belongs to region 2 and is recorded in region 2, in the right place.

(The `rpc-connection` spans — `f98d43a9…`, `caf43432…`, `2d2012cb…`, `d9f1a13f…` — are never finished at all:
`WasmRpcEntry::drop` (`wasm_rpc/mod.rs:644-654`) is what closes them, and the guest's `WorkflowAgent` proxy
objects were still alive at `SUSPEND`. `START SPAN` count 31 vs `FINISH SPAN` count 24 across the whole
oplog. This is expected, not the bug — but see §13.6, it *is* the reason the deferred-drop path in §13.5 is
reachable.)

Also ruled out by direct inspection: this is **not** the overlapping-`atomically()` bug fixed app-side by
`bdd966f`. Every `BEGIN ATOMIC REGION` in the window has its matching `END ATOMIC REGION` before the next
`BEGIN`, with a correct `begin index:` back-reference. The app-level "serialize suspends starts" fix is
working exactly as intended.

**So the misordering is not in the recording. It is in the consumption.** Something during replay consumed
`#01106` — region 2's own `future-invoke-result::get` entry, the only entry between the last known-good
cursor and `#01107` — and then still demanded a `HostCall`, landing on the structural `FinishSpan` at
`#01107`.

### 13.3 Which consumer produced the error string

`expected OplogEntry :: HostCall |` is the `stringify!($($cases |)+)` output of `get_oplog_entry!`
(`durable_host/mod.rs:5632-5650`) invoked with the single case `OplogEntry::HostCall`. There are exactly
three such call sites in the RPC/poll path:

| Site | Consumer |
|---|---|
| `durable_host/durability.rs:868` | `read_persisted_durable_function_invocation`, reached from every `Durability::replay()` — including `poll()`'s `durability.replay(self)` (`io/poll.rs:417`) |
| `durable_host/wasm_rpc/mod.rs:935` | `HostFutureInvokeResult::get`'s replay fallback, immediately after its stray-scan |
| (HTTP sites — not in this trace) | |

Critically, `durability.rs:1270-1296` consumes **first and validates second**: `replay_raw()` calls
`read_persisted_durable_function_invocation()` (which runs the destructive `get_oplog_entry!`) and only then
calls `validate_oplog_entry()` to compare `function_name`. So *any* non-`HostCall` entry at the cursor —
`FinishSpan`, `EndAtomicRegion`, `EndRemoteWrite` — traps inside the macro with this exact message, before
any identity check can run. **A replay consumer can destroy a structural entry it had no business reading.**
That is the class-level defect (§13.7).

### 13.4 The concurrency that makes this reachable: `poll()` batch size goes 1 → 2 at exactly region 2

Every `io::poll::poll` entry in the capture, with its recorded `count` and result:

```
#01068  count: 1   ok([0])
#01088  count: 1   ok([0])     <- region 1 RPC poll
#01095  count: 1   ok([0])     <- region 1 promise poll
#01104  count: 2   ok([1])     <- region 2 RPC poll   ** batch size becomes 2 here **
#01111  count: 2   ok([1])
#01120  count: 2   ok([1])     <- region 3 RPC poll
#01127  count: 2   ok([1])
#01136  count: 2   ok([1])     <- region 4 RPC poll
#01143  count: 2   ok([1])
```

From `#01104` onward a second pollable is permanently co-resident in every `poll()` batch and never becomes
ready (`ok([1])`, never `ok([0,1])`, never `ok([0])`). **Region 1 — the only region that replayed without
trapping — is the only region whose `poll()` had a batch of 1. The trap is at region 2, the first region with
a concurrent pollable batch.** That is the §2 Finding-B precondition, verbatim: `wstd`'s `Reactor` batching
2+ concurrently-pending pollables through a `HashMap`-keyed waker set whose iteration order is
per-process-randomized.

There was no crash/restart here — `#01147` is a clean `SUSPEND` (the guest awaiting the render promise,
`#01146` `wf_krea2_base_realism: awaiting render promise`). The trap is on the **resume replay** after that
suspend. The only `SNAPSHOT` in the entire oplog is at line 36 (near genesis), so this is a full
genesis replay of ~1100 entries against a freshly instantiated QuickJS guest — a different process, hence a
different `HashMap` seed and different GC timing from the live segment that recorded them.

### 13.5 Root cause: `invoke_result_seq` leaks on the deferred-`FutureInvokeResult`-delete path, so RPC identity is not replay-stable

The shipped fix's soundness rests entirely on `invoke_result_seq` being assigned identically in live and in
replay (§8.2.2, §8.8). It is not, because of a concrete gap in the clear path.

**The gap.** `HostFutureInvokeResult::drop` (`wasm_rpc/mod.rs:1135-1156`) has two branches:

```rust
match self.table().delete(this) {
    Ok(entry) => {
        for child_rep in &entry.child_pollables { self.state.rpc_pollable_to_parent.remove(child_rep); }
        // Only once the rep is truly freed back to the resource table (not deferred by
        // HasChildren below) — same rep-reuse rationale as clear_pollable_seq.
        self.state.clear_invoke_result_seq(future_rep);          // <-- cleared here
    }
    Err(ResourceTableError::HasChildren) => {
        let parent: Resource<FutureInvokeResult> = Resource::new_borrow(future_rep);
        self.table().get_mut(&parent)?.drop_pending = true;      // <-- deferred, NOT cleared (correct so far)
    }
    Err(err) => return Err(err.into()),
}
```

The deferred branch is correct in isolation: the rep is still occupied, so the seq must survive. But the
place where that deferral is *finally* honoured — `HostPollable::drop` in `io/poll.rs:196-220` — never
clears it:

```rust
if let Some(parent_rep) = parent_rep {
    self.state.rpc_pollable_to_parent.remove(&child_rep);
    ...
    if should_delete {
        let parent_owned: Resource<...> = Resource::new_own(parent_rep);
        if let Err(err) = self.table().delete(parent_owned) { ... }   // rep freed…
        // …and NO self.state.clear_invoke_result_seq(parent_rep);
    }
}
```

`clear_invoke_result_seq` is called from exactly one place in the entire crate (`wasm_rpc/mod.rs:1146`), so
this deferred path leaks unconditionally. Note the asymmetry with the pollable side, which is correct:
`io/poll.rs:186` calls `clear_pollable_seq(child_rep)` unconditionally at the top of the same function.

**Why the leak is not merely cosmetic.** `invoke_result_seq()` (`durable_host/mod.rs:4760-4775`) is
`entry(rep).or_insert_with(|| { next++ })`. A leaked entry for a rep that wasmtime has already freed means
that when the resource table hands that rep to the **next** `FutureInvokeResult`
(`async_invoke_and_await` → `self.table().push(...)` → `invoke_result_seq(fut.rep())`,
`wasm_rpc/mod.rs:498-528`), the lookup *hits* the stale entry: the new call silently inherits the old call's
seq **and `next_invoke_result_seq` is not incremented**. Two distinct RPC calls then share one identity, and
the counter is permanently offset. Additionally the leaked value never leaves
`tracked_concurrent_op_seqs()` (`durable_host/mod.rs:4870-4880` — it is literally
`self.invoke_result_seq.values()`), so it stays "tracked", i.e. stray-matchable, forever.

**Why live and replay take different branches.** Which branch `HostFutureInvokeResult::drop` takes depends
purely on whether the future's child pollable (created by `subscribe`, `wasm_rpc/mod.rs:658-673`) is still
alive at the moment the future is dropped. In this guest that is decided by **QuickJS finalizer/GC timing**,
which is not replay-stable: the live segment ran with real network latency and real suspends interleaved,
the replay segment runs at memory speed in a fresh process. The capture proves non-trivial pollable-lifetime
drift is already happening here — one pollable survives every region boundary from `#01104` onward
(`count: 2` forever), and the `rpc-connection` spans are never finished at all (§13.2), i.e. the guest's RPC
resources are living well past their logical use.

**The resulting trap, step by step, at region 2:**

1. Live recorded `#01106` tagged `DurableFunctionType::WriteRemoteConcurrent(S_live)`
   (`wasm_rpc/mod.rs:843-861`), where `S_live` is whatever the live-side counter produced under live's
   drop/GC ordering.
2. Replay reaches region 2's `get()`. `my_invoke_result_seq = S_replay` from
   `invoke_result_seq_if_assigned(this_rep)` (`wasm_rpc/mod.rs:896-899`) — assigned at dispatch under
   *replay's* drop/GC ordering. `S_replay != S_live`.
3. Cache miss: `take_pre_resolved_invoke_result(S_replay)` → `None` (`wasm_rpc/mod.rs:902-906`).
4. Stray-scan (`wasm_rpc/mod.rs:908-927`) evaluates `#01106` via
   `is_stray_concurrent_entry(entry, &tracked, Some(S_replay), None)`
   (`durable_host/mod.rs:4276-4282`):
   - `Some(S_live) != Some(S_replay)` → the "exclude my own identity" guard **does not fire**;
   - `tracked.invoke_result_seqs.contains(&S_live)` → **true**, because `S_live` is still in the map
     (either from the leaked entry, or from a sibling future that is still alive — every one of the four
     futures is still alive at this point, per §13.2).
   → **`#01106`, the caller's own entry, is classified as a stray, durably consumed, and cached under the
   wrong key `S_live`.**
5. The scan loop continues to `#01107` `FinishSpan` → `is_stray_concurrent_entry`'s `_ => false` arm →
   scan stops (correctly).
6. `get_oplog_entry!(self.state.replay_state, OplogEntry::HostCall)` (`wasm_rpc/mod.rs:935`) reads
   `#01107` → **`Unexpected oplog entry during replay: expected OplogEntry :: HostCall |, got FinishSpan {
   … span_id: SpanId(13566386660567402609) }`** — the observed error, with the observed span id and the
   observed timestamp, inside the atomic region that began at `#01098` (the observed `retry from: 1098`).

Every element of the production error text is accounted for, with no free parameters.

**Secondary exposure, same defect, different consumer.** The identical over-consumption is reachable through
`poll()` even without seq drift, and would produce a *byte-identical* error string via
`durability.rs:868`. `poll()`'s stray-scan passes `exclude_invoke_result_seq: None` by design
(`io/poll.rs:392-402`; §8.4: "poll() has no RPC identity of its own to exclude"), so **any** tracked
`GolemRpcFutureInvokeResultGet` entry at its cursor — including one that is about to be legitimately claimed
by a `get()` that simply has not run yet — is consumed. If the reactor reaches a `poll()` call while
`#01106` is at the cursor, `poll()` eats `#01106`, then `durability.replay(self)` demands its own
`IoPollPoll` and hits `#01107`. Such an extra `poll()` iteration is exactly what the recorded-positional
poll result makes possible: `HostResponsePollResult { result: Ok(vec![1]) }` is a list of **indices into the
guest-supplied `in_` vector**, and that vector's order comes straight from the reactor's randomized
`HashMap` iteration. Replaying `ok([1])` against a differently-ordered `in_` wakes the *wrong* task, which
makes no progress and re-enters `poll()`. This is the one channel of the original Finding-B non-determinism
that the shipped fix did **not** close: `ready()` was made identity-matched (`ReadLocalPollable(seq)`) and
`get()` was made identity-matched (`WriteRemoteConcurrent(seq)`), but `poll()`'s own recorded *answer* is
still positional.

### 13.6 Is this the same mechanism as Finding B? Yes — but it is a defect *in the fix*, not a new instance of the original bug

Same root non-determinism (guest-side `HashMap`-ordered reactor batching + GC-timing-dependent resource
lifetimes), same family, same call sites. But the failure is now one level up:

- **Finding B (fixed):** a positional consumer ate an entry belonging to someone else, then mis-decoded it.
- **This capture:** the stray-scanner built to prevent that ate an entry belonging to **itself**, then
  crashed on the next structural entry.

The stray-scan converts a *decode* failure into a *structural* failure. The observable symptom moved from
`expected io::poll::poll, got io::poll::pollable::ready` / `expected HostResponse::PollResult, got …` to
`expected HostCall, got FinishSpan`. That symptom shift is diagnostic: `FinishSpan` at the cursor can only
mean the consumer is exactly one entry past where it should be.

It is **not** the same mechanism as `GOLEM_SPAN_RECURSION_BUG.md`. That is a *serialization-size* bug
(`linked_context` chains growing O(N^K) across pipeline hops → `AgentStatusRecord` blob → SQLITE_TOOBIG),
already fixed via `without_linked_contexts()`. It touches `InvocationContextStack` construction and
`kv_storage`, never oplog replay ordering. The only overlap is the word "span". Checked and dismissed.
(One genuine adjacency worth recording: the `StartSpan` entries at `#01087`/`#01102`/`#01118`/`#01134` all
carry `linked span:` equal to their own parent — i.e. `add_link()` fired
(`durable_host/mod.rs:2815-2830`) because the `rpc-connection` parent was not in the current stack. That is
the growth source that bug fixed; here it is benign and unrelated to the trap.)

### 13.7 Fix design

Three parts. (1) is a straight correctness bug and should land regardless. (2) is the principled fix for the
class. (3) is optional depth.

#### 13.7.1 Plug the `invoke_result_seq` leak (required, small, independent)

`io/poll.rs`, in `HostPollable::drop`'s deferred-parent-deletion branch, mirror what the non-deferred branch
in `wasm_rpc/mod.rs:1146` does:

```rust
if should_delete {
    let parent_owned: Resource<golem_wasm::FutureInvokeResultEntry> = Resource::new_own(parent_rep);
    match self.table().delete(parent_owned) {
        Ok(_) => {
            // The rep is only NOW freed back to wasmtime's resource table — the deferred half of
            // HostFutureInvokeResult::drop's HasChildren branch (wasm_rpc/mod.rs:1148-1151), which
            // deliberately does not clear here because the rep was still occupied at that point.
            // Without this, the seq assignment leaks and the next FutureInvokeResult that reuses
            // this rep silently inherits it (invoke_result_seq's or_insert_with hits the stale
            // entry and does not bump next_invoke_result_seq) — two calls, one identity, and a
            // live/replay-divergent counter. See FINDING_B_FIX_DESIGN.md §13.5.
            self.state.clear_invoke_result_seq(parent_rep);
        }
        Err(err) => debug!(parent_rep, error = %err, "Deferred future invoke result delete failed"),
    }
}
```

Centralization note (DRY, and the reason the bug existed): "free the rep" and "clear its seq" are currently
two facts a caller has to remember to keep together, in two different files. Prefer extracting a single
`delete_future_invoke_result(&mut self, rep: u32)` helper that does both, and route *both* call sites
(`wasm_rpc/mod.rs:1139` and `io/poll.rs:212`) through it, so a third deletion path cannot reintroduce the
drift.

This alone does not make the fix sound — it removes one known source of identity drift, but the mechanism
below must not depend on identity being perfect.

#### 13.7.2 No replay consumer may destructively read an entry it has not positively identified as its own (required)

This is the real fix, and it is the generalization the shipped fix stopped one step short of. The invariant:

> **A replay consumer consumes an oplog entry only if that entry positively matches its own identity.
> Anything else is left in place, and the consumer synthesizes a legal "not yet" answer.**

`ready()` already obeys this and is provably order-robust because of it (`io/poll.rs:113-167`: predicate
`*seq == pollable_seq`, and on no-match `Ok(false)` — "not ready yet", a completely legal answer that makes
the guest loop and try again). Every other consumer violates it by ending in an unconditional
`get_oplog_entry!(…, OplogEntry::HostCall)` / `durability.replay()`. Bring them into line:

**a) `HostFutureInvokeResult::get` (`wasm_rpc/mod.rs:929-975`).** Replace the terminal unconditional read
with a predicate read on its own identity, and synthesize `Pending` on miss:

```rust
let peeked = self.state.replay_state.try_get_oplog_entry(|entry| matches!(entry,
    OplogEntry::HostCall {
        function_name: HostFunctionName::GolemRpcFutureInvokeResultGet,
        durable_function_type: DurableFunctionType::WriteRemoteConcurrent(seq), ..
    } if *seq == my_invoke_result_seq)
    // Legacy, pre-WriteRemoteConcurrent oplogs: untagged entries are consumed positionally,
    // exactly as before this change — unchanged behaviour for already-persisted history.
    || matches!(entry, OplogEntry::HostCall {
        function_name: HostFunctionName::GolemRpcFutureInvokeResultGet,
        durable_function_type: DurableFunctionType::WriteRemote, .. })
).await?;
match peeked {
    Some((_, OplogEntry::HostCall { response, .. })) => { /* existing decode, unchanged */ }
    _ => SerializableInvokeResult::Pending,   // legal today: get() already returns Ok(None) for Pending
}
```

`Pending` is not a new code path — `get()`'s existing `match serialized_invoke_result` already handles it
(`wasm_rpc/mod.rs:991-993`, `SerializableInvokeResult::Pending => Ok(None)`), and the
`if !matches!(…, Pending)` guard already skips `end_function`/`finish_span` for it. So on a miss the guest
simply re-registers and polls again — the same self-correcting loop `ready()` relies on. **This alone would
have turned the observed permanent trap into a transient extra loop iteration**, even with the §13.5 seq
drift still present.

**b) `poll()` (`io/poll.rs:414-417`).** Replace `durability.replay(self)` with a predicate read on
`HostFunctionName::IoPollPoll`. On miss, do not consume: synthesize a ready-set from state that is already
known — the indices of input pollables whose `pollable_seq` has a `pre_resolved_pollable_ready(seq) == true`,
plus (via `rpc_pollable_to_parent` → `invoke_result_seq_if_assigned`) the indices of pollables whose parent
future has a `pre_resolved_invoke_result` cached. Those caches exist precisely because a stray-scan already
consumed the corresponding entry, so reporting them ready is not a guess — it is replaying the recorded
answer to the correct pollable. If the synthesized set is empty, return `Ok(all indices)`: waking every
registered task is safe once (a) makes `get()` and `ready()` both identity-matched and self-correcting, and
it guarantees forward progress toward whichever task owns the next entry, instead of livelocking on
`Ok(vec![])`.

**c) HTTP stream reads / `check_write` / `write`** — the same treatment, using the `begin_idx` identity they
already carry. This should be coordinated with §12, which is changing the same call sites; §12's
`exclude_http_begin_idx` widening and this invariant are complementary, not competing (§12 widens *what is
recognized as someone else's*; this bounds *what may be consumed when nothing is recognized as mine*).

**Why this is strictly safer than the status quo, including for already-persisted oplogs.** Today a
consumer that finds a non-matching entry destroys it and then fails. Under the invariant it leaves it and
returns "not yet". The only behavioural change on the happy path is nil (the predicate matches, identical
consumption). On the unhappy path a permanent trap becomes a retry. The one genuinely new risk is an
infinite guest loop when the needed entry can never be produced — bounded by the existing replay-target /
suspend machinery, and strictly preferable to today's permanent, unrecoverable trap. The legacy untagged
fallback arms keep pre-`WriteRemoteConcurrent` oplogs byte-identical.

**Why `consume_stray_entries` itself does not need to change.** It is correct as written: it walks forward
and stops at the first non-match (`replay_state.rs:308-319`), and it correctly stopped at `#01107` here. The
bug is not that it scanned too far — it is that its `is_stray` predicate returned a false positive on the
caller's own entry (because identity drifted), and that the caller's *fallback* was unconditional. Fixing
the fallback makes the mechanism robust to predicate false-positives instead of fatally dependent on
predicate perfection. That dependency is the design smell worth removing: §8.3's "a caller must exclude its
own identity or this would wrongly consume its own genuine entry" is a correctness precondition enforced
only by a `!=` on a value that §13.5 proves is not replay-stable.

**Post-implementation refinement — bounding (b)'s retry with the actual remaining oplog size, not a flat
constant.** The first implementation of (b) bounded the "synthesize and retry" loop with a flat
`MAX_POLL_REPLAY_MISSES = 64`, reverting to the unconditional read after 64 consecutive misses at an
unmoved cursor. Reviewed and replaced: `64` is an arbitrary number with no relationship to the actual
problem size — it is exactly the kind of guessed constant this document's own rigor bar (§7, §11, §12.4)
rejects elsewhere. This directly implements what this section already gestured at above ("bounded by the
existing replay-target… machinery") rather than a round number.

The principled bound: replay operates over a fixed, already-recorded oplog for the duration of one replay
pass (it does not grow mid-replay), so the cursor can advance from position `K` to `replay_target()` only
by *someone* consuming one of the entries still between them — and there are at most
`replay_target() - K` such entries, hence at most that many distinct "someone else made progress" events
before the pass provably has nothing left to give. `record_poll_replay_miss()` now recomputes this
remaining-entry count on every miss (`replay_target().distance_from(cursor)`) and gives up once the
consecutive-miss streak at that fixed cursor exceeds it, instead of comparing against a flat constant.
This is not merely a larger or smaller number than 64 in practice — it is a *different kind* of bound: it
scales down to near-zero patience when almost nothing is left to replay (where 64 retries would have been
pure waste against a pass that's already nearly live), and scales up without an artificial ceiling for a
long remaining history where genuine cross-region interleaving (§13.4's `count: 1 → 2` batch growth is
exactly this) could still legitimately resolve it. The cursor-movement-resets-the-streak behavior is
unchanged — this only replaces the "how many chances" number with one tied to the problem's own bounded
state instead of a guess. Implemented in `record_poll_replay_miss()` (`durable_host/mod.rs`) and its call
site in `io/poll.rs`.

#### 13.7.3 Optional depth: make `IoPollPoll`'s recorded result identity-based, not positional

`HostResponsePollResult { result: Ok(vec![u32]) }` stores indices into the guest's `in_` list. That list's
order is guest-controlled and, for `wstd`'s `Reactor`, `HashMap`-randomized — so a replayed index does not
denote the same pollable it denoted live (§13.5, secondary exposure). Recording the ready pollables'
`pollable_seq` values alongside (not instead of) the indices, and remapping seq → current position on
replay, would close the last positional channel. Costs: a wire-format field on `HostResponsePollResult`
(with the index list retained for old-oplog compatibility), and assigning `pollable_seq` for every member of
`in_` inside `poll()` — which perturbs `next_pollable_seq` numbering relative to already-persisted oplogs
and therefore needs its own careful compatibility analysis. **Recommendation: not now.** 13.7.2(b) already
makes an extra `poll()` iteration harmless, which removes the trap without touching the wire format. Record
this as the known-remaining gap.

### 13.8 Test plan

- **Falsification, `get()` self-consumption (the exact capture):** oplog =
  `[HostCall{GolemRpcFutureInvokeResultGet, WriteRemoteConcurrent(S_live)}, FinishSpan{span_id}]`, caller
  replaying with `my_invoke_result_seq = S_replay != S_live` and `tracked.invoke_result_seqs ∋ S_live`.
  *Before*: asserts the current code consumes entry 1 as a stray and fails on entry 2 with literally
  `expected OplogEntry :: HostCall |, got FinishSpan` — the production string. *After*: asserts the entry is
  left in place and `get()` returns `Ok(None)` (Pending).
- **`poll()` self-consumption:** oplog =
  `[HostCall{GolemRpcFutureInvokeResultGet, WriteRemoteConcurrent(s)}, FinishSpan]` with `s` tracked;
  *before* reproduces the same string via `durability.rs:868`; *after* asserts `poll()` consumes nothing and
  synthesizes from cache.
- **Seq-leak regression (§13.7.1), no oplog involved:** drive `HostFutureInvokeResult::drop` into the
  `HasChildren` branch, then drop the child pollable, then assert `invoke_result_seq_if_assigned(rep)` is
  `None` and that a subsequent `invoke_result_seq(rep)` returns a *fresh* seq with `next_invoke_result_seq`
  incremented. This is the unit test that would have caught the leak — it needs no replay machinery at all.
- **Happy-path byte-identity:** all six existing `io/poll.rs` tests, the §11 `get()`/HTTP stray tests, and
  the Eighth capture's `recover_next_pollable_seq` test must pass unmodified — the predicate read must
  consume exactly what the unconditional read consumed whenever identity matches.
- **Legacy-oplog compatibility:** untagged `WriteRemote`-typed `GolemRpcFutureInvokeResultGet` entries are
  still consumed by `get()`'s replay (the fallback arm), asserted explicitly.
- **Full regression sweep:** same bar as §7/§11/§12.7 — full `--lib`, `durable_host::`, `rpc.rs` and
  `durability.rs` integration suites.

### 13.9 Status

**Design only.** Not implemented, not approved, not committed. No `cargo build` run, no container, golem
server or shared resource touched — offline analysis from
`b2d8d571-scene_plates.oplog.fresh` plus source reading, per this phase's scope. Ready for review.

**Post-implementation addendum — `blocking_read` shares `read`'s gap, found live.** Live-verifying §12's
implementation against `workspace-smoketest`'s `character_sheet` agent (cold-restart, forced replay) showed
the `check_write`/`write` fix working correctly — the agent made genuine further progress past the original
trap, through a `reset`, into an entirely new `run()` invocation — but then hit a new, structurally
identical trap further out: `expected io::poll::poll, got http::types::incoming_body_stream::blocking_read`,
`begin_index: 2253`, `WriteRemoteBatched(Some(OplogIndex(2190)))`. `HttpTypesIncomingBodyStreamBlockingRead`
is a separate `wasi:io/streams` host function from `HttpTypesIncomingBodyStreamRead` (blocking vs
non-blocking read of the same incoming HTTP body stream) but was never brought into the stray-entry
catalog — only `read()`'s non-blocking path was. Since both persist the identical `HostResponseStreamChunk`
shape for the same begin_idx identity, fixed by widening `stray_entry_identity()` and
`decode_and_cache_stray_entry()`'s existing `HttpTypesIncomingBodyStreamRead` arms to also match
`HttpTypesIncomingBodyStreamBlockingRead` (sharing `StrayEntryIdentity::HttpStreamRead` and
`pre_resolved_http_stream_chunk` — no new identity variant or cache needed), and giving
`blocking_read()`'s replay branch (`io/streams.rs`) the same cache→scan→fallback shape `read()` already
has. `skip`/`blocking_skip` were not touched — no live evidence for them yet, same bar as the rest of this
document.

Interaction with §12: independent captures, independent agents, but 13.7.2 proposes an invariant that
governs the same call sites §12 is widening. If both land, §12's widened `is_stray` predicates should be
paired with §13.7.2's bounded fallbacks rather than kept unconditional.

---

## 14. Toward a general mechanism — investigating whether cataloging can be eliminated

Status: **INVESTIGATION / DESIGN ONLY.** No code changed, no `cargo build` run, no container touched.
Worktree `/Users/hannes/work/golem-worktrees/finding-b-general-design`, branch
`hrapp/finding-b-general-mechanism-design`, at `6cb6203fd` ("fix: replace poll() replay-miss flat
constant with a remaining-oplog-size bound"), clean tree.

**Question asked:** every extension of the stray-entry mechanism so far (§8.1 `IoPollReady`, §8.4
`GolemRpcFutureInvokeResultGet`, §8.7 `HttpTypesIncomingBodyStreamRead`, §12
`HttpTypesOutgoingBodyStreamCheckWrite`+`Write`) has been driven by a *live production trap* naming
one more `(HostFunctionName, DurableFunctionType)` pair that had to be hand-added to
`stray_entry_identity()`'s match arms, `decode_and_cache_stray_entry()`'s match arms, and usually a
new typed cache. Is there a mechanism that derives participation from a property these functions
*already* have, so future ones are covered without editing a catalog?

**Answer in one line:** the catalog is eliminable, but not by the exact hypothesis proposed — the
hypothesis as stated (identity from `(variant_kind, inner_value)`, function-name-blind) is
**falsified by the real code**, and the corrected key `(function_name, inner_value)` *is* sound and
*does* eliminate the catalog. Detail below, with the counterexample, the corrected design, the
census of latent gaps, and the two places where irreducible per-call-site knowledge genuinely
remains.

### 14.1 What "the catalog" actually consists of, read from the code

Four separable concerns, currently entangled across two hand-maintained `match` statements and five
typed caches:

| # | Concern | Where it lives today | Generalizable? |
|---|---|---|---|
| 1 | **Identity extraction** — "which tracked operation does this entry belong to" | `stray_entry_identity()`, `durable_host/mod.rs:4331-4365` (5 arms) + `StrayEntryIdentity`, `mod.rs:4321-4327` (5 variants) | **Yes, fully** (§14.4) |
| 2 | **Tracked-identity source** — "is that operation currently live" | `tracked_concurrent_op_seqs()`, `mod.rs:5373-5383` — reads `pollable_seq` / `invoke_result_seq` / `open_http_requests` | **Yes, but needs one new registry** (§14.6) |
| 3 | **Decode + cache** — turn a stray entry into a cached answer | `decode_and_cache_stray_entry()`, `mod.rs:5421-5549` (5 arms) + 5 typed `pre_resolved_*` maps, `mod.rs:4145/4176/4189/4197/4205` | **Yes, fully** (§14.4) |
| 4 | **Own-entry predicate + legal "not yet" answer on a miss** (§13.7.2) | `is_own_poll_entry` `mod.rs:4395-4403`, `is_own_invoke_result_entry` `mod.rs:4376-4390`, `ready()`'s inline predicate `io/poll.rs:119-134` | Predicate: **yes** (§14.8). "Not yet" answer: **no — irreducible** (§14.5) |

Concerns 1 and 3 are the whack-a-mole. Concern 2 is the part nobody has had to touch yet because
all three existing identity families happened to already have a registry. Concern 4's *predicate*
half generalizes; its *answer* half does not, and that is the one genuine per-call-site residue.

### 14.2 Point 2 answered first — the full census, and the latent gaps it exposes

This is independently valuable regardless of the general-mechanism question, so it is stated first.
Method: mechanical extraction of every `Durability::<Pair>::new(ctx, DurableFunctionType::X)` site
under `golem-worker-executor/src/durable_host/` (a script matching the constructor and its
`DurableFunctionType` argument — 94 sites total), plus a separate grep for raw
`oplog.add_host_call(...)` sites that bypass `Durability` entirely, cross-referenced against
`stray_entry_identity()`'s five arms.

**`DurableFunctionType::ReadLocalPollable(u32)` — 1 producing site, covered.**

| Function | Site | In catalog? |
|---|---|---|
| `IoPollReady` | `io/poll.rs:72-81` | **yes** (`mod.rs:4333-4337`) |

**`DurableFunctionType::WriteRemoteConcurrent(u32)` — 1 producing site, covered.**

| Function | Site | In catalog? |
|---|---|---|
| `GolemRpcFutureInvokeResultGet` | `wasm_rpc/mod.rs:878-887` | **yes** (`mod.rs:4339-4343`) |

**`DurableFunctionType::WriteRemoteBatched(Some(_))` — 23 producing sites across 23 distinct
`HostFunctionName`s. 5 covered, 18 NOT.**

| Function | Site(s) | Response type | In catalog? |
|---|---|---|---|
| `HttpTypesIncomingBodyStreamRead` | `io/streams.rs:82-85` | `StreamChunk` | **yes** (`mod.rs:4345-4349`) |
| `HttpTypesOutgoingBodyStreamCheckWrite` | `io/streams.rs:364-367`, `423-426` | `StreamCheckWrite` | **yes** (`mod.rs:4351-4355`) |
| `HttpTypesOutgoingBodyStreamWrite` | `io/streams.rs:490-493`, `555-558`, `615-618` | `StreamWriteWithBytes` | **yes** (`mod.rs:4357-4361`) |
| `HttpTypesIncomingBodyStreamBlockingRead` | `io/streams.rs:167-170` | `StreamChunk` | **NO** in this worktree — reported fixed on another branch by widening the `Read` arm |
| `HttpTypesIncomingBodyStreamSkip` | `io/streams.rs:239-242` | `StreamSkip` | **NO** |
| `HttpTypesIncomingBodyStreamBlockingSkip` | `io/streams.rs:288-291` | `StreamSkip` | **NO** |
| `HttpTypesOutgoingBodyStreamFlush` | `io/streams.rs:715-718` | `StreamWriteResult` | **NO** |
| `HttpTypesOutgoingBodyStreamBlockingFlush` | `io/streams.rs:768-771` | `StreamWriteResult` | **NO** |
| `HttpTypesOutgoingBodyStreamWriteZeroes` | `io/streams.rs:845-848`, `935-938` | `StreamWriteZeroes` | **NO** |
| `HttpTypesOutgoingBodyStreamSplice` | `io/streams.rs:1016-1019` | `StreamSkip` | **NO** |
| `HttpTypesOutgoingBodyStreamBlockingSplice` | `io/streams.rs:1083-1086` | `StreamSkip` | **NO** |
| `HttpTypesFutureTrailersGet` | `http/types.rs:505-508` | `HttpFutureTrailersGet` | **NO** |
| **`HttpTypesFutureIncomingResponseGet`** | `http/types.rs:1507-1512` (raw `add_host_call`, not `Durability`) | `HttpResponse` | **NO — see below, the strongest latent gap** |
| `Rdbms{Mysql,Postgres,Ignite2}DbConnectionQueryStream` (×3) | `rdbms/mod.rs:322-329` | `GolemRdbmsRequest` | **NO** |
| `Rdbms{Mysql,Postgres,Ignite2}DbResultStreamGetColumns` (×3) | `rdbms/mod.rs:432-440` | `GolemRdbmsColumns` | **NO** |
| `Rdbms{Mysql,Postgres,Ignite2}DbResultStreamGetNext` (×3) | `rdbms/mod.rs:506-514` | `GolemRdbmsResultChunk` | **NO** |

(Response types read from the `host_payload_pairs!` table, `golem-common/src/model/oplog/payload/mod.rs:448-505`.)

Also checked and **correctly excluded** — these do NOT use any of the three tracked variants, so they
are outside this bug class by construction, not latent gaps: all `websocket/client.rs` calls
(`WriteRemote`, lines 115/211/301/392/516), all `blobstore/` calls (`ReadRemote`/`WriteRemote`), all
`keyvalue/` calls (`ReadRemote`/`WriteRemote`), `sockets/ip_name_lookup.rs:68` (`ReadRemote`),
`clocks/`, `random/`, `filesystem/`, `quota/`, and `golem/v1x.rs` (`ReadLocal`/`ReadRemote`/
`WriteLocal`/`WriteRemote`). The `rdbms` transaction family (`TxnQuery`/`TxnExecute`/
`TxnQueryStream`, `rdbms/mod.rs:627/696/755`) uses `WriteRemoteTransaction(Some(_))` — a *fourth*
variant with the same structural shape, deliberately left out of scope here (see §14.7(c)).

#### 14.2.1 The strongest latent gap: `http::types::future_incoming_response::get`

This one deserves calling out separately, because it has every property of the four already-confirmed
live traps and one more that makes it worse:

1. **It is tagged with a tracked variant.** `WriteRemoteBatched(Some(begin_index))`, using the
   *same* per-request `begin_index` from `open_http_requests` that the already-fixed
   `read()`/`check_write()`/`write()` use (`http/types.rs:1512`; `outgoing_http.rs:234-237` mints it).
2. **Its replay consumer is the raw, pre-fix, consume-then-validate pattern.**
   `http/types.rs:1212`: `let (_, oplog_entry) = get_oplog_entry!(self.state.replay_state,
   OplogEntry::HostCall)?;` — unconditional, no identity check, no stray-scan, no cache. It is one of
   only **two** remaining unconditional `OplogEntry::HostCall` consumers in the whole crate (the
   other being `read_persisted_durable_function_invocation`, `durability.rs:868`, which every
   `Durability::replay()` goes through). This is verbatim the shape §13.3/§13.7.2 identified as the
   class-level defect.
3. **It is the completion-fetch of a genuinely concurrent operation** — the awaited half of
   `fetch()`. `download-manager.ts:138`'s `Promise.all(batch.map(item => this.fetchBytes(item.url)))`
   puts N of these in flight simultaneously, which is exactly the §2 precondition. The §12.2 capture's
   own census counted **14** `http::types::future_incoming_response::get` calls in a single agent's
   oplog.
4. **It already has a legal "not yet" answer**, so §13.7.2's fix applies verbatim with no invention
   needed: `SerializableHttpResponse::Pending => Ok(None)` (`http/types.rs:1251`).
5. **Its failure mode includes the silent one.** Two concurrent requests' entries share
   `function_name`, so today a mis-ordered pair would be accepted by `get_oplog_entry!` *and* decode
   cleanly as `HostResponse::HttpResponse` — request A silently receiving request B's response
   headers/status. That is §8.7's "silent data misattribution", not a loud trap.

I did not find a captured oplog demonstrating this one; it is inferred from the code above, which is
the same standard of evidence §11 used to (correctly) predict the output-stream gap before §12
captured it live.

Secondary, weaker latent gaps worth recording: `HttpTypesFutureTrailersGet` (same shape, same
per-request identity, `Ok(None)` = Pending available, but only fires for chunked responses with
trailers — rare in practice), and the RDBMS result-stream family (same shape, per-stream identity,
but requires concurrently-awaited query streams and there is no registry for its `begin_index`
today — see §14.6).

### 14.3 The hypothesis as stated is falsified: `(variant_kind, inner_value)` is not an identity

The proposal was to derive identity purely from the `DurableFunctionType` variant shape and its
inner value, dropping the per-function match. Reading the real code, this is **many-to-one over both
consumers and payload shapes**, and adopting it would be a correctness regression, not a
generalization.

**Counterexample, concrete and verified.** A single outgoing HTTP request has exactly one
`begin_index` (`outgoing_http.rs:234-237` mints it once via
`begin_durable_function(&WriteRemoteBatched(None))`; `HttpRequestState.begin_index`,
`durable_host/mod.rs:3913-3917`, stores it once and covers both directions plus the response and
trailers futures). **Thirteen distinct `HostFunctionName`s record entries tagged
`WriteRemoteBatched(Some(that_same_begin_index))`** — `future_trailers::get`,
`future_incoming_response::get`, `incoming_body_stream::{read, blocking_read, skip, blocking_skip}`,
`outgoing_body_stream::{check_write, write, flush, blocking_flush, write_zeroes, splice,
blocking_splice}` (`payload/mod.rs:493-505`) — spanning **eight distinct response payload types**:
`HttpFutureTrailersGet`, `HttpResponse`, `StreamChunk`, `StreamSkip`, `StreamCheckWrite`,
`StreamWriteWithBytes`, `StreamWriteResult`, `StreamWriteZeroes`.

RDBMS reproduces the same shape independently: one result-stream's `begin_index`
(`ctx.table().get(entry)?.begin_index`, `rdbms/mod.rs:431`, `505`) is shared by
`DbConnectionQueryStream`, `DbResultStreamGetColumns` and `DbResultStreamGetNext` — three functions,
three response types (`GolemRdbmsRequest`, `GolemRdbmsColumns`, `GolemRdbmsResultChunk`).

Two concrete failures follow:

1. **Cache mis-delivery — a correctness regression.** With a cache keyed by `begin_idx` alone, a
   stray `check_write` entry consumed by some scan is cached under key `begin_idx`. The next
   consumer to look up `begin_idx` may be `write()`, which does
   `HostResponse::try_into::<HostResponseStreamWriteWithBytes>()` and gets
   `Err("Expected StreamWriteWithBytes, got StreamCheckWrite(...)")` (the macro-generated
   `TryFrom`, `golem-common/src/base_model/oplog/oplog_macro.rs:264-274`). Loud rather than silent
   — but the entry is already durably consumed and the answer is gone, so this converts a working
   replay into a permanent trap. Strictly worse than today.
2. **`StrayEntryScan`'s dedup collapses.** `StrayEntryScan::accept` (`mod.rs:4442-4455`) refuses a
   second entry per distinct identity, precisely because each cache holds one answer. With
   `begin_idx` as the whole identity, a single HTTP request would be able to defer *one* entry
   total across all thirteen functions — so the §12 capture's `check_write`+`write` pair (106 of
   each, 1:1 lockstep, `§12.2`) could not both be deferred, which is exactly what §12.6 established
   is required.

The `WriteRemoteBatched(Some(_))` variant marks *"this entry belongs to batch N"*, not *"this entry
belongs to operation N"*. Batches are not operations. That distinction is what the current code
encodes by having five `StrayEntryIdentity` variants for three `DurableFunctionType` variants.

### 14.4 The corrected hypothesis IS sound: key on `(function_name, inner_value)`

Replacing the five-variant enum with a two-field key restores one-to-one identity while keeping the
derivation fully mechanical — no per-function arm anywhere.

```rust
/// Which concurrently-tracked operation an oplog entry belongs to. Derived structurally from the
/// entry itself: the DurableFunctionType variant says WHICH identity namespace the entry lives in
/// and carries the identity VALUE; the function name distinguishes operations that legitimately
/// share one value (one HTTP request's begin_index is shared by 13 host functions with 8 response
/// shapes — see §14.3). No per-function catalog: any entry carrying one of the three
/// concurrency-tracking variants participates automatically.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StrayEntryIdentity {
    function_name: HostFunctionName,
    namespace: IdentityNamespace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IdentityNamespace {
    Pollable(u32),      // from ReadLocalPollable(seq)
    InvokeResult(u32),  // from WriteRemoteConcurrent(seq)
    Batch(OplogIndex),  // from WriteRemoteBatched(Some(begin_idx))
}

/// The entire replacement for stray_entry_identity()'s five hand-written arms.
pub fn stray_entry_identity(entry: &OplogEntry) -> Option<StrayEntryIdentity> {
    let OplogEntry::HostCall { function_name, durable_function_type, .. } = entry else {
        return None;
    };
    let namespace = match durable_function_type {
        DurableFunctionType::ReadLocalPollable(seq) => IdentityNamespace::Pollable(*seq),
        DurableFunctionType::WriteRemoteConcurrent(seq) => IdentityNamespace::InvokeResult(*seq),
        DurableFunctionType::WriteRemoteBatched(Some(idx)) => IdentityNamespace::Batch(*idx),
        _ => return None,
    };
    Some(StrayEntryIdentity { function_name: function_name.clone(), namespace })
}
```

Five arms keyed on function names become three arms keyed on variant shapes, and the set of
participating functions becomes closed-by-construction: whoever writes
`Durability::<NewThing>::new(self, DurableFunctionType::WriteRemoteBatched(Some(idx)))` gets
stray-participation for free, with no second list to remember.

**`decode_and_cache_stray_entry()` (5 arms, ~130 lines, `mod.rs:5421-5549`) disappears entirely**,
replaced by a single generic cache holding un-narrowed `HostResponse` values:

```rust
/// REPLAY-ONLY: answers consumed early by a DIFFERENT operation's stray-scan, held un-narrowed for
/// their real owner to take and narrow itself. Replaces the five typed pre_resolved_* maps.
pre_resolved_stray: HashMap<StrayEntryIdentity, HostResponse>,
```

```rust
pub async fn decode_and_cache_stray_entry(&mut self, idx: OplogIndex, entry: OplogEntry)
    -> Result<(), WorkerExecutorError>
{
    let identity = stray_entry_identity(&entry)
        .ok_or_else(|| WorkerExecutorError::runtime("scan matched a non-identity entry"))?;
    let OplogEntry::HostCall { response, .. } = entry else { unreachable!() };
    let host_response: HostResponse =
        self.oplog.download_payload(response).await.map_err(WorkerExecutorError::runtime)?;
    self.pre_resolved_stray.insert(identity, host_response);
    Ok(())
}
```

**Is that round-trip actually supported? Verified by reading, not assumed:**

- `OplogEntry::HostCall.response` is `OplogPayload<HostResponse>`
  (`golem-common/src/base_model/oplog/mod.rs:106-118`).
- `Oplog::download_payload` already returns the *enum* `HostResponse`, not a narrowed type — this
  is what the current `decode_and_cache_stray_entry` calls (`mod.rs:5433-5437`), immediately
  followed by `host_response.try_into()` to narrow (`mod.rs:5438-5440`). **The download and the
  narrowing are already two separate steps**; the change is purely to stop performing the second
  one at scan time.
- The narrowing is the macro-generated `impl TryFrom<HostResponse> for HostResponse<Case>`
  (`oplog_macro.rs:264-274`) — a pure `match`, `Error = String`, no side effects, so it is equally
  valid at any later point.
- `HostResponse` derives `Clone, Debug, PartialEq` (`oplog_macro.rs:213`), so it is storable in a
  map by value.
- Every consumer already has its target type available at compile time:
  `HostPayloadPair::Resp: Into<HostResponse> + TryFrom<HostResponse, Error = String> + Clone`
  (`golem-common/src/model/oplog/payload/mod.rs:435`), and every `Durability<Pair>` is
  parameterised by exactly that `Pair`. `Durability::replay()` already performs this identical
  narrowing on the non-cached path (`durability.rs:1260-1268`).

So the answer to "does `OplogPayload`/`HostResponse` support round-tripping this way" is **yes,
verified** — and the design is not even a new capability, it is a *removal* of a premature
narrowing that the current code performs at the wrong place.

**One real, small prerequisite found:** `HostFunctionName` is generated with
`#[derive(Debug, Clone, PartialEq, desert_rust::BinaryCodec)]` (`oplog_macro.rs:326-327`) — it has
neither `Eq` nor `Hash`, and `DurableFunctionType` has `Eq` but not `Hash`
(`golem-common/src/model/oplog/raw_types.rs:308-317`). Using either in a `HashMap` key requires
adding those derives. Both are fieldless enums plus one `Custom(String)` arm, so the derives are
mechanical and total — but this is a `golem-common` change, not a `golem-worker-executor`-local one,
and should be called out rather than discovered at compile time.

### 14.5 Question 1 answered: what irreducible per-type knowledge remains

Three things remain per-call-site. Two of them are *not* the problem being solved; one is real.

1. **Narrowing the cached `HostResponse` to the concrete response type at the point of use.**
   Already free — `Pair::Resp` is a compile-time associated type on the `Durability<Pair>` the call
   site already constructs. **This is emphatically NOT the same problem as the catalog**, and the
   distinction is worth stating precisely: the catalog is a *centralized* list that must be kept in
   sync with a *decentralized* set of call sites — the failure mode is a call site existing that the
   central list has never heard of, which is undiscoverable by the compiler and only surfaces as a
   production trap. Narrowing-at-use has the opposite structure: the knowledge lives at the one
   place that already has it, the compiler enforces it, and a new call site cannot be "forgotten"
   because it does not exist until someone writes it. Confirmed by reading: `durability.rs:1260-1268`
   already does exactly this narrowing today for every one of the 92 `durability.replay(...)` call
   sites in the crate, and nobody maintains a list of them.

2. **Consumer-side side effects on a cache hit.** e.g. `future_incoming_response::get`'s
   `self.table().push(incoming_response)` / `continue_http_request(...)` / `state.response_status`
   mutation (`http/types.rs:1251-1275`). These already live at the call site and are unaffected —
   the stray-scan only downloads and caches the raw payload; the owner still performs its own
   effects when it takes the cached answer.

3. **The legal "not yet" answer on an own-entry miss (§13.7.2) — genuinely irreducible.** There is
   no way to derive from `(function_name, DurableFunctionType)` that
   `SerializableInvokeResult::Pending`, or `SerializableHttpResponse::Pending`, or `Ok(false)`, or
   `Ok(None)` is the value that makes the guest retry harmlessly. It is a semantic property of each
   host function's contract. **However**, a safe *generic default* exists and is strictly better
   than today: on a miss, **do not consume, and return
   `WorkerExecutorError::unexpected_oplog_entry`**. Today's behaviour is *consume, then* fail
   (`durability.rs:1287-1294`: `read_persisted_durable_function_invocation()` runs the destructive
   `get_oplog_entry!` at `durability.rs:868` and only then `validate_oplog_entry` runs) — which is
   precisely the §13.3 defect that destroys structural entries and makes a recoverable divergence
   permanent. So the generalization can ship with "non-destructive error" as the default for all 92
   `Durability::replay()` sites, and the handful of functions with a real Pending answer opt in
   individually. That opt-in is a per-call-site override, not a central catalog.

### 14.6 Question 2's other half: the tracked-identity source, and why it must NOT be dropped

The tempting further simplification — "if every consumer participates, we don't need
`tracked_concurrent_op_seqs()` at all; any entry with a tracked-shape variant is deferrable" — is
**unsound**, and there is a concrete counterexample in the shipped code.

**Counterexample.** `check_write()` has three replay-reachable branches, and only the first consults
the cache:
- `io/streams.rs:397-415` — the `is_http` branch: cache → scan → `durability.replay`. Participates.
- `io/streams.rs:417-430` — the `replaying_http_batch` branch: plain `durability.replay(self)`. Does
  **not** consult `pre_resolved_http_stream_check_write`.
- `io/streams.rs:432-465` — the post-snapshot-restore branch: a bare function-name-only
  `try_get_oplog_entry`. Does **not** consult the cache either.

The second and third branches are reached exactly when `open_http_requests` is empty — which is also
exactly when `tracked_concurrent_op_seqs()` returns an empty `http_begin_indexes` set
(`mod.rs:5377-5381`), so today nothing defers their entries and they see them. Remove the tracked-set
guard and some other consumer's scan can now defer an entry that one of these two branches is about
to look for; the post-snapshot branch's predicate then returns `None` and it **falls through to a
live `HostOutputStream::check_write` while still replaying** (`io/streams.rs:466-470`). That is a
real regression, and it is the precise scope caveat already documented in
`tracked_concurrent_op_seqs()`'s own doc comment (`mod.rs:5365-5372`) and §11.

**So: generalize the source, keep the guard.** The generalization is straightforward and, notably,
covers the whole `WriteRemoteBatched` family in one stroke:

```rust
/// Every currently-open batched-remote-write region, by its BeginRemoteWrite index. Inserted in
/// begin_function() when a WriteRemoteBatched(None) call mints one, removed in end_function() when
/// the matching EndRemoteWrite is written/consumed — the single authoritative pair of points, so a
/// new batched host function needs no registry work of its own.
open_batches: HashSet<OplogIndex>,
```

Verified this is the right pair of points by reading them: `begin_function`
(`durable_host/mod.rs:1203-1345`) takes the `BeginRemoteWrite` branch for
`WriteRemoteBatched(None)` **unconditionally, independent of `assume_idempotence`**
(`mod.rs:1212-1217`), writing the entry when live (`mod.rs:1219-1225`) and consuming it via
`get_oplog_entry!(.., OplogEntry::BeginRemoteWrite)` when replaying (`mod.rs:1227-1228`) — so the
index is symmetric across live and replay, which is exactly the property §8.2.1 found *missing* from
RPC's rejected `begin_index` idea. `end_function` (`mod.rs:1348-1381`) is the symmetric close
(`mod.rs:1365-1376`).

This registry has **the same scope** as today's `open_http_requests`-derived set — it is repopulated
on a full replay that re-executes `handle()`, and is not repopulated when resuming past a snapshot
taken mid-request (in which case `begin_function` is not re-executed either, for the same reason).
So it is a strict generalization with no scope change: HTTP behaves identically, and RDBMS
result-streams gain a registry they do not have today for free. `pollable_seq` and
`invoke_result_seq` stay exactly as they are (one producing site each, §14.2).

Net effect on `tracked_concurrent_op_seqs()`: three sources instead of three sources — but the third
is now shape-derived rather than HTTP-specific, so **adding a new `WriteRemoteBatched` host function
requires zero registry work**, which was the last remaining per-function obligation.

### 14.7 Question 3 answered: where generalization could be unsound — five candidates checked

I looked specifically for a use of one of the three variants that means something *other than*
"these are fungible concurrent siblings". Findings:

**(a) `RdbmsXxxDbConnectionQueryStream` mints its own identity in the same call that records its
entry.** `rdbms/mod.rs:322-329`: `begin_durable_function(&WriteRemoteBatched(None))` on line 322-324
immediately followed by `Durability::<T::ConnQueryStream>::new(ctx,
WriteRemoteBatched(Some(begin_index)))` on line 325-329. Unlike pollables (created before `ready()`),
RPC futures (created in `async_invoke_and_await`, §8.8) and HTTP streams (opened in `handle()`), this
identity does not exist before its own entry does — so a `ConnQueryStream` entry can never
*legitimately* be a stray for someone else's scan. Under the `open_batches` registry it would
nonetheless become deferrable (registration happens on the immediately preceding line). Traced
through: **not unsound** — if a sibling's scan defers it, the owner's own generalized replay finds it
in the cache under the identical key. It is dead machinery for that function, not wrong machinery.
Worth a code comment so a future reader does not mistake it for an oversight.

**(b) The same function is sometimes tracked and sometimes not.**
`db_result_stream_durable_get_columns`/`get_next` (`rdbms/mod.rs:432-437`, `506-511`) pick
`WriteRemoteTransaction(Some(idx))` when the stream is inside a transaction and
`WriteRemoteBatched(Some(idx))` otherwise. So participation is a property of the *entry*, not of the
*function*. This is not unsound (the transactional case simply does not participate, conservatively),
but it means the design's own framing must be "any **entry** tagged with one of these variants", not
"any **function** tagged with one of these variants". Stated here so the invariant is not
mis-transcribed later.

**(c) `WriteRemoteTransaction(Some(_))` is a fourth variant of identical structural shape.**
`raw_types.rs:343-344`; used at `rdbms/mod.rs:627/696/755` and the two sites in (b). Deliberately
left out. Reasoning: transactional RDBMS operations are serialized within a transaction by
construction, so the "concurrent siblings" precondition does not hold the way it does for pollables /
RPC futures / HTTP body streams; and unlike the other three, `no_concurrent_side_effect`
(`golem-common/src/model/oplog/mod.rs:99-118`) treats it as batch-scoped in the same way — i.e. the
engine already assumes transactional entries are not interleaved with foreign ones. Adding it should
require its own live evidence, exactly as §11/§12 required for each HTTP direction. **This is the
one place where the "recognize any of the tracked variant shapes" rule must remain a deliberate
three-item list rather than "anything carrying an inner value"** — the list is over *variants*
(closed, 3 entries, changes only when the enum changes) rather than over *functions* (open, 23 and
growing), which is the whole point.

**(d) Repeated calls under a single identity, and the implicit FIFO assumption.** `check_write`/
`write` fire 106 times each under one `begin_index` (§12.2); `StreamGetNext` fires once per row
chunk under one stream `begin_index`. The cache holds one answer per key, and `StrayEntryScan`
(`mod.rs:4417-4456`) plus `cached_stray_identities()` (`mod.rs:5389-5414`) bound a scan to one entry
per identity including already-cached-but-uncollected ones. That machinery transfers verbatim to
`(function_name, inner_value)` keys — indeed it becomes *more* precise, since today's five-variant
enum already keys `check_write` and `write` separately and the new key does the same for the other
18 functions. The bound relies on an assumption worth making explicit: **the first entry deferred
for key K must be the one K's owner wants next.** That holds because each key corresponds to one
WASI resource read/written by one guest task in order (WASI resource ownership makes two tasks
sharing a `Resource<InputStream>` impossible). It would not hold for a hypothetical identity shared
by two independent consumers — none found.

**(e) `end_durable_function` is skipped on the cache-hit path — a pre-existing asymmetry the
generalization removes.** Today's three cache-hit paths (`io/streams.rs:133-136` for `read()`,
`io/streams.rs:396-402` for `check_write()`, `wasm_rpc/mod.rs:928-932` for `get()`) return the cached
value directly, bypassing `Durability::replay_raw` and therefore its
`ctx.end_durable_function(&self.function_type, self.begin_index, false)` call
(`durability.rs:1293-1294`). For `WriteRemoteBatched(Some(_))` `end_function` itself is a no-op
(`mod.rs:1379`), but the `DurabilityHost` wrapper additionally performs
`commit_oplog_and_update_state(CommitLevel::DurableOnly)` for any `WriteRemoteBatched(_)`
(`durability.rs:817-828`), which the cache-hit path therefore skips. I did not find a failure mode
this causes on the replay path (it commits already-persisted state), so this is recorded as an
inconsistency rather than a bug — but note that pushing the mechanism *inside* `Durability::replay()`
(§14.9) makes the cache hit go through the same tail and removes the asymmetry for free.

**No case was found in which deferring a tracked-shape entry is semantically wrong.** The dangers are
all about *identity resolution* (a) — (d) and *coverage* (§14.6), not about deferral itself.

### 14.8 Question 4 answered: §13.7.2's own-entry predicates generalize too

Yes — and to a single function with a three-row table, not one predicate per consumer.

The three predicates in the code today:

| Consumer | Predicate | Own-identity test | Legacy allowance |
|---|---|---|---|
| `ready()` | inline, `io/poll.rs:119-134` | `(IoPollReady, ReadLocalPollable(seq))`, `seq == my_seq` | `(IoPollReady, ReadLocal)` |
| `get()` | `is_own_invoke_result_entry`, `mod.rs:4376-4390` | `(GolemRpcFutureInvokeResultGet, WriteRemoteConcurrent(seq))`, `seq == my_seq` | `(GolemRpcFutureInvokeResultGet, WriteRemote)` |
| `poll()` | `is_own_poll_entry`, `mod.rs:4395-4403` | `(IoPollPoll, *)` | n/a |

All three are the same shape: *same function name* AND *(same identity value OR an untagged legacy
predecessor of the same variant family)*. The general form:

```rust
/// Does this entry positively identify as the caller's own? The §13.7.2 invariant, expressed once.
/// `expected` is the caller's own DurableFunctionType — the exact value it tags its LIVE entry with
/// — so the identity comparison needs no per-call-site code.
pub fn is_own_host_call_entry(
    entry: &OplogEntry,
    expected_fn: &HostFunctionName,
    expected: &DurableFunctionType,
) -> bool {
    let OplogEntry::HostCall { function_name, durable_function_type, .. } = entry else {
        return false;
    };
    function_name == expected_fn
        && (durable_function_type == expected
            || is_legacy_untagged_predecessor(durable_function_type, expected))
}

/// Pre-tagging oplogs recorded these entries with the untagged parent variant. Closed 3-row table,
/// keyed on the ENUM, so it changes only when DurableFunctionType changes — never per function.
fn is_legacy_untagged_predecessor(actual: &DurableFunctionType, expected: &DurableFunctionType) -> bool {
    matches!(
        (actual, expected),
        (DurableFunctionType::ReadLocal,   DurableFunctionType::ReadLocalPollable(_))
      | (DurableFunctionType::WriteRemote, DurableFunctionType::WriteRemoteConcurrent(_))
    )
}
```

Checked against each existing predicate:

- **`ready()`**: `expected = ReadLocalPollable(my_seq)`, `expected_fn = IoPollReady` → identical
  behaviour including the legacy `ReadLocal` arm. ✔
- **`get()`**: `expected = WriteRemoteConcurrent(my_seq)` → identical including the legacy
  `WriteRemote` arm. ✔
- **`poll()`**: `expected = ReadLocal` (the value `io/poll.rs:277-278` tags its live entry with — the
  only production `IoPollPoll` site, verified by grep). The general form additionally requires
  `durable_function_type == ReadLocal`, i.e. it is strictly *stricter* than
  `is_own_poll_entry`'s function-name-only match. Since no other `DurableFunctionType` has ever been
  used for `IoPollPoll`, the two are equivalent on all real oplogs. ✔
- **`WriteRemoteBatched(Some(_))` needs no legacy row**: verified that no production
  `Durability::<_>::new` site ever passes `WriteRemoteBatched(None)` (the census in §14.2 found zero;
  `None` appears only at `begin_durable_function`/`end_durable_function` call sites —
  `http/mod.rs:31`, `outgoing_http.rs:235,414`, `rdbms/mod.rs:323,380,602`), so every persisted
  `WriteRemoteBatched` HostCall entry has always carried `Some(idx)`.

**Bonus, and this is the substantive part:** because `Durability<Pair>` already holds both halves —
`Pair::HOST_FUNCTION_NAME` (a const, `payload/mod.rs:441`) and `self.function_type`
(`durability.rs:1037`) — this predicate can be applied *inside* `Durability::replay()` with **no
argument from the call site at all**. That is what turns the fix from "five call sites" into "all 92".

### 14.9 Concrete design

Five pieces. (1)–(3) are the catalog elimination; (4) is the payoff; (5) is staging.

**1. `golem-common`:** add `Eq, Hash` to the `HostFunctionName` derive (`oplog_macro.rs:326`) and
`Hash` to `DurableFunctionType` (`raw_types.rs:308-317`). No behavioural change; both are required
purely to use them as `HashMap` keys.

**2. `durable_host/mod.rs`:** replace `StrayEntryIdentity` (5 variants, `mod.rs:4321-4327`) and
`stray_entry_identity()` (5 arms, `mod.rs:4331-4365`) with the struct + 3-arm shape match of §14.4.
Replace the five `pre_resolved_*` maps (`mod.rs:4145`, `4176`, `4189`, `4197`, `4205`) and their ten
`record_/take_` accessors (`mod.rs:5049-5248`) with one `pre_resolved_stray:
HashMap<StrayEntryIdentity, HostResponse>` and one `record_/take_` pair. Replace
`decode_and_cache_stray_entry()`'s 5 arms (`mod.rs:5421-5549`, ~130 lines) with the 8-line generic
download-and-store of §14.4. `cached_stray_identities()` (`mod.rs:5389-5414`) collapses to
`self.pre_resolved_stray.keys().cloned().collect()`. `is_stray_concurrent_entry`
(`mod.rs:4291-4312`) keeps its structure but takes `exclude: Option<&StrayEntryIdentity>` instead of
the two ad-hoc `exclude_invoke_result_seq` / `exclude_http_begin_idx` parameters, and consults a
`tracked` set of `IdentityNamespace` values rather than three typed sets.

**3. `tracked_concurrent_op_seqs()`** (`mod.rs:5373-5383`) returns a single
`HashSet<IdentityNamespace>` built from `pollable_seq.values()`, `invoke_result_seq.values()`, and
the new `open_batches` registry (§14.6) — the last replacing the `open_http_requests`-derived set,
maintained in `begin_function` (`mod.rs:1219-1228`) / `end_function` (`mod.rs:1365-1376`).
`is_pollable_pre_resolved_ready` (`mod.rs:5258-5272`), used by `poll()`'s synthesis path, rewrites
against the single map with `StrayEntryIdentity { function_name: IoPollReady, .. }` /
`{ function_name: GolemRpcFutureInvokeResultGet, .. }` lookups.

**4. Push the whole protocol into `Durability::replay()`** — the reason to do any of this. Add three
methods to the `DurabilityHost` trait (`durability.rs:501-545`, and to `MockDurabilityHost`,
`durability.rs:1669+`):

```rust
async fn consume_and_cache_stray_entries(&mut self, exclude: Option<&StrayEntryIdentity>)
    -> Result<(), WorkerExecutorError>;
fn take_pre_resolved_stray(&mut self, id: &StrayEntryIdentity) -> Option<HostResponse>;
async fn try_read_own_host_call(&mut self, fname: &HostFunctionName, dft: &DurableFunctionType)
    -> Result<Option<PersistedDurableFunctionInvocation>, WorkerExecutorError>;
```

and restructure `Durability::replay_raw` (`durability.rs:1270-1296`):

```rust
pub async fn replay_raw(&self, ctx: &mut impl DurabilityHost) -> Result<HostResponse, WorkerExecutorError> {
    // ... unchanged PersistNothing warning ...
    let identity = stray_identity_of(Pair::HOST_FUNCTION_NAME, &self.function_type);

    let response = match &identity {
        // 1. Untracked function type — unchanged legacy path, byte for byte.
        None => {
            let e = ctx.read_persisted_durable_function_invocation().await?;
            Self::validate_oplog_entry(&e, Pair::FQFN, self.begin_index)?;
            e.response
        }
        Some(id) => {
            // 2. A sibling's scan may already hold my answer.
            if let Some(cached) = ctx.take_pre_resolved_stray(id) {
                cached
            } else {
                // 3. Defer past strays belonging to OTHER tracked operations.
                ctx.consume_and_cache_stray_entries(Some(id)).await?;
                // 4. §13.7.2: consume only a positively-identified own entry.
                match ctx.try_read_own_host_call(&Pair::HOST_FUNCTION_NAME, &self.function_type).await? {
                    Some(e) => e.response,
                    // 5. Generic default: non-destructive error. Call sites with a legal
                    //    "not yet" answer override via replay_or(not_yet) — see below.
                    None => return Err(WorkerExecutorError::unexpected_oplog_entry(
                        Pair::FQFN, "no matching entry at cursor")),
                }
            }
        }
    };
    ctx.end_durable_function(&self.function_type, self.begin_index, false).await?;
    Ok(response)
}

/// Opt-in for host functions that have a legal "not yet" answer (§13.7.2, §14.5.3):
/// `HttpTypesFutureIncomingResponseGet` -> SerializableHttpResponse::Pending,
/// `HttpTypesFutureTrailersGet` -> Ok(None), RPC get() -> SerializableInvokeResult::Pending, ...
pub async fn replay_or(&self, ctx: &mut impl DurabilityHost, not_yet: Pair::Resp)
    -> Result<Pair::Resp, WorkerExecutorError> { /* as above, returning not_yet at step 5 */ }
```

With this, **all 92 `durability.replay(...)` / `replay_raw(...)` call sites in the crate get the
mechanism at once**, and the five bespoke cache-then-scan-then-fallback blocks currently hand-written
at `io/streams.rs:133-148`, `io/streams.rs:396-415`, `io/streams.rs:~520-545` (`write`),
`wasm_rpc/mod.rs:926-985` and `io/poll.rs:394-470` collapse to `durability.replay(self)` /
`durability.replay_or(self, Pending)`. Note `poll()` and `ready()` cannot go through
`Durability::replay()` as written (both hand-roll their reads and `ready()` never constructs a
`Durability` on the replay path, `io/poll.rs:112-167`) — they keep their bespoke code, but consume
the same three shared primitives.

`http/types.rs:1212`'s raw `get_oplog_entry!(.., OplogEntry::HostCall)` (§14.2.1) is the one site
that must be converted by hand rather than inherited, since it never used `Durability` in the first
place. Converting it to a `Durability::<HttpTypesFutureIncomingResponseGet>` + `replay_or(self,
HostResponseHttpResponse { response: SerializableHttpResponse::Pending })` closes the single
highest-risk latent gap found in this investigation.

**5. Staging.** Land in three separately-falsifiable commits, not one: (i) key/cache unification
(pure refactor — all existing `stray_entry_tests` (`mod.rs:4458-4790`) and `replay_state.rs` scan
tests must pass **unmodified**, which is the falsification bar: the refactor is correct iff it is
invisible); (ii) the `open_batches` registry replacing `http_begin_indexes` (same bar, plus a new
test that an RDBMS-shaped `begin_index` is now tracked); (iii) `Durability::replay()` integration
plus the `http/types.rs:1212` conversion (the behavioural change; needs its own before/after
falsification pair per §12.7/§13.8's pattern, seeded from the `future_incoming_response::get` shape).

### 14.10 Verdict

**The generalization is sound and worth building, with one correction and one caveat.**

- **Correction:** identity must be `(function_name, inner_value)`, not `(variant_kind, inner_value)`.
  The function-name-blind form is falsified by 13 HTTP host functions sharing one `begin_index` across
  8 response types (§14.3) — adopting it would be a correctness regression, not a generalization.
- **Caveat:** the *catalog of functions* is fully eliminable; the *list of participating
  `DurableFunctionType` variants* is not, and should not be — it stays a deliberate 3-row match over
  a closed enum (`WriteRemoteTransaction` is excluded on purpose, §14.7(c)). That list changes only
  when the enum changes, which is a compiler-visible event, unlike a function catalog.
- The tracked-identity guard must be kept, not dropped (§14.6, counterexample in `check_write`'s two
  non-participating replay branches) — but its source generalizes to an `open_batches` registry
  maintained at the single `begin_function`/`end_function` pair, after which a new
  `WriteRemoteBatched` host function needs *zero* engine changes to participate.
- The one genuinely irreducible per-type fact is the legal "not yet" answer (§14.5.3), and it has a
  safe generic default (non-destructive error) that is strictly better than today's
  consume-then-fail.

**What was verified by reading code vs. inferred.** Verified by reading: the full census in §14.2
(mechanical extraction over 94 `Durability::new` sites plus a raw-`add_host_call` grep); the 13
functions / 8 response types sharing one HTTP `begin_index` (`payload/mod.rs:493-505` cross-referenced
with `HttpRequestState.begin_index`, `mod.rs:3913-3917`); the `HostResponse` download/narrow
separation and its `TryFrom` (`oplog_macro.rs:264-274`, `mod.rs:5433-5440`, `durability.rs:1260-1268`);
the missing `Eq`/`Hash` derives (`oplog_macro.rs:326`, `raw_types.rs:308-317`); `begin_function`'s
unconditional `BeginRemoteWrite` for `WriteRemoteBatched(None)` on both live and replay
(`mod.rs:1212-1228`); `check_write`'s three replay branches and which consult the cache
(`io/streams.rs:397-465`); `future_incoming_response::get`'s unconditional
`get_oplog_entry!(.., HostCall)` and its `Pending` arm (`http/types.rs:1212`, `:1251`); the
equivalence of all three existing own-entry predicates to the general form (§14.8). Inferred, not
proven: that `future_incoming_response::get` is *live*-reachable as a trap (no capture yet — the
argument is structural, at the same evidence standard §11 used to correctly predict §12's
output-stream trap); that the `end_durable_function` cache-hit asymmetry (§14.7(e)) is harmless; and
that the RDBMS result-stream family is a real-world concurrency risk at all (it has the shape, but
no observed concurrent-query-stream workload).

### 14.11 Test plan sketch

- **Falsification of §14.3's counterexample (must exist before the refactor lands):** a test that
  builds a `check_write` and a `write` entry sharing one `begin_idx`, runs a scan, and asserts *both*
  are deferred and each is returned to its own consumer narrowed correctly. Under the rejected
  function-name-blind key this test cannot pass — that is the permanent proof the correction was
  necessary.
- **Refactor-invisibility (the bar for staging step (i)):** every existing test in
  `mod.rs`'s `stray_entry_tests` (`mod.rs:4458-4790`) and `replay_state.rs`'s scan tests passes with
  **zero edits** beyond the mechanical `StrayEntryIdentity` constructor change.
- **New-function-participates-for-free:** a test that constructs an oplog entry for a function *not*
  named anywhere in the engine's stray code (e.g. `HttpTypesOutgoingBodyStreamFlush`), tagged
  `WriteRemoteBatched(Some(tracked_idx))`, and asserts `stray_entry_identity` recognizes it and
  `decode_and_cache_stray_entry` caches it. This is the test that proves the catalog is gone.
- **Tracked-guard preserved:** the same entry with `tracked_idx` *not* in `open_batches` must be
  rejected — the §14.6 counterexample, as a regression test.
- **`open_batches` lifecycle:** register on `begin_function(WriteRemoteBatched(None))`, deregister on
  `end_function`, symmetric live/replay; plus the negative case (mid-request snapshot → empty set →
  nothing deferred, matching §11's documented scope).
- **`future_incoming_response::get` before/after pair**, seeded from the shape in §14.2.1: two
  concurrent requests' `HttpTypesFutureIncomingResponseGet` entries with swapped order. *Before*:
  assert the current `get_oplog_entry!` path silently returns request B's response to request A
  (the silent-misattribution failure mode). *After*: assert A defers B's entry, caches it, and reads
  its own.
- **Legacy-oplog byte-identity:** untagged `WriteRemote`-typed `GolemRpcFutureInvokeResultGet` and
  `ReadLocal`-typed `IoPollReady` entries still consumed by their owners via
  `is_legacy_untagged_predecessor` (§14.8).
- **Full regression sweep** at the §7/§11/§12.7/§13.8 bar: full `--lib`, `durable_host::`, and the
  `rpc.rs` / `durability.rs` integration suites.

### 14.12 Status

**Design only.** Nothing implemented, nothing committed, no build run. Ready for review.

### 14.13 Implementation status — what actually landed

Status: **IMPLEMENTED.** Branch `hrapp/finding-b-general-mechanism-design`, five commits on top of
`2b873f4b9`. `cargo build --release -p golem-worker-executor` clean;
`cargo test -p golem-worker-executor --lib --release` = **488 passed, 0 failed, 0 ignored, 0
filtered** (461 before this work, +27 new).

**Landed as designed:**

| §14 proposal | Landed |
|---|---|
| `Eq`/`Hash` on `HostFunctionName` (`oplog_macro.rs:326`) | yes — `Hash` on `DurableFunctionType` turned out unnecessary (see below) |
| `StrayEntryIdentity = (HostFunctionName, IdentityNamespace)` | yes, verbatim |
| `stray_entry_identity()` → 3-arm shape match, no per-function arms | yes |
| Five typed `pre_resolved_*` maps → one `HashMap<StrayEntryIdentity, HostResponse>` | yes |
| `decode_and_cache_stray_entry()`'s five arms (~130 lines) → generic download-and-store | yes |
| `cached_stray_identities()` → `keys().cloned()` | yes |
| `open_batches` registry in `begin_function`/`end_function` | yes |
| Tracked-identity guard kept (§14.6 counterexample) | yes, with a dedicated regression test each side |
| Push the protocol into `Durability::replay_raw` (§14.9 item 4) | yes — see scope note below |
| `http/types.rs`'s raw `get_oplog_entry!` converted by hand | yes |

**Three deliberate deviations, each with its reason:**

1. **`Hash` on `DurableFunctionType` was not needed.** §14.9 item 1 called for it, but the identity
   is keyed on the purpose-built `IdentityNamespace` (which carries only `u32`/`OplogIndex`), not on
   `DurableFunctionType` itself. Adding a derive to a `golem-common` wire type with no consumer would
   have been dead API surface, so it was dropped. `Eq`/`Hash` on `HostFunctionName` — the half that
   *is* a map key — landed as proposed.

2. **§14.9's steps 4–5 (`try_read_own_host_call` + the non-destructive-miss default, and
   `replay_or`) were NOT adopted.** `replay_raw`'s integration is cache → scan → **the existing,
   unchanged** `read_persisted_durable_function_invocation()` + `validate_oplog_entry()`. Reasoning,
   arrived at in the code rather than from the design:
   - The scan alone is *sufficient* for every gap §14.2 catalogued, including §14.2.1's silent
     misdelivery: request A's scan recognizes and defers B's `future_incoming_response::get` entry
     before A reads, so A reads its own. The regression test
     `concurrent_future_response_entries_reach_their_own_callers` asserts exactly that, on payload
     content, not merely "no trap".
   - Making the final read identity-strict across all ~92 sites would newly *reject* any entry whose
     `durable_function_type` differs from what the replaying call computes, where today only the
     function name is validated. That is a behaviour change on paths with no evidence of a problem —
     the opposite of the "no behaviour change expected on already-fixed call sites" bar §14.11 set.
   - The non-destructive miss additionally needs a legal "not yet" answer per function (§14.5.3) and,
     where none exists, risks converting a trap into an unbounded guest retry loop. `poll()` needed a
     whole bounded-miss mechanism (`record_poll_replay_miss`) to make that safe for ONE call site;
     generalising it to 92 is a separate change with its own risk budget.

   §13.7.2's non-destructive reads therefore stay exactly where they already are — `poll()`,
   `ready()`, RPC `get()` — and are unchanged by this work. Extending them is tracked as follow-on,
   not silently skipped.

3. **`read` and `blocking_read` now carry SEPARATE identities.** The prior point-fix folded
   `HttpTypesIncomingBodyStreamBlockingRead` into `read`'s identity because the cache was typed by
   response shape and both produce `StreamChunk`. With the cache keyed by function name that hack is
   unnecessary and strictly worse: replay re-executes the same guest code and therefore the same
   variant, so the per-function key is the correct one, and a scan can now defer one entry of each
   instead of one in total.

**One semantic widening worth flagging, because it changed an existing test.** Exclusion is now
per-*identity*, where §12.5 made it per-*request*: a `read()` on request A now defers A's own
`check_write`/`write` entries instead of refusing them. This is safe and strictly better — under the
old rule such an entry fell through to `read()`'s own oplog read and trapped on the function-name
mismatch; now it is cached under its own key and collected by the call that owns it, which only works
because `Durability::replay_raw` makes *every* consumer a collector. The old test
(`excludes_own_http_request_identity_across_both_directions`) was rewritten as
`exclusion_is_per_identity_not_per_request`, with `each_deferred_entry_is_returned_to_its_own_owner`
added as the §14.3-counterexample proof that the three functions sharing one `begin_index` each get
their own answer back, narrowed to their own type.

**Audit of the 18 gaps against real code, as §14 required — all confirmed to have the bug pattern
before the fix, none excluded:** all sixteen `Durability`-based ones
(`skip`/`blocking_skip`/`blocking_read`, `flush`/`blocking_flush`/`write_zeroes`/`splice`/
`blocking_splice`, `future_trailers::get`, and `DbConnectionQueryStream`/`DbResultStreamGetColumns`/
`DbResultStreamGetNext` × 3 dialects) had a bare `durability.replay(ctx)` on their replay branch —
unconditional positional consumption, no identity check — and all sixteen are fixed by the
`replay_raw` change alone, with no per-function work. `future_incoming_response::get` had the raw
`get_oplog_entry!` and needed the hand conversion. The RDBMS transaction family
(`WriteRemoteTransaction(Some(_))`) remains deliberately out of scope per §14.7(c), pinned by
`transactional_entries_never_participate`.

**Tests added (27):** the §14.3 counterexample; catalog-elimination across all nineteen
previously-uncovered `HostFunctionName`s; the tracked-guard both ways; consumer-side vs entry-side
identity agreement plus the closed three-variant list; transactional exclusion; the §14.2.1
silent-misdelivery before/after pair on payload content; and one cursor-level deferral regression per
newly-wired family (outgoing stream, incoming stream, trailers, RDBMS). Every pre-existing
`stray_entry_tests` and `replay_state::tests` case survives with only the mechanical identity-
constructor change, except the one documented above.

## 15. Fourteenth capture — one identity repeating back to back strands its own second entry

Status: **IMPLEMENTED.** Branch `hrapp/scene-plates-sequential-poll-trap`.

### 15.1 The live capture

`WorkerAgent("workspace-smoketest@1.0","3e0f1d89-bd02-4bff-a96e-3374a03f2399@character_sheet")`,
oplog entries `#02254` and `#02255` — both `HttpTypesIncomingBodyStreamBlockingRead`, both
`WriteRemoteBatched(Some(OplogIndex(2190)))`, i.e. **the same `StrayEntryIdentity`, twice in a row**.
The trap that followed:

```
Unexpected oplog entry during replay: expected io::poll::poll,
got http::types::incoming_body_stream::blocking_read
```

Raw evidence: `oplog-backups/2026-08-30_WorkerAgent_workspace-smoketest_3e0f1d89-character_sheet_POSTFIX_CHECKWRITE-FIXED_BLOCKINGREAD-TRAP.oplog`.

Two `blocking_read()` calls on one incoming body stream is not an exotic shape — it is what a chunked
response body that doesn't arrive in a single call always produces, the incoming-direction twin of
§12.2's 106 `check_write`/`write` pairs.

### 15.2 Root cause: the one-answer-per-identity cache, and the scan bound built on top of it

Both halves of §14's general mechanism assumed at most one pending answer per identity:

- `pre_resolved_stray: HashMap<StrayEntryIdentity, HostResponse>` — one slot per key.
- `StrayEntryScan` therefore refused a second entry for an identity it had already consumed, **or
  one whose answer was still uncollected in the cache** (`cached_stray_identities()` seeded the
  scan's `consumed` set). Documented in §12/§14.7(d) as protecting against a scan running away
  through a whole write cluster and keeping only the last answer of each kind.

The bound is correct given a single-slot cache, and it was correct that different *functions*
sharing one namespace (`check_write` vs `write`) need distinct identities. What neither half handled
is the same exact `(function_name, namespace)` pair repeating for genuinely distinct calls.

The failure sequence, reconstructed against the code:

1. `poll()`'s replay calls `consume_and_cache_stray_entries(None)` (`io/poll.rs`). The scan defers
   `#02254`, caches it, then **refuses `#02255`** and stops, leaving it at the cursor.
2. `poll()` then reads with `is_own_poll_entry` — misses, since `#02255` sits there.
3. It synthesizes a ready-set and returns (§13.7.2's non-destructive miss), and the guest re-polls.
4. On the retry, `stray_entry_scan()` re-seeds `consumed` from `cached_stray_identities()`. `#02254`'s
   answer is still uncollected, so the identity is still blocked and `#02255` is refused **again**.
   The cursor has not moved.
5. `record_poll_replay_miss()` counts consecutive misses at that unmoved cursor. Once the streak
   exceeds the entries remaining ahead of it, `poll()` falls back to the original unconditional
   `durability.replay(self)` — which consumes `#02255` and reports the mismatch. Permanent trap.

This also explains why the analogous `IoPollReady` shape never trapped this way: `ready()`'s owner
*does* get scheduled between poll retries, collects the cached answer, frees the identity, and the
next scan then consumes the second occurrence. `blocking_read`'s owner was not scheduled, so the
streak ran to the bound. The mechanism's self-healing was load-bearing and merely happened to work.

### 15.3 The fix

**Key the cache on `(StrayEntryIdentity, occurrence_index)`, with the occurrence index carried
implicitly as FIFO queue position** — `PreResolvedStrayCache`, a named type wrapping
`HashMap<StrayEntryIdentity, VecDeque<HostResponse>>` with `record`/`take`/`peek`.

This is the brief's design; the deviation is only in how `occurrence_index` is represented, and it
is deliberate. An explicit index has to be agreed on by two parties — the scan that defers an entry
and the owner that later collects it — and neither can derive it from the other without a shared
counter that both mutate (the scan on defer, the owner on *every* collect, including collects that
bypass the cache and read the oplog directly). That is a write index and a read index over one
per-identity sequence, i.e. exactly a FIFO queue, with the counters made explicit and therefore
capable of drifting. Making the position implicit removes the failure mode instead of managing it.

Its correctness rests on one invariant, already established in §14.7(d) and unchanged here: **one
identity's entries are produced, deferred and collected in oplog order.** The replay cursor advances
monotonically, so entries enter a queue in oplog order; a `StrayEntryIdentity` corresponds to one
WASI resource owned by one guest task (WASI resource ownership makes two tasks sharing a
`Resource<InputStream>` impossible), so its calls are serialized and its Nth call wants its Nth
entry. Pushing at the back and popping at the front therefore cannot get out of step.

**Snapshot recovery needs no handling here, and this was verified rather than assumed.** Unlike
`pollable_seq`/`invoke_result_seq` — whose values must stay consistent with *already-persisted*
entries, hence their snapshot-restore machinery — the cache is pure per-replay-pass scratch state:
constructed empty (`PreResolvedStrayCache::default()`), never serialized, never read outside a
replay pass. Nothing about queue position spans a snapshot boundary.

**The scan bound is removed entirely** (question 2 of the brief). §12's stated reason for it was
cache collision, which no longer exists. No *other* reason survives scrutiny, and re-adding one
would reintroduce this exact trap: a scan stops at the first entry it does not accept, so a
contiguous run of foreign tracked entries must be cleared in full before the caller's own entry can
be reached — consuming less than all of it cannot make progress, and whatever is left behind is
reachable by nobody. `StrayEntryScan` is now bounded by the only two things that genuinely bound it,
both already in `is_stray_concurrent_entry`: an entry belonging to no tracked operation, and an
entry carrying the caller's OWN identity. It consequently became stateless (`accept(&self)`), and
`cached_stray_identities()` was deleted.

The trade-off, recorded in `StrayEntryScan`'s doc comment: one scan can now hold a whole contiguous
foreign run's payloads in memory at once (a 106-pair body cluster) rather than leaving most in the
oplog. That run has to be consumed before the caller can proceed either way — only retention
differs, and it is released as each owner collects. Note this is not the common path: in-order
replay defers nothing, because each consumer's own entry is at the cursor and its own identity is
excluded, so the scan stops immediately at zero entries.

**Question 3 (every consumer computing its own occurrence index) needs no per-consumer work.** All
four collection paths — `Durability::replay_raw` (~92 sites), `ready()`, RPC `get()`,
`future_incoming_response::get` — already funnel through `take_pre_resolved_stray(&identity)`, and
`poll()`'s synthesis peek through `is_pollable_pre_resolved_ready`. Making those pop/peek the queue
front is the entire consumer-side change; no call site learns a new concept. The positive-identity
match against the live cursor is unaffected: it matches on identity, and successive occurrences are
indistinguishable there by construction — whichever is at the cursor IS the caller's next one,
because the cursor advances in oplog order.

### 15.4 Test results

`cargo build --release -p golem-worker-executor` clean; `cargo clippy --release --lib --tests` clean.
`cargo test -p golem-worker-executor --lib --release` = **493 passed, 0 failed, 0 ignored, 0
filtered** (488 before: −2 obsolete bound tests, +7 new).

New tests, all asserting payload CONTENT rather than merely "no crash" — a wrong-slot bug here is a
silent misdelivery (§14.2.1's failure mode), not necessarily a trap:

| Test | Asserts |
|---|---|
| `two_blocking_reads_on_one_stream_each_replay_to_their_own_answer` (replay_state) | The capture verbatim: two same-identity `blocking_read` entries + `poll_entry`, at the real replay cursor. BOTH defer; each replays to its own chunk (`first`/`second`) in order; `poll()`'s own entry is then reachable. Fails on the old bound at the first assertion. |
| `many_occurrences_of_one_identity_all_replay_in_order` (replay_state) | Same with N=5 — confirms N=2 is not special-cased. |
| `a_repeated_identity_still_stops_its_own_owners_scan` (replay_state) | The surviving bound: a `blocking_read`'s own scan defers the foreign `check_write` but leaves both of its own entries at the cursor. |
| `stray_scan_walks_every_occurrence_of_a_repeated_identity` (replay_state) | Rewrite of `stray_scan_stops_at_the_second_entry_for_one_identity` — same 4-entry `check_write`/`write` cluster, now asserting all 4 defer and `poll()` gets through. |
| `repeated_occurrences_of_one_identity_are_returned_in_call_order` (mod) | `PreResolvedStrayCache` directly: 3 chunks in, `peek` and `take` both yield the owner's NEXT answer in order, queue drains to empty. |
| `queues_of_different_identities_are_independent` (mod) | Interleaved `blocking_read`/`write` records; collecting one identity does not shift the other's ordering. |
| `scan_accepts_every_occurrence_of_a_repeated_identity` (mod) | Replaces `scan_accepts_at_most_one_entry_per_identity` — the inverse assertion, plus the two surviving bounds. |
| `scan_still_stops_at_every_occurrence_of_its_own_identity` (mod) | Exclusion is per-identity and repetition-insensitive. |

Two tests were **deleted** as assertions of the now-removed bound, not adapted:
`scan_accepts_at_most_one_entry_per_identity` and `scan_refuses_identities_whose_answer_is_already_cached`
(the latter tested `cached_stray_identities()` seeding, the exact mechanism behind step 4 above).
`each_deferred_entry_is_returned_to_its_own_owner` — §14.3's counterexample — was retargeted from a
hand-rolled `HashMap` simulation onto the real `PreResolvedStrayCache`, so it now exercises
production code rather than a re-implementation of it.

`tests/rpc.rs` / `tests/durability.rs` compile clean under `cargo check --release --tests` but were
**not run**: this worktree has only 5 of the ~21 `test-components/*.wasm` fixtures built, so the
integration harness aborts during component cache warm-up before any test body executes. An
environmental gap, unrelated to this change. Live verification is deliberately out of scope here.
