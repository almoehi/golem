# Finding B fix design (v2) — poll() replay must defer to N stray same-batch ready() entries

Status: **DESIGN ONLY — not implemented.** v2 supersedes v1 (single-stray-entry design) per
explicit direction: the fix must generalize to an arbitrary number of batched pollables, any
number of which may have resolved out of the guest's structural check order — not be bounded to
or validated only for 2-pollable batches. This revision also resolves/bounds the three open
questions raised on v1.

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
