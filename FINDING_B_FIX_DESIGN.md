# Finding B fix design — poll() replay must defer to a stray same-batch ready() entry

Status: **DESIGN ONLY — not implemented.** Per the coordinator's explicit request, this is
presented for review before any code changes, given real ambiguity (flagged in "Open questions"
below) and higher correctness risk than the Eighth capture's fix.

Seed: Tenth capture (`/Users/hannes/work/golem/oplog-backups/README.md`), live trap on `main`,
`WorkerAgent(...,"1f93ff64-...@scene_backdrops")`, retry from oplog index 834, `reps: [20, 24]`.

## 1. Restating the root cause precisely

The decoded sequence (rep 20 = long-lived pollable, `seq: 150`; rep 24 = fresh RPC pollable,
`seq: 154`):

```
poll([20,24]) enter  → peek(840)=IoPollPoll(ReadLocal)              matched: true  → consumed
ready(rep=20,seq=150) → peek(841)=IoPollReady(ReadLocalPollable(154)) matched: false → synthesize false
poll([20,24]) enter AGAIN → peek(841)=IoPollReady(ReadLocalPollable(154))  <- crash here
CRASH: expected io::poll::poll, got io::poll::pollable::ready (begin_index: 834)
```

Entry 841 (the recorded `true` confirmation) was recorded, on LIVE, for rep 24 — not rep 20.
`ready()`'s replay (`try_get_oplog_entry`, seq-predicate) **correctly** rejects it when asked
about rep 20 (`150 != 154`) and synthesizes `false`, exactly as designed since the Eighth
capture's fix. The bug is downstream: the guest's replayed control flow then calls `poll()`
again, and `poll()`'s replay path (`Durability::replay()` → `read_persisted_durable_function_
invocation()` → `get_oplog_entry!(HostCall)`) consumes **whatever HostCall entry is next**,
unconditionally and positionally, with no per-pollable identity check at all. It finds entry 841
(still sitting there, correctly un-consumed by the rejected `ready(rep=20)` check) and treats it
as if it must be poll()'s own `IoPollPoll` entry, because that's the only shape `poll()`'s replay
knows how to consume. Type mismatch → hard crash.

**On the guest-level "why" this happens** (why does replay's call order supply an "extra" poll()
call that live's own recording implies didn't happen at that exact position): this investigation
does not have visibility into the compiled QuickJS/TS-SDK bytecode driving the real `WorkerAgent`,
so a fully mechanistic explanation isn't available (this matches round 14's open item #1: no
round 8–15 synthetic reproduction has driven the real TS-SDK/QuickJS async runtime — every
attempt used raw Rust `WasmRpc`/`io::poll` directly). The coordinator's live capture already
establishes this empirically and reproducibly at the engine level, which is sufficient to design
against: **whatever the guest-level cause, the engine-level contract "poll()'s replay always
finds a genuine `IoPollPoll` entry positionally next" is not actually guaranteed once a batch
has more than one pollable** — a stray, already-recorded-true `IoPollReady` entry for one of the
batch's OTHER members can legitimately sit at the cursor when `poll()` is asked to replay. The fix
targets this contract directly, independent of why the guest ended up calling poll() again.

## 2. Proposed fix (Tier 1 — recommended)

Reuse the identity-matching primitive `ready()` already trusts (`pollable_seq`), applied
symmetrically to `poll()`'s replay path. Two new primitives, both additive and read-only (zero
effect on existing behavior):

**`ReplayState::peek_oplog_entry`** (`replay_state.rs`, alongside `try_get_oplog_entry`) — a pure,
non-consuming peek. Unlike `try_get_oplog_entry` (consumes on predicate match), this always
rewinds — calling it twice in a row returns the same entry, cursor untouched:

```rust
pub async fn peek_oplog_entry(&mut self) -> Result<(OplogIndex, OplogEntry), WorkerExecutorError> {
    let saved_replay_idx = self.last_replayed_index.get();
    let saved_next_skipped_region = {
        let internal = self.internal.read().await;
        internal.next_skipped_region.clone()
    };
    let read_idx = self.last_replayed_index.get().next();
    let entry = self.internal_get_next_oplog_entry().await?;

    self.rewind_replay_buffer(read_idx, entry.clone()); // OplogEntry: Clone (derived)
    self.last_replayed_index.set(saved_replay_idx);
    let mut internal = self.internal.write().await;
    internal.next_skipped_region = saved_next_skipped_region;

    Ok((read_idx, entry))
}
```

**`PrivateDurableWorkerState::pollable_seq_if_assigned`** (`durable_host/mod.rs`, alongside
`pollable_seq`) — a non-mutating lookup:

```rust
/// Returns `rep`'s already-assigned seq, if any, WITHOUT assigning a fresh one. Unlike
/// `pollable_seq()`, this must never mutate `next_pollable_seq` — poll()'s replay uses this
/// to check whether a peeked entry belongs to one of its own batch members without changing
/// seq assignment order/values relative to what LIVE recorded (LIVE only ever assigns seqs
/// from ready(), never from poll() — see pollable_seq's doc comment). Perturbing that order
/// would itself be a new live/replay divergence and would risk breaking replay of already-
/// persisted oplogs recorded under the current (ready()-only) assignment order.
pub fn pollable_seq_if_assigned(&self, rep: u32) -> Option<u32> {
    self.pollable_seq.get(&rep).copied()
}
```

**`Host::poll`'s replay branch** (`io/poll.rs`) — peek before consuming:

```rust
} else {
    // REPLAY. Peek without consuming first, to distinguish poll()'s own entry from a stray
    // same-batch ready() confirmation recorded out of the guest's replayed structural check
    // order (Tenth capture, FINDING_B_FIX_DESIGN.md). Zero effect on the happy path: a
    // genuine IoPollPoll entry falls through to the unchanged `durability.replay(self)` call
    // below exactly as before.
    let (_, peeked_entry) = self.state.replay_state.peek_oplog_entry().await?;

    let stray_batch_index = match &peeked_entry {
        OplogEntry::HostCall {
            function_name: HostFunctionName::IoPollReady,
            durable_function_type: DurableFunctionType::ReadLocalPollable(seq),
            ..
        } => in_.iter().enumerate().find_map(|(idx, r)| {
            (self.state.pollable_seq_if_assigned(r.rep()) == Some(*seq)).then_some(idx)
        }),
        _ => None,
    };

    match stray_batch_index {
        Some(idx) => {
            trace!(
                agent_id = %self.owned_agent_id,
                reps = ?in_.iter().map(|r| r.rep()).collect::<Vec<_>>(),
                stray_index = idx,
                "POLLCALL_TRACE poll() REPLAY deferring to stray same-batch ready() entry"
            );
            // Do NOT consume — leave it for that pollable's own subsequent ready() call.
            // Synthesize poll()'s hint result (WASI's poll() contract is "may be ready, go
            // check": the guest's existing ready()-after-poll() confirmation pattern, already
            // established since round ten, means returning a hint here is safe under the
            // contract the guest already relies on).
            Ok(HostResponsePollResult { result: Ok(vec![idx as u32]) })
        }
        // Not a stray same-batch entry — unchanged existing path, including its existing
        // crash behavior for genuinely unrelated/unexpected entries.
        None => Ok(durability.replay(self).await?),
    }
};
```

Everything downstream (`match result { Ok(result) => ..., Err(duration) => ... }`) is unchanged.

### Why this is safe

- **Happy path unchanged.** The one new `peek_oplog_entry` call is pure/non-mutating; when it
  finds a genuine `IoPollPoll` entry, `stray_batch_index` is `None` and control falls straight
  into the exact same `durability.replay(self).await?` call that runs today — byte-for-byte
  identical behavior, including its existing crash detection for anything genuinely unexpected.
- **No seq-assignment perturbation.** `pollable_seq_if_assigned` never mutates
  `next_pollable_seq` — it can only return a seq that some *prior* `ready()` call already
  assigned. This preserves the Eighth-capture/Option-2 fix's invariant that seq assignment order
  is driven exclusively by `ready()` call order, so this change cannot alter counter values for
  already-persisted oplogs (backward compatible, unlike a hypothetical fix that made `poll()`
  itself assign seqs).
- **No persistence.** This is a REPLAY-only branch — nothing is ever written to the oplog here;
  `LIVE`'s branch (`durability.is_live()`) is untouched.
- **Self-correcting per call, no multi-entry lookahead needed.** If poll() is called again after
  this (because the guest's busy-loop hasn't yet seen its own pollable resolve), the cursor is
  still sitting at the same un-consumed stray entry — the guest's own subsequent `ready()` call on
  the hinted index will consume it via the existing, unchanged `ready()` mechanism. Only ONE
  peek is needed per `poll()` invocation; the loop itself provides the "try again" semantics.

## 3. Test plan

### 3a. Unit test, seeded directly from the Tenth capture's byte pattern (primary)

A hand-authored deterministic Rust guest cannot literally force replay's call *order* to diverge
from live's own recorded order (both runs execute the same instructions given the same fed-back
answers) — see "Open question 3" below for why an integration-level repro of the *exact*
divergence is not straightforwardly constructible. The most direct and honest way to seed a test
from this trace is therefore at the same level the existing `try_get_oplog_entry_*` tests already
operate (`replay_state.rs`'s test module, using `MutableBatchOplog`): construct the exact entry
shapes from the Tenth capture and assert the new logic's behavior directly.

```rust
#[test]
async fn poll_replay_defers_to_stray_same_batch_ready_entry() {
    // Entry shapes lifted directly from the Tenth capture: an IoPollPoll(ReadLocal) entry
    // (poll()'s own, already consumed in the real trace) followed by an
    // IoPollReady(ReadLocalPollable(154)) entry — rep 24's confirmed-true, recorded before
    // the guest's replayed control flow reached the matching ready() call for it.
    let io_poll_ready_for_seq_154 = OplogEntry::HostCall {
        timestamp: Timestamp::now_utc(),
        function_name: HostFunctionName::IoPollReady,
        request: OplogPayload::Inline(Box::new(HostRequest::NoInput(HostRequestNoInput {}))),
        response: OplogPayload::Inline(Box::new(HostResponse::PollReady(
            HostResponsePollReady { result: Ok(true) },
        ))),
        durable_function_type: DurableFunctionType::ReadLocalPollable(154),
    };
    let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
        (OplogIndex::INITIAL, OplogEntry::NoOp { timestamp: Timestamp::now_utc() }),
        (OplogIndex::INITIAL.next(), io_poll_ready_for_seq_154),
    ])));
    let mut state = ReplayState::new(owned_agent_id, oplog, DeletedRegions::new()).await.unwrap();

    // peek_oplog_entry must be non-consuming and repeatable.
    let (idx1, entry1) = state.peek_oplog_entry().await.unwrap();
    let (idx2, entry2) = state.peek_oplog_entry().await.unwrap();
    assert_eq!(idx1, idx2);
    assert!(matches!(entry1, OplogEntry::HostCall { function_name: HostFunctionName::IoPollReady, .. }));
    assert_eq!(entry1, entry2);

    // Then: the stray-batch-index resolution logic (extracted so it's directly testable, or
    // inlined and asserted via a small helper) must map seq 154 -> index 1 for a batch built
    // as [rep_for_seq_150 (index 0), rep_for_seq_154 (index 1)], and leave the entry
    // unconsumed afterward (a follow-up try_get_oplog_entry for IoPollReady(154) must still
    // match — proving nothing was silently dropped).
}
```

Also add (or extend an existing) `DurableWorkerCtx`/`PrivateDurableWorkerState`-level unit test
for `pollable_seq_if_assigned`: assert it returns `None` for an unseen rep, `Some(seq)` after a
prior `pollable_seq(rep)` call, and — critically — that calling it does **not** advance
`next_pollable_seq` (call it N times, confirm the next *mutating* `pollable_seq()` call on a new
rep still gets the value it would have gotten with zero `pollable_seq_if_assigned` calls in
between).

### 3b. Best-effort integration-level probe (secondary, may not reproduce — that's expected)

Extend round 15's `long_lived_timer_batched_with_fresh_rpc_after_n_regions` pattern with a
variant where the **guest's fixed structural check order and the pollables' real relative speed
are deliberately reversed** — check index 0 first (matching a slow ~150ms `inc_by_slow` call)
and index 1 second (a fast, near-instant local RPC or `inc_by`), so index 1 is very likely to
have genuinely resolved *before* index 0 gets checked structurally, while the busy-loop's
own logic (`ready(0)` → false → `poll()` again, never reaching `ready(1)` in the same pass)
mirrors the trace's shape. This is a best-effort construction, not a proven reproduction — per
round 14/15 and the Tenth capture's own framing, the real divergence may depend on guest-level
(QuickJS) scheduling nondeterminism no hand-authored deterministic Rust can force. Flagging this
sub-item as **may legitimately stay negative**; the unit test in 3a is the test that actually
pins down the fix's correctness and is not allowed to be skipped.

### 3c. Falsification (bidirectional, same bar as every prior round)

- **Before the fix**: unit test 3a's "does not crash" assertion must FAIL against the current
  `poll()` replay code (confirms the test actually exercises the bug).
- **After the fix**: unit test 3a passes; all four pre-existing `try_get_oplog_entry_*` tests
  still pass unmodified; the full `durable_host::` unit suite (64 tests as of round 15) still
  passes.

### 3d. Full regression sweep

- Rounds 8–15's integration tests (`rpc.rs`, `durability.rs`) — everything from
  `timer_races_real_http_and_survives_worker_replay` through round 15's
  `long_lived_timer_batched_with_fresh_rpc_survives_cold_replay`.
- The Eighth capture's fix regression test:
  `atomic_rpc_call_across_periodic_snapshot_survives_cold_replay`.
- Note the pre-existing, unrelated `WorkerActivator` test-infra flake documented in
  `ROUND_FIFTEEN_FINDINGS.md` (confirmed independent of any of this investigation's changes) —
  expect it may still intermittently fail `sequential_atomic_double_ready_rpc_calls_same_target_
  n4_survives_cold_replay` when run alone; re-run alongside siblings or re-run once if hit.

## 4. Open questions (why this is going out for review before implementation)

1. **Single-stray-entry-per-call sufficiency.** The design assumes at most one stray entry sits
   at the cursor per `poll()` invocation, with the guest's own retry loop naturally handling
   further strays on subsequent calls. Is there a scenario (e.g. a 3+-pollable batch) where
   *multiple* consecutive stray entries could be recorded before `poll()`'s own entry, requiring
   a look-ahead beyond one peek? The Tenth capture's 2-pollable case doesn't exercise this; I
   don't have a live example of a 3+-pollable batch to confirm either way.
2. **Unassigned-seq batch member (Tier 2, deferred).** If the stray entry actually belongs to a
   batch member whose `ready()` has *never* been called yet in this replay (no seq assigned,
   `pollable_seq_if_assigned` returns `None`), Tier 1 falls through to today's crash — no worse
   than current behavior, but not a fix for that specific sub-case either. Confirmed structurally:
   entry 841 belongs to rep 24, and per the trace rep 24's `ready()` genuinely was already called
   once (the very first "ready(rep=24,seq=154) -> synthesize false" step) before either poll()
   call in this window — so Tier 1 fully covers the Tenth capture's exact case. Whether the
   unassigned-seq sub-case occurs in practice (and needs its own follow-up) is unconfirmed.
3. **The guest-level "why" is genuinely unexplained.** Section 1 is explicit that this design
   does not know why replay's call order can differ from live's implied order at the QuickJS/
   TS-SDK level. The fix is engine-level and defensive (matches the entry it finds, regardless of
   why), which I believe is the right posture given we can't inspect the compiled guest bytecode
   — but flagging that "we don't fully understand the trigger, only how to make replay robust to
   its symptom" is itself worth a second opinion, in case there's a guest-level (app-code) angle
   worth pursuing in parallel that would make this engine change less necessary or scope it
   differently.
4. **Trace-line accuracy after this fix ships.** Round 15's `validate_oplog_entry` enrichment
   (already shipped, `203380c85`) logs `actual_durable_function_type` on every mismatch. Once
   this fix lands, the specific mismatch pattern it enriches for (a stray same-batch
   `ReadLocalPollable` entry hit during `poll()`'s replay) will no longer reach that error path at
   all for the case this fix covers — the enrichment remains correct and useful for genuinely
   unrelated mismatches, no change needed there, just noting for completeness.

## 5. Ask

Proceeding to implement + test per the plan above once reviewed, unless directed otherwise. Given
open questions 1–3 in particular, a second look before merging feels warranted — this is an
engine-level replay-control-flow change, not a "recover a lost value" fix.
