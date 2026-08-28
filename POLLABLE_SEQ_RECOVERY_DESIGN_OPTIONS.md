# Design exploration: alternatives to the O(N) `next_pollable_seq` recovery scan

**Status: design review, nothing implemented.** The user raised a real concern about
`recover_next_pollable_seq()`'s O(N) oplog scan (committed in `c21259436`, see
`ROUND_TWELVE_THIRTEEN_FIX.md`) — it re-reads the entire oplog from genesis through the snapshot
index on every snapshot-based resume, which reintroduces exactly the cost snapshotting exists to
avoid, for long-lived, snapshot-heavy workers. This document explores alternatives from first
principles, given the explicit relaxed constraint that **backward compatibility with
already-persisted state is not required** — a genuine clean-slate redesign, not just a patch on
top of the existing scan. The scan stays in place as documented baseline; nothing here is
implemented yet.

## Why the O(N) scan is a real concern, not a theoretical one

`next_pollable_seq` only ever increments — it never resets except at construction. A worker's true
counter value at any point in its life is proportional to how many distinct pollables it has ever
observed via `ready()`, which for a busy, long-lived agent grows without bound (this
investigation's own "Eighth capture" worker had reached `seq: 160` after a modest amount of
activity). The scan's cost is proportional to the *entire oplog prefix* before the latest snapshot
— not just the pollable-related entries in it, since every entry in that range has to be read and
pattern-matched to find the ones that are. For a worker alive for days with periodic
snapshotting, that prefix can run into the tens or hundreds of thousands of entries, and — because
resumption from a snapshot happens exactly when a worker needs to be reloaded (after eviction, or a
process restart) — this scan runs on a path that is directly in the way of getting a worker back
into service. The `1024`-entry chunk size means a 100K-entry prefix costs ~100 sequential
`read_many` round trips against durable storage, every single time such a worker resumes. If
resumes are frequent (e.g. a memory-constrained deployment cycling workers through eviction
regularly), this cost compounds directly into user-visible latency. The concern is legitimate.

## Option 1 (baseline, already implemented): O(N) retroactive scan

Scan the durable oplog from genesis through the snapshot index for the highest recorded
`ReadLocalPollable(N)`, initialize the fresh counter to `N + 1`.

- **Correctness**: correct for every pollable-consuming code path that actually exists in this
  codebase today (verified in `ROUND_TWELVE_THIRTEEN_FIX.md`'s "Known limitation" section: RPC-future
  pollables always confirm within one `atomically()` region, which cannot span an
  invocation/snapshot boundary; promise-backed pollables bypass `pollable_seq` entirely via
  `poll()`'s early-suspend fast path). Has a *theoretical* gap for a pollable whose `ready()` was
  called (spending a counter slot) but never confirmed `true` before a snapshot — invisible to any
  oplog scan by construction (Bug #1's skip-`false` optimization means it was never recorded at
  all) — but nothing in this codebase can currently produce that shape.
- **Complexity**: lowest of any option here — one self-contained function, no format changes, no
  other files touched, already implemented and regression-tested.
- **Performance**: O(N) in oplog size before the snapshot, paid on every snapshot-based resume.
  This is the concern driving this document.
- **Snapshot format impact**: none.
- **Multiple snapshot generations**: irrelevant to the cost — the scan always runs from genesis to
  the *latest* snapshot regardless of how many generations lie in between; cost is purely a
  function of total oplog size in that range, not generation count.

## Option 2: embed the counter directly in the snapshot entry (fix direction (a), now unblocked)

This was the Eighth capture's originally-considered direction (a), set aside at the time because it
looked like it needed an app-level `saveSnapshot`/`loadSnapshot` change or a backward-compatible
migration path. Neither is actually true, and the relaxed constraint removes the concern that set it
aside.

**Mechanism**: `OplogEntry::Snapshot` (`golem-common/src/base_model/oplog/mod.rs`) is defined via
this codebase's macro DSL, `raw { data: payload::OplogPayload<Vec<u8>>, mime_type: String }` /
`public { data: PublicSnapshotData }` — the `raw`/`public` split already means engine-internal wire
fields don't have to appear in the API-facing type (`mime_type` itself is `raw`-only today,
confirming this pattern already exists). Add `next_pollable_seq: u32` to `raw` only. This is a
`u32` — trivially inline, no `OplogPayload` indirection needed (that exists for the potentially
large app-level snapshot bytes, not relevant here). The same field needs adding to
`UpdateDescription::SnapshotBased`'s payload shape (the update-triggered snapshot path
`try_load_snapshot` also reads from) — mechanically identical, not separately explored here.

**Write side**: `invocation_loop.rs`'s periodic-snapshot-save flow (`RunningWorker`'s handling of
`AgentInvocationResult::SaveSnapshot`, around where it currently calls `OplogEntry::snapshot(payload,
snapshot.mime_type)`) already runs with the live `Store`/`Ctx` in scope (it just invoked the app's
`save-snapshot` WASM export against it) — reading `next_pollable_seq` off
`durable_ctx().state` at that point and passing it into the entry constructor is a same-shaped
change to an already-identified call site, not new plumbing.

**Read side**: replace `recover_next_pollable_seq()`'s scan body with a single `oplog.read(snapshot_idx)`
(one entry, not a range) and extract the field directly from it — same call site, same
signature, same place in `PrivateDurableWorkerState::new()` the current fix lives in, so this stays
a **localized swap**, not a restructuring of the constructor/`try_load_snapshot()` control flow
(considered and rejected below as more invasive for no benefit).

- **Correctness**: strictly better than Option 1 — captures the *exact* true value at the instant
  the snapshot is taken (a live read of the actual counter, not a reconstruction from a subset of
  what happens to be durably recorded), closing Option 1's theoretical unconfirmed-pollable gap
  entirely, with no dependency on reasoning about which code paths can or can't produce that shape.
- **Complexity**: moderate. This codebase has already done a change of this exact shape once —
  adding `DurableFunctionType::ReadLocalPollable(u32)` (`f89c8b85d`) touched the raw-type
  declaration, the write site, the read site, and (per `GOLEM_IO_POLL_BUG.md`'s own files-changed
  table for that fix) a small number of secondary conversion sites (protobuf, public-type mapping).
  Expect a similar footprint here. Genuinely simpler than that precedent, though: no
  `#[desert(evolution())]` backward-compatibility reasoning needed, no dual-format
  (`ReadLocalPollable` vs legacy `ReadLocal`) fallback logic to write, since old snapshots recorded
  before this change are explicitly out of scope.
- **Performance**: O(1) at resume — reading the snapshot entry is already mandatory (that's how the
  app-level payload is obtained today); the counter comes along in the same read at zero additional
  I/O cost. This is the entire point of this option.
- **Snapshot format impact**: yes, by design — a new field on `Snapshot` and `SnapshotBased`. Since
  backward compatibility isn't required, this is a clean one-time addition, not a migration.
- **Multiple snapshot generations**: irrelevant here too, for a different reason than Option 1 —
  resume only ever consults the *latest* snapshot (`last_automatic_snapshot_index`), so intervening
  generations are never read at all under this scheme.
- **Structural alternative considered and rejected**: moving the read into `try_load_snapshot()`
  itself (which already reads this same entry) instead of a second, separate O(1) read in `new()`.
  Marginally saves one redundant (but still O(1), still cheap) entry read, at the cost of
  restructuring the flow so a value gets pushed into an already-constructed context after the fact
  rather than supplied at construction time — more moving parts for a saving that doesn't matter at
  this scale. Not recommended.

## Option 3: an identity scheme that doesn't need a recoverable counter at all

Explored two variants; both rejected.

**3a — oplog-position-derived identity** (tag a pollable by the oplog index of some durable marker
recorded at its *creation*, instead of a synthetic ordinal). Attractive in principle — an oplog
index is inherently stable and never needs "recovery" at a snapshot boundary, it's just a position.
Rejected because pollable *creation* is not currently a durably recorded event at all — that's
precisely why `pollable_seq` (a purely in-memory, call-order-derived ordinal) was invented as a
workaround in the first place (`f89c8b85d`). Making this work would require adding a new durable
oplog entry at every pollable creation (not just at snapshot time), which increases oplog write
volume roughly in proportion to poll activity — working directly against the concern that motivated
Bug #1's skip-`false` optimization in the first place (`GOLEM_IO_POLL_BUG.md`). Trading a
well-understood, bounded resume-time cost (Option 1 or 2) for an unbounded increase in steady-state
oplog volume is a worse trade, not a better one.

**3b — atomic-region-scoped counter** (reset the ordinal at `mark_begin_operation` instead of at
instance construction, since atomic regions never span an invocation — and therefore never span a
snapshot — boundary by construction). This would genuinely eliminate the snapshot-baseline problem,
but *only* for pollables inside `atomically()` regions. Every other pollable-consuming call shape
(timer races, raw HTTP fetches outside atomic wrapping — both exercised by this investigation's own
regression tests, `timer_race_rep_reuse_across_many_sequential_cycles` et al.) still needs some
other, instance-lifetime-scoped counter with the exact same baseline problem this whole
investigation is about. This would mean shipping *two* identity mechanisms selected by call shape
instead of one uniform mechanism — a net complexity increase for no correctness or performance gain
over Option 2, which handles every pollable kind uniformly. Rejected.

## Related state checked for the same class of bug

While tracing this, checked whether `active_atomic_regions` and `current_retry_point` — the other
two `PrivateDurableWorkerState` fields reset unconditionally at construction — have the same
"wrong baseline after snapshot-based resume" problem `next_pollable_seq` did. They don't, for a
structural reason worth recording:

- `active_atomic_regions` is always legitimately empty at any snapshot boundary — snapshots only
  fire between fully completed external invocations (empty WASM call stack), and no atomic region
  can be "in progress" when the call stack is empty. Resetting to `Vec::new()` is always correct.
- `current_retry_point` is *overwritten* (not incremented) on every `ReadLocal`-type call
  (`begin_function()`'s else branch, `durable_host/mod.rs`) to reflect "the last written non-hint
  entry" — it self-corrects to the right value on the very first relevant call after any resume,
  regardless of what it was reset to. `next_pollable_seq`'s bug specifically depends on it being a
  *monotonically incrementing counter from a remembered baseline* rather than a value that gets
  refreshed from current position on every use — `current_retry_point` doesn't share that shape.

No other latent instance of this bug class found.

## Recommendation

**Option 2** (embed the counter in the snapshot entry, read as a single O(1) lookup at resume).
It is strictly more correct than the current Option 1 baseline, removes the performance concern
entirely rather than mitigating it, and — now that backward compatibility is explicitly not a
constraint — is no longer meaningfully more complex to implement than Option 1 was, following a
change shape (`raw`/`public` DSL field addition + write-site + read-site update) this codebase has
already executed successfully once for `ReadLocalPollable` itself.

Suggested sequencing if approved: keep Option 1's `recover_next_pollable_seq()` and its regression
test (`atomic_rpc_call_across_periodic_snapshot_survives_cold_replay`) exactly as committed — the
test's assertions don't change, only the mechanism satisfying them does, so it becomes the
regression guard for Option 2 for free. Swap the scan body for the single-entry read, add the
write-site change, update the doc comments (which currently describe the scan-based rationale) to
describe the new mechanism, and re-run the same regression sweep from `ROUND_TWELVE_THIRTEEN_FIX.md`
plus one additional case worth adding: a resume that has to skip past *multiple* snapshot
generations (verifying only the latest is ever consulted, matching
`periodic_snapshot_recovery_survives_a_second_snapshot_generation`'s existing coverage of that
scenario for the app-level payload).

Not implementing any of this without your go-ahead, per the ask.
