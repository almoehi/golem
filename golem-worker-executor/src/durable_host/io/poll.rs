// Copyright 2024-2026 Golem Cloud
//
// Licensed under the Golem Source License v1.1 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://license.golem.cloud/LICENSE
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::durable_host::durability::InFunctionRetryHost;
use crate::durable_host::wasm_rpc::delete_future_invoke_result;
use crate::durable_host::{
    Durability, DurabilityHost, DurableWorkerCtx, IdentityNamespace, StrayEntryIdentity,
    SuspendForSleep, is_own_poll_entry,
};
use crate::metrics::ephemeral::{dec_promise_waiting, inc_promise_waiting};
use crate::services::oplog::OplogOps;
use crate::services::{HasOplog, HasWorker};
use crate::workerctx::WorkerCtx;
use chrono::{Duration, Utc};
use futures::pin_mut;
use golem_common::model::Timestamp;
use golem_common::model::agent::AgentMode;
use golem_common::model::oplog::host_functions::{HostFunctionName, IoPollPoll, IoPollReady};
use golem_common::model::oplog::{AgentError, EphemeralSleepTooLongError};
use golem_common::model::oplog::{
    DurableFunctionType, HostRequestNoInput, HostRequestPollCount, HostResponsePollReady,
    HostResponsePollResult, OplogEntry,
};
use golem_service_base::error::worker_executor::{InterruptKind, WorkerExecutorError};
use tracing::{debug, trace, warn};
use wasmtime::component::Resource;
use wasmtime_wasi::IoView as _;
use wasmtime_wasi::p2::bindings::io::poll::{Host, HostPollable, Pollable};

impl<Ctx: WorkerCtx> HostPollable for DurableWorkerCtx<Ctx> {
    async fn ready(&mut self, self_: Resource<Pollable>) -> wasmtime::Result<bool> {
        self.observe_function_call("io::poll:pollable", "ready");

        // Capture rep before self_ is consumed by HostPollable::ready / try_get_oplog_entry.
        let pollable_rep = self_.rep();
        // Logical, call-order-derived identity for this pollable — stable across live/replay
        // and across a snapshot-based restore, unlike the raw wasmtime rep (see
        // `pollable_seq`'s doc comment on PrivateDurableWorkerState for the full rationale).
        let pollable_seq = self.state.pollable_seq(pollable_rep);

        if self.durable_execution_state().is_live {
            let result = {
                let mut view = self.as_wasi_view();
                HostPollable::ready(&mut view.io_data(), self_)
                    .await
                    .map_err(|err| err.to_string())
            };
            // ready=false is "not yet, retry" — it carries no replay-essential information
            // and accounts for ~75% of oplog entries per fetch(). Skip recording it.
            // Replay synthesizes false for any gap in IoPollReady entries (see else branch).
            if result == Ok(false) {
                return Ok(false);
            }
            // Record with the pollable's logical seq so replay can match this entry to the
            // correct pollable (ReadLocalPollable instead of plain ReadLocal).
            let durability = Durability::<IoPollReady>::new(
                self,
                DurableFunctionType::ReadLocalPollable(pollable_seq),
            )
            .await?;
            let r = durability
                .persist(
                    self,
                    HostRequestNoInput {},
                    HostResponsePollReady { result },
                )
                .await?;
            r.result.map_err(wasmtime::Error::msg)
        } else {
            // Finding B (FINDING_B_FIX_DESIGN.md): a batched poll() call's replay may have
            // already consumed THIS pollable's own IoPollReady entry on our behalf — it can
            // appear in the oplog before the guest's replayed structural check order reaches
            // this ready() call (wstd's Reactor batches concurrently-pending pollables via a
            // HashMap-keyed waker set whose iteration order is per-process-randomized). If so,
            // the oplog has nothing left to find for this seq — this cached answer is
            // authoritative.
            let my_identity = StrayEntryIdentity::new(
                HostFunctionName::IoPollReady,
                IdentityNamespace::Pollable(pollable_seq),
            );
            let ready = if let Some(pre_resolved) = self.state.take_pre_resolved_stray(&my_identity)
            {
                let payload: HostResponsePollReady = pre_resolved
                    .try_into()
                    .map_err(|e: String| wasmtime::Error::msg(e))?;
                // A recorded error is reported as "not ready yet", matching what this cached
                // path has always returned (the answer used to be stored pre-collapsed as a
                // bool); the guest's next `ready()` call re-reads it if it is still relevant.
                let ready = payload.result.unwrap_or(false);
                trace!(
                    agent_id = %self.owned_agent_id,
                    rep = pollable_rep,
                    seq = pollable_seq,
                    result = ready,
                    "POLLREADY_TRACE ready() REPLAY using answer pre-resolved by an earlier poll() call"
                );
                ready
            } else {
                // Replay: consume the next IoPollReady entry only if it was recorded for THIS
                // specific pollable (matched by logical seq — see pollable_seq's doc comment).
                // This prevents a timer pollable from stealing an IoPollReady=true entry that was
                // recorded for an output-stream pollable, which would cause the WASM to think the
                // timer fired, drop the FutureIncomingResponse early, and crash with "expected
                // EndRemoteWrite, got CheckWrite".
                // Legacy ReadLocal entries (pre-dating per-pollable tagging) are consumed by any
                // pollable, matching the original (pre-Bug-2-fix) behavior for old oplogs.
                // Captured from inside the predicate (which sees the entry a refusal never hands
                // back): is the replay cursor parked on a STRUCTURAL entry — one no host-call
                // consumer can ever claim? See the miss branch below for why that changes the
                // answer this call must synthesize.
                let mut cursor_is_host_call = false;
                let peeked = self
                    .state
                    .replay_state
                    .try_get_oplog_entry(|entry| {
                        cursor_is_host_call = matches!(entry, OplogEntry::HostCall { .. });
                        match entry {
                            OplogEntry::HostCall {
                                function_name: HostFunctionName::IoPollReady,
                                durable_function_type: DurableFunctionType::ReadLocalPollable(seq),
                                ..
                            } => *seq == pollable_seq,
                            OplogEntry::HostCall {
                                function_name: HostFunctionName::IoPollReady,
                                durable_function_type: DurableFunctionType::ReadLocal,
                                ..
                            } => true,
                            _ => false,
                        }
                    })
                    .await?;
                match peeked {
                    Some((_, OplogEntry::HostCall { response, .. })) => {
                        let host_response = self
                            .public_state
                            .worker()
                            .oplog()
                            .download_payload(response)
                            .await
                            .map_err(wasmtime::Error::msg)?;
                        let payload: HostResponsePollReady = host_response
                            .try_into()
                            .map_err(|e: String| wasmtime::Error::msg(e))?;
                        payload.result.map_err(wasmtime::Error::msg)?
                    }
                    // No entry matched this pollable's seq — it genuinely hasn't become ready yet.
                    // Unlike the old rep-based scheme, this can no longer be a false negative caused
                    // by rep drift across a restore (seq is call-order-derived, not resource-table-
                    // derived — see pollable_seq's doc comment), so no RPC-specific recovery fallback
                    // is needed here any more (see the corresponding removal in poll()'s replay path,
                    // with a regression test covering the original "rpc pollable infinite replay loop
                    // after snapshot restore" scenario this fallback used to guard against).
                    _ => {
                        // "Not ready yet" is the right answer while the cursor still holds host-call
                        // entries: this pollable's own entry may simply be further along, and some
                        // other consumer will claim what is here first.
                        //
                        // It is the WRONG answer once the cursor is parked on a STRUCTURAL entry
                        // (`EndRemoteWrite` closing an open batch, `FinishSpan`, ...). No host-call
                        // consumer can ever claim such an entry; its owner is `end_function`/the
                        // span machinery, which run from GUEST CONTROL FLOW, not by consuming the
                        // replay cursor. So the cursor cannot move until the guest stops waiting and
                        // finishes the operation — and answering `false` here tells it to keep
                        // waiting, which it can only do forever.
                        //
                        // `poll()`'s own synthesis already resolves this the other way: on the same
                        // miss it reports every input pollable as ready ("wake everything"). The two
                        // paths contradicting each other is precisely the livelock observed in
                        // FINDING_B_FIX_DESIGN.md §15.6 — `poll()` says ready, `ready()` says false,
                        // and the guest spins between them until `record_poll_replay_miss`'s bound
                        // converts the spin into a trap. Agreeing with `poll()` is what lets the
                        // guest proceed to the read that collects its (already pre-resolved) answer
                        // and then drop the resource, which is what finally consumes the marker.
                        //
                        // Reporting ready is not a guess about the data: the actual read is itself
                        // replay-guarded and non-destructive, so a guest that reads on this hint
                        // either collects a cached answer or is told, correctly, that nothing of
                        // its own is at the cursor.
                        let synthesized = !cursor_is_host_call;
                        trace!(
                            agent_id = %self.owned_agent_id,
                            rep = pollable_rep,
                            seq = pollable_seq,
                            cursor_is_host_call,
                            synthesized,
                            "POLLREADY_TRACE ready() REPLAY no match, synthesizing"
                        );
                        synthesized
                    }
                }
            };
            if ready {
                self.drive_replayed_filesystem_pollables(&[pollable_rep])
                    .await?;
            }
            Ok(ready)
        }
    }

    async fn block(&mut self, self_: Resource<Pollable>) -> wasmtime::Result<()> {
        self.observe_function_call("io::poll:pollable", "block");
        let in_ = vec![self_];
        let _ = self.poll(in_).await?;

        Ok(())
    }

    fn drop(&mut self, rep: Resource<Pollable>) -> wasmtime::Result<()> {
        self.observe_function_call("io::poll:pollable", "drop");
        let child_rep = rep.rep();

        // A dropped rep can be reused by wasmtime's resource table for an unrelated future
        // pollable — clear its seq assignment so that pollable gets a fresh one instead of
        // wrongly inheriting this one's identity (see pollable_seq's doc comment).
        self.state.clear_pollable_seq(child_rep);
        self.state.filesystem_stream_pollables.remove(&child_rep);

        // Check if this pollable is a child of a FutureInvokeResult
        let parent_rep = self.state.rpc_pollable_to_parent.get(&child_rep).copied();

        {
            let mut view = self.as_wasi_view();
            HostPollable::drop(&mut view.io_data(), rep)?;
        }

        // If this child belonged to a FutureInvokeResult whose drop was deferred,
        // finalize the parent deletion now that this child is gone.
        if let Some(parent_rep) = parent_rep {
            self.state.rpc_pollable_to_parent.remove(&child_rep);
            let parent: Resource<golem_wasm::FutureInvokeResultEntry> =
                Resource::new_borrow(parent_rep);
            let should_delete = if let Ok(entry) = self.table().get_mut(&parent) {
                entry.child_pollables.retain(|r| *r != child_rep);
                entry.drop_pending && entry.child_pollables.is_empty()
            } else {
                false
            };

            if should_delete {
                let parent_owned: Resource<golem_wasm::FutureInvokeResultEntry> =
                    Resource::new_own(parent_rep);
                // Must go through delete_future_invoke_result, not table().delete() directly:
                // this is the point where the deferred half of HostFutureInvokeResult::drop's
                // HasChildren branch finally frees the rep, so this is where the rep's
                // invoke_result_seq assignment must be cleared too. Doing only the former leaked
                // the seq, letting the next future that reuses this rep inherit a stale identity
                // (FINDING_B_FIX_DESIGN.md §13.5).
                if let Err(err) = delete_future_invoke_result(self, parent_owned) {
                    debug!(
                        parent_rep,
                        error = %err,
                        "Deferred future invoke result delete failed"
                    );
                }
            }
        }

        Ok(())
    }
}

impl<Ctx: WorkerCtx> Host for DurableWorkerCtx<Ctx> {
    async fn poll(&mut self, in_: Vec<Resource<Pollable>>) -> wasmtime::Result<Vec<u32>> {
        // check if all pollables are promise backed. In this case we can suspend immediately
        // This check only needs to be done in live mode, as we will never even persist the oplog entry for polling
        // if we suspended in the last pass. Doing it this way also prevents us from initializing the promises until we are actually in live mode.
        if self.durable_execution_state().is_live && self.agent_mode() != AgentMode::Ephemeral {
            let promise_backed_pollables = self.state.promise_backed_pollables.read().await;
            let mut all_blocked = true;

            for res in &in_ {
                if let Some(promise_handle) = promise_backed_pollables.get(&res.rep()) {
                    let ready = promise_handle.is_ready().await;
                    if ready {
                        all_blocked = false;
                        break;
                    }
                } else {
                    all_blocked = false;
                    break;
                }
            }

            if all_blocked {
                debug!("Suspending worker until a promise gets completed");
                return Err(wasmtime::Error::from_anyhow(
                    InterruptKind::Suspend(Timestamp::now_utc()).into(),
                ));
            }
        };

        // The logical identity of every input pollable, in the guest's list order. Recorded
        // live and compared on replay so the recorded ready set is mapped onto the CURRENT list
        // by identity rather than by position: wstd's reactor builds this list by iterating a
        // `HashMap` keyed on a per-instance counter, so after a snapshot restore (or any other
        // fresh instance) the same pollables can arrive in a different order, and a positional
        // replay then wakes the wrong future — e.g. a fetch's 1e19-ns timeout timer instead of
        // its response (see `map_recorded_poll_ready`). Peeked, never assigned: see
        // `peek_pollable_seq` for why `poll()` must not mint identities.
        let targets: Vec<Option<u32>> = in_
            .iter()
            .map(|pollable| self.state.peek_pollable_seq(pollable.rep()))
            .collect();

        let durability =
            Durability::<IoPollPoll>::new(self, DurableFunctionType::ReadLocal).await?;
        let replaying = !durability.is_live();
        // Replay only: keep the input reps for `drive_replayed_filesystem_pollables` below
        // (skipped on the live path, which hands `in_` itself to the real poll).
        let in_reps: Vec<u32> = if replaying {
            in_.iter().map(|pollable| pollable.rep()).collect()
        } else {
            Vec::new()
        };

        let result: Result<HostResponsePollResult, Duration> = if durability.is_live() {
            let interrupt_signal = self
                .execution_status
                .read()
                .unwrap()
                .create_await_interrupt_signal();

            let count = in_.len();
            let record_ephemeral_promise_wait = if self.agent_mode() == AgentMode::Ephemeral {
                let promise_backed_pollables = self.state.promise_backed_pollables.read().await;
                let mut all_blocked = true;

                for res in &in_ {
                    if let Some(promise_handle) = promise_backed_pollables.get(&res.rep()) {
                        let ready = promise_handle.is_ready().await;
                        if ready {
                            all_blocked = false;
                            break;
                        }
                    } else {
                        all_blocked = false;
                        break;
                    }
                }

                all_blocked && !in_.is_empty()
            } else {
                false
            };
            let ephemeral_poll_timeout = if self.agent_mode() == AgentMode::Ephemeral {
                Some(self.state.config.suspend.ephemeral_max_sleep)
            } else {
                None
            };

            let result = {
                let mut view = self.as_wasi_view();
                let mut io_data = view.io_data();
                let poll = Host::poll(&mut io_data, in_);
                pin_mut!(poll);

                let _promise_waiting = PromiseWaiting::new(record_ephemeral_promise_wait);

                if let Some(timeout_duration) = ephemeral_poll_timeout {
                    let timeout = tokio::time::sleep(timeout_duration);
                    pin_mut!(timeout);

                    tokio::select! {
                        result = &mut poll => {
                            result
                        }
                        interrupt_kind = interrupt_signal => {
                            return Err(wasmtime::Error::from_anyhow(interrupt_kind.into()));
                        }
                        _ = &mut timeout => {
                            let max_nanos = std_duration_to_nanos(timeout_duration);
                            return Err(ephemeral_sleep_too_long_error(max_nanos, max_nanos));
                        }
                    }
                } else {
                    tokio::select! {
                        result = &mut poll => {
                            result
                        }
                        interrupt_kind = interrupt_signal => {
                            return Err(wasmtime::Error::from_anyhow(interrupt_kind.into()));
                        }
                    }
                }
            };

            match is_suspend_for_sleep(&result) {
                Some(duration) => Err(duration),
                None => Ok(durability
                    .persist(
                        self,
                        HostRequestPollCount { count },
                        HostResponsePollResult {
                            result: result.map_err(|err| err.to_string()),
                            targets: Some(targets),
                        },
                    )
                    .await?),
            }
        } else {
            // Previously this branch contained a recovery path for "ready() fails to match
            // IoPollReady(old_rep) by rep after snapshot restore, leaving an orphaned entry for
            // poll() to clean up". That recovery is no longer needed: ready() now tags entries
            // with a call-order-derived logical seq (see pollable_seq's doc comment on
            // PrivateDurableWorkerState) instead of the raw wasmtime resource-table rep, and a
            // pollable can never have a poll loop in flight across a snapshot-based restore in
            // the first place (snapshots only ever fire between fully completed external
            // invocations — see `on_external_invocation_completed`/the `Periodic` check above in
            // `invocation_loop.rs`). So ready() no longer produces spurious mismatches for a
            // pollable that legitimately owns an upcoming entry.
            //
            // Finding B (FINDING_B_FIX_DESIGN.md): a batch of 2+ pollables — or a pollable
            // racing a concurrently-dispatched RPC call (Eleventh capture) — CAN leave a stray
            // entry positioned before poll()'s own next entry: recorded live for a DIFFERENT
            // concurrently-tracked operation, but out of the guest's replayed structural check
            // order (`wstd`'s `Reactor` batches concurrently-pending operations via a
            // `HashMap`-keyed waker set whose iteration order is per-process-randomized, so
            // live's real completion order isn't guaranteed to match replay's structural check
            // order). `ready()`'s own seq-predicate correctly refuses to consume such an entry
            // when asked about the wrong pollable (leaving it in place) — but poll()'s replay
            // previously had no equivalent identity check, so it would try to consume that same
            // still-pending entry as if it must be its own `IoPollPoll` type and crash. Walk
            // forward, consuming (for real — `try_get_oplog_entry` durably advances past a
            // match) zero or more such stray entries of ANY known kind (`IoPollReady` for any
            // currently-tracked pollable, `GolemRpcFutureInvokeResultGet` for any currently-
            // tracked RPC call, or an HTTP body-stream read/check_write/write for any currently-
            // open request — poll() has no RPC or HTTP identity of its own to exclude), caching
            // each one's answer for its real owner's own later replay to consult — until the
            // next entry is NOT one of those, at which point poll()'s own predicate read below
            // takes over. No batch-size, entry-kind or per-identity-occurrence assumption: N
            // stray entries of any recognized kind, in any order — including several for the
            // SAME identity in a row, such as a chunked body's repeated blocking_read() calls
            // — is simply N loop iterations, each answer queued for its owner in oplog order
            // (see StrayEntryScan and `pre_resolved_stray`).
            self.state.consume_and_cache_stray_entries(None).await?;

            // Whatever's left is consumed only if it positively identifies as poll()'s own
            // entry (FINDING_B_FIX_DESIGN.md §13.7.2): a replay consumer must never destroy an
            // entry it has not identified as its own. The previous unconditional
            // `durability.replay()` read-then-validate would consume whatever sat at the cursor
            // — including a structural entry such as `FinishSpan` — and only then fail, turning
            // any predicate/identity imperfection into a permanent, unrecoverable trap.
            // Same capture as `ready()`: a refusal never hands the entry back, so record from
            // inside the predicate whether the cursor is parked on a structural entry.
            let mut cursor_is_host_call = false;
            let peeked = self
                .state
                .replay_state
                .try_get_oplog_entry(|entry| {
                    cursor_is_host_call = matches!(entry, OplogEntry::HostCall { .. });
                    is_own_poll_entry(entry)
                })
                .await?;

            match peeked {
                Some((idx, OplogEntry::HostCall { response, .. })) => {
                    self.state.reset_poll_replay_miss();
                    trace!(
                        agent_id = %self.owned_agent_id,
                        matched_oplog_index = %idx,
                        "POLLCALL_TRACE poll() REPLAY matched own IoPollPoll entry"
                    );
                    let host_response = self
                        .public_state
                        .worker()
                        .oplog()
                        .download_payload(response)
                        .await
                        .map_err(wasmtime::Error::msg)?;
                    let payload: HostResponsePollResult = host_response
                        .try_into()
                        .map_err(|e: String| wasmtime::Error::msg(e))?;
                    Ok(self.ready_set_for_current_targets(payload, &targets))
                }
                // The give-up fallback, but ONLY while the cursor holds a host-call entry.
                //
                // `record_poll_replay_miss`'s bound reasons that once consecutive misses at an
                // unmoved cursor exceed the entries remaining ahead of it, nothing can ever
                // claim that position. That argument counts oplog CONSUMERS — and it is simply
                // false for a structural entry: `EndRemoteWrite`'s owner is `end_function`,
                // driven by guest control flow (the guest dropping the HTTP resource), not by
                // consuming the replay cursor. Falling back there is guaranteed to fail — the
                // read can only report "expected HostCall, got EndRemoteWrite" — so it converts
                // a recoverable state into a trap while diagnosing nothing
                // (FINDING_B_FIX_DESIGN.md §15.6). With `ready()` now agreeing with this
                // synthesis, the guest can make the control-flow progress that moves the cursor.
                _ if cursor_is_host_call && self.state.record_poll_replay_miss() => {
                    // A host-call entry IS something a `poll()` could legitimately have owned,
                    // so here the bound's counting argument holds and reporting the original
                    // failure is the correct, diagnosable outcome.
                    Ok(durability.replay(self).await?)
                }
                _ => {
                    // "Not ready yet" — the legal, self-correcting answer, mirroring what
                    // `ready()` already does on a predicate miss. Report as ready whichever
                    // input pollables already have a pre-resolved answer waiting (a stray-scan
                    // consumed their entry on their behalf, so replaying that answer to the
                    // correct pollable is not a guess); if none do, wake everything so whichever
                    // task owns the next entry gets a chance to claim it, rather than
                    // livelocking on an empty ready-set.
                    let mut ready: Vec<u32> = Vec::new();
                    for (index, pollable) in in_.iter().enumerate() {
                        if self.state.is_pollable_pre_resolved_ready(pollable.rep()) {
                            ready.push(index as u32);
                        }
                    }
                    if ready.is_empty() {
                        ready = (0..in_.len() as u32).collect();
                    }
                    trace!(
                        agent_id = %self.owned_agent_id,
                        ?ready,
                        "POLLCALL_TRACE poll() REPLAY no own entry at cursor, synthesizing"
                    );
                    Ok(HostResponsePollResult {
                        result: Ok(ready),
                        targets: None,
                    })
                }
            }
        };

        match result {
            Ok(result) => {
                let ready = result.result.map_err(wasmtime::Error::msg)?;
                if replaying {
                    let ready_reps: Vec<u32> = ready
                        .iter()
                        .filter_map(|index| in_reps.get(*index as usize).copied())
                        .collect();
                    self.drive_replayed_filesystem_pollables(&ready_reps)
                        .await?;
                }
                Ok(ready)
            }
            Err(duration) => {
                if self.agent_mode() == AgentMode::Ephemeral {
                    let max = self.state.config.suspend.ephemeral_max_sleep;
                    Err(ephemeral_sleep_too_long_error(
                        duration_to_nanos(duration),
                        std_duration_to_nanos(max),
                    ))
                } else {
                    self.state.sleep_until(Utc::now() + duration).await?;
                    Err(wasmtime::Error::from_anyhow(
                        InterruptKind::Suspend(Timestamp::now_utc()).into(),
                    ))
                }
            }
        }
    }
}

impl<Ctx: WorkerCtx> DurableWorkerCtx<Ctx> {
    /// Replay only: `replayed_poll_answer`, plus a trace of any translation it made.
    fn ready_set_for_current_targets(
        &self,
        payload: HostResponsePollResult,
        current_targets: &[Option<u32>],
    ) -> HostResponsePollResult {
        let recorded_ready = payload.result.clone();
        let (answer, mapping) = replayed_poll_answer(payload, current_targets);
        match mapping {
            PollReplayMapping::Legacy | PollReplayMapping::Unchanged => {}
            PollReplayMapping::Remapped => debug!(
                agent_id = %self.owned_agent_id,
                recorded_targets = ?answer.targets,
                ?recorded_ready,
                ?current_targets,
                mapped = ?answer.result,
                "POLLCALL_TRACE poll() REPLAY target list order differs from live, ready set remapped by identity"
            ),
            PollReplayMapping::TargetSetMismatch => warn!(
                agent_id = %self.owned_agent_id,
                recorded_targets = ?answer.targets,
                ?recorded_ready,
                ?current_targets,
                "POLLCALL_TRACE poll() REPLAY target set differs from live, falling back to positional ready indexes"
            ),
        }
        answer
    }

    /// Replay only: actually wait for every filesystem-stream pollable among `ready_reps` — the
    /// pollables the replayed `poll()`/`ready()` answer just reported as ready.
    ///
    /// File streams are re-executed live during replay (they are not durable), but wasmtime's
    /// file streams only move out of `Waiting` inside `Pollable::ready`. Returning the recorded
    /// answer alone would leave the stream exactly where live execution found it BEFORE it
    /// waited, so the guest's next read would still return nothing (or check-write still grant
    /// no permit) and it would `block()` again: consuming later operations' recorded poll
    /// entries, then spinning forever once they run out (video-harness #216).
    ///
    /// The wait always terminates, even when the answer was synthesized rather than recorded
    /// (`poll()`'s "wake everything" / `ready()`'s structural-cursor fallback): a file stream's
    /// readiness is a pending local file read/write finishing, and an idle input stream's
    /// `ready()` simply starts a read-ahead. (It does not observe interrupts — acceptable for
    /// regular files; a FIFO in the agent filesystem could delay an interrupt during replay.)
    /// It calls wasmtime's own `block`, not the durable wrapper, so it never touches the oplog.
    /// Durable resources' pollables are never driven: their replayed readiness is
    /// authoritative, and their real operations are not re-issued during replay.
    async fn drive_replayed_filesystem_pollables(
        &mut self,
        ready_reps: &[u32],
    ) -> wasmtime::Result<()> {
        for rep in ready_reps {
            if self.state.filesystem_stream_pollables.contains(rep) {
                let mut view = self.as_wasi_view();
                HostPollable::block(&mut view.io_data(), Resource::new_borrow(*rep)).await?;
            }
        }
        Ok(())
    }
}

struct PromiseWaiting(bool);

impl PromiseWaiting {
    fn new(enabled: bool) -> Self {
        if enabled {
            inc_promise_waiting();
        }
        Self(enabled)
    }
}

impl Drop for PromiseWaiting {
    fn drop(&mut self) {
        if self.0 {
            dec_promise_waiting();
        }
    }
}

fn ephemeral_sleep_too_long_error(requested_nanos: u64, max_nanos: u64) -> wasmtime::Error {
    wasmtime::Error::from_anyhow(anyhow::anyhow!(WorkerExecutorError::InvocationFailed {
        error: AgentError::EphemeralSleepTooLong(EphemeralSleepTooLongError {
            requested_nanos,
            max_nanos,
        }),
        stderr: String::new(),
    }))
}

fn duration_to_nanos(duration: Duration) -> u64 {
    duration
        .to_std()
        .map(std_duration_to_nanos)
        .unwrap_or(u64::MAX)
}

fn std_duration_to_nanos(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn is_suspend_for_sleep<T>(result: &Result<T, wasmtime::Error>) -> Option<Duration> {
    if let Err(err) = result {
        // Walk the error source chain, since wasmtime::Error may wrap the original error
        let mut current: Option<&dyn std::error::Error> = Some(err.as_ref());
        while let Some(e) = current {
            if let Some(SuspendForSleep(duration)) = e.downcast_ref::<SuspendForSleep>() {
                return Some(Duration::from_std(*duration).unwrap());
            }
            current = e.source();
        }
        None
    } else {
        None
    }
}

/// Maps a recorded `poll()` ready set onto the guest's CURRENT target list, by pollable
/// identity instead of by position.
///
/// `recorded_targets` / `current_targets` describe each input pollable, in list order, by its
/// call-order-derived logical identity (`pollable_seq`; `None` = untracked, see
/// `peek_pollable_seq`). `recorded_ready` is the live answer, as indexes into
/// `recorded_targets`. Returns the same answer as indexes into `current_targets`.
///
/// Why positions are not enough: the order of the list is the GUEST's choice, and it need not be
/// replay-stable. wstd's reactor (wasm-rquickjs's, too) builds it by iterating a `HashMap` whose
/// keys carry a process-wide counter, so a fresh instance — after a snapshot restore, an update
/// or a revert — presents the same pollables in a different order than the live run did, and
/// the recorded index then names a different pollable (2026-10-04: a fetch's 1e19-ns timeout
/// woken instead of its response, failing the fetch in replay only).
///
/// Matching rule: the k-th recorded occurrence of an identity maps to the k-th current
/// occurrence of the same identity. That covers a pollable listed twice (two waiters on one
/// pollable), and matches untracked pollables by their ordinal among the untracked ones — so a
/// list with no tracked pollable at all maps exactly positionally, as before. Consequently only
/// TRACKED pollables are protected against reordering: if a guest permutes two untracked ones,
/// they are still matched positionally (and the multiset check cannot notice). In wstd the only
/// untracked pollable is the reactor's single `READY_POLLABLE`, so this does not arise there.
///
/// The answer keeps the RECORDED order (the order the live guest saw, and woke its waiters in),
/// rather than being re-sorted by current position.
///
/// `None` when the two lists do not name the same pollables (as a multiset) or an index is out
/// of range: replay has then diverged for some other reason, and no identity mapping exists.
pub(crate) fn map_recorded_poll_ready(
    recorded_targets: &[Option<u32>],
    recorded_ready: &[u32],
    current_targets: &[Option<u32>],
) -> Option<Vec<u32>> {
    let mut recorded_sorted = recorded_targets.to_vec();
    let mut current_sorted = current_targets.to_vec();
    recorded_sorted.sort_unstable();
    current_sorted.sort_unstable();
    if recorded_sorted != current_sorted {
        return None;
    }

    recorded_ready
        .iter()
        .map(|&recorded_index| {
            let recorded_index = recorded_index as usize;
            let identity = recorded_targets.get(recorded_index)?;
            let occurrence = recorded_targets[..recorded_index]
                .iter()
                .filter(|target| *target == identity)
                .count();
            current_targets
                .iter()
                .enumerate()
                .filter(|(_, target)| *target == identity)
                .nth(occurrence)
                .map(|(current_index, _)| current_index as u32)
        })
        .collect()
}

/// How a replayed `poll()` answer was translated (see `replayed_poll_answer`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PollReplayMapping {
    /// Entry written before `targets` existed, or a recorded error: returned unchanged.
    Legacy,
    /// Identity mapping yields the recorded indexes: unchanged.
    Unchanged,
    /// Same pollables in a different order: ready indexes translated by identity.
    Remapped,
    /// Not the same pollables as live: returned unchanged (positional — the pre-`targets`
    /// behavior, and the best remaining guess).
    TargetSetMismatch,
}

/// Translates a recorded `poll()` answer into indexes of the guest's CURRENT input list
/// (`current_targets`), via `map_recorded_poll_ready`. Falls back to the recorded indexes
/// unchanged — the positional behavior every entry had before `targets` existed — for an entry
/// that predates the field, a recorded error, or a target set that differs from live.
pub(crate) fn replayed_poll_answer(
    payload: HostResponsePollResult,
    current_targets: &[Option<u32>],
) -> (HostResponsePollResult, PollReplayMapping) {
    let (Ok(recorded_ready), Some(recorded_targets)) = (&payload.result, &payload.targets) else {
        return (payload, PollReplayMapping::Legacy);
    };
    match map_recorded_poll_ready(recorded_targets, recorded_ready, current_targets) {
        Some(mapped) if &mapped == recorded_ready => (payload, PollReplayMapping::Unchanged),
        Some(mapped) => (
            HostResponsePollResult {
                result: Ok(mapped),
                targets: payload.targets,
            },
            PollReplayMapping::Remapped,
        ),
        None => (payload, PollReplayMapping::TargetSetMismatch),
    }
}

#[cfg(test)]
mod tests {
    use super::{PollReplayMapping, map_recorded_poll_ready, replayed_poll_answer};
    use golem_common::model::oplog::HostResponsePollResult;
    use test_r::test;

    /// Backward compatibility: an entry written before `targets` existed (decoded with
    /// `targets: None`) replays exactly as before — positionally — even if the guest's list is
    /// permuted (nothing to map it by).
    #[test]
    fn legacy_entry_without_targets_replays_positionally() {
        let recorded = HostResponsePollResult {
            result: Ok(vec![1]),
            targets: None,
        };
        let (answer, mapping) = replayed_poll_answer(recorded.clone(), &[Some(3), Some(7)]);
        assert_eq!(mapping, PollReplayMapping::Legacy);
        assert_eq!(answer, recorded);
    }

    #[test]
    fn recorded_error_is_replayed_unchanged() {
        let recorded = HostResponsePollResult {
            result: Err("boom".to_string()),
            targets: Some(vec![Some(7), Some(3)]),
        };
        let (answer, mapping) = replayed_poll_answer(recorded.clone(), &[Some(3), Some(7)]);
        assert_eq!(mapping, PollReplayMapping::Legacy);
        assert_eq!(answer, recorded);
    }

    #[test]
    fn permuted_entry_is_remapped_and_keeps_its_targets() {
        let recorded = HostResponsePollResult {
            result: Ok(vec![1]),
            targets: Some(vec![Some(7), Some(3)]),
        };
        let (answer, mapping) = replayed_poll_answer(recorded, &[Some(3), Some(7)]);
        assert_eq!(mapping, PollReplayMapping::Remapped);
        assert_eq!(answer.result, Ok(vec![0]));
        assert_eq!(answer.targets, Some(vec![Some(7), Some(3)]));
    }

    #[test]
    fn same_order_entry_is_unchanged() {
        let recorded = HostResponsePollResult {
            result: Ok(vec![1]),
            targets: Some(vec![Some(7), Some(3)]),
        };
        let (answer, mapping) = replayed_poll_answer(recorded.clone(), &[Some(7), Some(3)]);
        assert_eq!(mapping, PollReplayMapping::Unchanged);
        assert_eq!(answer, recorded);
    }

    #[test]
    fn mismatched_target_set_falls_back_to_positional() {
        let recorded = HostResponsePollResult {
            result: Ok(vec![1]),
            targets: Some(vec![Some(7), Some(3)]),
        };
        let (answer, mapping) = replayed_poll_answer(recorded.clone(), &[Some(3), Some(8)]);
        assert_eq!(mapping, PollReplayMapping::TargetSetMismatch);
        assert_eq!(answer, recorded);
    }

    /// The 2026-10-04 live failure, distilled: live recorded `poll([timer#7, response#3]) ->
    /// [1]` (the response future); after a snapshot restore the guest's reactor builds the same
    /// list in the opposite order. Positional replay would wake the 1e19-ns timer instead.
    #[test]
    fn permuted_target_list_maps_ready_set_by_identity() {
        let recorded_targets = [Some(7), Some(3)];
        let current_targets = [Some(3), Some(7)];
        assert_eq!(
            map_recorded_poll_ready(&recorded_targets, &[1], &current_targets),
            Some(vec![0]),
            "the response future (seq 3) must be woken, not whatever sits at live position 1"
        );
    }

    /// Three pollables (body-write backpressure + response future + timer), rotated, with two
    /// of them ready: each must land on its own pollable, and the answer must keep the RECORDED
    /// order (the live wake order), not be re-sorted into current positions.
    #[test]
    fn three_way_permutation_keeps_live_wake_order() {
        let recorded_targets = [Some(10), Some(11), Some(12)];
        let current_targets = [Some(12), Some(10), Some(11)];
        assert_eq!(
            map_recorded_poll_ready(&recorded_targets, &[0, 2], &current_targets),
            Some(vec![1, 0])
        );
    }

    /// Untracked pollables (no `ready()` ever called on them, e.g. wstd's `READY_POLLABLE`) are
    /// matched by their ordinal among the untracked ones — positional within that class.
    #[test]
    fn untracked_pollables_match_by_ordinal_among_untracked() {
        let recorded_targets = [Some(4), Some(5), None];
        let current_targets = [Some(5), Some(4), None];
        assert_eq!(
            map_recorded_poll_ready(&recorded_targets, &[0, 2], &current_targets),
            Some(vec![1, 2])
        );
    }

    /// The same pollable can appear twice in one list (two `WaitFor`s on one `AsyncPollable`):
    /// the k-th recorded occurrence maps to the k-th current occurrence.
    #[test]
    fn duplicate_pollable_occurrences_map_in_order() {
        let recorded_targets = [Some(2), Some(9), Some(2)];
        let current_targets = [Some(9), Some(2), Some(2)];
        assert_eq!(
            map_recorded_poll_ready(&recorded_targets, &[0, 2], &current_targets),
            Some(vec![1, 2])
        );
    }

    /// A different set of pollables than recorded means replay has diverged in some other way;
    /// no identity mapping is attempted (the caller falls back to positional).
    #[test]
    fn different_target_set_is_not_mapped() {
        assert_eq!(
            map_recorded_poll_ready(&[Some(1), Some(2)], &[1], &[Some(1), Some(8)]),
            None
        );
        assert_eq!(
            map_recorded_poll_ready(&[Some(1), Some(2)], &[1], &[Some(1)]),
            None
        );
        assert_eq!(map_recorded_poll_ready(&[Some(1)], &[3], &[Some(1)]), None);
    }

    /// Unchanged order is the identity mapping.
    #[test]
    fn same_order_is_identity() {
        let targets = [Some(1), None, Some(2)];
        assert_eq!(
            map_recorded_poll_ready(&targets, &[0, 1, 2], &targets),
            Some(vec![0, 1, 2])
        );
    }
}
