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
use tracing::{debug, trace};
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
            if let Some(pre_resolved) = self.state.take_pre_resolved_stray(&my_identity) {
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
                return Ok(ready);
            }

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
                    payload.result.map_err(wasmtime::Error::msg)
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
                    Ok(synthesized)
                }
            }
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

        let durability =
            Durability::<IoPollPoll>::new(self, DurableFunctionType::ReadLocal).await?;

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
                    Ok(payload)
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
                    Ok(HostResponsePollResult { result: Ok(ready) })
                }
            }
        };

        match result {
            Ok(result) => result.result.map_err(wasmtime::Error::msg),
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
