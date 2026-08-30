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
use crate::durable_host::replay_state::ReplayState;
use crate::durable_host::{Durability, DurabilityHost, DurableWorkerCtx, SuspendForSleep};
use crate::metrics::ephemeral::{dec_promise_waiting, inc_promise_waiting};
use crate::services::oplog::{Oplog, OplogOps};
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
use std::collections::HashSet;
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
                .persist(self, HostRequestNoInput {}, HostResponsePollReady { result })
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
            if let Some(pre_resolved) = self.state.take_pre_resolved_pollable_ready(pollable_seq)
            {
                trace!(
                    agent_id = %self.owned_agent_id,
                    rep = pollable_rep,
                    seq = pollable_seq,
                    result = pre_resolved,
                    "POLLREADY_TRACE ready() REPLAY using answer pre-resolved by an earlier poll() call"
                );
                return Ok(pre_resolved);
            }

            // Replay: consume the next IoPollReady entry only if it was recorded for THIS
            // specific pollable (matched by logical seq — see pollable_seq's doc comment).
            // This prevents a timer pollable from stealing an IoPollReady=true entry that was
            // recorded for an output-stream pollable, which would cause the WASM to think the
            // timer fired, drop the FutureIncomingResponse early, and crash with "expected
            // EndRemoteWrite, got CheckWrite".
            // Legacy ReadLocal entries (pre-dating per-pollable tagging) are consumed by any
            // pollable, matching the original (pre-Bug-2-fix) behavior for old oplogs.
            let peeked = self
                .state
                .replay_state
                .try_get_oplog_entry(|entry| match entry {
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
                _ => Ok(false),
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
                if let Err(err) = self.table().delete(parent_owned) {
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
            // Finding B (FINDING_B_FIX_DESIGN.md): a batch of 2+ pollables CAN still leave a
            // stray IoPollReady(seq) entry positioned before poll()'s own next entry — recorded
            // live for one of THIS batch's members, but out of the guest's replayed structural
            // check order (wstd's Reactor batches concurrently-pending pollables via a
            // HashMap-keyed waker set whose iteration order is per-process-randomized, so live's
            // real completion order among a batch's members isn't guaranteed to match replay's
            // structural check order). `ready()`'s own seq-predicate correctly refuses to
            // consume such an entry when asked about the WRONG pollable (leaving it in place) —
            // but poll()'s replay previously had no equivalent identity check, so it would try
            // to consume that same still-pending entry as if it must be its own IoPollPoll type
            // and crash. Walk forward, consuming (for real — try_get_oplog_entry durably
            // advances past a match) zero or more such stray entries belonging to THIS batch,
            // caching each one's answer for the owning pollable's own later ready() call to
            // consult (`pre_resolved_pollable_ready`) — until the next entry is NOT one of
            // those, at which point defer entirely to the unchanged, existing
            // `durability.replay()` path: either it's poll()'s own genuine entry (happy path,
            // byte-for-byte unchanged), or it's genuinely unexpected and the existing crash
            // path fires exactly as it does today. No batch-size assumption: N stray entries in
            // any order is simply N loop iterations.
            let batch_seqs: HashSet<u32> = in_
                .iter()
                .filter_map(|r| self.state.pollable_seq_if_assigned(r.rep()))
                .collect();
            let reps_for_trace = in_.iter().map(|r| r.rep()).collect::<Vec<_>>();

            let mut pre_resolved = Vec::new();
            consume_stray_ready_entries(
                &mut self.state.replay_state,
                self.public_state.worker().oplog().as_ref(),
                &batch_seqs,
                |seq, ready| pre_resolved.push((seq, ready)),
            )
            .await?;
            for (seq, ready) in pre_resolved {
                trace!(
                    agent_id = %self.owned_agent_id,
                    reps = ?reps_for_trace,
                    seq,
                    ready,
                    "POLLCALL_TRACE poll() REPLAY consumed stray same-batch ready() entry, caching"
                );
                self.state.record_pre_resolved_pollable_ready(seq, ready);
            }

            // Whatever's left (if anything) is not a stray same-batch entry — defer entirely to
            // the unchanged, existing path: either it's poll()'s own genuine entry (happy path)
            // or a genuinely unexpected mismatch (existing crash path).
            Ok(durability.replay(self).await?)
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

/// Core mechanism for the Finding B fix (FINDING_B_FIX_DESIGN.md): walks forward through the
/// oplog, consuming (durably — `try_get_oplog_entry` advances the cursor on every match, never
/// re-visiting a consumed entry) zero or more "stray" `IoPollReady(seq)` entries belonging to
/// one of `batch_seqs`'s pollables. Such an entry can appear before `poll()`'s own next entry
/// when it was recorded live for a batch member, but out of the guest's replayed structural
/// check order — `wstd`'s `Reactor` batches concurrently-pending pollables via a
/// `HashMap`-keyed waker set whose iteration order is per-process-randomized, so live's real
/// completion order among a batch's members is not guaranteed to match replay's structural
/// check order. `ready()`'s own seq-predicate already refuses to consume such an entry for the
/// wrong pollable (leaving it in place); this is the symmetric fix for `poll()`, which
/// previously had no such identity check and would crash trying to consume it as its own entry.
///
/// Each stray's `(seq, ready)` answer is reported via `record_stray` so the caller can cache it
/// (`PrivateDurableWorkerState::record_pre_resolved_pollable_ready`) for that pollable's own
/// later `ready()` call to consult first. Returns as soon as the next entry does NOT match —
/// the caller is responsible for that entry (either `poll()`'s own genuine entry, or a
/// genuinely unrelated mismatch), via the unchanged `durability.replay()` path; this function
/// never touches or assumes anything about `poll()`'s own entry type. No batch-size assumption
/// anywhere: N stray entries, in any order, is simply N loop iterations.
async fn consume_stray_ready_entries(
    replay_state: &mut ReplayState,
    oplog: &dyn Oplog,
    batch_seqs: &HashSet<u32>,
    mut record_stray: impl FnMut(u32, bool),
) -> Result<(), WorkerExecutorError> {
    loop {
        let stray = replay_state
            .try_get_oplog_entry(|entry| match entry {
                OplogEntry::HostCall {
                    function_name: HostFunctionName::IoPollReady,
                    durable_function_type: DurableFunctionType::ReadLocalPollable(seq),
                    ..
                } => batch_seqs.contains(seq),
                _ => false,
            })
            .await?;

        match stray {
            Some((
                _idx,
                OplogEntry::HostCall {
                    durable_function_type: DurableFunctionType::ReadLocalPollable(seq),
                    response,
                    ..
                },
            )) => {
                let host_response = oplog
                    .download_payload(response)
                    .await
                    .map_err(WorkerExecutorError::runtime)?;
                let payload: HostResponsePollReady = host_response
                    .try_into()
                    .map_err(WorkerExecutorError::runtime)?;
                record_stray(seq, payload.result.unwrap_or(false));
            }
            _ => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::oplog::CommitLevel;
    use async_trait::async_trait;
    use golem_common::model::component::ComponentId;
    use golem_common::model::environment::EnvironmentId;
    use golem_common::model::oplog::{
        HostRequest, HostResponse, OplogIndex, OplogPayload, PayloadId, PersistenceLevel,
        RawOplogPayload,
    };
    use golem_common::model::regions::DeletedRegions;
    use golem_common::model::{AgentId, OwnedAgentId, Timestamp};
    use std::collections::BTreeMap;
    use std::fmt::{Debug, Formatter};
    use std::sync::{Arc, Mutex};
    use test_r::test;

    /// Local test-only oplog double — mirrors `replay_state.rs`'s own `MutableBatchOplog`
    /// (private to that module's test block, so duplicated here rather than exposed as shared
    /// library code for one small test-only mock).
    struct MutableBatchOplog {
        entries: Mutex<BTreeMap<OplogIndex, OplogEntry>>,
    }

    impl MutableBatchOplog {
        fn new(entries: BTreeMap<OplogIndex, OplogEntry>) -> Self {
            Self {
                entries: Mutex::new(entries),
            }
        }
    }

    impl Debug for MutableBatchOplog {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MutableBatchOplog").finish()
        }
    }

    #[async_trait]
    impl Oplog for MutableBatchOplog {
        async fn add(&self, _entry: OplogEntry) -> OplogIndex {
            unimplemented!()
        }

        async fn drop_prefix(&self, _last_dropped_id: OplogIndex) -> u64 {
            unimplemented!()
        }

        async fn commit(&self, _level: CommitLevel) -> BTreeMap<OplogIndex, OplogEntry> {
            unimplemented!()
        }

        async fn current_oplog_index(&self) -> OplogIndex {
            *self.entries.lock().unwrap().last_key_value().unwrap().0
        }

        async fn last_added_non_hint_entry(&self) -> Option<OplogIndex> {
            None
        }

        async fn wait_for_replicas(&self, _replicas: u8, _timeout: std::time::Duration) -> bool {
            unimplemented!()
        }

        async fn read(&self, oplog_index: OplogIndex) -> OplogEntry {
            self.entries.lock().unwrap()[&oplog_index].clone()
        }

        async fn read_many(
            &self,
            oplog_index: OplogIndex,
            n: u64,
        ) -> BTreeMap<OplogIndex, OplogEntry> {
            self.entries
                .lock()
                .unwrap()
                .range(oplog_index..)
                .take(n as usize)
                .map(|(index, entry)| (*index, entry.clone()))
                .collect()
        }

        async fn length(&self) -> u64 {
            self.entries.lock().unwrap().len() as u64
        }

        async fn upload_raw_payload(&self, _data: Vec<u8>) -> Result<RawOplogPayload, String> {
            unimplemented!()
        }

        async fn download_raw_payload(
            &self,
            _payload_id: PayloadId,
            _md5_hash: Vec<u8>,
        ) -> Result<Vec<u8>, String> {
            unimplemented!()
        }

        async fn switch_persistence_level(&self, _mode: PersistenceLevel) {
            unimplemented!()
        }
    }

    fn io_poll_ready_entry(seq: u32, result: bool) -> OplogEntry {
        OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::IoPollReady,
            request: OplogPayload::Inline(Box::new(HostRequest::NoInput(
                HostRequestNoInput {},
            ))),
            response: OplogPayload::Inline(Box::new(HostResponse::PollReady(
                HostResponsePollReady { result: Ok(result) },
            ))),
            durable_function_type: DurableFunctionType::ReadLocalPollable(seq),
        }
    }

    fn io_poll_poll_entry() -> OplogEntry {
        OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::IoPollPoll,
            request: OplogPayload::Inline(Box::new(HostRequest::PollCount(
                HostRequestPollCount { count: 2 },
            ))),
            response: OplogPayload::Inline(Box::new(HostResponse::PollResult(
                HostResponsePollResult { result: Ok(vec![0]) },
            ))),
            durable_function_type: DurableFunctionType::ReadLocal,
        }
    }

    async fn new_replay_state(oplog: Arc<MutableBatchOplog>) -> ReplayState {
        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "test".to_string(),
        };
        ReplayState::new(
            OwnedAgentId::new(EnvironmentId::new(), &agent_id),
            oplog,
            DeletedRegions::new(),
        )
        .await
        .unwrap()
    }

    /// Regression test seeded directly from the Tenth capture's exact entry sequence
    /// (`FINDING_B_FIX_DESIGN.md`): a batched poll([rep20, rep24]) — seq 150 for rep 20, seq
    /// 154 for rep 24 — where entry 841 (rep 24's confirmed-true `IoPollReady`) sits where
    /// poll()'s own next entry was expected, because rep 24 resolved before the guest's
    /// replayed structural check order reached rep 20. Before the fix, `poll()`'s replay
    /// crashed here (`expected io::poll::poll, got io::poll::pollable::ready`); this exercises
    /// the exact mechanism that now prevents it.
    #[test]
    async fn consumes_single_stray_matching_tenth_capture_shape() {
        let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
            (
                OplogIndex::INITIAL,
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                },
            ),
            (OplogIndex::INITIAL.next(), io_poll_ready_entry(154, true)),
            // poll()'s own genuine entry — must have been recorded too (that's WHY replay is
            // happening for this call at all); without a trailing entry here, "nothing left to
            // find" is indistinguishable from "the oplog was truncated mid-recording", a
            // different (and correctly still-erroring) scenario this test isn't about.
            (
                OplogIndex::INITIAL.next().next(),
                io_poll_poll_entry(),
            ),
        ])));
        let mut replay_state = new_replay_state(oplog.clone()).await;
        let batch_seqs = HashSet::from([150, 154]);

        let mut strays = Vec::new();
        consume_stray_ready_entries(&mut replay_state, oplog.as_ref(), &batch_seqs, |seq, ready| {
            strays.push((seq, ready))
        })
        .await
        .unwrap();

        assert_eq!(
            strays,
            vec![(154, true)],
            "must consume exactly rep 24's stray entry and report it as ready"
        );
        // Durably consumed — try_get_oplog_entry must find no MORE entries matching rep 24's
        // seq (no double-consumption, no entry silently dropped without being reported)...
        assert!(
            replay_state
                .try_get_oplog_entry(|e| matches!(
                    e,
                    OplogEntry::HostCall {
                        function_name: HostFunctionName::IoPollReady,
                        durable_function_type: DurableFunctionType::ReadLocalPollable(154),
                        ..
                    }
                ))
                .await
                .unwrap()
                .is_none(),
            "the stray entry must be consumed exactly once; nothing should remain after it"
        );
        // ...while poll()'s own genuine entry is still there, completely untouched, exactly
        // where the Tenth capture's crash used to happen when this function didn't exist.
        assert!(
            replay_state
                .try_get_oplog_entry(|e| matches!(
                    e,
                    OplogEntry::HostCall {
                        function_name: HostFunctionName::IoPollPoll,
                        ..
                    }
                ))
                .await
                .unwrap()
                .is_some(),
            "poll()'s own entry must remain for the caller's durability.replay() to consume"
        );
    }

    /// "Before" half of the bidirectional falsification (the "after" half is the test above):
    /// reproduces the OLD, pre-fix consumption mechanism — `Durability::replay_raw()`'s
    /// `read_persisted_durable_function_invocation()` (`get_oplog_entry!(replay_state,
    /// OplogEntry::HostCall)`, i.e. "read the next HostCall entry unconditionally, whatever it
    /// is") followed by `validate_oplog_entry`'s function-name check — directly against the
    /// Tenth capture's exact oplog shape, proving it genuinely mismatches. This confirms the
    /// crafted scenario above is a real reproduction of the production crash's trigger
    /// condition ("expected io::poll::poll, got io::poll::pollable::ready"), not merely a shape
    /// that happens to exercise the new code path.
    #[test]
    async fn old_unconditional_consume_would_have_hit_the_production_mismatch() {
        let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
            (
                OplogIndex::INITIAL,
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                },
            ),
            (OplogIndex::INITIAL.next(), io_poll_ready_entry(154, true)),
        ])));
        let mut replay_state = new_replay_state(oplog.clone()).await;

        // The OLD mechanism: consume the next entry unconditionally — no per-pollable identity
        // check, whatever's there MUST be poll()'s own entry (get_oplog_entry() is
        // try_get_oplog_entry(|_| true), i.e. matches and consumes anything).
        let (_idx, entry) = replay_state.get_oplog_entry().await.unwrap();
        let OplogEntry::HostCall { function_name, .. } = entry else {
            panic!("crafted entry is always a HostCall variant");
        };

        assert_eq!(
            function_name,
            HostFunctionName::IoPollReady,
            "sanity check: this crafted oplog must genuinely reproduce the Tenth capture's \
             trigger — if this ever fails, the test no longer represents a real reproduction"
        );
        assert_ne!(
            function_name,
            HostFunctionName::IoPollPoll,
            "the OLD unconditional mechanism would crash here (Durability::validate_oplog_entry \
             expects IoPollPoll, finds IoPollReady) — exactly the defect \
             consume_stray_ready_entries (tested above) fixes by checking identity FIRST, \
             before ever reaching this unconditional read"
        );
    }

    /// N=2 strays, in the SAME order they appear in the oplog, followed by poll()'s own genuine
    /// entry — proving the mechanism generalizes past a single stray (no batch-size assumption).
    #[test]
    async fn consumes_multiple_strays_before_genuine_poll_entry() {
        let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
            (
                OplogIndex::INITIAL,
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                },
            ),
            (OplogIndex::INITIAL.next(), io_poll_ready_entry(154, true)),
            (
                OplogIndex::INITIAL.next().next(),
                io_poll_ready_entry(161, true),
            ),
            (
                OplogIndex::INITIAL.next().next().next(),
                io_poll_poll_entry(),
            ),
        ])));
        let mut replay_state = new_replay_state(oplog.clone()).await;
        let batch_seqs = HashSet::from([150, 154, 161, 172]);

        let mut strays = Vec::new();
        consume_stray_ready_entries(&mut replay_state, oplog.as_ref(), &batch_seqs, |seq, ready| {
            strays.push((seq, ready))
        })
        .await
        .unwrap();

        assert_eq!(
            strays,
            vec![(154, true), (161, true)],
            "both strays must be consumed and reported, in oplog order"
        );
        // poll()'s own genuine entry must be left untouched for the caller's existing
        // durability.replay() path to consume normally.
        let next = replay_state
            .try_get_oplog_entry(|e| {
                matches!(
                    e,
                    OplogEntry::HostCall {
                        function_name: HostFunctionName::IoPollPoll,
                        ..
                    }
                )
            })
            .await
            .unwrap();
        assert!(
            next.is_some(),
            "poll()'s own entry must remain for the caller to consume, untouched by this function"
        );
    }

    /// Same as above but with the two strays in the OPPOSITE relative order, proving the
    /// mechanism doesn't depend on which stray arrives first.
    #[test]
    async fn consumes_multiple_strays_regardless_of_order() {
        let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
            (
                OplogIndex::INITIAL,
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                },
            ),
            (OplogIndex::INITIAL.next(), io_poll_ready_entry(161, true)),
            (
                OplogIndex::INITIAL.next().next(),
                io_poll_ready_entry(154, true),
            ),
            (
                OplogIndex::INITIAL.next().next().next(),
                io_poll_poll_entry(),
            ),
        ])));
        let mut replay_state = new_replay_state(oplog.clone()).await;
        let batch_seqs = HashSet::from([150, 154, 161, 172]);

        let mut strays = Vec::new();
        consume_stray_ready_entries(&mut replay_state, oplog.as_ref(), &batch_seqs, |seq, ready| {
            strays.push((seq, ready))
        })
        .await
        .unwrap();

        assert_eq!(strays, vec![(161, true), (154, true)]);
    }

    /// A `ready()` entry that does NOT belong to any member of the current batch must be left
    /// entirely untouched — this function's scope is exactly "stray same-batch entries," not
    /// "swallow anything that isn't poll()'s own entry."
    #[test]
    async fn ignores_ready_entry_for_seq_outside_the_batch() {
        let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
            (
                OplogIndex::INITIAL,
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                },
            ),
            (OplogIndex::INITIAL.next(), io_poll_ready_entry(999, true)),
        ])));
        let mut replay_state = new_replay_state(oplog.clone()).await;
        let batch_seqs = HashSet::from([150, 154]);

        let mut strays = Vec::new();
        consume_stray_ready_entries(&mut replay_state, oplog.as_ref(), &batch_seqs, |seq, ready| {
            strays.push((seq, ready))
        })
        .await
        .unwrap();

        assert!(strays.is_empty());
        // Left untouched for the existing crash path to correctly fire on (it genuinely is not
        // poll()'s own entry either).
        assert!(
            replay_state
                .try_get_oplog_entry(|e| matches!(
                    e,
                    OplogEntry::HostCall {
                        function_name: HostFunctionName::IoPollReady,
                        durable_function_type: DurableFunctionType::ReadLocalPollable(999),
                        ..
                    }
                ))
                .await
                .unwrap()
                .is_some(),
            "the unrelated entry must still be sitting there, completely untouched"
        );
    }

    /// When the very next entry is already poll()'s own genuine entry (no strays at all), this
    /// function must do nothing and leave it for the caller — the common, unchanged happy path.
    #[test]
    async fn no_op_when_next_entry_is_already_genuine_poll_entry() {
        let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
            (
                OplogIndex::INITIAL,
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                },
            ),
            (OplogIndex::INITIAL.next(), io_poll_poll_entry()),
        ])));
        let mut replay_state = new_replay_state(oplog.clone()).await;
        let batch_seqs = HashSet::from([150, 154]);

        let mut strays = Vec::new();
        consume_stray_ready_entries(&mut replay_state, oplog.as_ref(), &batch_seqs, |seq, ready| {
            strays.push((seq, ready))
        })
        .await
        .unwrap();

        assert!(strays.is_empty());
        let next = replay_state
            .try_get_oplog_entry(|e| {
                matches!(
                    e,
                    OplogEntry::HostCall {
                        function_name: HostFunctionName::IoPollPoll,
                        ..
                    }
                )
            })
            .await
            .unwrap();
        assert!(next.is_some(), "poll()'s entry must remain, untouched");
    }
}
