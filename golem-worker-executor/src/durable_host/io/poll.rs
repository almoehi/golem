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
use crate::durable_host::{Durability, DurabilityHost, DurableWorkerCtx, SuspendForSleep};
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
            trace!(
                agent_id = %self.owned_agent_id,
                rep = pollable_rep,
                seq = pollable_seq,
                result = ?result,
                "POLLREADY_TRACE ready() LIVE"
            );
            // ready=false is "not yet, retry" — it carries no replay-essential information
            // and accounts for ~75% of oplog entries per fetch(). Skip recording it.
            // Replay synthesizes false for any gap in IoPollReady entries (see else branch).
            if result == Ok(false) {
                return Ok(false);
            }
            // Record with the pollable's logical seq so replay can match this entry to the
            // correct pollable (ReadLocalPollable instead of plain ReadLocal).
            let durable_function_type = DurableFunctionType::ReadLocalPollable(pollable_seq);
            trace!(
                agent_id = %self.owned_agent_id,
                rep = pollable_rep,
                seq = pollable_seq,
                durable_function_type = ?durable_function_type,
                "POLLREADY_TRACE ready() LIVE about to persist with this exact tag"
            );
            let durability = Durability::<IoPollReady>::new(self, durable_function_type).await?;
            let r = durability
                .persist(self, HostRequestNoInput {}, HostResponsePollReady { result })
                .await?;
            r.result.map_err(wasmtime::Error::msg)
        } else {
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
                Some((idx, OplogEntry::HostCall { response, .. })) => {
                    trace!(
                        agent_id = %self.owned_agent_id,
                        rep = pollable_rep,
                        seq = pollable_seq,
                        matched_oplog_index = %idx,
                        "POLLREADY_TRACE ready() REPLAY matched entry"
                    );
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
                    trace!(
                        agent_id = %self.owned_agent_id,
                        rep = pollable_rep,
                        seq = pollable_seq,
                        "POLLREADY_TRACE ready() REPLAY no match, synthesizing false"
                    );
                    Ok(false)
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
        trace!(
            agent_id = %self.owned_agent_id,
            is_live = self.durable_execution_state().is_live,
            reps = ?in_.iter().map(|r| r.rep()).collect::<Vec<_>>(),
            "POLLCALL_TRACE poll() enter"
        );
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
                trace!(
                    agent_id = %self.owned_agent_id,
                    reps = ?in_.iter().map(|r| r.rep()).collect::<Vec<_>>(),
                    "POLLCALL_TRACE poll() LIVE early-suspend (no IoPollPoll entry persisted)"
                );
                return Err(wasmtime::Error::from_anyhow(
                    InterruptKind::Suspend(Timestamp::now_utc()).into(),
                ));
            }
        };

        let durability =
            Durability::<IoPollPoll>::new(self, DurableFunctionType::ReadLocal).await?;
        trace!(
            agent_id = %self.owned_agent_id,
            durability_is_live = durability.is_live(),
            "POLLCALL_TRACE poll() Durability<IoPollPoll> constructed"
        );

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
            // pollable that legitimately owns an upcoming entry, and poll() can rely on the
            // normal replay path unconditionally.
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
