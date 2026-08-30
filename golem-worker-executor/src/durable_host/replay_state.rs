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

use crate::services::oplog::{Oplog, OplogOps};
use golem_common::model::component::ComponentRevision;
use golem_common::model::invocation_context::InvocationContextStack;
use golem_common::model::oplog::host_functions::HostFunctionName;
use golem_common::model::oplog::{
    AtomicOplogIndex, HostResponse, HostResponseGolemApiFork, LogLevel, OplogEntry, OplogIndex,
    PersistenceLevel,
};
use golem_common::model::regions::{DeletedRegions, OplogRegion};
use golem_common::model::{
    AgentInvocationPayload, AgentInvocationResult, ForkResult, IdempotencyKey, OwnedAgentId,
};
use golem_service_base::error::worker_executor::WorkerExecutorError;
use metrohash::MetroHash128;
use std::collections::{HashSet, VecDeque};
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::RwLock;
use tracing::{debug, trace};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum ReplayEvent {
    ReplayFinished,
    UpdateReplayed { new_revision: ComponentRevision },
    ForkReplayed { new_phantom_id: Uuid },
}

#[derive(Debug, Clone)]
pub struct AgentInvocationStartedEntry {
    pub idempotency_key: IdempotencyKey,
    pub invocation_payload: AgentInvocationPayload,
    pub invocation_context: InvocationContextStack,
}

#[derive(Debug, Clone)]
pub struct ReplayState {
    owned_agent_id: OwnedAgentId,
    oplog: Arc<dyn Oplog>,
    replay_target: AtomicOplogIndex,
    /// The oplog index of the last replayed entry
    last_replayed_index: AtomicOplogIndex,
    /// The oplog index of the last non-hint entry read
    last_replayed_non_hint_index: AtomicOplogIndex,
    internal: Arc<RwLock<InternalReplayState>>,
    has_seen_logs: Arc<AtomicBool>,
    replay_buffer: VecDeque<(OplogIndex, OplogEntry)>,
}

const REPLAY_READ_CHUNK_SIZE: u64 = 1024;

#[derive(Debug, Clone)]
struct InternalReplayState {
    pub skipped_regions: DeletedRegions,
    pub next_skipped_region: Option<OplogRegion>,
    /// Hashes of log entries persisted since the last read non-hint oplog entry
    pub log_hashes: HashSet<(u64, u64)>,
    /// Updates that were encountered while reading the oplog
    pub pending_replay_events: Vec<ReplayEvent>,
}

impl ReplayState {
    pub async fn new(
        owned_agent_id: OwnedAgentId,
        oplog: Arc<dyn Oplog>,
        skipped_regions: DeletedRegions,
    ) -> Result<Self, WorkerExecutorError> {
        let next_skipped_region = skipped_regions.find_next_deleted_region(OplogIndex::NONE);
        let last_oplog_index = oplog.current_oplog_index().await;
        let mut result = Self {
            owned_agent_id,
            oplog,
            last_replayed_index: AtomicOplogIndex::from_oplog_index(OplogIndex::NONE),
            last_replayed_non_hint_index: AtomicOplogIndex::from_oplog_index(OplogIndex::NONE),
            replay_target: AtomicOplogIndex::from_oplog_index(last_oplog_index),
            internal: Arc::new(RwLock::new(InternalReplayState {
                skipped_regions,
                next_skipped_region,
                log_hashes: HashSet::new(),
                pending_replay_events: Vec::new(),
            })),
            has_seen_logs: Arc::new(AtomicBool::new(false)),
            replay_buffer: VecDeque::new(),
        };
        result.move_replay_idx(OplogIndex::INITIAL).await; // By this we handle initial skipped regions applied by manual updates correctly
        result.skip_forward().await?;
        Ok(result)
    }

    pub async fn drop_override_and_restart(&mut self) -> Result<(), WorkerExecutorError> {
        {
            let mut internal = self.internal.write().await;
            internal.skipped_regions.drop_override();
            internal.next_skipped_region = internal
                .skipped_regions
                .find_next_deleted_region(OplogIndex::NONE);
            internal.log_hashes.clear();
            internal.pending_replay_events.clear();
        }
        self.last_replayed_index.set(OplogIndex::NONE);
        self.last_replayed_non_hint_index.set(OplogIndex::NONE);
        self.move_replay_idx(OplogIndex::INITIAL).await;
        self.skip_forward().await
    }

    pub async fn switch_to_live(&mut self) {
        if !self.is_live() {
            self.record_replay_event(ReplayEvent::ReplayFinished).await;
        }
        self.last_replayed_index.set(self.replay_target.get());
    }

    pub fn last_replayed_index(&self) -> OplogIndex {
        self.last_replayed_index.get()
    }

    pub fn last_replayed_non_hint_index(&self) -> OplogIndex {
        self.last_replayed_non_hint_index.get()
    }

    pub fn replay_target(&self) -> OplogIndex {
        self.replay_target.get()
    }

    pub fn set_replay_target(&mut self, new_target: OplogIndex) {
        if new_target < self.replay_target.get() {
            self.replay_buffer.clear();
        }
        self.replay_target.set(new_target)
    }

    pub async fn is_in_skipped_region(&self, oplog_index: OplogIndex) -> bool {
        let internal = self.internal.read().await;
        internal.skipped_regions.is_in_deleted_region(oplog_index)
    }

    /// Returns whether we are in live mode where we are executing new calls.
    pub fn is_live(&self) -> bool {
        self.last_replayed_index.get() == self.replay_target.get()
    }

    /// Returns whether we are in replay mode where we are replaying old calls.
    pub fn is_replay(&self) -> bool {
        !self.is_live()
    }

    async fn record_replay_event(&mut self, event: ReplayEvent) {
        self.internal
            .write()
            .await
            .pending_replay_events
            .push(event)
    }

    pub async fn take_new_replay_events(&mut self) -> Vec<ReplayEvent> {
        std::mem::take(&mut self.internal.write().await.pending_replay_events)
    }

    /// Reads the next oplog entry, and skips every hint entry following it.
    /// Returns the oplog index of the entry read, no matter how many more hint entries
    /// were read.
    ///
    /// Returns an error if the underlying read fails (e.g. missing oplog entry,
    /// corrupted GolemApiFork payload) so the worker can fail the agent with a
    /// non-retriable trap rather than panicking the executor.
    pub async fn get_oplog_entry(
        &mut self,
    ) -> Result<(OplogIndex, OplogEntry), WorkerExecutorError> {
        // The closure always returns true, so the outer Option is always Some(...)
        // when the underlying read succeeds.
        Ok(self
            .try_get_oplog_entry(|_| true)
            .await?
            .expect("try_get_oplog_entry with always-true predicate must return Some"))
    }

    /// Checks whether the currently read `entry` is a hint entry is valid for replay, or
    /// if a new oplog index should be tried instead.
    ///
    /// For hint entries, the next tried oplog index is the next one. When reaching
    /// persist-nothing zones, it points to the end of the zone.
    ///
    /// If the entry is a hint entry, the result is `Some` and contains the current last
    /// read index, so the next read will get the next one.
    /// If the entry is the beginning of a persist-nothing zone, the result will be `Some`
    /// containing the _end_ of the zone so the next read will get the first entry outside
    /// the zone.
    /// If the entry is not a hint entry the result is `None`.
    ///
    async fn should_skip_to(&self, entry: &OplogEntry) -> Option<OplogIndex> {
        if entry.is_hint() {
            // Keeping the last replayed index as-is, so the next attempt will read the next one
            Some(self.last_replayed_index())
        } else if let OplogEntry::ChangePersistenceLevel {
            persistence_level, ..
        } = &entry
        {
            if persistence_level == &PersistenceLevel::PersistNothing {
                let begin_index = self.last_replayed_index();
                let end_index = self
                    .lookup_oplog_entry(begin_index, |entry, _idx| match entry {
                        OplogEntry::ChangePersistenceLevel {
                            persistence_level, ..
                        } => persistence_level != &PersistenceLevel::PersistNothing,
                        OplogEntry::AgentInvocationFinished { .. } => true,
                        _ => false,
                    })
                    .await;

                if let Some(end_index) = end_index {
                    Some(end_index)
                } else {
                    // The zone has not been closed
                    Some(self.replay_target())
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Reads the next oplog entry, and if it matches the given condition, skips
    /// every hint entry following it and returns the oplog index of the entry read.
    /// If the condition is not met, returns None and the current replay state remains
    /// unchanged.
    ///
    /// The auto-skipped hint entries can be of two kind:
    /// - A set of oplog entry cases are always hint entries. They manipulate the worker status
    ///   but are non-deterministic from the replay's point of view.
    /// - Every oplog entry recorded in persist-nothing zones. These are there for observability,
    ///   but they never participate in the replay. A persist-nothing zone is bounded by two
    ///   ChangePersistenceLevel entries, or if the closing one is missing, it is up to the end of the
    ///   oplog.
    pub async fn try_get_oplog_entry(
        &mut self,
        condition: impl FnOnce(&OplogEntry) -> bool,
    ) -> Result<Option<(OplogIndex, OplogEntry)>, WorkerExecutorError> {
        let saved_replay_idx = self.last_replayed_index.get();
        let saved_next_skipped_region = {
            let internal = self.internal.read().await;
            internal.next_skipped_region.clone()
        };

        let read_idx = self.last_replayed_index.get().next();
        let entry = self.internal_get_next_oplog_entry().await?;

        // Generic peek trace: every try_get_oplog_entry caller (poll/RPC/HTTP/etc replay
        // matching) goes through here, so this is the single point to see what entry was
        // actually sitting at the replay cursor and whether the caller's predicate accepted
        // it — without needing per-call-site tracing to reconstruct a mismatch. `entry`'s
        // Debug impl is bounded (OplogPayload only prints bytes_len, not raw bytes; see
        // payload::mod.rs), so this is safe to leave at trace level unconditionally.
        let matched = condition(&entry);
        trace!(
            agent_id = %self.owned_agent_id,
            oplog_index = %read_idx,
            entry = ?entry,
            matched,
            "TRYGET_TRACE try_get_oplog_entry peeked entry"
        );

        if matched {
            self.skip_forward().await?;
            self.last_replayed_non_hint_index.set(read_idx);

            Ok(Some((read_idx, entry)))
        } else {
            self.rewind_replay_buffer(read_idx, entry);
            self.last_replayed_index.set(saved_replay_idx);
            let mut internal = self.internal.write().await;
            internal.next_skipped_region = saved_next_skipped_region;

            Ok(None)
        }
    }

    /// Walks forward consuming (durably, via `try_get_oplog_entry` — never re-visits a consumed
    /// entry) zero or more oplog entries matching `is_stray`, handing each one to `on_stray` for
    /// caller-specific decode+cache. Returns as soon as an entry does NOT match — the caller
    /// handles that entry itself (its own genuine entry, or a real mismatch) via its existing,
    /// unchanged replay path.
    ///
    /// Entry-type-agnostic by design: this is the shared mechanism behind the fix for a class of
    /// bug (`FINDING_B_FIX_DESIGN.md`) where a positional/unconditional replay consumer (poll(),
    /// RPC's future-invoke-result::get(), concurrent HTTP body-stream reads) can find a "stray"
    /// entry recorded for a DIFFERENT concurrently-tracked operation sitting where its own next
    /// entry was expected — because real-world completion order among concurrently in-flight
    /// operations doesn't have to match the guest's replayed structural check order. Callers
    /// decide what "stray" means (via `is_stray`) and what to do with one (via `on_stray`); this
    /// function only knows how to walk forward and consume matches.
    ///
    /// `is_stray` is `FnMut` so a caller may bound the walk with per-scan state if it ever needs
    /// to; `StrayEntryScan`, the only caller, deliberately does not — see its doc comment for
    /// why a per-identity occurrence bound strands entries rather than protecting anything.
    pub async fn consume_stray_entries(
        &mut self,
        mut is_stray: impl FnMut(&OplogEntry) -> bool,
        mut on_stray: impl FnMut(OplogIndex, OplogEntry),
    ) -> Result<(), WorkerExecutorError> {
        loop {
            match self.try_get_oplog_entry(&mut is_stray).await? {
                Some((idx, entry)) => on_stray(idx, entry),
                None => return Ok(()),
            }
        }
    }

    fn rewind_replay_buffer(&mut self, idx: OplogIndex, entry: OplogEntry) {
        if self
            .replay_buffer
            .front()
            .map(|(front_idx, _)| *front_idx != idx)
            .unwrap_or(true)
        {
            self.replay_buffer.push_front((idx, entry));
        }
    }

    async fn skip_forward(&mut self) -> Result<(), WorkerExecutorError> {
        // Skipping hint entries and recording log entries
        let mut logs = HashSet::new();
        while self.is_replay() {
            let saved_replay_idx = self.last_replayed_index.get();
            let saved_next_skipped_region = {
                let internal = self.internal.read().await;
                internal.next_skipped_region.clone()
            };
            let entry = self.internal_get_next_oplog_entry().await?;
            match self.should_skip_to(&entry).await {
                Some(last_read_idx) => {
                    // Recording seen log entries
                    if let OplogEntry::Log {
                        level,
                        context,
                        message,
                        ..
                    } = &entry
                    {
                        let hash = Self::hash_log_entry(*level, context, message);
                        logs.insert(hash);
                    }

                    // Moving the replay pointer. Leaving last_replayed_non_hint_index unchanged, because this is a hint entry.
                    self.last_replayed_index.set(last_read_idx);
                    // TODO: what to do with next_skipped_region if we jumped forward to end of persist-nothing zone?
                }
                None => {
                    // We've found the first non-hint entry after the first read one,
                    // so we move everything back the last position (saved_replay_idx), including
                    // possibly skipped regions.
                    self.rewind_replay_buffer(saved_replay_idx.next(), entry);
                    self.last_replayed_index.set(saved_replay_idx);
                    let mut internal = self.internal.write().await;
                    // TODO: cache the last hint entry to avoid reading it again
                    internal.next_skipped_region = saved_next_skipped_region;
                    break;
                }
            }
        }

        self.has_seen_logs
            .store(!logs.is_empty(), Ordering::Relaxed);
        let mut internal = self.internal.write().await;
        internal.log_hashes = logs;
        Ok(())
    }

    /// Returns true if the given log entry has been seen since the last non-hint oplog entry.
    pub async fn seen_log(&self, level: LogLevel, context: &str, message: &str) -> bool {
        if self.has_seen_logs.load(Ordering::Relaxed) {
            let hash = Self::hash_log_entry(level, context, message);
            let internal = self.internal.read().await;
            internal.log_hashes.contains(&hash)
        } else {
            false
        }
    }

    /// Removes a seen log from the set. If the set becomes empty, `seen_log` becomes a cheap operation
    pub async fn remove_seen_log(&self, level: LogLevel, context: &str, message: &str) {
        let hash = Self::hash_log_entry(level, context, message);
        let mut internal = self.internal.write().await;
        internal.log_hashes.remove(&hash);
        self.has_seen_logs
            .store(!internal.log_hashes.is_empty(), Ordering::Relaxed);
    }

    fn hash_log_entry(level: LogLevel, context: &str, message: &str) -> (u64, u64) {
        let mut hasher = MetroHash128::new();
        hasher.write_u8(level as u8);
        hasher.write(context.as_bytes());
        hasher.write(message.as_bytes());
        hasher.finish128()
    }

    /// Gets the next oplog entry, no matter if it is hint or not, following jumps.
    ///
    /// Returns an error (rather than panicking) if the expected entry is missing
    /// or if the eager `GolemApiFork` payload inspection fails. The caller (and
    /// transitively any host function) propagates the error up so the worker
    /// fails the agent with a non-retriable trap instead of crashing the
    /// executor process.
    async fn internal_get_next_oplog_entry(&mut self) -> Result<OplogEntry, WorkerExecutorError> {
        let read_idx = self.last_replayed_index.get().next();

        while self
            .replay_buffer
            .front()
            .map(|(idx, _)| *idx < read_idx)
            .unwrap_or(false)
        {
            self.replay_buffer.pop_front();
        }

        if self
            .replay_buffer
            .front()
            .map(|(idx, _)| *idx > read_idx)
            .unwrap_or(false)
        {
            self.replay_buffer.clear();
        }

        if self.replay_buffer.is_empty() {
            let remaining = u64::from(self.replay_target.get())
                .saturating_sub(u64::from(read_idx))
                .saturating_add(1);
            self.replay_buffer = self
                .read_oplog(read_idx, remaining.min(REPLAY_READ_CHUNK_SIZE))
                .await
                .into_iter()
                .collect();

            if self
                .replay_buffer
                .front()
                .map(|(idx, _)| *idx != read_idx)
                .unwrap_or(true)
            {
                self.replay_buffer = self.read_oplog(read_idx, 1).await.into_iter().collect();
            }
        }

        let oplog_entry = if let Some((idx, oplog_entry)) = self.replay_buffer.pop_front()
            && idx == read_idx
        {
            oplog_entry
        } else {
            // Use `unexpected_oplog_entry` so the typing survives the wasmtime
            // round-trip and `TrapType::from_error` classifies it as a
            // non-retriable internal error rather than a policy-retriable
            // `Runtime`/`Unknown` failure (retrying replay against the same
            // truncated oplog would just fail again).
            return Err(WorkerExecutorError::unexpected_oplog_entry(
                "next oplog entry to replay",
                format!(
                    "missing oplog entry for {} at index {}; replay target = {}, last replayed non-hint index = {}",
                    self.owned_agent_id,
                    read_idx,
                    self.replay_target.get(),
                    self.last_replayed_non_hint_index.get()
                ),
            ));
        };

        // record side effects that need to be applied at the next opportunity
        if let OplogEntry::SuccessfulUpdate {
            target_revision, ..
        } = oplog_entry
        {
            self.record_replay_event(ReplayEvent::UpdateReplayed {
                new_revision: target_revision,
            })
            .await
        }
        if let OplogEntry::HostCall {
            function_name,
            response,
            ..
        } = &oplog_entry
            && function_name == &HostFunctionName::GolemApiFork
        {
            let response = self
                .oplog
                .download_payload(response.clone())
                .await
                .map_err(|err| {
                    WorkerExecutorError::runtime(format!(
                        "failed to download GolemApiFork oplog payload at index {read_idx}: {err}"
                    ))
                })?;
            let result: HostResponseGolemApiFork =
                if let HostResponse::GolemApiFork(result) = response {
                    result
                } else {
                    return Err(WorkerExecutorError::unexpected_oplog_entry(
                        "HostResponse::GolemApiFork",
                        format!("{response:?}"),
                    ));
                };
            if result.result == Ok(ForkResult::Forked) {
                self.record_replay_event(ReplayEvent::ForkReplayed {
                    new_phantom_id: result.forked_phantom_id,
                })
                .await;
            }
        }

        if read_idx == self.replay_target.get() {
            self.record_replay_event(ReplayEvent::ReplayFinished).await
        }

        self.move_replay_idx(read_idx).await;

        Ok(oplog_entry)
    }

    async fn move_replay_idx(&mut self, new_idx: OplogIndex) {
        self.last_replayed_index.set(new_idx);
        self.get_out_of_skipped_region().await;
        while self
            .replay_buffer
            .front()
            .map(|(idx, _)| *idx <= self.last_replayed_index.get())
            .unwrap_or(false)
        {
            self.replay_buffer.pop_front();
        }
    }

    pub async fn lookup_oplog_entry(
        &self,
        begin_idx: OplogIndex,
        check: impl Fn(&OplogEntry, OplogIndex) -> bool,
    ) -> Option<OplogIndex> {
        match self
            .lookup_oplog_entry_with_condition(begin_idx, check, |_, _| true)
            .await
        {
            OplogEntryLookupResult::Found { index, .. } => Some(index),
            OplogEntryLookupResult::NotFound { .. } => None,
        }
    }

    pub async fn lookup_oplog_entry_with_condition(
        &self,
        begin_idx: OplogIndex,
        end_check: impl Fn(&OplogEntry, OplogIndex) -> bool,
        for_all_intermediate: impl Fn(&OplogEntry, OplogIndex) -> bool,
    ) -> OplogEntryLookupResult {
        self.lookup_oplog_entry_with_condition_and_state(
            begin_idx,
            |entry, idx, ()| end_check(entry, idx),
            |entry, idx, ()| for_all_intermediate(entry, idx),
            (),
            |_, _, ()| {},
        )
        .await
    }

    pub async fn lookup_oplog_entry_with_condition_and_state<State>(
        &self,
        begin_idx: OplogIndex,
        end_check: impl Fn(&OplogEntry, OplogIndex, &State) -> bool,
        for_all_intermediate: impl Fn(&OplogEntry, OplogIndex, &State) -> bool,
        mut state: State,
        mut update_state: impl FnMut(&OplogEntry, OplogIndex, &mut State),
    ) -> OplogEntryLookupResult {
        let replay_target = self.replay_target.get();
        let mut start = self.last_replayed_index.get().next();

        let mut current_next_skip_region = self.internal.read().await.next_skipped_region.clone();
        let mut violation = false;

        while start < replay_target {
            let entries = self.read_oplog(start, REPLAY_READ_CHUNK_SIZE).await;
            for (idx, entry) in &entries {
                if current_next_skip_region
                    .as_ref()
                    .map(|r| r.contains(*idx))
                    .unwrap_or(false)
                {
                    // If we are in the current skip region, ignore the entry
                    continue;
                }
                if current_next_skip_region
                    .as_ref()
                    .map(|r| &r.end == idx)
                    .unwrap_or(false)
                {
                    // if we are at the end of the current skip region, find the next one
                    current_next_skip_region = self
                        .internal
                        .read()
                        .await
                        .skipped_regions
                        .find_next_deleted_region(idx.next());
                }

                update_state(entry, *idx, &mut state);

                if end_check(entry, begin_idx, &state) {
                    return OplogEntryLookupResult::Found {
                        index: *idx,
                        entry: Box::new(entry.clone()),
                        violates_for_all: violation,
                    };
                }

                if !for_all_intermediate(entry, begin_idx, &state) {
                    violation = true;
                }
            }
            start = start.range_end(entries.len() as u64).next();
        }

        OplogEntryLookupResult::NotFound {
            violates_for_all: violation,
        }
    }

    pub async fn get_oplog_entry_agent_invocation_started(
        &mut self,
    ) -> Result<Option<AgentInvocationStartedEntry>, WorkerExecutorError> {
        loop {
            if self.is_replay() {
                let (_, oplog_entry) = self.get_oplog_entry().await?;
                match oplog_entry {
                    OplogEntry::AgentInvocationStarted {
                        idempotency_key,
                        payload,
                        trace_id,
                        trace_states,
                        invocation_context: spans,
                        ..
                    } => {
                        let invocation_payload =
                            self.oplog.download_payload(payload).await.map_err(|err| {
                                WorkerExecutorError::runtime(format!(
                                    "failed to deserialize agent invocation payload: {err}"
                                ))
                            })?;

                        let invocation_context =
                            InvocationContextStack::from_oplog_data(trace_id, trace_states, spans);

                        break Ok(Some(AgentInvocationStartedEntry {
                            idempotency_key,
                            invocation_payload,
                            invocation_context,
                        }));
                    }
                    entry if entry.is_hint() => {}
                    _ => {
                        break Err(WorkerExecutorError::unexpected_oplog_entry(
                            "AgentInvocationStarted",
                            format!("{oplog_entry:?}"),
                        ));
                    }
                }
            } else {
                break Ok(None);
            }
        }
    }

    pub async fn get_oplog_entry_agent_invocation_finished(
        &mut self,
    ) -> Result<Option<AgentInvocationResult>, WorkerExecutorError> {
        loop {
            if self.is_replay() {
                let (_, oplog_entry) = self.get_oplog_entry().await?;
                match oplog_entry {
                    OplogEntry::AgentInvocationFinished { result, .. } => {
                        let result: AgentInvocationResult =
                            self.oplog.download_payload(result).await.map_err(|err| {
                                WorkerExecutorError::runtime(format!(
                                    "failed to deserialize agent invocation result payload: {err}"
                                ))
                            })?;

                        break Ok(Some(result));
                    }
                    entry if entry.is_hint() => {}
                    _ => {
                        break Err(WorkerExecutorError::unexpected_oplog_entry(
                            "AgentInvocationFinished",
                            format!("{oplog_entry:?}"),
                        ));
                    }
                }
            } else {
                break Ok(None);
            }
        }
    }

    async fn get_out_of_skipped_region(&mut self) {
        if self.is_replay() {
            let mut internal = self.internal.write().await;
            let update_next_skipped_region = match &internal.next_skipped_region {
                Some(region) if region.start == (self.last_replayed_index.get().next()) => {
                    let target = region.end.next(); // we want to continue reading _after_ the region
                    debug!(
                        "Worker reached skipped region at {}, jumping to {} (oplog size: {})",
                        region.start,
                        target,
                        self.replay_target.get()
                    );
                    self.last_replayed_index.set(target.previous()); // so we set the last replayed index to the end of the region

                    true
                }
                _ => false,
            };

            if update_next_skipped_region {
                internal.next_skipped_region = internal
                    .skipped_regions
                    .find_next_deleted_region(self.last_replayed_index.get());
            }
        }
    }

    async fn read_oplog(&self, idx: OplogIndex, n: u64) -> Vec<(OplogIndex, OplogEntry)> {
        let result: Vec<(OplogIndex, OplogEntry)> =
            self.oplog.read_many(idx, n).await.into_iter().collect();
        result
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum OplogEntryLookupResult {
    Found {
        index: OplogIndex,
        entry: Box<OplogEntry>,
        violates_for_all: bool,
    },
    NotFound {
        violates_for_all: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable_host::{
        IdentityNamespace, PreResolvedStrayCache, StrayEntryIdentity, StrayEntryScan,
        TrackedConcurrentOpSeqs, is_own_invoke_result_entry, is_own_poll_entry,
        stray_entry_identity,
    };
    use crate::services::oplog::CommitLevel;
    use async_trait::async_trait;
    use golem_common::model::component::ComponentId;
    use golem_common::model::environment::EnvironmentId;
    use golem_common::model::oplog::{DurableFunctionType, PayloadId, RawOplogPayload};
    use golem_common::model::{AgentId, Timestamp};
    use std::collections::{BTreeMap, HashSet};
    use std::fmt::{Debug, Formatter};
    use std::sync::Mutex;
    use std::time::Duration;
    use test_r::test;

    struct SparseBatchOplog;

    struct MutableBatchOplog {
        entries: Mutex<BTreeMap<OplogIndex, OplogEntry>>,
    }

    impl MutableBatchOplog {
        fn new(entries: BTreeMap<OplogIndex, OplogEntry>) -> Self {
            Self {
                entries: Mutex::new(entries),
            }
        }

        fn replace(&self, index: OplogIndex, entry: OplogEntry) {
            self.entries.lock().unwrap().insert(index, entry);
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

        async fn wait_for_replicas(&self, _replicas: u8, _timeout: Duration) -> bool {
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

    impl Debug for SparseBatchOplog {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SparseBatchOplog").finish()
        }
    }

    #[async_trait]
    impl Oplog for SparseBatchOplog {
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
            OplogIndex::from_u64(3)
        }

        async fn last_added_non_hint_entry(&self) -> Option<OplogIndex> {
            None
        }

        async fn wait_for_replicas(&self, _replicas: u8, _timeout: Duration) -> bool {
            unimplemented!()
        }

        async fn read(&self, _oplog_index: OplogIndex) -> OplogEntry {
            OplogEntry::NoOp {
                timestamp: Timestamp::now_utc(),
            }
        }

        async fn read_many(
            &self,
            oplog_index: OplogIndex,
            n: u64,
        ) -> BTreeMap<OplogIndex, OplogEntry> {
            let entry = OplogEntry::NoOp {
                timestamp: Timestamp::now_utc(),
            };
            if n == 1 || oplog_index == OplogIndex::INITIAL.next() {
                BTreeMap::from([(oplog_index, entry)])
            } else {
                BTreeMap::from([(oplog_index.next(), entry)])
            }
        }

        async fn length(&self) -> u64 {
            3
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

    #[test]
    async fn replay_reads_sparse_batch_entries_individually() {
        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "test".to_string(),
        };
        let mut state = ReplayState::new(
            OwnedAgentId::new(EnvironmentId::new(), &agent_id),
            Arc::new(SparseBatchOplog),
            DeletedRegions::new(),
        )
        .await
        .unwrap();

        assert!(matches!(
            state.get_oplog_entry().await.unwrap().1,
            OplogEntry::NoOp { .. }
        ));
        assert!(matches!(
            state.get_oplog_entry().await.unwrap().1,
            OplogEntry::NoOp { .. }
        ));
    }

    /// Verifies the predicate used in `poll()`'s rep-mismatch fallback (our fix):
    /// when the next oplog entry is `IoPollReady`, `try_get_oplog_entry` should
    /// consume it and return `Some`.
    #[test]
    async fn try_get_oplog_entry_matches_io_poll_ready() {
        use golem_common::model::oplog::{
            DurableFunctionType, HostRequest, HostRequestNoInput, HostResponse,
            HostResponsePollReady, OplogPayload,
        };
        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "test".to_string(),
        };
        let io_poll_ready = OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::IoPollReady,
            request: OplogPayload::Inline(Box::new(HostRequest::NoInput(HostRequestNoInput {}))),
            response: OplogPayload::Inline(Box::new(HostResponse::PollReady(
                HostResponsePollReady { result: Ok(true) },
            ))),
            durable_function_type: DurableFunctionType::ReadLocalPollable(42),
        };
        // ReplayState::new() processes OplogIndex::INITIAL; the first readable
        // entry for try_get_oplog_entry is at INITIAL.next().
        let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
            (
                OplogIndex::INITIAL,
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                },
            ),
            (OplogIndex::INITIAL.next(), io_poll_ready),
        ])));
        let mut state = ReplayState::new(
            OwnedAgentId::new(EnvironmentId::new(), &agent_id),
            oplog,
            DeletedRegions::new(),
        )
        .await
        .unwrap();

        let matched = state
            .try_get_oplog_entry(|e| {
                matches!(
                    e,
                    OplogEntry::HostCall {
                        function_name: HostFunctionName::IoPollReady,
                        ..
                    }
                )
            })
            .await
            .unwrap();

        assert!(
            matched.is_some(),
            "predicate should match IoPollReady and consume the entry"
        );
    }

    /// Verifies the predicate used in `poll()`'s rep-mismatch fallback does NOT
    /// consume an `IoPollPoll` entry (the normal case handled by `durability.replay()`).
    #[test]
    async fn try_get_oplog_entry_does_not_match_io_poll_poll() {
        use golem_common::model::oplog::{
            DurableFunctionType, HostRequest, HostRequestPollCount, HostResponse,
            HostResponsePollResult, OplogPayload,
        };
        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "test".to_string(),
        };
        let io_poll_poll = OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::IoPollPoll,
            request: OplogPayload::Inline(Box::new(HostRequest::PollCount(HostRequestPollCount {
                count: 1,
            }))),
            response: OplogPayload::Inline(Box::new(HostResponse::PollResult(
                HostResponsePollResult {
                    result: Ok(vec![0]),
                },
            ))),
            durable_function_type: DurableFunctionType::ReadLocal,
        };
        let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
            (
                OplogIndex::INITIAL,
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                },
            ),
            (OplogIndex::INITIAL.next(), io_poll_poll),
        ])));
        let mut state = ReplayState::new(
            OwnedAgentId::new(EnvironmentId::new(), &agent_id),
            oplog,
            DeletedRegions::new(),
        )
        .await
        .unwrap();

        let matched = state
            .try_get_oplog_entry(|e| {
                matches!(
                    e,
                    OplogEntry::HostCall {
                        function_name: HostFunctionName::IoPollReady,
                        ..
                    }
                )
            })
            .await
            .unwrap();

        assert!(
            matched.is_none(),
            "predicate must not consume IoPollPoll when looking for IoPollReady"
        );
    }

    #[test]
    async fn lowering_replay_target_discards_prefetched_future_entries() {
        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "test".to_string(),
        };
        let original = OplogEntry::NoOp {
            timestamp: Timestamp::from(1),
        };
        let replacement = OplogEntry::NoOp {
            timestamp: Timestamp::from(2),
        };
        let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
            (OplogIndex::INITIAL, original.clone()),
            (OplogIndex::INITIAL.next(), original.clone()),
            (OplogIndex::INITIAL.next().next(), original.clone()),
        ])));
        let mut state = ReplayState::new(
            OwnedAgentId::new(EnvironmentId::new(), &agent_id),
            oplog.clone(),
            DeletedRegions::new(),
        )
        .await
        .unwrap();

        state.set_replay_target(OplogIndex::INITIAL.next());
        state.get_oplog_entry().await.unwrap();
        oplog.replace(OplogIndex::INITIAL.next().next(), replacement.clone());
        state.set_replay_target(OplogIndex::INITIAL.next().next());

        assert_eq!(
            state.get_oplog_entry().await.unwrap().1,
            replacement,
            "replay must not consume entries prefetched before its target moved backward"
        );
    }

    /// Regression test for: "Unexpected oplog entry during replay: expected io::poll::poll,
    /// got http::types::outgoing_body_stream::check_write"
    ///
    /// Pathology: a periodic snapshot is taken mid-HTTP-batch (between BeginRemoteWrite and
    /// EndRemoteWrite). On restore, `open_http_requests` and `replaying_http_batch` are both
    /// gone (runtime state not persisted). The next oplog entry is `CheckWrite` (from the
    /// original HTTP call). Without the fix, `check_write()` falls through to the plain-WASI
    /// branch (no oplog access), stalling the cursor. The subsequent `poll()` then reads the
    /// `CheckWrite` entry expecting `IoPollPoll` → permanent WASM trap.
    ///
    /// The fix peeks with `try_get_oplog_entry` and consumes the `CheckWrite` entry, keeping
    /// the cursor in sync. This test verifies that `try_get_oplog_entry` correctly matches and
    /// consumes `HttpTypesOutgoingBodyStreamCheckWrite` entries.
    #[test]
    async fn try_get_oplog_entry_matches_http_outgoing_check_write() {
        use golem_common::model::oplog::host_functions::HostFunctionName;
        use golem_common::model::oplog::types::SerializableHttpMethod;
        use golem_common::model::oplog::{
            DurableFunctionType, HostRequest, HostRequestHttpRequest, HostResponse,
            HostResponseStreamCheckWrite, OplogPayload,
        };
        use std::collections::HashMap;

        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "test".to_string(),
        };

        let check_write_entry = OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::HttpTypesOutgoingBodyStreamCheckWrite,
            request: OplogPayload::Inline(Box::new(HostRequest::HttpRequest(
                HostRequestHttpRequest {
                    uri: "https://api.example.com/".to_string(),
                    method: SerializableHttpMethod::Post,
                    headers: HashMap::new(),
                },
            ))),
            response: OplogPayload::Inline(Box::new(HostResponse::StreamCheckWrite(
                HostResponseStreamCheckWrite { result: Ok(8192) },
            ))),
            durable_function_type: DurableFunctionType::WriteRemoteBatched(Some(
                OplogIndex::INITIAL,
            )),
        };

        let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
            (
                OplogIndex::INITIAL,
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                },
            ),
            (OplogIndex::INITIAL.next(), check_write_entry),
        ])));

        let mut state = ReplayState::new(
            OwnedAgentId::new(EnvironmentId::new(), &agent_id),
            oplog,
            DeletedRegions::new(),
        )
        .await
        .unwrap();

        // This predicate mirrors the fix in check_write()'s else-branch.
        let matched = state
            .try_get_oplog_entry(|entry| {
                matches!(
                    entry,
                    OplogEntry::HostCall {
                        function_name: HostFunctionName::HttpTypesOutgoingBodyStreamCheckWrite,
                        ..
                    }
                )
            })
            .await
            .unwrap();

        assert!(
            matched.is_some(),
            "post-snapshot-restore fix must consume the CheckWrite entry to unblock poll()"
        );
    }

    /// Companion to `try_get_oplog_entry_matches_http_outgoing_check_write`: verifies that
    /// `try_get_oplog_entry` does NOT consume an `IoPollPoll` entry when looking for a
    /// `CheckWrite`. After the fix consumes a CheckWrite, the next entry (`IoPollPoll`) must
    /// remain unconsumed for `poll()`'s normal replay path.
    #[test]
    async fn try_get_oplog_entry_does_not_match_io_poll_poll_for_check_write_predicate() {
        use golem_common::model::oplog::host_functions::HostFunctionName;
        use golem_common::model::oplog::{
            DurableFunctionType, HostRequest, HostRequestPollCount, HostResponse,
            HostResponsePollResult, OplogPayload,
        };

        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "test".to_string(),
        };

        let poll_entry = OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::IoPollPoll,
            request: OplogPayload::Inline(Box::new(HostRequest::PollCount(HostRequestPollCount {
                count: 1,
            }))),
            response: OplogPayload::Inline(Box::new(HostResponse::PollResult(
                HostResponsePollResult {
                    result: Ok(vec![0]),
                },
            ))),
            durable_function_type: DurableFunctionType::ReadLocal,
        };

        let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
            (
                OplogIndex::INITIAL,
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                },
            ),
            (OplogIndex::INITIAL.next(), poll_entry),
        ])));

        let mut state = ReplayState::new(
            OwnedAgentId::new(EnvironmentId::new(), &agent_id),
            oplog,
            DeletedRegions::new(),
        )
        .await
        .unwrap();

        // The check_write fix predicate must not consume an IoPollPoll entry.
        let matched = state
            .try_get_oplog_entry(|entry| {
                matches!(
                    entry,
                    OplogEntry::HostCall {
                        function_name: HostFunctionName::HttpTypesOutgoingBodyStreamCheckWrite,
                        ..
                    }
                )
            })
            .await
            .unwrap();

        assert!(
            matched.is_none(),
            "CheckWrite predicate must not consume IoPollPoll — that would break the poll() replay path"
        );
    }

    /// Empirical gate for the "remove rep-matching, use wildcard/positional matching"
    /// proposal (GOLEM_IO_POLL_BUG.md "Third Bug" resolution): reproduces Bug #2
    /// (cross-pollable IoPollReady theft) to prove wildcard matching is NOT safe given
    /// `try_get_oplog_entry`'s actual semantics — it only peeks the IMMEDIATE next entry
    /// and never searches forward, so an entry recorded for pollable B (rep=99) sitting at
    /// the front of the buffer WILL be wrongly consumed by pollable A (rep=42) under a
    /// wildcard (type-only) predicate, exactly as it was before the rep-tagging fix.
    ///
    /// Scenario: live execution called `P_timer.ready()` (false, unrecorded — Bug #1
    /// optimization) then `P_stream.ready()` (true, recorded as `ReadLocalPollable(99)`).
    /// On replay, `P_timer.ready()` (rep=42) is called first, in the same deterministic
    /// order. The immediate-next oplog entry is `P_stream`'s recorded `true` entry — there
    /// is nothing else in front of it, because the `false` call was never recorded.
    #[test]
    async fn wildcard_matching_would_reproduce_bug2_pollable_theft() {
        use golem_common::model::oplog::{
            DurableFunctionType, HostRequest, HostRequestNoInput, HostResponse,
            HostResponsePollReady, OplogPayload,
        };
        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "test".to_string(),
        };
        const P_STREAM_REP: u32 = 99;
        const P_TIMER_REP: u32 = 42;

        // Only P_stream's `true` result was ever recorded — P_timer's `false` was skipped
        // (Bug #1 optimization), so there is no entry for it at all.
        let io_poll_ready_for_p_stream = OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::IoPollReady,
            request: OplogPayload::Inline(Box::new(HostRequest::NoInput(HostRequestNoInput {}))),
            response: OplogPayload::Inline(Box::new(HostResponse::PollReady(
                HostResponsePollReady { result: Ok(true) },
            ))),
            durable_function_type: DurableFunctionType::ReadLocalPollable(P_STREAM_REP),
        };
        let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
            (
                OplogIndex::INITIAL,
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                },
            ),
            (OplogIndex::INITIAL.next(), io_poll_ready_for_p_stream),
        ])));
        let mut state = ReplayState::new(
            OwnedAgentId::new(EnvironmentId::new(), &agent_id),
            oplog,
            DeletedRegions::new(),
        )
        .await
        .unwrap();

        // P_timer.ready() replays first (deterministic call order matches live). Under a
        // WILDCARD predicate (type-only, no rep check — the Bug-3-doc proposal), this steals
        // P_stream's entry.
        let wildcard_matched = state
            .try_get_oplog_entry(|entry| {
                matches!(
                    entry,
                    OplogEntry::HostCall {
                        function_name: HostFunctionName::IoPollReady,
                        ..
                    }
                )
            })
            .await
            .unwrap();

        assert!(
            wildcard_matched.is_some(),
            "BUG-2 REPRODUCED: wildcard matching consumed P_stream's entry on P_timer's call \
             (rep={P_TIMER_REP}), even though it was recorded for a different pollable \
             (rep={P_STREAM_REP}). try_get_oplog_entry only peeks the immediate-next entry \
             and never searches forward, so positional/wildcard matching is NOT safe once \
             Bug #1's skip-false optimization means some calls leave no entry at all. \
             Removing rep-matching (GOLEM_IO_POLL_BUG.md's 'Third Bug' resolution) would \
             reintroduce this exact bug — do not implement it as literally proposed."
        );
    }

    /// Proves the exact mechanism `durable_host/mod.rs` (`PrivateDurableWorkerState::new`,
    /// ~line 4162) uses for snapshot-based recovery: entries from `INITIAL.next()` through
    /// `last_snapshot_index` are placed in a `DeletedRegions` override and are NEVER visited
    /// by replay — reading resumes directly at `last_snapshot_index + 1`.
    ///
    /// This is the empirical basis for the seq-based pollable identity design: since entries
    /// 1..=snapshot_idx (which would include any PRE-snapshot pollable-creation entries) are
    /// structurally unreachable after a snapshot-based resume, a "first-seen-this-session"
    /// sequence counter (reset fresh on every resume, live or replay) can never collide with
    /// or need to continue from a value assigned before the snapshot — there is nothing on
    /// the other side of the skip to be consistent with, PROVIDED no pollable created before
    /// the snapshot can still be in-flight after it (verified separately: both snapshot
    /// triggers — `on_external_invocation_completed` and the `Periodic` policy check in
    /// `invocation_loop.rs` — only ever fire between fully-completed external invocations,
    /// i.e. with an empty WASM call stack and therefore no in-flight pollable).
    #[test]
    async fn snapshot_recovery_skip_region_is_never_replayed() {
        use golem_common::model::oplog::{
            DurableFunctionType, HostRequest, HostRequestNoInput, HostResponse,
            HostResponsePollReady, OplogPayload,
        };
        use golem_common::model::regions::DeletedRegionsBuilder;

        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "test".to_string(),
        };

        // Entries 1..=3 simulate a completed pre-snapshot invocation (including a pollable
        // creation + IoPollReady entry that must NEVER be visited post-restore). Entry 4 is
        // where a snapshot was taken (last_snapshot_index = 4). Entry 5 simulates a fresh
        // pollable creation entry in the invocation that runs after the restore.
        let pre_snapshot_ready = OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::IoPollReady,
            request: OplogPayload::Inline(Box::new(HostRequest::NoInput(HostRequestNoInput {}))),
            response: OplogPayload::Inline(Box::new(HostResponse::PollReady(
                HostResponsePollReady { result: Ok(true) },
            ))),
            durable_function_type: DurableFunctionType::ReadLocalPollable(7),
        };
        let post_snapshot_ready = OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::IoPollReady,
            request: OplogPayload::Inline(Box::new(HostRequest::NoInput(HostRequestNoInput {}))),
            response: OplogPayload::Inline(Box::new(HostResponse::PollReady(
                HostResponsePollReady { result: Ok(true) },
            ))),
            durable_function_type: DurableFunctionType::ReadLocalPollable(0), // seq=0, first-seen this session
        };
        let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
            (
                OplogIndex::INITIAL,
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                },
            ),
            (
                OplogIndex::from_u64(2),
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                },
            ),
            (OplogIndex::from_u64(3), pre_snapshot_ready),
            (
                OplogIndex::from_u64(4),
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                }, // stand-in for the Snapshot marker entry itself
            ),
            (OplogIndex::from_u64(5), post_snapshot_ready),
        ])));

        let last_snapshot_index = OplogIndex::from_u64(4);
        let skipped_regions =
            DeletedRegionsBuilder::from_regions(vec![OplogRegion::from_index_range(
                OplogIndex::INITIAL.next()..=last_snapshot_index,
            )])
            .build();

        let mut state = ReplayState::new(
            OwnedAgentId::new(EnvironmentId::new(), &agent_id),
            oplog,
            skipped_regions,
        )
        .await
        .unwrap();

        // First read after construction must land on entry 5 (seq=0 post-snapshot pollable),
        // NEVER on entry 3 (the pre-snapshot pollable at seq=7, which — if the fresh
        // this-session counter reassigned seq=0 to some new pollable — must not be
        // reachable/confusable with it).
        let (index, entry) = state.get_oplog_entry().await.unwrap();
        assert_eq!(
            index,
            OplogIndex::from_u64(5),
            "replay after snapshot-based restore must resume at last_snapshot_index + 1, \
             skipping entries 1..=4 entirely"
        );
        assert!(
            matches!(
                entry,
                OplogEntry::HostCall {
                    durable_function_type: DurableFunctionType::ReadLocalPollable(0),
                    ..
                }
            ),
            "the post-snapshot entry (seq=0, freshly assigned this session) must be the one \
             read — the pre-snapshot entry (seq=7) must never be visited, proving a \
             first-seen-this-session counter cannot collide with pre-snapshot assignments"
        );
    }

    /// Mirrors `wildcard_matching_would_reproduce_bug2_pollable_theft`, but exercises the
    /// ACTUAL fixed predicate shape used in `poll.rs::ready()` post-fix: match on
    /// `ReadLocalPollable(seq)` with `seq` equality, where `seq` is a call-order-derived
    /// logical id (as `PrivateDurableWorkerState::pollable_seq` assigns), NOT the raw
    /// wasmtime rep. Confirms this correctly avoids Bug #2: P_timer's replay call (seq=0,
    /// assigned first — mirroring live's assignment order) must NOT consume P_stream's
    /// entry (seq=1), and P_stream's own later call (seq=1) must consume it correctly.
    #[test]
    async fn seq_based_matching_resolves_bug2_correctly() {
        use golem_common::model::oplog::{
            DurableFunctionType, HostRequest, HostRequestNoInput, HostResponse,
            HostResponsePollReady, OplogPayload,
        };
        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "test".to_string(),
        };
        const P_TIMER_SEQ: u32 = 0; // first pollable observed (live and replay alike)
        const P_STREAM_SEQ: u32 = 1; // second pollable observed

        // Only P_stream's `true` was recorded (seq=1) — P_timer's `false` (seq=0) was skipped.
        let io_poll_ready_for_p_stream = OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::IoPollReady,
            request: OplogPayload::Inline(Box::new(HostRequest::NoInput(HostRequestNoInput {}))),
            response: OplogPayload::Inline(Box::new(HostResponse::PollReady(
                HostResponsePollReady { result: Ok(true) },
            ))),
            durable_function_type: DurableFunctionType::ReadLocalPollable(P_STREAM_SEQ),
        };
        let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
            (
                OplogIndex::INITIAL,
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                },
            ),
            (OplogIndex::INITIAL.next(), io_poll_ready_for_p_stream),
        ])));
        let mut state = ReplayState::new(
            OwnedAgentId::new(EnvironmentId::new(), &agent_id),
            oplog,
            DeletedRegions::new(),
        )
        .await
        .unwrap();

        // P_timer.ready() replays first (seq=0) — the exact predicate shape from poll.rs.
        let timer_matched = state
            .try_get_oplog_entry(|entry| match entry {
                OplogEntry::HostCall {
                    function_name: HostFunctionName::IoPollReady,
                    durable_function_type: DurableFunctionType::ReadLocalPollable(seq),
                    ..
                } => *seq == P_TIMER_SEQ,
                _ => false,
            })
            .await
            .unwrap();
        assert!(
            timer_matched.is_none(),
            "seq-based matching must NOT let P_timer (seq=0) steal P_stream's entry (seq=1)"
        );

        // P_stream.ready() (seq=1) must now correctly consume its own entry.
        let stream_matched = state
            .try_get_oplog_entry(|entry| match entry {
                OplogEntry::HostCall {
                    function_name: HostFunctionName::IoPollReady,
                    durable_function_type: DurableFunctionType::ReadLocalPollable(seq),
                    ..
                } => *seq == P_STREAM_SEQ,
                _ => false,
            })
            .await
            .unwrap();
        assert!(
            stream_matched.is_some(),
            "P_stream (seq=1) must correctly consume its own entry once P_timer's (non-matching) \
             peek has rewound the buffer"
        );
    }

    /// N=4 concurrent-pollable scenario matching the production `scene_plates` trap (4
    /// concurrently-dispatched `wf_krea2_base_realism` renders, each with its own
    /// createPromise()/awaitPromise()-style poll loop). Only pollables 1 and 3 ever return
    /// `true` (recorded); pollables 0 and 2 return `false` repeatedly (never recorded) before
    /// eventually also returning `true`. Verifies seq-based matching resolves every `ready()`
    /// call to the correct pollable regardless of how many times each is polled first.
    #[test]
    async fn seq_based_matching_resolves_four_way_concurrent_pollables() {
        use golem_common::model::oplog::{
            DurableFunctionType, HostRequest, HostRequestNoInput, HostResponse,
            HostResponsePollReady, OplogPayload,
        };
        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "test".to_string(),
        };

        fn ready_entry(seq: u32) -> OplogEntry {
            OplogEntry::HostCall {
                timestamp: Timestamp::now_utc(),
                function_name: HostFunctionName::IoPollReady,
                request: OplogPayload::Inline(Box::new(HostRequest::NoInput(
                    HostRequestNoInput {},
                ))),
                response: OplogPayload::Inline(Box::new(HostResponse::PollReady(
                    HostResponsePollReady { result: Ok(true) },
                ))),
                durable_function_type: DurableFunctionType::ReadLocalPollable(seq),
            }
        }

        // Recording order as it happened live: pollable 1 becomes ready first, then pollable 3.
        // Pollables 0 and 2 never recorded anything (always false so far).
        let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
            (
                OplogIndex::INITIAL,
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                },
            ),
            (OplogIndex::INITIAL.next(), ready_entry(1)),
            (OplogIndex::from_u64(3), ready_entry(3)),
        ])));
        let mut state = ReplayState::new(
            OwnedAgentId::new(EnvironmentId::new(), &agent_id),
            oplog,
            DeletedRegions::new(),
        )
        .await
        .unwrap();

        let matches_seq = |seq: u32| {
            move |entry: &OplogEntry| match entry {
                OplogEntry::HostCall {
                    function_name: HostFunctionName::IoPollReady,
                    durable_function_type: DurableFunctionType::ReadLocalPollable(s),
                    ..
                } => *s == seq,
                _ => false,
            }
        };

        // Replay polls all 4 pollables in round-robin order (0,1,2,3), same as live.
        assert!(
            state
                .try_get_oplog_entry(matches_seq(0))
                .await
                .unwrap()
                .is_none(),
            "pollable 0 (not yet ready) must not steal pollable 1's entry"
        );
        assert!(
            state
                .try_get_oplog_entry(matches_seq(1))
                .await
                .unwrap()
                .is_some(),
            "pollable 1 must consume its own entry"
        );
        assert!(
            state
                .try_get_oplog_entry(matches_seq(2))
                .await
                .unwrap()
                .is_none(),
            "pollable 2 (not yet ready) must not steal pollable 3's entry"
        );
        assert!(
            state
                .try_get_oplog_entry(matches_seq(3))
                .await
                .unwrap()
                .is_some(),
            "pollable 3 must consume its own entry, now that it's the immediate-next one"
        );
    }

    /// Mixed ready()/poll() scenario: an `IoPollReady` entry for one pollable must never be
    /// wrongly consumed by a `poll()` call expecting `IoPollPoll` (and vice versa), even when
    /// they're adjacent in the oplog — the type check alone (not just the seq check) must gate
    /// this, matching poll.rs's post-fix behavior where poll()'s replay path no longer has any
    /// IoPollReady-consuming fallback.
    #[test]
    async fn mixed_ready_and_poll_entries_do_not_cross_match() {
        use golem_common::model::oplog::{
            DurableFunctionType, HostRequest, HostRequestNoInput, HostRequestPollCount,
            HostResponse, HostResponsePollReady, HostResponsePollResult, OplogPayload,
        };
        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "test".to_string(),
        };
        let io_poll_ready = OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::IoPollReady,
            request: OplogPayload::Inline(Box::new(HostRequest::NoInput(HostRequestNoInput {}))),
            response: OplogPayload::Inline(Box::new(HostResponse::PollReady(
                HostResponsePollReady { result: Ok(true) },
            ))),
            durable_function_type: DurableFunctionType::ReadLocalPollable(0),
        };
        let io_poll_poll = OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::IoPollPoll,
            request: OplogPayload::Inline(Box::new(HostRequest::PollCount(HostRequestPollCount {
                count: 1,
            }))),
            response: OplogPayload::Inline(Box::new(HostResponse::PollResult(
                HostResponsePollResult {
                    result: Ok(vec![0]),
                },
            ))),
            durable_function_type: DurableFunctionType::ReadLocal,
        };
        let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
            (
                OplogIndex::INITIAL,
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                },
            ),
            (OplogIndex::INITIAL.next(), io_poll_ready),
            (OplogIndex::from_u64(3), io_poll_poll),
        ])));
        let mut state = ReplayState::new(
            OwnedAgentId::new(EnvironmentId::new(), &agent_id),
            oplog,
            DeletedRegions::new(),
        )
        .await
        .unwrap();

        // poll()'s predicate (IoPollPoll-only) must not consume the IoPollReady entry in front.
        let poll_matched = state
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
            poll_matched.is_none(),
            "poll() must not consume an IoPollReady entry — no fallback exists for this any more"
        );

        // ready()'s predicate (seq=0) correctly consumes it instead.
        let ready_matched = state
            .try_get_oplog_entry(|e| match e {
                OplogEntry::HostCall {
                    function_name: HostFunctionName::IoPollReady,
                    durable_function_type: DurableFunctionType::ReadLocalPollable(seq),
                    ..
                } => *seq == 0,
                _ => false,
            })
            .await
            .unwrap();
        assert!(ready_matched.is_some());

        // Now poll() correctly finds its own entry, next in line.
        let poll_matched2 = state
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
        assert!(poll_matched2.is_some());
    }

    /// `consume_stray_entries` (the generic primitive behind the Finding B fix's widened
    /// mechanism — `FINDING_B_FIX_DESIGN.md` §8.4/§8.7) must walk forward consuming every
    /// matching entry in a row, handing each to the callback, and stop (without consuming) the
    /// moment an entry doesn't match — regardless of what "matching" means, since this
    /// primitive is entirely entry-type-agnostic.
    #[test]
    async fn consume_stray_entries_walks_past_n_matches_then_stops() {
        use golem_common::model::oplog::{
            DurableFunctionType, HostRequest, HostRequestNoInput, HostRequestPollCount,
            HostResponse, HostResponsePollReady, HostResponsePollResult, OplogPayload,
        };
        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "test".to_string(),
        };
        let stray = |seq: u32| OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::IoPollReady,
            request: OplogPayload::Inline(Box::new(HostRequest::NoInput(HostRequestNoInput {}))),
            response: OplogPayload::Inline(Box::new(HostResponse::PollReady(
                HostResponsePollReady { result: Ok(true) },
            ))),
            durable_function_type: DurableFunctionType::ReadLocalPollable(seq),
        };
        let genuine = OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::IoPollPoll,
            request: OplogPayload::Inline(Box::new(HostRequest::PollCount(HostRequestPollCount {
                count: 2,
            }))),
            response: OplogPayload::Inline(Box::new(HostResponse::PollResult(
                HostResponsePollResult {
                    result: Ok(vec![0]),
                },
            ))),
            durable_function_type: DurableFunctionType::ReadLocal,
        };
        let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
            (
                OplogIndex::INITIAL,
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                },
            ),
            (OplogIndex::INITIAL.next(), stray(150)),
            (OplogIndex::INITIAL.next().next(), stray(161)),
            (OplogIndex::INITIAL.next().next().next(), genuine),
        ])));
        let mut state = ReplayState::new(
            OwnedAgentId::new(EnvironmentId::new(), &agent_id),
            oplog,
            DeletedRegions::new(),
        )
        .await
        .unwrap();

        let mut consumed = Vec::new();
        state
            .consume_stray_entries(
                |e| {
                    matches!(
                        e,
                        OplogEntry::HostCall {
                            function_name: HostFunctionName::IoPollReady,
                            ..
                        }
                    )
                },
                |idx, entry| consumed.push((idx, entry)),
            )
            .await
            .unwrap();

        assert_eq!(consumed.len(), 2, "must consume both strays, in order");
        assert!(matches!(
            consumed[0].1,
            OplogEntry::HostCall {
                durable_function_type: DurableFunctionType::ReadLocalPollable(150),
                ..
            }
        ));
        assert!(matches!(
            consumed[1].1,
            OplogEntry::HostCall {
                durable_function_type: DurableFunctionType::ReadLocalPollable(161),
                ..
            }
        ));

        // The genuine entry (not matching the predicate) must be left completely untouched.
        let genuine_found = state
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
            genuine_found.is_some(),
            "the non-matching entry must survive consume_stray_entries untouched"
        );
    }

    /// N=0 case: when the very next entry doesn't match, `consume_stray_entries` must do
    /// nothing and leave it for the caller — the common, unchanged happy path.
    #[test]
    async fn consume_stray_entries_no_op_when_nothing_matches() {
        use golem_common::model::oplog::{
            DurableFunctionType, HostRequest, HostRequestPollCount, HostResponse,
            HostResponsePollResult, OplogPayload,
        };
        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "test".to_string(),
        };
        let genuine = OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::IoPollPoll,
            request: OplogPayload::Inline(Box::new(HostRequest::PollCount(HostRequestPollCount {
                count: 1,
            }))),
            response: OplogPayload::Inline(Box::new(HostResponse::PollResult(
                HostResponsePollResult {
                    result: Ok(vec![0]),
                },
            ))),
            durable_function_type: DurableFunctionType::ReadLocal,
        };
        let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
            (
                OplogIndex::INITIAL,
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                },
            ),
            (OplogIndex::INITIAL.next(), genuine),
        ])));
        let mut state = ReplayState::new(
            OwnedAgentId::new(EnvironmentId::new(), &agent_id),
            oplog,
            DeletedRegions::new(),
        )
        .await
        .unwrap();

        let mut consumed = Vec::new();
        state
            .consume_stray_entries(
                |e| {
                    matches!(
                        e,
                        OplogEntry::HostCall {
                            function_name: HostFunctionName::IoPollReady,
                            ..
                        }
                    )
                },
                |idx, entry| consumed.push((idx, entry)),
            )
            .await
            .unwrap();

        assert!(consumed.is_empty());
        let genuine_found = state
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
            genuine_found.is_some(),
            "poll()'s entry must remain, untouched"
        );
    }

    /// "Before" half of the bidirectional falsification for the Eleventh capture: reproduces
    /// the OLD, pre-fix consumption mechanism (`get_oplog_entry()`, i.e. unconditional
    /// consumption — what `poll()`'s replay used before this round's widening) directly
    /// against a crafted oplog matching the Eleventh capture's exact shape — a stray
    /// `GolemRpcFutureInvokeResultGet` entry sitting where `poll()`'s own `IoPollPoll` entry
    /// was expected — proving it genuinely mismatches (the exact production crash: "expected
    /// io::poll::poll, got golem::rpc::future-invoke-result::get"). The "after" half is
    /// `stray_entry_tests::recognizes_tracked_invoke_result_stray_from_poll_context` and the
    /// actual `Host::poll` implementation (`io/poll.rs`), which now widens its scan to
    /// recognize and defer to exactly this entry shape before ever reaching this unconditional
    /// fallback.
    #[test]
    async fn old_unconditional_consume_would_have_hit_eleventh_capture_mismatch() {
        use golem_common::model::oplog::types::SerializableInvokeResult;
        use golem_common::model::oplog::{
            DurableFunctionType, HostRequest, HostRequestNoInput, HostResponse,
            HostResponseGolemRpcInvokeGet, OplogPayload,
        };

        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "test".to_string(),
        };
        let stray_rpc_entry = OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::GolemRpcFutureInvokeResultGet,
            request: OplogPayload::Inline(Box::new(HostRequest::NoInput(HostRequestNoInput {}))),
            response: OplogPayload::Inline(Box::new(HostResponse::GolemRpcInvokeGet(
                HostResponseGolemRpcInvokeGet {
                    result: SerializableInvokeResult::Pending,
                },
            ))),
            durable_function_type: DurableFunctionType::WriteRemoteConcurrent(5),
        };
        let oplog = Arc::new(MutableBatchOplog::new(BTreeMap::from([
            (
                OplogIndex::INITIAL,
                OplogEntry::NoOp {
                    timestamp: Timestamp::now_utc(),
                },
            ),
            (OplogIndex::INITIAL.next(), stray_rpc_entry),
        ])));
        let mut state = ReplayState::new(
            OwnedAgentId::new(EnvironmentId::new(), &agent_id),
            oplog,
            DeletedRegions::new(),
        )
        .await
        .unwrap();

        // The OLD mechanism: consume the next entry unconditionally — no per-operation identity
        // check, whatever's there MUST be poll()'s own entry.
        let (_idx, entry) = state.get_oplog_entry().await.unwrap();
        let OplogEntry::HostCall { function_name, .. } = entry else {
            panic!("crafted entry is always a HostCall variant");
        };

        assert_eq!(
            function_name,
            HostFunctionName::GolemRpcFutureInvokeResultGet,
            "sanity check: this crafted oplog must genuinely reproduce the Eleventh capture's \
             trigger — if this ever fails, the test no longer represents a real reproduction"
        );
        assert_ne!(
            function_name,
            HostFunctionName::IoPollPoll,
            "the OLD unconditional mechanism would crash here (expected io::poll::poll, got \
             golem::rpc::future-invoke-result::get) — exactly the defect the widened \
             is_stray_concurrent_entry mechanism (durable_host/mod.rs) fixes by checking \
             identity FIRST, before ever reaching this unconditional read"
        );
    }

    // ---------------------------------------------------------------------------------------
    // FINDING_B_FIX_DESIGN.md §12 / §13 regressions
    // ---------------------------------------------------------------------------------------

    fn noop_entry() -> OplogEntry {
        OplogEntry::NoOp {
            timestamp: Timestamp::now_utc(),
        }
    }

    fn rpc_invoke_get_entry(seq: u32) -> OplogEntry {
        use golem_common::model::oplog::types::SerializableInvokeResult;
        use golem_common::model::oplog::{
            HostRequest, HostRequestNoInput, HostResponse, HostResponseGolemRpcInvokeGet,
            OplogPayload,
        };
        OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::GolemRpcFutureInvokeResultGet,
            request: OplogPayload::Inline(Box::new(HostRequest::NoInput(HostRequestNoInput {}))),
            response: OplogPayload::Inline(Box::new(HostResponse::GolemRpcInvokeGet(
                HostResponseGolemRpcInvokeGet {
                    result: SerializableInvokeResult::Pending,
                },
            ))),
            durable_function_type: DurableFunctionType::WriteRemoteConcurrent(seq),
        }
    }

    fn finish_span_entry() -> OplogEntry {
        OplogEntry::FinishSpan {
            timestamp: Timestamp::now_utc(),
            span_id: golem_common::model::invocation_context::SpanId::generate(),
        }
    }

    fn outgoing_check_write_entry(begin_idx: OplogIndex) -> OplogEntry {
        use golem_common::model::oplog::{
            HostRequest, HostRequestNoInput, HostResponse, HostResponseStreamCheckWrite,
            OplogPayload,
        };
        OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::HttpTypesOutgoingBodyStreamCheckWrite,
            request: OplogPayload::Inline(Box::new(HostRequest::NoInput(HostRequestNoInput {}))),
            response: OplogPayload::Inline(Box::new(HostResponse::StreamCheckWrite(
                HostResponseStreamCheckWrite {
                    result: Ok(1048576),
                },
            ))),
            durable_function_type: DurableFunctionType::WriteRemoteBatched(Some(begin_idx)),
        }
    }

    fn outgoing_write_entry(begin_idx: OplogIndex) -> OplogEntry {
        use golem_common::model::oplog::{
            HostRequest, HostRequestNoInput, HostResponse, HostResponseStreamWriteWithBytes,
            OplogPayload,
        };
        OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::HttpTypesOutgoingBodyStreamWrite,
            request: OplogPayload::Inline(Box::new(HostRequest::NoInput(HostRequestNoInput {}))),
            response: OplogPayload::Inline(Box::new(HostResponse::StreamWriteWithBytes(
                HostResponseStreamWriteWithBytes { result: Ok(vec![]) },
            ))),
            durable_function_type: DurableFunctionType::WriteRemoteBatched(Some(begin_idx)),
        }
    }

    fn poll_entry() -> OplogEntry {
        use golem_common::model::oplog::{
            HostRequest, HostRequestPollCount, HostResponse, HostResponsePollResult, OplogPayload,
        };
        OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name: HostFunctionName::IoPollPoll,
            request: OplogPayload::Inline(Box::new(HostRequest::PollCount(HostRequestPollCount {
                count: 2,
            }))),
            response: OplogPayload::Inline(Box::new(HostResponse::PollResult(
                HostResponsePollResult {
                    result: Ok(vec![0]),
                },
            ))),
            durable_function_type: DurableFunctionType::ReadLocal,
        }
    }

    async fn replay_state_over(entries: Vec<OplogEntry>) -> ReplayState {
        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "test".to_string(),
        };
        // ReplayState::new() consumes OplogIndex::INITIAL, so entries start at INITIAL.next().
        let mut map = BTreeMap::from([(OplogIndex::INITIAL, noop_entry())]);
        let mut idx = OplogIndex::INITIAL;
        for entry in entries {
            idx = idx.next();
            map.insert(idx, entry);
        }
        ReplayState::new(
            OwnedAgentId::new(EnvironmentId::new(), &agent_id),
            Arc::new(MutableBatchOplog::new(map)),
            DeletedRegions::new(),
        )
        .await
        .unwrap()
    }

    /// Thirteenth capture, verbatim (FINDING_B_FIX_DESIGN.md §13.5): a `get()` whose own
    /// `invoke_result_seq` drifted between live and replay classifies its OWN entry as a
    /// sibling's stray and consumes it — leaving the cursor on the region's `FinishSpan`.
    ///
    /// Before §13.7.2, `get()`'s fallback was an unconditional
    /// `get_oplog_entry!(.., OplogEntry::HostCall)`, which consumed that `FinishSpan` and only
    /// then failed with "expected OplogEntry :: HostCall |, got FinishSpan" — permanent, since
    /// every retry replays identically. With the predicate read, the `FinishSpan` survives
    /// untouched and `get()` reports Pending, i.e. the drift degrades to one extra guest loop.
    #[test]
    async fn get_replay_leaves_finish_span_when_invoke_result_seq_drifts() {
        let seq_live = 5u32;
        let seq_replay = 6u32;
        let mut state =
            replay_state_over(vec![rpc_invoke_get_entry(seq_live), finish_span_entry()]).await;

        // The stray-scan misclassifies the caller's own entry (identity drift) and consumes it.
        let tracked =
            TrackedConcurrentOpSeqs::new(HashSet::from([IdentityNamespace::InvokeResult(
                seq_live,
            )]));
        let exclude = StrayEntryIdentity::new(
            HostFunctionName::GolemRpcFutureInvokeResultGet,
            IdentityNamespace::InvokeResult(seq_replay),
        );
        let scan = StrayEntryScan::new(tracked, Some(exclude));
        let mut consumed = Vec::new();
        state
            .consume_stray_entries(|e| scan.accept(e), |idx, e| consumed.push((idx, e)))
            .await
            .unwrap();
        assert_eq!(
            consumed.len(),
            1,
            "the drifted-identity entry is consumed as a stray — this is the precondition the \
             hardening must survive, not something it prevents"
        );

        // get()'s hardened fallback: predicate read on its OWN identity.
        let peeked = state
            .try_get_oplog_entry(|e| is_own_invoke_result_entry(e, seq_replay))
            .await
            .unwrap();
        assert!(
            peeked.is_none(),
            "a FinishSpan must never satisfy get()'s own-identity predicate"
        );

        // ... and the structural entry is still there for whoever legitimately consumes it.
        let survivor = state
            .try_get_oplog_entry(|e| matches!(e, OplogEntry::FinishSpan { .. }))
            .await
            .unwrap();
        assert!(
            survivor.is_some(),
            "the FinishSpan must survive the predicate read — consuming it is the defect"
        );
    }

    /// Happy path must be byte-identical: when identity matches, the predicate read consumes
    /// exactly what the old unconditional read consumed.
    #[test]
    async fn get_replay_consumes_its_own_tagged_entry() {
        let mut state = replay_state_over(vec![rpc_invoke_get_entry(5), finish_span_entry()]).await;
        let peeked = state
            .try_get_oplog_entry(|e| is_own_invoke_result_entry(e, 5))
            .await
            .unwrap();
        assert!(peeked.is_some());
    }

    /// Legacy oplogs recorded before per-call tagging carry `WriteRemote` and no identity —
    /// they must keep being consumed positionally, unchanged.
    #[test]
    async fn get_replay_consumes_legacy_untagged_entry() {
        let legacy = match rpc_invoke_get_entry(0) {
            OplogEntry::HostCall {
                timestamp,
                function_name,
                request,
                response,
                ..
            } => OplogEntry::HostCall {
                timestamp,
                function_name,
                request,
                response,
                durable_function_type: DurableFunctionType::WriteRemote,
            },
            other => other,
        };
        let mut state = replay_state_over(vec![legacy]).await;
        let peeked = state
            .try_get_oplog_entry(|e| is_own_invoke_result_entry(e, 12345))
            .await
            .unwrap();
        assert!(
            peeked.is_some(),
            "untagged legacy entries must still be consumed regardless of the caller's seq"
        );
    }

    /// Twelfth capture (§12): an outgoing-body `check_write` entry sitting where `poll()`'s own
    /// `IoPollPoll` entry was expected. `poll()`'s scan must now recognize and consume it (with
    /// its `write` partner), and `poll()`'s own entry must survive to be read normally.
    #[test]
    async fn poll_replay_defers_to_stray_outgoing_body_write_entries() {
        let begin_idx = OplogIndex::from_u64(42);
        let mut state = replay_state_over(vec![
            outgoing_check_write_entry(begin_idx),
            outgoing_write_entry(begin_idx),
            poll_entry(),
        ])
        .await;

        let tracked =
            TrackedConcurrentOpSeqs::new(HashSet::from([IdentityNamespace::Batch(begin_idx)]));
        let scan = StrayEntryScan::new(tracked, None);
        let mut consumed = Vec::new();
        state
            .consume_stray_entries(|e| scan.accept(e), |idx, e| consumed.push((idx, e)))
            .await
            .unwrap();

        assert_eq!(
            consumed.len(),
            2,
            "both halves of the per-chunk write protocol must be recognized"
        );
        let peeked = state.try_get_oplog_entry(is_own_poll_entry).await.unwrap();
        assert!(
            peeked.is_some(),
            "poll()'s own entry must be reachable once the strays are out of the way"
        );
    }

    /// FINDING_B_FIX_DESIGN.md §15, at the real replay cursor: a multi-chunk body records
    /// `check_write`/`write` pairs for the SAME request back to back, so one identity repeats.
    /// The scan must walk the WHOLE cluster — the earlier one-entry-per-identity bound stopped
    /// at the second occurrence and stranded it at the cursor, where `poll()` (which has no way
    /// to consume someone else's stream entry) trapped on it.
    #[test]
    async fn stray_scan_walks_every_occurrence_of_a_repeated_identity() {
        let begin_idx = OplogIndex::from_u64(42);
        let mut state = replay_state_over(vec![
            outgoing_check_write_entry(begin_idx),
            outgoing_write_entry(begin_idx),
            outgoing_check_write_entry(begin_idx),
            outgoing_write_entry(begin_idx),
            poll_entry(),
        ])
        .await;

        let tracked =
            TrackedConcurrentOpSeqs::new(HashSet::from([IdentityNamespace::Batch(begin_idx)]));
        let scan = StrayEntryScan::new(tracked, None);
        let mut consumed = Vec::new();
        state
            .consume_stray_entries(|e| scan.accept(e), |idx, e| consumed.push((idx, e)))
            .await
            .unwrap();

        assert_eq!(
            consumed.len(),
            4,
            "every occurrence must be deferred — anything left behind is unreachable"
        );
        assert!(
            state
                .try_get_oplog_entry(is_own_poll_entry)
                .await
                .unwrap()
                .is_some(),
            "poll()'s own entry must be reachable once the entire cluster is out of the way"
        );
    }

    /// The exact live capture behind FINDING_B_FIX_DESIGN.md §15
    /// (`WorkerAgent("workspace-smoketest@1.0", "...@character_sheet")`, oplog `#02254`/`#02255`):
    /// TWO `blocking_read()` entries on ONE incoming body stream — same `(function_name,
    /// namespace)` identity, back to back — followed by the `poll()` entry that trapped with
    /// `expected io::poll::poll, got http::types::incoming_body_stream::blocking_read`.
    ///
    /// Asserts CONTENT, not merely "no crash": the two calls must get their own respective
    /// chunks, in order. A wrong-slot bug would be a silent misdelivery (§14.2.1's failure
    /// mode), which a crash-only assertion would not catch.
    #[test]
    async fn two_blocking_reads_on_one_stream_each_replay_to_their_own_answer() {
        use golem_common::model::oplog::{HostResponse, HostResponseStreamChunk};
        let begin_idx = OplogIndex::from_u64(2190);
        let chunk = |bytes: &[u8]| {
            batched_entry(
                HostFunctionName::HttpTypesIncomingBodyStreamBlockingRead,
                begin_idx,
                HostResponse::StreamChunk(HostResponseStreamChunk {
                    result: Ok(bytes.to_vec()),
                }),
            )
        };
        let mut state =
            replay_state_over(vec![chunk(b"first"), chunk(b"second"), poll_entry()]).await;

        // poll() scans with no identity of its own to exclude.
        let tracked =
            TrackedConcurrentOpSeqs::new(HashSet::from([IdentityNamespace::Batch(begin_idx)]));
        let scan = StrayEntryScan::new(tracked, None);
        let mut consumed = Vec::new();
        state
            .consume_stray_entries(|e| scan.accept(e), |idx, e| consumed.push((idx, e)))
            .await
            .unwrap();

        assert_eq!(
            consumed.len(),
            2,
            "BOTH blocking_read entries must be deferred; stranding #02255 is the bug"
        );

        // Cache them exactly as `decode_and_cache_stray_entry` does, then hand them back to the
        // owner in call order — the two halves must agree on which occurrence is which.
        let identity = StrayEntryIdentity::new(
            HostFunctionName::HttpTypesIncomingBodyStreamBlockingRead,
            IdentityNamespace::Batch(begin_idx),
        );
        let mut cache = PreResolvedStrayCache::default();
        for (_, entry) in consumed {
            assert_eq!(stray_entry_identity(&entry).as_ref(), Some(&identity));
            let OplogEntry::HostCall { response, .. } = entry else {
                unreachable!()
            };
            let golem_common::model::oplog::OplogPayload::Inline(boxed) = response else {
                unreachable!("test entries are built inline")
            };
            cache.record(identity.clone(), *boxed);
        }
        for expected in [b"first".to_vec(), b"second".to_vec()] {
            let taken: HostResponseStreamChunk = cache
                .take(&identity)
                .expect("an answer is waiting for this call")
                .try_into()
                .expect("narrows to blocking_read's own response type");
            assert_eq!(taken.result, Ok(expected));
        }
        assert!(cache.take(&identity).is_none());

        // ... and poll()'s own entry is now reachable, which is what used to trap.
        assert!(
            state
                .try_get_oplog_entry(is_own_poll_entry)
                .await
                .unwrap()
                .is_some(),
            "poll() must find its own entry, not the stranded second blocking_read"
        );
    }

    /// The `EndRemoteWrite` that closes a batch — the structural entry that immediately follows
    /// a body-stream cluster, carrying a `begin_index: OplogIndex` field of the very same shape
    /// and value as the identity namespace the cluster's entries are keyed by
    /// (`Batch(OplogIndex)`). Superficially "matching" that field is exactly the confusion the
    /// tests below rule out.
    fn end_remote_write_entry(begin_index: OplogIndex) -> OplogEntry {
        OplogEntry::EndRemoteWrite {
            timestamp: Timestamp::now_utc(),
            begin_index,
        }
    }

    /// FINDING_B_FIX_DESIGN.md §15.5, recognition half: a cluster of exactly two same-identity
    /// entries followed by the batch's own `EndRemoteWrite`. The scan must drain both entries
    /// and STOP at the structural one, never consuming it — even though that entry carries a
    /// `begin_index` field holding the identical `OplogIndex` the cluster is keyed by.
    #[test]
    async fn a_structural_entry_closing_the_batch_is_never_consumed_by_a_scan() {
        use golem_common::model::oplog::{HostResponse, HostResponseStreamChunk};
        let begin_idx = OplogIndex::from_u64(2190);
        let read = |bytes: &[u8]| {
            batched_entry(
                HostFunctionName::HttpTypesIncomingBodyStreamBlockingRead,
                begin_idx,
                HostResponse::StreamChunk(HostResponseStreamChunk {
                    result: Ok(bytes.to_vec()),
                }),
            )
        };
        let mut state = replay_state_over(vec![
            read(b"body"),
            read(b""),
            end_remote_write_entry(begin_idx),
        ])
        .await;

        // Identity derivation must reject the structural entry outright — it is not a HostCall,
        // so it has no identity at all, regardless of its begin_index.
        assert_eq!(
            stray_entry_identity(&end_remote_write_entry(begin_idx)),
            None
        );

        let tracked =
            TrackedConcurrentOpSeqs::new(HashSet::from([IdentityNamespace::Batch(begin_idx)]));
        let scan = StrayEntryScan::new(tracked, None);
        let mut consumed = Vec::new();
        state
            .consume_stray_entries(|e| scan.accept(e), |idx, e| consumed.push((idx, e)))
            .await
            .unwrap();

        assert_eq!(consumed.len(), 2, "both reads defer, the marker does not");
        let survivor = state
            .try_get_oplog_entry(|e| matches!(e, OplogEntry::EndRemoteWrite { .. }))
            .await
            .unwrap();
        assert!(
            survivor.is_some(),
            "EndRemoteWrite must still be at the cursor for end_function, its real owner"
        );
    }

    /// §15.5, consumption half — the actual live trap. After the scan clears the cluster the
    /// cursor sits on `EndRemoteWrite`, and the post-scan read must REFUSE it non-destructively.
    /// The unconditional `get_oplog_entry!(.., OplogEntry::HostCall)` used before consumed it and
    /// only then reported "expected OplogEntry::HostCall, got EndRemoteWrite", destroying the
    /// engine's region bookkeeping and making the failure permanent instead of retriable.
    #[test]
    async fn a_post_scan_read_refuses_a_structural_entry_without_consuming_it() {
        use golem_common::model::oplog::{HostResponse, HostResponseStreamChunk};
        let begin_idx = OplogIndex::from_u64(2190);
        let mut state = replay_state_over(vec![
            batched_entry(
                HostFunctionName::HttpTypesIncomingBodyStreamBlockingRead,
                begin_idx,
                HostResponse::StreamChunk(HostResponseStreamChunk { result: Ok(vec![]) }),
            ),
            end_remote_write_entry(begin_idx),
        ])
        .await;

        let tracked =
            TrackedConcurrentOpSeqs::new(HashSet::from([IdentityNamespace::Batch(begin_idx)]));
        let scan = StrayEntryScan::new(tracked, None);
        state
            .consume_stray_entries(|e| scan.accept(e), |_, _| {})
            .await
            .unwrap();

        // BEFORE (the bug): `get_oplog_entry!` expands to `get_oplog_entry()`, i.e.
        // `try_get_oplog_entry(|_| true)`. Reproduced here against the real primitive to pin
        // what the old post-scan read did — it CONSUMES the structural entry, and the caller's
        // "expected HostCall, got EndRemoteWrite" is reported only afterwards, too late.
        // (`poll_entry` is only a trailing sentinel so the cursor stays inside the replay target
        // after the destructive read; nothing about it matters beyond being a later entry.)
        let mut destructive =
            replay_state_over(vec![end_remote_write_entry(begin_idx), poll_entry()]).await;
        let (_, eaten) = destructive.get_oplog_entry().await.unwrap();
        assert!(
            matches!(eaten, OplogEntry::EndRemoteWrite { .. }),
            "the unconditional read consumes whatever sits at the cursor — the defect"
        );
        assert!(
            destructive
                .try_get_oplog_entry(|e| matches!(e, OplogEntry::EndRemoteWrite { .. }))
                .await
                .unwrap()
                .is_none(),
            "and it is gone: end_function can never find it again"
        );

        // AFTER (the fix): the same read, guarded by the predicate
        // `try_read_persisted_durable_function_invocation` and
        // `future_incoming_response::get`'s replay both now use.
        let attempt = state
            .try_get_oplog_entry(|entry| matches!(entry, OplogEntry::HostCall { .. }))
            .await
            .unwrap();
        assert!(
            attempt.is_none(),
            "a HostCall-only read must not match EndRemoteWrite"
        );

        // ... and crucially, the refusal left the cursor untouched.
        let survivor = state
            .try_get_oplog_entry(|e| matches!(e, OplogEntry::EndRemoteWrite { .. }))
            .await
            .unwrap();
        assert!(
            survivor.is_some(),
            "the refused entry must survive for end_function; consuming it is the §15.5 bug"
        );
    }

    /// Generalization of the above beyond `EndRemoteWrite`: any non-`HostCall` variant that can
    /// follow a cluster must be refused identically. `FinishSpan` (the entry immediately after
    /// `EndRemoteWrite` in the live capture) has no `begin_index` at all, `EndAtomicRegion` has
    /// one — neither may be consumed by a HostCall read.
    #[test]
    async fn every_structural_variant_is_refused_by_a_host_call_read() {
        let begin_idx = OplogIndex::from_u64(2190);
        for structural in [
            end_remote_write_entry(begin_idx),
            finish_span_entry(),
            OplogEntry::EndAtomicRegion {
                timestamp: Timestamp::now_utc(),
                begin_index: begin_idx,
            },
        ] {
            assert_eq!(
                stray_entry_identity(&structural),
                None,
                "{structural:?} must carry no stray identity"
            );
            let mut state = replay_state_over(vec![structural.clone()]).await;
            let attempt = state
                .try_get_oplog_entry(|entry| matches!(entry, OplogEntry::HostCall { .. }))
                .await
                .unwrap();
            assert!(
                attempt.is_none(),
                "{structural:?} must not satisfy a HostCall read"
            );
        }
    }

    /// N=2 is not special-cased: five occurrences of one identity behave identically.
    #[test]
    async fn many_occurrences_of_one_identity_all_replay_in_order() {
        use golem_common::model::oplog::{HostResponse, HostResponseStreamChunk};
        let begin_idx = OplogIndex::from_u64(2190);
        let expected: Vec<Vec<u8>> = (0u8..5).map(|n| vec![n]).collect();
        let mut entries: Vec<OplogEntry> = expected
            .iter()
            .map(|bytes| {
                batched_entry(
                    HostFunctionName::HttpTypesIncomingBodyStreamBlockingRead,
                    begin_idx,
                    HostResponse::StreamChunk(HostResponseStreamChunk {
                        result: Ok(bytes.clone()),
                    }),
                )
            })
            .collect();
        entries.push(poll_entry());
        let mut state = replay_state_over(entries).await;

        let tracked =
            TrackedConcurrentOpSeqs::new(HashSet::from([IdentityNamespace::Batch(begin_idx)]));
        let scan = StrayEntryScan::new(tracked, None);
        let mut consumed = Vec::new();
        state
            .consume_stray_entries(|e| scan.accept(e), |idx, e| consumed.push((idx, e)))
            .await
            .unwrap();

        assert_eq!(consumed.len(), expected.len());
        let identity = StrayEntryIdentity::new(
            HostFunctionName::HttpTypesIncomingBodyStreamBlockingRead,
            IdentityNamespace::Batch(begin_idx),
        );
        let mut cache = PreResolvedStrayCache::default();
        for (_, entry) in consumed {
            let OplogEntry::HostCall { response, .. } = entry else {
                unreachable!()
            };
            let golem_common::model::oplog::OplogPayload::Inline(boxed) = response else {
                unreachable!()
            };
            cache.record(identity.clone(), *boxed);
        }
        for want in expected {
            let taken: HostResponseStreamChunk = cache
                .take(&identity)
                .expect("answer")
                .try_into()
                .expect("narrows");
            assert_eq!(taken.result, Ok(want));
        }
        assert!(
            state
                .try_get_oplog_entry(is_own_poll_entry)
                .await
                .unwrap()
                .is_some()
        );
    }

    /// The other bound is unchanged and still load-bearing: a scan run on behalf of one
    /// `blocking_read()` call must NOT swallow the next `blocking_read()` entry on the same
    /// stream, however many are queued — that entry is the caller's own next answer.
    #[test]
    async fn a_repeated_identity_still_stops_its_own_owners_scan() {
        use golem_common::model::oplog::{HostResponse, HostResponseStreamChunk};
        let begin_idx = OplogIndex::from_u64(2190);
        let read = || {
            batched_entry(
                HostFunctionName::HttpTypesIncomingBodyStreamBlockingRead,
                begin_idx,
                HostResponse::StreamChunk(HostResponseStreamChunk { result: Ok(vec![]) }),
            )
        };
        let mut state = replay_state_over(vec![
            outgoing_check_write_entry(begin_idx),
            read(),
            read(),
            poll_entry(),
        ])
        .await;

        let tracked =
            TrackedConcurrentOpSeqs::new(HashSet::from([IdentityNamespace::Batch(begin_idx)]));
        let own = StrayEntryIdentity::new(
            HostFunctionName::HttpTypesIncomingBodyStreamBlockingRead,
            IdentityNamespace::Batch(begin_idx),
        );
        let scan = StrayEntryScan::new(tracked, Some(own));
        let mut consumed = Vec::new();
        state
            .consume_stray_entries(|e| scan.accept(e), |idx, e| consumed.push((idx, e)))
            .await
            .unwrap();

        assert_eq!(
            consumed.len(),
            1,
            "only the foreign check_write may be deferred; the caller's own reads must survive"
        );
        assert!(
            state
                .try_get_oplog_entry(|e| matches!(
                    e,
                    OplogEntry::HostCall {
                        function_name: HostFunctionName::HttpTypesIncomingBodyStreamBlockingRead,
                        ..
                    }
                ))
                .await
                .unwrap()
                .is_some(),
            "the caller's own first blocking_read entry must still be at the cursor"
        );
    }

    /// Builds a `HostCall` entry tagged `WriteRemoteBatched(Some(begin_idx))` for an arbitrary
    /// host function and response — the generalized mechanism is function- and shape-agnostic,
    /// so newly-covered families are exercised through this one builder.
    fn batched_entry(
        function_name: HostFunctionName,
        begin_idx: OplogIndex,
        response: golem_common::model::oplog::HostResponse,
    ) -> OplogEntry {
        use golem_common::model::oplog::{HostRequest, HostRequestNoInput, OplogPayload};
        OplogEntry::HostCall {
            timestamp: Timestamp::now_utc(),
            function_name,
            request: OplogPayload::Inline(Box::new(HostRequest::NoInput(HostRequestNoInput {}))),
            response: OplogPayload::Inline(Box::new(response)),
            durable_function_type: DurableFunctionType::WriteRemoteBatched(Some(begin_idx)),
        }
    }

    /// A `future_incoming_response::get` entry carrying a status code that identifies WHICH
    /// concurrent request recorded it — the whole point of the §14.2.1 regression below is that
    /// the two are otherwise indistinguishable.
    fn future_response_entry(begin_idx: OplogIndex, status: u16) -> OplogEntry {
        use golem_common::model::oplog::HostResponse;
        use golem_common::model::oplog::HostResponseHttpResponse;
        use golem_common::model::oplog::types::{
            SerializableHttpResponse, SerializableResponseHeaders,
        };
        batched_entry(
            HostFunctionName::HttpTypesFutureIncomingResponseGet,
            begin_idx,
            HostResponse::HttpResponse(HostResponseHttpResponse {
                response: SerializableHttpResponse::HeadersReceived(SerializableResponseHeaders {
                    status,
                    headers: std::collections::HashMap::new(),
                }),
            }),
        )
    }

    fn response_status_of(entry: &OplogEntry) -> u16 {
        use golem_common::model::oplog::types::SerializableHttpResponse;
        use golem_common::model::oplog::{HostResponse, OplogPayload};
        let OplogEntry::HostCall {
            response: OplogPayload::Inline(boxed),
            ..
        } = entry
        else {
            panic!("expected an inline HostCall entry")
        };
        match boxed.as_ref() {
            HostResponse::HttpResponse(r) => match &r.response {
                SerializableHttpResponse::HeadersReceived(h) => h.status,
                other => panic!("expected HeadersReceived, got {other:?}"),
            },
            other => panic!("expected HttpResponse, got {other:?}"),
        }
    }

    /// FINDING_B_FIX_DESIGN.md §14.2.1 — the highest-severity latent gap, and the only one whose
    /// failure mode is SILENT rather than a trap.
    ///
    /// Two `fetch()`es are in flight concurrently (a `Promise.all` fan-out). Their
    /// `http::types::future_incoming_response::get` entries share both `function_name` and
    /// response shape, and land in completion order — B first — while the replayed guest reaches
    /// A's structural check first. The old unconditional positional read therefore did not trap;
    /// it accepted B's entry and decoded it cleanly as A's own response, handing request A
    /// request B's status and headers.
    ///
    /// Correctness, not merely absence of a trap, is what this asserts: A must end up with 201
    /// and B with 202.
    #[test]
    async fn concurrent_future_response_entries_reach_their_own_callers() {
        let a = OplogIndex::from_u64(10);
        let b = OplogIndex::from_u64(20);

        // The pre-fix failure mode, pinned: a positional read at A's turn returns B's entry.
        let mut naive = replay_state_over(vec![
            future_response_entry(b, 202),
            future_response_entry(a, 201),
        ])
        .await;
        let (_, first) = naive
            .try_get_oplog_entry(|_| true)
            .await
            .unwrap()
            .expect("an entry is at the cursor");
        assert_eq!(
            response_status_of(&first),
            202,
            "precondition: an unconditional positional read hands request A request B's \
             response — silently, since both decode to the same type"
        );

        // With the mechanism: A defers B's entry into the cache and reads its own.
        let mut state = replay_state_over(vec![
            future_response_entry(b, 202),
            future_response_entry(a, 201),
        ])
        .await;
        let tracked = TrackedConcurrentOpSeqs::new(HashSet::from([
            IdentityNamespace::Batch(a),
            IdentityNamespace::Batch(b),
        ]));
        let a_identity = StrayEntryIdentity::new(
            HostFunctionName::HttpTypesFutureIncomingResponseGet,
            IdentityNamespace::Batch(a),
        );
        let b_identity = StrayEntryIdentity::new(
            HostFunctionName::HttpTypesFutureIncomingResponseGet,
            IdentityNamespace::Batch(b),
        );

        let scan = StrayEntryScan::new(tracked, Some(a_identity.clone()));
        let mut deferred = Vec::new();
        state
            .consume_stray_entries(|e| scan.accept(e), |idx, e| deferred.push((idx, e)))
            .await
            .unwrap();

        assert_eq!(deferred.len(), 1, "exactly B's entry is deferred");
        assert_eq!(
            stray_entry_identity(&deferred[0].1),
            Some(b_identity),
            "the deferred entry is cached under B's identity, not A's"
        );
        assert_eq!(
            response_status_of(&deferred[0].1),
            202,
            "request B's own response is what got cached for B"
        );

        let (_, own) = state
            .try_get_oplog_entry(|e| stray_entry_identity(e) == Some(a_identity.clone()))
            .await
            .unwrap()
            .expect("A's own entry is now at the cursor");
        assert_eq!(
            response_status_of(&own),
            201,
            "request A must receive its OWN response, not its sibling's"
        );
    }

    /// New coverage, outgoing stream family (§14.2): `flush` / `blocking_flush` /
    /// `write_zeroes` / `splice` / `blocking_splice` were never in the hand-written catalog, so
    /// any of them sitting at the cursor trapped a concurrent `poll()`. They now defer cleanly,
    /// each into its own cache slot under the request's shared `begin_index`.
    #[test]
    async fn poll_replay_defers_previously_uncatalogued_outgoing_stream_entries() {
        use golem_common::model::oplog::{
            HostResponse, HostResponseStreamSkip, HostResponseStreamWriteResult,
            HostResponseStreamWriteZeroes,
        };
        let begin_idx = OplogIndex::from_u64(42);
        let entries = vec![
            batched_entry(
                HostFunctionName::HttpTypesOutgoingBodyStreamFlush,
                begin_idx,
                HostResponse::StreamWriteResult(HostResponseStreamWriteResult { result: Ok(()) }),
            ),
            batched_entry(
                HostFunctionName::HttpTypesOutgoingBodyStreamBlockingFlush,
                begin_idx,
                HostResponse::StreamWriteResult(HostResponseStreamWriteResult { result: Ok(()) }),
            ),
            batched_entry(
                HostFunctionName::HttpTypesOutgoingBodyStreamWriteZeroes,
                begin_idx,
                HostResponse::StreamWriteZeroes(HostResponseStreamWriteZeroes { result: Ok(8) }),
            ),
            batched_entry(
                HostFunctionName::HttpTypesOutgoingBodyStreamSplice,
                begin_idx,
                HostResponse::StreamSkip(HostResponseStreamSkip { result: Ok(8) }),
            ),
            batched_entry(
                HostFunctionName::HttpTypesOutgoingBodyStreamBlockingSplice,
                begin_idx,
                HostResponse::StreamSkip(HostResponseStreamSkip { result: Ok(8) }),
            ),
            poll_entry(),
        ];
        let expected_strays = entries.len() - 1;
        let mut state = replay_state_over(entries).await;

        let tracked =
            TrackedConcurrentOpSeqs::new(HashSet::from([IdentityNamespace::Batch(begin_idx)]));
        let scan = StrayEntryScan::new(tracked, None);
        let mut consumed = Vec::new();
        state
            .consume_stray_entries(|e| scan.accept(e), |idx, e| consumed.push((idx, e)))
            .await
            .unwrap();

        assert_eq!(
            consumed.len(),
            expected_strays,
            "every outgoing-stream operation must defer, not just check_write/write"
        );
        let peeked = state.try_get_oplog_entry(is_own_poll_entry).await.unwrap();
        assert!(
            peeked.is_some(),
            "poll()'s own entry must be reachable once the strays are out of the way"
        );
    }

    /// New coverage, incoming stream family (§14.2): `blocking_read` (previously covered only by
    /// being folded into `read`'s identity), `skip` and `blocking_skip`.
    #[test]
    async fn poll_replay_defers_incoming_stream_skip_and_blocking_read_entries() {
        use golem_common::model::oplog::{
            HostResponse, HostResponseStreamChunk, HostResponseStreamSkip,
        };
        let begin_idx = OplogIndex::from_u64(42);
        let entries = vec![
            batched_entry(
                HostFunctionName::HttpTypesIncomingBodyStreamBlockingRead,
                begin_idx,
                HostResponse::StreamChunk(HostResponseStreamChunk {
                    result: Ok(vec![1]),
                }),
            ),
            batched_entry(
                HostFunctionName::HttpTypesIncomingBodyStreamSkip,
                begin_idx,
                HostResponse::StreamSkip(HostResponseStreamSkip { result: Ok(4) }),
            ),
            batched_entry(
                HostFunctionName::HttpTypesIncomingBodyStreamBlockingSkip,
                begin_idx,
                HostResponse::StreamSkip(HostResponseStreamSkip { result: Ok(4) }),
            ),
            poll_entry(),
        ];
        let expected_strays = entries.len() - 1;
        let mut state = replay_state_over(entries).await;

        let tracked =
            TrackedConcurrentOpSeqs::new(HashSet::from([IdentityNamespace::Batch(begin_idx)]));
        let scan = StrayEntryScan::new(tracked, None);
        let mut consumed = Vec::new();
        state
            .consume_stray_entries(|e| scan.accept(e), |idx, e| consumed.push((idx, e)))
            .await
            .unwrap();

        assert_eq!(consumed.len(), expected_strays);
        assert!(
            state
                .try_get_oplog_entry(is_own_poll_entry)
                .await
                .unwrap()
                .is_some()
        );
    }

    /// New coverage, `future_trailers::get` (§14.2): same per-request identity as the body
    /// streams, fires only for chunked responses carrying trailers.
    #[test]
    async fn poll_replay_defers_future_trailers_get_entries() {
        use golem_common::model::oplog::{HostResponse, HostResponseHttpFutureTrailersGet};
        let begin_idx = OplogIndex::from_u64(42);
        let mut state = replay_state_over(vec![
            batched_entry(
                HostFunctionName::HttpTypesFutureTrailersGet,
                begin_idx,
                HostResponse::HttpFutureTrailersGet(HostResponseHttpFutureTrailersGet {
                    result: Ok(None),
                }),
            ),
            poll_entry(),
        ])
        .await;

        let tracked =
            TrackedConcurrentOpSeqs::new(HashSet::from([IdentityNamespace::Batch(begin_idx)]));
        let scan = StrayEntryScan::new(tracked, None);
        let mut consumed = Vec::new();
        state
            .consume_stray_entries(|e| scan.accept(e), |idx, e| consumed.push((idx, e)))
            .await
            .unwrap();

        assert_eq!(consumed.len(), 1);
        assert!(
            state
                .try_get_oplog_entry(is_own_poll_entry)
                .await
                .unwrap()
                .is_some()
        );
    }

    /// New coverage, RDBMS result streams (§14.2): `query-stream` / `get-columns` / `get-next`
    /// share one stream's `begin_index` across three response shapes — the same many-functions-
    /// one-value structure that falsified the value-only identity key (§14.3). Each must land in
    /// its own cache slot, and a concurrently-tracked HTTP request's entry interleaved among
    /// them must defer too.
    #[test]
    async fn stray_scan_defers_rdbms_result_stream_entries() {
        use golem_common::model::oplog::types::SerializableRdbmsError;
        use golem_common::model::oplog::{
            HostResponse, HostResponseGolemRdbmsColumns, HostResponseGolemRdbmsRequest,
            HostResponseGolemRdbmsResultChunk,
        };
        let stream = OplogIndex::from_u64(30);
        let request = OplogIndex::from_u64(40);
        let entries = vec![
            batched_entry(
                HostFunctionName::RdbmsPostgresDbConnectionQueryStream,
                stream,
                HostResponse::GolemRdbmsRequest(HostResponseGolemRdbmsRequest {
                    // The scan never inspects the payload — only the identity — so the cheapest
                    // constructible variant of this shape is enough to pin participation.
                    request: Err(SerializableRdbmsError::Other("test".to_string())),
                }),
            ),
            batched_entry(
                HostFunctionName::RdbmsPostgresDbResultStreamGetColumns,
                stream,
                HostResponse::GolemRdbmsColumns(HostResponseGolemRdbmsColumns {
                    result: Ok(vec![]),
                }),
            ),
            batched_entry(
                HostFunctionName::RdbmsPostgresDbResultStreamGetNext,
                stream,
                HostResponse::GolemRdbmsResultChunk(HostResponseGolemRdbmsResultChunk {
                    result: Ok(None),
                }),
            ),
            outgoing_check_write_entry(request),
            poll_entry(),
        ];
        let expected_strays = entries.len() - 1;
        let mut state = replay_state_over(entries).await;

        let tracked = TrackedConcurrentOpSeqs::new(HashSet::from([
            IdentityNamespace::Batch(stream),
            IdentityNamespace::Batch(request),
        ]));
        let scan = StrayEntryScan::new(tracked, None);
        let mut consumed = Vec::new();
        state
            .consume_stray_entries(|e| scan.accept(e), |idx, e| consumed.push((idx, e)))
            .await
            .unwrap();

        assert_eq!(
            consumed.len(),
            expected_strays,
            "all three result-stream functions plus the interleaved HTTP entry must defer"
        );
        assert!(
            state
                .try_get_oplog_entry(is_own_poll_entry)
                .await
                .unwrap()
                .is_some()
        );
    }

    /// The §14.6 counterexample as a regression: with no batch registered as open, a batched
    /// entry must survive the scan untouched. `check_write`'s `replaying_http_batch` and
    /// post-snapshot-restore branches read the oplog directly without consulting the cache, and
    /// are reached exactly in that state — deferring an entry there would strand them.
    #[test]
    async fn untracked_batch_entries_survive_a_scan() {
        let begin_idx = OplogIndex::from_u64(42);
        let mut state =
            replay_state_over(vec![outgoing_check_write_entry(begin_idx), poll_entry()]).await;

        let scan = StrayEntryScan::new(TrackedConcurrentOpSeqs::new(HashSet::new()), None);
        let mut consumed = Vec::new();
        state
            .consume_stray_entries(|e| scan.accept(e), |idx, e| consumed.push((idx, e)))
            .await
            .unwrap();

        assert!(
            consumed.is_empty(),
            "nothing may be deferred with no open batch"
        );
        let survivor = state
            .try_get_oplog_entry(|e| {
                matches!(
                    e,
                    OplogEntry::HostCall {
                        function_name: HostFunctionName::HttpTypesOutgoingBodyStreamCheckWrite,
                        ..
                    }
                )
            })
            .await
            .unwrap();
        assert!(
            survivor.is_some(),
            "the check_write entry must still be at the cursor for its own bare read"
        );
    }
}
