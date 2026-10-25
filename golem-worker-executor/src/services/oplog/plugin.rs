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

use crate::model::ExecutionStatus;
use crate::model::event::InternalWorkerEvent;
use crate::services::component::ComponentService;
use crate::services::oplog::{CommitLevel, OpenOplogs, Oplog, OplogConstructor, OplogService};
use crate::services::shard::ShardService;
use crate::services::worker_activator::WorkerActivator;
use crate::services::worker_event::WorkerEventService;
use crate::services::worker_proxy::WorkerProxy;
use crate::services::{
    HasComponentService, HasOplogProcessorPlugin, HasShardService, HasWorkerActivator,
};
use crate::workerctx::WorkerCtx;
use anyhow::anyhow;
use async_lock::Mutex;
use async_lock::{RwLock, RwLockUpgradableReadGuard};
use async_trait::async_trait;
use golem_common::model::account::AccountId;
use golem_common::model::agent::{AgentMode, ParsedAgentId, Principal};
use golem_common::model::component::{ComponentId, ComponentRevision, InstalledPlugin};
use golem_common::model::environment::EnvironmentId;
use golem_common::model::environment_plugin_grant::EnvironmentPluginGrantId;
use golem_common::model::invocation_context::InvocationContextStack;
use golem_common::model::oplog::{
    OplogEntry, OplogIndex, PayloadId, PersistenceLevel, RawOplogPayload,
};
use golem_common::model::plugin_registration::PluginRegistrationId;
use golem_common::model::{
    AgentId, AgentInvocation, AgentMetadata, AgentStatusRecord, IdempotencyKey, InvocationStatus,
    OwnedAgentId, ScanCursor, ShardId,
};
use golem_common::read_only_lock;
use golem_common::related_span;
use golem_common::tracing::TraceOrigin;
use golem_service_base::error::worker_executor::WorkerExecutorError;
use golem_service_base::model::component::Component;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::Instrument;
use uuid::{Uuid, uuid};

/// Per-plugin live state tracked by `ForwardingOplogState` for exactly-once delivery.
/// Seeded from `AgentStatusRecord.oplog_processor_checkpoints` on construction,
/// updated synchronously after each checkpoint write.
#[derive(Clone, Debug)]
struct LivePluginState {
    target_agent_id: Option<AgentId>,
    confirmed_up_to: OplogIndex,
    sending_up_to: OplogIndex,
    send_in_progress: bool,
    last_batch_start: OplogIndex,
    /// When the current `sending_up_to` batch was (last) enqueued. `None` means either
    /// there's nothing unconfirmed, or this state was seeded from a checkpoint (e.g.
    /// after a restart) with no record of when the send happened — treated as already
    /// past the retry grace window, so its status gets checked on the very next flush.
    sending_since: Option<Instant>,
}

/// How long an enqueued-but-unconfirmed batch is given to actually complete (as observed
/// via `lookup_invocation_status`) before it's treated as failed/lost and retried. Checked
/// on every flush tick (see `plugin_max_elapsed_time`, default 5s) rather than via a single
/// long-lived wait, so a stuck batch is retried promptly instead of silently discarded.
const PENDING_BATCH_RETRY_AFTER: Duration = Duration::from_secs(60);

#[async_trait]
pub trait OplogProcessorPlugin: Send + Sync {
    /// Resolves or creates a target plugin worker for the given plugin, returning its AgentId.
    async fn resolve_target(
        &self,
        environment_id: EnvironmentId,
        plugin: &InstalledPlugin,
    ) -> Result<AgentId, WorkerExecutorError>;

    /// Enqueues oplog entries to the specified target plugin worker.
    /// Any `Ok(())` means durable delivery — the batch has been accepted.
    async fn send(
        &self,
        worker_metadata: AgentMetadata,
        plugin: &InstalledPlugin,
        target_agent_id: &AgentId,
        initial_oplog_index: OplogIndex,
        entries: Vec<OplogEntry>,
    ) -> Result<(), WorkerExecutorError>;

    /// Evicts the cached plugin worker for the given plugin if it is still `failed_target`,
    /// forcing a fresh instance to be created on the next `resolve_target` call. A failure
    /// reported against an older target never evicts a newer one.
    async fn invalidate_target(
        &self,
        environment_id: EnvironmentId,
        plugin: &InstalledPlugin,
        failed_target: &AgentId,
    );

    async fn on_shard_assignment_changed(&self) -> Result<(), WorkerExecutorError>;

    async fn is_local(&self, agent_id: &AgentId) -> Result<bool, WorkerExecutorError>;

    async fn lookup_invocation_status(
        &self,
        environment_id: EnvironmentId,
        plugin: &InstalledPlugin,
        target_agent_id: &AgentId,
        caller_account_id: AccountId,
        idempotency_key: &IdempotencyKey,
    ) -> Result<InvocationStatus, WorkerExecutorError>;
}

/// An implementation of the `OplogProcessorPlugin` trait that runs a single instance of each
/// used plugin on each worker executor node.
pub struct PerExecutorOplogProcessorPlugin<Ctx: WorkerCtx> {
    workers: Arc<RwLock<HashMap<WorkerKey, RunningPlugin>>>,
    /// Bumped by `invalidate_target` so the next `resolve_target` derives a different
    /// (fresh) plugin worker name for that key. In-memory only: after a restart every key
    /// starts at generation 0 again, so the plugin worker of a healthy plugin keeps the
    /// same identity across restarts.
    generations: Arc<std::sync::Mutex<HashMap<WorkerKey, u64>>>,
    component_service: Arc<dyn ComponentService>,
    shard_service: Arc<dyn ShardService>,
    worker_activator: Arc<dyn WorkerActivator<Ctx>>,
    worker_proxy: Arc<dyn WorkerProxy>,
}

// TODO: Once cross-environment RPC is enabled via host.wit, the cache key should
// use the component's canonical environment_id rather than the caller-provided one.
// Currently all plugin workers live in the same environment as their callers so
// the inconsistency is harmless — a cache miss just triggers a component lookup.
type WorkerKey = (EnvironmentId, PluginRegistrationId);

#[derive(Debug, Clone)]
struct RunningPlugin {
    pub owned_agent_id: OwnedAgentId,
    pub configuration: BTreeMap<String, String>,
    pub component_revision: ComponentRevision,
}

impl<Ctx: WorkerCtx> PerExecutorOplogProcessorPlugin<Ctx> {
    pub fn new(
        component_service: Arc<dyn ComponentService>,
        shard_service: Arc<dyn ShardService>,
        worker_activator: Arc<dyn WorkerActivator<Ctx>>,
        worker_proxy: Arc<dyn WorkerProxy>,
    ) -> Self {
        Self {
            workers: Arc::new(RwLock::new(HashMap::new())),
            generations: Arc::new(std::sync::Mutex::new(HashMap::new())),
            component_service,
            shard_service,
            worker_activator,
            worker_proxy,
        }
    }

    async fn resolve_plugin_worker(
        &self,
        environment_id: EnvironmentId,
        plugin: &InstalledPlugin,
    ) -> Result<RunningPlugin, WorkerExecutorError> {
        let workers = self.workers.upgradable_read().await;
        let key = (environment_id, plugin.plugin_registration_id);
        match workers.get(&key) {
            Some(running_plugin) => Ok(running_plugin.clone()),
            None => {
                let mut workers = RwLockUpgradableReadGuard::upgrade(workers).await;
                match workers.get(&key) {
                    Some(agent_id) => Ok(agent_id.clone()),
                    None => {
                        let plugin_component_id = plugin
                            .oplog_processor_component_id
                            .ok_or(anyhow!("missing oplog processor plugin component id"))?;
                        let plugin_component_revision =
                            plugin.oplog_processor_component_revision.ok_or(anyhow!(
                                "missing oplog processor plugin component revision"
                            ))?;

                        let generation = self.generation_of(&key);
                        let agent_id = self.generate_agent_id_for(
                            plugin_component_id,
                            &PluginWorkerIdentity {
                                environment_id,
                                plugin_registration_id: plugin.plugin_registration_id,
                                component_revision: plugin_component_revision,
                                generation,
                            },
                        )?;
                        // Resolve the component's authoritative environment_id.
                        let plugin_component = self
                            .component_service
                            .get_metadata(plugin_component_id, Some(plugin_component_revision))
                            .await?;
                        let owned_agent_id = OwnedAgentId {
                            environment_id: plugin_component.environment_id,
                            agent_id: agent_id.clone(),
                        };
                        let running_plugin = RunningPlugin {
                            owned_agent_id: owned_agent_id.clone(),
                            configuration: plugin.parameters.clone(),
                            component_revision: plugin_component_revision,
                        };
                        workers.insert(key, running_plugin.clone());
                        Ok(running_plugin)
                    }
                }
            }
        }
    }

    fn generation_of(&self, key: &WorkerKey) -> u64 {
        let generations = self.generations.lock().unwrap();
        generations.get(key).copied().unwrap_or(0)
    }

    fn generate_agent_id_for(
        &self,
        plugin_component_id: ComponentId,
        identity: &PluginWorkerIdentity,
    ) -> Result<AgentId, WorkerExecutorError> {
        let current_assignment = self.shard_service.current_assignment()?;
        Ok(generate_local_plugin_agent_id(
            plugin_component_id,
            identity,
            &current_assignment.shard_ids,
            current_assignment.number_of_shards,
        ))
    }
}

/// Everything that identifies the one plugin worker an executor runs for a plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PluginWorkerIdentity {
    /// Environment of the source agents served (the `WorkerKey`'s environment).
    environment_id: EnvironmentId,
    plugin_registration_id: PluginRegistrationId,
    /// The plugin component revision the worker runs: a new revision gets a new worker,
    /// never an old worker silently running an outdated revision.
    component_revision: ComponentRevision,
    /// See `PerExecutorOplogProcessorPlugin::generations`.
    generation: u64,
}

/// Derives the name of the plugin worker this executor runs for `identity`.
///
/// The name is deterministic: the first candidate (`n = 0, 1, ...`) whose shard is
/// owned by this executor. With an unchanged shard assignment every restart derives the
/// SAME worker, so a restart neither orphans the previous plugin worker (and its oplog)
/// nor leaves source agents retrying batches against a worker nobody uses any more.
/// Executors owning disjoint shard sets derive different workers. If `force_in_shard`
/// holds no shard below `number_of_shards`, the first candidate is used.
fn generate_local_plugin_agent_id(
    component_id: ComponentId,
    identity: &PluginWorkerIdentity,
    force_in_shard: &HashSet<ShardId>,
    number_of_shards: usize,
) -> AgentId {
    let candidate = |n: u64| AgentId {
        component_id,
        agent_id: plugin_worker_name(identity, n),
    };
    let in_range = |shard: &ShardId| (0..number_of_shards as i64).contains(&shard.value());
    if number_of_shards == 0 || !force_in_shard.iter().any(in_range) {
        return candidate(0);
    }
    (0u64..)
        .map(candidate)
        .find(|agent_id| {
            force_in_shard.contains(&ShardId::from_agent_id(agent_id, number_of_shards))
        })
        .expect("an owned shard is hit by some candidate")
}

fn plugin_worker_name(identity: &PluginWorkerIdentity, candidate: u64) -> String {
    let mut buf = Vec::with_capacity(96);
    buf.extend_from_slice(b"oplog-proc-worker-v1\0");
    buf.extend_from_slice(identity.environment_id.0.as_bytes());
    buf.extend_from_slice(identity.plugin_registration_id.0.as_bytes());
    buf.extend_from_slice(&identity.component_revision.get().to_be_bytes());
    buf.extend_from_slice(&identity.generation.to_be_bytes());
    buf.extend_from_slice(&candidate.to_be_bytes());
    Uuid::new_v5(&OPLOG_PROC_NS, &buf).to_string()
}

#[async_trait]
impl<Ctx: WorkerCtx> OplogProcessorPlugin for PerExecutorOplogProcessorPlugin<Ctx> {
    async fn resolve_target(
        &self,
        environment_id: EnvironmentId,
        plugin: &InstalledPlugin,
    ) -> Result<AgentId, WorkerExecutorError> {
        let running_plugin = self.resolve_plugin_worker(environment_id, plugin).await?;
        Ok(running_plugin.owned_agent_id.agent_id)
    }

    async fn send(
        &self,
        worker_metadata: AgentMetadata,
        plugin: &InstalledPlugin,
        target_agent_id: &AgentId,
        initial_oplog_index: OplogIndex,
        entries: Vec<OplogEntry>,
    ) -> Result<(), WorkerExecutorError> {
        let running_plugin = self
            .resolve_plugin_worker(worker_metadata.environment_id, plugin)
            .await?;

        let batch_last_index = if entries.is_empty() {
            initial_oplog_index
        } else {
            initial_oplog_index.range_end(entries.len() as u64)
        };
        let idempotency_key = oplog_processor_idempotency_key(
            &worker_metadata.owned_agent_id().agent_id,
            &plugin.environment_plugin_grant_id,
            initial_oplog_index,
            batch_last_index,
        );

        let is_local = self.is_local(target_agent_id).await?;

        if is_local {
            // Local path: activate and invoke directly.
            //
            // Must use the target plugin worker's OWN environment_id (already resolved
            // above via resolve_plugin_worker, from the plugin component's authoritative
            // metadata) — NOT worker_metadata.environment_id, which belongs to the SOURCE
            // agent whose oplog is being flushed. When the plugin component lives in a
            // different environment than the source agent (the common case for a shared,
            // per-executor oplog-processor plugin), using the source's environment_id here
            // constructs an OwnedAgentId that disagrees with the one used to key this
            // worker's own oplog-payload blob storage namespace on recovery
            // (FileSystemBlobStorage's BlobStorageNamespace::OplogPayload path includes
            // environment_id). The mismatch is invisible while the worker stays resident
            // in memory (the live in-process payload cache masks it), but on any cold
            // recovery (process restart, eviction) `download_raw_payload` looks up the
            // wrong environment_id directory and permanently fails with "Payload not
            // found" — even though the payload is physically present under the correct
            // (plugin's own) environment_id. This traps the worker in `recover_instance_state`
            // forever, so every future flush to this shared plugin worker enqueues
            // successfully but is never confirmed complete.
            let target_owned = OwnedAgentId::new(
                running_plugin.owned_agent_id.environment_id,
                target_agent_id,
            );

            let worker = self
                .worker_activator
                .get_or_create_running(
                    &target_owned,
                    None,
                    Vec::new(),
                    Some(running_plugin.component_revision),
                    None,
                    &InvocationContextStack::fresh(),
                    Principal::anonymous(),
                )
                .await?;

            let account_id = worker_metadata.created_by;
            let _result = worker
                .invoke(AgentInvocation::ProcessOplogEntries {
                    idempotency_key,
                    account_id,
                    config: running_plugin
                        .configuration
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                    metadata: worker_metadata.into(),
                    first_entry_index: initial_oplog_index,
                    entries,
                })
                .await?;
        } else {
            // Remote path: route through worker service to the executor
            // that currently owns the recorded target worker
            let proto_metadata = {
                use std::collections::HashMap;
                let latest_status = &worker_metadata.last_known_status;
                golem_api_grpc::proto::golem::worker::AgentMetadata {
                    agent_id: Some(worker_metadata.agent_id.clone().into()),
                    environment_id: Some(worker_metadata.environment_id.into()),
                    env: HashMap::from_iter(worker_metadata.env.iter().cloned()),
                    config: worker_metadata
                        .config
                        .clone()
                        .into_iter()
                        .map(Into::into)
                        .collect(),
                    created_by: Some(worker_metadata.created_by.into()),
                    component_revision: latest_status.component_revision.into(),
                    status: Into::<golem_api_grpc::proto::golem::worker::AgentStatus>::into(
                        latest_status.status,
                    )
                    .into(),
                    retry_count: latest_status
                        .current_retry_state
                        .iter()
                        .max_by_key(|(idx, _)| **idx)
                        .map(|(_, state)| state.retry_count())
                        .unwrap_or_default(),
                    pending_invocation_count: latest_status.pending_invocations.len() as u64,
                    updates: Vec::new(),
                    created_at: Some(worker_metadata.created_at.into()),
                    last_error: None,
                    component_size: latest_status.component_size,
                    total_linear_memory_size: latest_status.total_linear_memory_size,
                    owned_resources: Vec::new(),
                    active_plugins: latest_status
                        .active_plugins
                        .iter()
                        .map(|id| (*id).into())
                        .collect(),
                    skipped_regions: latest_status
                        .skipped_regions
                        .regions()
                        .map(|region| region.clone().into())
                        .collect(),
                    deleted_regions: latest_status
                        .deleted_regions
                        .regions()
                        .map(|region| region.clone().into())
                        .collect(),
                }
            };
            let proto_entries: Vec<golem_api_grpc::proto::golem::worker::RawOplogEntry> = entries
                .into_iter()
                .map(golem_api_grpc::proto::golem::worker::RawOplogEntry::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    WorkerExecutorError::unknown(format!(
                        "Failed to convert oplog entries to proto: {e}"
                    ))
                })?;

            self.worker_proxy
                .process_oplog_entries(
                    target_agent_id,
                    running_plugin.owned_agent_id.environment_id,
                    running_plugin.component_revision,
                    idempotency_key,
                    worker_metadata.created_by,
                    running_plugin
                        .configuration
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                    proto_metadata,
                    initial_oplog_index,
                    proto_entries,
                )
                .await
                .map_err(|e| {
                    WorkerExecutorError::unknown(format!(
                        "Remote oplog processor delivery failed: {e}"
                    ))
                })?;
        }

        Ok(())
    }

    async fn invalidate_target(
        &self,
        environment_id: EnvironmentId,
        plugin: &InstalledPlugin,
        failed_target: &AgentId,
    ) {
        let key = (environment_id, plugin.plugin_registration_id);
        let mut workers = self.workers.write().await;
        if workers
            .get(&key)
            .is_some_and(|running| running.owned_agent_id.agent_id == *failed_target)
        {
            workers.remove(&key);
            *self.generations.lock().unwrap().entry(key).or_insert(0) += 1;
        }
    }

    async fn is_local(&self, agent_id: &AgentId) -> Result<bool, WorkerExecutorError> {
        let assignment = self.shard_service.current_assignment()?;
        let shard_id = ShardId::from_agent_id(agent_id, assignment.number_of_shards);
        Ok(assignment.shard_ids.contains(&shard_id))
    }

    async fn on_shard_assignment_changed(&self) -> Result<(), WorkerExecutorError> {
        let new_assignment = self.shard_service.current_assignment()?;

        let mut workers = self.workers.write().await;
        let keys = workers.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let entry = workers.entry(key);
            match entry {
                Entry::Occupied(entry) => {
                    let shard_id = ShardId::from_agent_id(
                        &entry.get().owned_agent_id.agent_id,
                        new_assignment.number_of_shards,
                    );
                    if new_assignment.shard_ids.contains(&shard_id) {
                        continue;
                    } else {
                        // The worker is removed from the in-memory map, but we leave it running to finish any pending invocations.
                        // As there is no other reference to this worker id after dropping it here, there won't be any new invocations
                        // sent to it, and it will eventually suspend and got archived.
                        entry.remove();
                    }
                }
                Entry::Vacant(_) => {}
            }
        }

        Ok(())
    }

    async fn lookup_invocation_status(
        &self,
        environment_id: EnvironmentId,
        _plugin: &InstalledPlugin,
        target_agent_id: &AgentId,
        caller_account_id: AccountId,
        idempotency_key: &IdempotencyKey,
    ) -> Result<InvocationStatus, WorkerExecutorError> {
        self.worker_proxy
            .lookup_invocation_status(
                target_agent_id,
                idempotency_key.clone(),
                caller_account_id,
                Some(environment_id),
            )
            .await
            .map_err(|e| {
                WorkerExecutorError::unknown(format!("Lookup invocation status failed: {e}"))
            })
    }
}

impl<Ctx: WorkerCtx> Clone for PerExecutorOplogProcessorPlugin<Ctx> {
    fn clone(&self) -> Self {
        Self {
            workers: self.workers.clone(),
            generations: self.generations.clone(),
            component_service: self.component_service.clone(),
            shard_service: self.shard_service.clone(),
            worker_activator: self.worker_activator.clone(),
            worker_proxy: self.worker_proxy.clone(),
        }
    }
}

impl<Ctx: WorkerCtx> HasComponentService for PerExecutorOplogProcessorPlugin<Ctx> {
    fn component_service(&self) -> Arc<dyn ComponentService> {
        self.component_service.clone()
    }
}

impl<Ctx: WorkerCtx> HasShardService for PerExecutorOplogProcessorPlugin<Ctx> {
    fn shard_service(&self) -> Arc<dyn ShardService> {
        self.shard_service.clone()
    }
}

impl<Ctx: WorkerCtx> HasWorkerActivator<Ctx> for PerExecutorOplogProcessorPlugin<Ctx> {
    fn worker_activator(&self) -> Arc<dyn WorkerActivator<Ctx>> {
        self.worker_activator.clone()
    }
}

impl<Ctx: WorkerCtx> HasOplogProcessorPlugin for PerExecutorOplogProcessorPlugin<Ctx> {
    fn oplog_processor_plugin(&self) -> Arc<dyn OplogProcessorPlugin> {
        Arc::new(self.clone())
    }
}

#[derive(Clone)]
struct CreateOplogConstructor {
    owned_agent_id: OwnedAgentId,
    agent_mode: AgentMode,
    initial_entry: Option<OplogEntry>,
    inner: Arc<dyn OplogService>,
    last_oplog_index: Option<OplogIndex>,
    oplog_plugins: Arc<dyn OplogProcessorPlugin>,
    components: Arc<dyn ComponentService>,
    initial_worker_metadata: AgentMetadata,
    last_known_status: read_only_lock::tokio::ReadOnlyLock<AgentStatusRecord>,
    execution_status: read_only_lock::std::ReadOnlyLock<ExecutionStatus>,
    plugin_max_commit_count: usize,
    plugin_max_elapsed_time: Duration,
    plugin_max_batch_entries: u64,
}

impl CreateOplogConstructor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        owned_agent_id: OwnedAgentId,
        agent_mode: AgentMode,
        initial_entry: Option<OplogEntry>,
        inner: Arc<dyn OplogService>,
        last_oplog_index: Option<OplogIndex>,
        oplog_plugins: Arc<dyn OplogProcessorPlugin>,
        components: Arc<dyn ComponentService>,
        initial_worker_metadata: AgentMetadata,
        last_known_status: read_only_lock::tokio::ReadOnlyLock<AgentStatusRecord>,
        execution_status: read_only_lock::std::ReadOnlyLock<ExecutionStatus>,
        plugin_max_commit_count: usize,
        plugin_max_elapsed_time: Duration,
        plugin_max_batch_entries: u64,
    ) -> Self {
        Self {
            owned_agent_id,
            agent_mode,
            initial_entry,
            inner,
            last_oplog_index,
            oplog_plugins,
            components,
            initial_worker_metadata,
            last_known_status,
            execution_status,
            plugin_max_commit_count,
            plugin_max_elapsed_time,
            plugin_max_batch_entries,
        }
    }
}

#[async_trait]
impl OplogConstructor for CreateOplogConstructor {
    async fn create_oplog(self, close: Box<dyn FnOnce() + Send + Sync>) -> Arc<dyn Oplog> {
        let last_oplog_index = match self.last_oplog_index {
            Some(idx) => idx,
            None => {
                self.inner
                    .get_last_index(&self.owned_agent_id, self.agent_mode)
                    .await
            }
        };
        let inner = if let Some(initial_entry) = self.initial_entry {
            self.inner
                .create(
                    &self.owned_agent_id,
                    self.agent_mode,
                    initial_entry,
                    self.initial_worker_metadata.clone(),
                    self.last_known_status.clone(),
                    self.execution_status.clone(),
                )
                .await
        } else {
            self.inner
                .open(
                    &self.owned_agent_id,
                    self.agent_mode,
                    Some(last_oplog_index),
                    self.initial_worker_metadata.clone(),
                    self.last_known_status.clone(),
                    self.execution_status.clone(),
                )
                .await
        };

        Arc::new(
            ForwardingOplog::new(
                inner,
                self.oplog_plugins,
                self.components,
                self.initial_worker_metadata,
                self.last_known_status,
                last_oplog_index,
                close,
                self.plugin_max_commit_count,
                self.plugin_max_elapsed_time,
                self.plugin_max_batch_entries,
            )
            .await,
        )
    }
}

pub struct ForwardingOplogService {
    pub inner: Arc<dyn OplogService>,
    oplogs: OpenOplogs,

    oplog_plugins: Arc<dyn OplogProcessorPlugin>,
    components: Arc<dyn ComponentService>,
    plugin_max_commit_count: usize,
    plugin_max_elapsed_time: Duration,
    plugin_max_batch_entries: u64,
}

impl ForwardingOplogService {
    pub fn new(
        inner: Arc<dyn OplogService>,
        oplog_plugins: Arc<dyn OplogProcessorPlugin>,
        components: Arc<dyn ComponentService>,
        plugin_max_commit_count: usize,
        plugin_max_elapsed_time: Duration,
        plugin_max_batch_entries: u64,
    ) -> Self {
        Self {
            inner,
            oplogs: OpenOplogs::new("forwarding_oplog_service"),
            oplog_plugins,
            components,
            plugin_max_commit_count,
            plugin_max_elapsed_time,
            plugin_max_batch_entries,
        }
    }
}

impl Debug for ForwardingOplogService {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForwardingOplogService").finish()
    }
}

#[async_trait]
impl OplogService for ForwardingOplogService {
    async fn create(
        &self,
        owned_agent_id: &OwnedAgentId,
        agent_mode: AgentMode,
        initial_entry: OplogEntry,
        initial_worker_metadata: AgentMetadata,
        last_known_status: read_only_lock::tokio::ReadOnlyLock<AgentStatusRecord>,
        execution_status: read_only_lock::std::ReadOnlyLock<ExecutionStatus>,
    ) -> Arc<dyn Oplog + 'static> {
        self.oplogs
            .get_or_open(
                &owned_agent_id.agent_id,
                CreateOplogConstructor::new(
                    owned_agent_id.clone(),
                    agent_mode,
                    Some(initial_entry),
                    self.inner.clone(),
                    Some(OplogIndex::INITIAL),
                    self.oplog_plugins.clone(),
                    self.components.clone(),
                    initial_worker_metadata,
                    last_known_status,
                    execution_status,
                    self.plugin_max_commit_count,
                    self.plugin_max_elapsed_time,
                    self.plugin_max_batch_entries,
                ),
            )
            .await
    }

    async fn open(
        &self,
        owned_agent_id: &OwnedAgentId,
        agent_mode: AgentMode,
        last_oplog_index: Option<OplogIndex>,
        initial_worker_metadata: AgentMetadata,
        last_known_status: read_only_lock::tokio::ReadOnlyLock<AgentStatusRecord>,
        execution_status: read_only_lock::std::ReadOnlyLock<ExecutionStatus>,
    ) -> Arc<dyn Oplog + 'static> {
        self.oplogs
            .get_or_open(
                &owned_agent_id.agent_id,
                CreateOplogConstructor::new(
                    owned_agent_id.clone(),
                    agent_mode,
                    None,
                    self.inner.clone(),
                    last_oplog_index,
                    self.oplog_plugins.clone(),
                    self.components.clone(),
                    initial_worker_metadata,
                    last_known_status,
                    execution_status,
                    self.plugin_max_commit_count,
                    self.plugin_max_elapsed_time,
                    self.plugin_max_batch_entries,
                ),
            )
            .await
    }

    async fn get_last_index(
        &self,
        owned_agent_id: &OwnedAgentId,
        agent_mode: AgentMode,
    ) -> OplogIndex {
        self.inner.get_last_index(owned_agent_id, agent_mode).await
    }

    async fn delete(&self, owned_agent_id: &OwnedAgentId, agent_mode: AgentMode) {
        self.inner.delete(owned_agent_id, agent_mode).await
    }

    async fn read(
        &self,
        owned_agent_id: &OwnedAgentId,
        agent_mode: AgentMode,
        idx: OplogIndex,
        n: u64,
    ) -> BTreeMap<OplogIndex, OplogEntry> {
        self.inner.read(owned_agent_id, agent_mode, idx, n).await
    }

    async fn exists(&self, owned_agent_id: &OwnedAgentId, agent_mode: AgentMode) -> bool {
        self.inner.exists(owned_agent_id, agent_mode).await
    }

    async fn scan_for_component(
        &self,
        environment_id: &EnvironmentId,
        component_id: &ComponentId,
        modes: Option<AgentMode>,
        cursor: ScanCursor,
        count: u64,
    ) -> Result<(ScanCursor, Vec<OwnedAgentId>), WorkerExecutorError> {
        self.inner
            .scan_for_component(environment_id, component_id, modes, cursor, count)
            .await
    }

    async fn upload_raw_payload(
        &self,
        owned_agent_id: &OwnedAgentId,
        agent_mode: AgentMode,
        data: Vec<u8>,
    ) -> Result<RawOplogPayload, String> {
        self.inner
            .upload_raw_payload(owned_agent_id, agent_mode, data)
            .await
    }

    async fn download_raw_payload(
        &self,
        owned_agent_id: &OwnedAgentId,
        agent_mode: AgentMode,
        payload_id: PayloadId,
        md5_hash: Vec<u8>,
    ) -> Result<Vec<u8>, String> {
        self.inner
            .download_raw_payload(owned_agent_id, agent_mode, payload_id, md5_hash)
            .await
    }
}

/// A wrapper for `Oplog` that periodically sends buffered oplog entries to oplog processor plugins
pub struct ForwardingOplog {
    inner: Arc<dyn Oplog>,
    state: Arc<Mutex<ForwardingOplogState>>,
    max_commit_count: usize,
    timer: Option<JoinHandle<()>>,
    close_fn: Option<Box<dyn FnOnce() + Send + Sync>>,
}

impl ForwardingOplog {
    pub async fn new(
        inner: Arc<dyn Oplog>,
        oplog_plugins: Arc<dyn OplogProcessorPlugin>,
        components: Arc<dyn ComponentService>,
        initial_worker_metadata: AgentMetadata,
        last_known_status: read_only_lock::tokio::ReadOnlyLock<AgentStatusRecord>,
        last_oplog_idx: OplogIndex,
        close_fn: Box<dyn FnOnce() + Send + Sync>,
        max_commit_count: usize,
        max_elapsed_time: Duration,
        max_batch_entries: u64,
    ) -> Self {
        // Seed per-plugin live state from the current status snapshot (not the
        // potentially stale initial_worker_metadata.last_known_status)
        let plugin_state = {
            let seed_status = last_known_status.read().await;
            let mut state = HashMap::new();
            for (grant_id, cp) in &seed_status.oplog_processor_checkpoints {
                state.insert(
                    *grant_id,
                    LivePluginState {
                        target_agent_id: cp.target_agent_id.clone(),
                        confirmed_up_to: cp.confirmed_up_to,
                        sending_up_to: cp.sending_up_to,
                        send_in_progress: false,
                        last_batch_start: cp.last_batch_start,
                        // Unknown how long ago this was sent (e.g. seeded after a
                        // restart) — treated as already past the retry grace window,
                        // so any unconfirmed range gets its status checked immediately.
                        sending_since: None,
                    },
                );
            }
            // Also seed entries for active plugins that have no checkpoint yet (active from Create)
            for grant_id in &seed_status.active_plugins {
                state.entry(*grant_id).or_insert(LivePluginState {
                    target_agent_id: None,
                    confirmed_up_to: OplogIndex::NONE,
                    sending_up_to: OplogIndex::NONE,
                    send_in_progress: false,
                    last_batch_start: OplogIndex::NONE,
                    sending_since: None,
                });
            }
            state
        };

        // Captured here, where the caller's context is still current: each flush
        // tick links back to it rather than running inside it.
        let flush_origin = TraceOrigin::capture_current();
        let agent_id = initial_worker_metadata.agent_id.clone();
        let state = Arc::new(Mutex::new(ForwardingOplogState {
            buffer: VecDeque::new(),
            buffer_start_idx: last_oplog_idx.next(),
            commit_count: 0,
            last_send: Instant::now(),
            oplog_plugins,
            initial_worker_metadata,
            last_known_status,
            last_oplog_idx,
            last_committed_idx: last_oplog_idx,
            components,
            inner: inner.clone(),
            plugin_state,
            pending_direct_commits: BTreeMap::new(),
            worker_event_service: None,
            max_batch_entries,
        }));

        let timer = tokio::spawn({
            let state = state.clone();
            async move {
                loop {
                    tokio::time::sleep(max_elapsed_time).await;
                    async {
                        let mut state = state.lock().await;
                        state.try_locality_recovery().await;
                        state.try_flush().await;
                    }
                    .instrument(related_span!(
                        flush_origin,
                        tracing::Level::INFO,
                        "oplog_forwarding_flush",
                        agent_id = %agent_id
                    ))
                    .await;
                }
            }
        });
        Self {
            inner,
            state,
            max_commit_count,
            timer: Some(timer),
            close_fn: Some(close_fn),
        }
    }

    pub async fn set_worker_event_service(
        &self,
        worker_event_service: Arc<dyn WorkerEventService>,
    ) {
        let mut state = self.state.lock().await;
        state.worker_event_service = Some(worker_event_service);
    }
}

impl Debug for ForwardingOplog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForwardingOplog").finish()
    }
}

impl Drop for ForwardingOplog {
    fn drop(&mut self) {
        if let Some(close_fn) = self.close_fn.take() {
            close_fn();
        }
        if let Some(timer) = self.timer.take() {
            timer.abort();
        }
    }
}

#[async_trait]
impl Oplog for ForwardingOplog {
    async fn add(&self, entry: OplogEntry) -> OplogIndex {
        let mut state = self.state.lock().await;
        state.buffer.push_back(entry.clone());
        state.last_oplog_idx = state.last_oplog_idx.next();
        self.inner.add(entry).await
    }

    async fn drop_prefix(&self, last_dropped_id: OplogIndex) -> u64 {
        self.inner.drop_prefix(last_dropped_id).await
    }

    async fn commit(&self, level: CommitLevel) -> BTreeMap<OplogIndex, OplogEntry> {
        let mut state = self.state.lock().await;
        let mut result = self.inner.commit(level).await;
        // Update last_committed_idx from committed entries
        if let Some(max_idx) = result.keys().max()
            && *max_idx > state.last_committed_idx
        {
            state.last_committed_idx = *max_idx;
        }
        state.commit_count += 1;
        if state.commit_count >= self.max_commit_count {
            state.try_flush().await;
        }
        // Merge entries committed directly to inner during flush
        // so the Worker folds them into AgentStatusRecord
        result.append(&mut state.pending_direct_commits);
        result
    }

    async fn current_oplog_index(&self) -> OplogIndex {
        self.inner.current_oplog_index().await
    }

    async fn last_added_non_hint_entry(&self) -> Option<OplogIndex> {
        self.inner.last_added_non_hint_entry().await
    }

    async fn wait_for_replicas(&self, replicas: u8, timeout: Duration) -> bool {
        self.inner.wait_for_replicas(replicas, timeout).await
    }

    async fn read(&self, oplog_index: OplogIndex) -> OplogEntry {
        self.inner.read(oplog_index).await
    }

    async fn read_many(&self, oplog_index: OplogIndex, n: u64) -> BTreeMap<OplogIndex, OplogEntry> {
        self.inner.read_many(oplog_index, n).await
    }

    async fn length(&self) -> u64 {
        self.inner.length().await
    }

    async fn upload_raw_payload(&self, data: Vec<u8>) -> Result<RawOplogPayload, String> {
        self.inner.upload_raw_payload(data).await
    }

    async fn download_raw_payload(
        &self,
        payload_id: PayloadId,
        md5_hash: Vec<u8>,
    ) -> Result<Vec<u8>, String> {
        self.inner.download_raw_payload(payload_id, md5_hash).await
    }

    async fn switch_persistence_level(&self, mode: PersistenceLevel) {
        self.inner.switch_persistence_level(mode).await;
    }

    fn inner(&self) -> Option<Arc<dyn Oplog>> {
        Some(self.inner.clone())
    }
}

struct ForwardingOplogState {
    buffer: VecDeque<OplogEntry>,
    /// Oplog index corresponding to the first entry in `buffer`.
    buffer_start_idx: OplogIndex,
    commit_count: usize,
    last_send: Instant,
    oplog_plugins: Arc<dyn OplogProcessorPlugin>,
    initial_worker_metadata: AgentMetadata,
    last_known_status: read_only_lock::tokio::ReadOnlyLock<AgentStatusRecord>,
    last_oplog_idx: OplogIndex,
    last_committed_idx: OplogIndex,
    components: Arc<dyn ComponentService>,
    inner: Arc<dyn Oplog>,
    plugin_state: HashMap<EnvironmentPluginGrantId, LivePluginState>,
    /// Entries committed directly to inner (bypassing `ForwardingOplog::commit()`),
    /// to be surfaced in the next `ForwardingOplog::commit()` return value
    /// so the Worker folds them into `AgentStatusRecord`.
    pending_direct_commits: BTreeMap<OplogIndex, OplogEntry>,
    worker_event_service: Option<Arc<dyn WorkerEventService>>,
    /// Upper bound on the number of oplog entries shipped to a plugin in one batch
    /// (`oplog.plugin_max_batch_entries`). A backlog is drained one bounded batch at a
    /// time, so memory per in-flight batch stays bounded however far a plugin fell behind.
    max_batch_entries: u64,
}

impl ForwardingOplogState {
    /// Cursor-driven flush: for each plugin with unsent entries, send a batch.
    /// Handles retries of in-flight batches, first-send target resolution,
    /// pre-send and confirmation checkpoint writes.
    ///
    /// Entries are always read from the persisted oplog (canonical source) to avoid
    /// buffer/index drift caused by checkpoint entries injected during flush.
    pub async fn try_flush(&mut self) {
        let status = self.last_known_status.read().await.clone();
        let flush_set = self.reconcile_plugin_state(&status);

        if flush_set.is_empty() {
            self.finish_empty_flush();
        } else if let Some((metadata, component_metadata)) =
            self.prepare_flush_context(&status).await
        {
            let committed_tail = self.last_committed_idx;

            for grant_id in flush_set {
                self.flush_one_plugin(grant_id, committed_tail, &metadata, &component_metadata)
                    .await;
            }

            self.prune_buffer();
            self.finish_flush_cycle();
        }
    }

    /// Sync plugin_state with the current status: add newly active plugins,
    /// keep existing local state for already-tracked plugins, remove stale
    /// entries that are neither active nor in-flight.
    /// Returns the flush set (active ∪ in-flight plugin grant IDs).
    fn reconcile_plugin_state(
        &mut self,
        status: &AgentStatusRecord,
    ) -> Vec<EnvironmentPluginGrantId> {
        for grant_id in &status.active_plugins {
            if let Entry::Vacant(e) = self.plugin_state.entry(*grant_id) {
                let live = if let Some(cp) = status.oplog_processor_checkpoints.get(grant_id) {
                    LivePluginState {
                        target_agent_id: cp.target_agent_id.clone(),
                        confirmed_up_to: cp.confirmed_up_to,
                        sending_up_to: cp.sending_up_to,
                        send_in_progress: false,
                        last_batch_start: cp.last_batch_start,
                        sending_since: None,
                    }
                } else {
                    LivePluginState {
                        target_agent_id: None,
                        confirmed_up_to: OplogIndex::NONE,
                        sending_up_to: OplogIndex::NONE,
                        send_in_progress: false,
                        last_batch_start: OplogIndex::NONE,
                        sending_since: None,
                    }
                };
                e.insert(live);
            }
        }

        self.plugin_state.retain(|grant_id, state| {
            status.active_plugins.contains(grant_id) || state.sending_up_to > state.confirmed_up_to
        });

        self.plugin_state
            .iter()
            .filter(|(id, state)| {
                status.active_plugins.contains(id) || state.sending_up_to > state.confirmed_up_to
            })
            .map(|(id, _)| *id)
            .collect()
    }

    /// Build the AgentMetadata and fetch component metadata needed for the flush.
    /// Returns `None` if component metadata cannot be retrieved.
    async fn prepare_flush_context(
        &self,
        status: &AgentStatusRecord,
    ) -> Option<(AgentMetadata, Component)> {
        let metadata = AgentMetadata {
            last_known_status: status.clone(),
            ..self.initial_worker_metadata.clone()
        };

        match self
            .components
            .get_metadata(
                metadata.owned_agent_id().component_id(),
                Some(status.component_revision),
            )
            .await
        {
            Ok(component_metadata) => Some((metadata, component_metadata)),
            Err(err) => {
                tracing::error!(
                    "Failed to get component metadata for oplog processor flush: {err}"
                );
                None
            }
        }
    }

    /// Flush a single plugin: resolve target, read entries, write checkpoints, send.
    ///
    /// Uses enqueue-and-confirm: after a successful `send()` (which only enqueues
    /// the invocation), we immediately write the confirmation checkpoint. A background
    /// monitoring task is spawned to observe errors and emit plugin_error events.
    async fn flush_one_plugin(
        &mut self,
        grant_id: EnvironmentPluginGrantId,
        committed_tail: OplogIndex,
        metadata: &AgentMetadata,
        component_metadata: &Component,
    ) {
        let live = match self.plugin_state.get(&grant_id) {
            Some(s) if !s.send_in_progress => s.clone(),
            _ => return,
        };

        let agent_type =
            ParsedAgentId::parse_agent_type_name(&self.initial_worker_metadata.agent_id.agent_id)
                .ok();
        let plugin = match agent_type
            .as_ref()
            .and_then(|t| component_metadata.metadata.agent_type_plugins(t))
            .and_then(|plugins| {
                plugins
                    .iter()
                    .find(|p| p.environment_plugin_grant_id == grant_id)
            }) {
            Some(p) => p.clone(),
            None => return,
        };

        // A previously-sent batch is still unconfirmed. `confirmed_up_to` only ever
        // advances on OBSERVED completion (never on successful enqueue — see the
        // `Ok(())` arm of `send()` below) so this is reached both right after a normal
        // send and on every subsequent flush tick until the batch is confirmed one way
        // or another. Check its real status here, synchronously, using the `&mut self`
        // this function already has — this is what lets a lost/failed/never-run
        // invocation actually get retried by the batch selection below, instead of
        // being silently and permanently skipped.
        let mut live = live;
        if live.sending_up_to > live.confirmed_up_to
            && let Some(target_agent_id) = live.target_agent_id.clone()
        {
            let idempotency_key = oplog_processor_idempotency_key(
                &self.initial_worker_metadata.agent_id,
                &grant_id,
                live.last_batch_start,
                live.sending_up_to,
            );
            match self
                .oplog_plugins
                .lookup_invocation_status(
                    metadata.environment_id,
                    &plugin,
                    &target_agent_id,
                    metadata.created_by,
                    &idempotency_key,
                )
                .await
            {
                Ok(InvocationStatus::Complete) => {
                    self.write_checkpoint(
                        grant_id,
                        &target_agent_id,
                        live.sending_up_to,
                        live.sending_up_to,
                        live.last_batch_start,
                    )
                    .await;
                    live.confirmed_up_to = live.sending_up_to;
                    if let Some(s) = self.plugin_state.get_mut(&grant_id) {
                        s.confirmed_up_to = live.sending_up_to;
                    }
                }
                other => {
                    let still_pending = matches!(other, Ok(InvocationStatus::Pending));
                    let elapsed = live
                        .sending_since
                        .map(|t| t.elapsed())
                        .unwrap_or(Duration::MAX);
                    if still_pending && elapsed < PENDING_BATCH_RETRY_AFTER {
                        // Genuinely still in flight and within the grace window — leave
                        // it be, don't attempt to send anything new on top of it, and
                        // check again on the next flush tick.
                        return;
                    }
                    tracing::warn!(
                        plugin_name = plugin.plugin_name,
                        source_agent = %self.initial_worker_metadata.agent_id,
                        batch_start = %live.last_batch_start,
                        batch_end = %live.sending_up_to,
                        ?other,
                        elapsed_secs = elapsed.as_secs(),
                        "Oplog processor: unconfirmed batch did not complete, will retry"
                    );
                    if let Some(event_service) = &self.worker_event_service {
                        event_service.emit_event(
                            InternalWorkerEvent::plugin_error(
                                &format!("{grant_id}"),
                                &format!(
                                    "Batch [{}..{}] did not complete ({other:?}), retrying",
                                    live.last_batch_start, live.sending_up_to
                                ),
                            ),
                            true,
                        );
                    }
                    // Fall through — the batch selection below re-selects this exact
                    // range as a retry, since sending_up_to is still > confirmed_up_to.
                }
            }
        }

        let batch = select_batch(
            live.confirmed_up_to,
            live.sending_up_to,
            committed_tail,
            self.max_batch_entries,
        );

        let (batch_start, batch_end, is_retry) = match batch {
            Some(b) => b,
            None => return,
        };

        tracing::info!(
            plugin_name = plugin.plugin_name,
            source_agent = %self.initial_worker_metadata.agent_id,
            batch_start = %batch_start,
            batch_end = %batch_end,
            is_retry,
            "Oplog processor: flushing batch to plugin"
        );

        let recorded_target = match &live.target_agent_id {
            Some(recorded)
                if !self
                    .is_stale_local_target(metadata, &plugin, recorded)
                    .await =>
            {
                Some(recorded.clone())
            }
            _ => None,
        };
        let target_agent_id = if let Some(id) = recorded_target {
            id
        } else {
            match self
                .oplog_plugins
                .resolve_target(metadata.environment_id, &plugin)
                .await
            {
                Ok(id) => {
                    tracing::info!(
                        plugin_name = plugin.plugin_name,
                        target_agent = %id,
                        previous_target = ?live.target_agent_id,
                        "Oplog processor: resolved target plugin worker"
                    );
                    // Cursors are kept as they are: an unconfirmed range stays
                    // unconfirmed and is (re)sent to the new target below.
                    self.write_checkpoint(
                        grant_id,
                        &id,
                        live.confirmed_up_to,
                        live.sending_up_to,
                        live.last_batch_start,
                    )
                    .await;
                    if let Some(s) = self.plugin_state.get_mut(&grant_id) {
                        s.target_agent_id = Some(id.clone());
                    }
                    id
                }
                Err(err) => {
                    tracing::error!("Failed to resolve target for plugin {grant_id}: {err}");
                    return;
                }
            }
        };

        // Capture locality before the send so error handling uses the
        // same classification as the actual send path.
        let target_is_local = self
            .oplog_plugins
            .is_local(&target_agent_id)
            .await
            .unwrap_or(true);

        if let Some(s) = self.plugin_state.get_mut(&grant_id) {
            s.send_in_progress = true;
        }

        let batch_count = (batch_end.as_u64() - batch_start.as_u64() + 1) as usize;
        let entries = self.read_batch(batch_start, batch_count).await;

        if entries.len() != batch_count {
            tracing::error!(
                "Expected {batch_count} entries for plugin {grant_id} [{batch_start}..{batch_end}], got {}",
                entries.len()
            );
            if let Some(s) = self.plugin_state.get_mut(&grant_id) {
                s.send_in_progress = false;
            }
            return;
        }

        // If ALL entries are checkpoint entries, skip — nothing meaningful to deliver.
        // Advance the cursor past them so we don't re-read the same range forever.
        if entries
            .iter()
            .all(|e| matches!(e, OplogEntry::OplogProcessorCheckpoint { .. }))
        {
            if let Some(s) = self.plugin_state.get_mut(&grant_id) {
                s.confirmed_up_to = batch_end;
                s.sending_up_to = batch_end;
                s.send_in_progress = false;
            }
            return;
        }

        if !is_retry {
            self.write_checkpoint(
                grant_id,
                &target_agent_id,
                live.confirmed_up_to,
                batch_end,
                batch_start,
            )
            .await;
            if let Some(s) = self.plugin_state.get_mut(&grant_id) {
                s.sending_up_to = batch_end;
                s.last_batch_start = batch_start;
            }
        }

        match self
            .oplog_plugins
            .send(
                metadata.clone(),
                &plugin,
                &target_agent_id,
                batch_start,
                entries,
            )
            .await
        {
            Ok(()) => {
                tracing::info!(
                    plugin_name = plugin.plugin_name,
                    source_agent = %metadata.agent_id,
                    target_agent = %target_agent_id,
                    batch_start = %batch_start,
                    batch_end = %batch_end,
                    "Oplog processor: batch enqueued successfully"
                );
                // Deliberately NOT confirmed here — enqueue succeeding only means the
                // invocation was accepted into the target plugin worker's queue, not
                // that it ran. `confirmed_up_to` only advances once a later flush tick
                // observes `InvocationStatus::Complete` for this exact batch (see the
                // status-check block at the top of this function) — that's what makes
                // a lost/failed/never-run invocation retryable instead of a permanent,
                // silent loss of this batch's telemetry.
                if let Some(s) = self.plugin_state.get_mut(&grant_id) {
                    s.sending_up_to = batch_end;
                    s.send_in_progress = false;
                    s.last_batch_start = batch_start;
                    s.sending_since = Some(Instant::now());
                }
            }
            Err(err) => {
                tracing::error!("Failed to enqueue oplog entries to plugin {grant_id}: {err}");
                if let Some(event_service) = &self.worker_event_service {
                    event_service.emit_event(
                        InternalWorkerEvent::plugin_error(
                            &format!("{grant_id}"),
                            &format!("Failed to enqueue batch [{batch_start}..{batch_end}]: {err}"),
                        ),
                        true,
                    );
                }

                if target_is_local {
                    // Local failure: invalidate target so next flush creates a fresh instance
                    self.oplog_plugins
                        .invalidate_target(metadata.environment_id, &plugin, &target_agent_id)
                        .await;
                    if let Some(s) = self.plugin_state.get_mut(&grant_id) {
                        s.target_agent_id = None;
                        s.send_in_progress = false;
                    }
                } else {
                    // Remote failure: keep the recorded target to avoid duplicating
                    // delivery across two different plugin workers. The batch remains
                    // pending and will be retried against the same recorded target.
                    if let Some(s) = self.plugin_state.get_mut(&grant_id) {
                        s.send_in_progress = false;
                    }
                }
            }
        }
    }

    /// A recorded target is stale when this executor owns it but it is not the plugin
    /// worker this executor currently runs for the plugin: nothing sends to such a worker
    /// any more (it belongs to an earlier generation, e.g. one invalidated after a failed
    /// send, or to a plugin component revision no longer installed), so batches recorded
    /// against it would be retried against it forever, waking it (and replaying its
    /// whole oplog) on every attempt. A target on another executor is never stale here:
    /// delivery keeps going to it and `try_locality_recovery` migrates away from it.
    ///
    /// Re-targeting is at-least-once: an unconfirmed batch is re-sent to the current
    /// worker (after the status check on the stale one, which only reads its status and
    /// does not run it), and plugin state derived by the stale worker for this source
    /// agent (e.g. spans opened in earlier batches) is not carried over.
    async fn is_stale_local_target(
        &self,
        metadata: &AgentMetadata,
        plugin: &InstalledPlugin,
        recorded: &AgentId,
    ) -> bool {
        if !matches!(self.oplog_plugins.is_local(recorded).await, Ok(true)) {
            return false;
        }
        match self
            .oplog_plugins
            .resolve_target(metadata.environment_id, plugin)
            .await
        {
            Ok(current) if current != *recorded => {
                tracing::warn!(
                    plugin_name = plugin.plugin_name,
                    source_agent = %self.initial_worker_metadata.agent_id,
                    stale_target = %recorded,
                    current_target = %current,
                    "Oplog processor: recorded target plugin worker is stale, re-targeting"
                );
                true
            }
            _ => false,
        }
    }

    /// Read `count` entries starting at `start` from the buffer if available,
    /// otherwise fall back to the persisted oplog.
    async fn read_batch(&self, start: OplogIndex, count: usize) -> Vec<OplogEntry> {
        if count == 0 {
            return Vec::new();
        }

        if !self.buffer.is_empty() && start >= self.buffer_start_idx {
            let buffer_end_idx = self.buffer_start_idx.range_end(self.buffer.len() as u64);
            let request_end_idx = start.range_end(count as u64);

            if request_end_idx <= buffer_end_idx {
                let offset = (start.as_u64() - self.buffer_start_idx.as_u64()) as usize;
                return self
                    .buffer
                    .iter()
                    .skip(offset)
                    .take(count)
                    .cloned()
                    .collect();
            }
        }

        self.inner
            .read_many(start, count as u64)
            .await
            .into_values()
            .collect()
    }

    /// Write an OplogProcessorCheckpoint entry, commit it, and update index tracking.
    async fn write_checkpoint(
        &mut self,
        grant_id: EnvironmentPluginGrantId,
        target_agent_id: &AgentId,
        confirmed_up_to: OplogIndex,
        sending_up_to: OplogIndex,
        last_batch_start: OplogIndex,
    ) {
        let checkpoint = OplogEntry::OplogProcessorCheckpoint {
            timestamp: golem_common::model::Timestamp::now_utc(),
            plugin_grant_id: grant_id,
            target_agent_id: target_agent_id.clone(),
            confirmed_up_to,
            sending_up_to,
            last_batch_start,
        };
        self.buffer.push_back(checkpoint.clone());
        let idx = self.inner.add(checkpoint).await;
        self.last_oplog_idx = idx;
        let committed = self.inner.commit(CommitLevel::Always).await;
        if let Some(max_idx) = committed.keys().max().copied() {
            self.last_committed_idx = self.last_committed_idx.max(max_idx);
        }
        // Track all directly committed entries so ForwardingOplog::commit()
        // can surface them to the Worker for status folding
        self.pending_direct_commits.extend(committed);
    }

    /// Prune buffer: drain entries that ALL active/in-flight plugins have confirmed past.
    fn prune_buffer(&mut self) {
        if self.plugin_state.is_empty() || self.buffer.is_empty() {
            return;
        }

        let min_confirmed = self
            .plugin_state
            .values()
            .map(|s| s.confirmed_up_to)
            .min()
            .unwrap_or(OplogIndex::NONE);

        if min_confirmed >= self.buffer_start_idx {
            let drain_up_to = min_confirmed.as_u64() - self.buffer_start_idx.as_u64() + 1;
            let drain_count = (drain_up_to as usize).min(self.buffer.len());
            self.buffer.drain(..drain_count);
            self.buffer_start_idx =
                OplogIndex::from_u64(self.buffer_start_idx.as_u64() + drain_count as u64);
        }
    }

    fn finish_empty_flush(&mut self) {
        self.buffer.clear();
        self.buffer_start_idx = self.last_oplog_idx.next();
        self.last_send = Instant::now();
        self.commit_count = 0;
    }

    fn finish_flush_cycle(&mut self) {
        self.last_send = Instant::now();
        self.commit_count = 0;
    }

    /// Periodic locality recovery: for each plugin whose target worker is on a
    /// remote executor (after shard reassignment), check if the old target has
    /// caught up and, if so, migrate to a new local plugin worker.
    ///
    /// This is an optimization — correctness is preserved regardless because
    /// delivery always goes to the recorded `target_agent_id` with deterministic
    /// idempotency keys.
    async fn try_locality_recovery(&mut self) {
        let status = self.last_known_status.read().await.clone();
        // Ensure plugin_state is reconciled with current status
        self.reconcile_plugin_state(&status);
        let environment_id = self.initial_worker_metadata.environment_id;

        // Collect candidates: plugins with a target that might be non-local
        let candidates: Vec<(EnvironmentPluginGrantId, AgentId)> = self
            .plugin_state
            .iter()
            .filter(|(_, state)| {
                state.target_agent_id.is_some()
                    && !state.send_in_progress
                    && state.sending_up_to <= state.confirmed_up_to
            })
            .map(|(grant_id, state)| (*grant_id, state.target_agent_id.clone().unwrap()))
            .collect();

        if candidates.is_empty() {
            return;
        }

        let component_metadata = match self.prepare_flush_context(&status).await {
            Some((_, cm)) => cm,
            None => return,
        };

        for (grant_id, old_target) in candidates {
            // Check if the target is already local
            match self.oplog_plugins.is_local(&old_target).await {
                Ok(true) => continue, // already local
                Ok(false) => {}       // non-local, try recovery
                Err(err) => {
                    tracing::warn!(
                        plugin = %grant_id,
                        error = %err,
                        "Locality recovery: failed to check locality"
                    );
                    continue;
                }
            }

            let agent_type = ParsedAgentId::parse_agent_type_name(
                &self.initial_worker_metadata.agent_id.agent_id,
            )
            .ok();
            let plugin = match agent_type
                .as_ref()
                .and_then(|t| component_metadata.metadata.agent_type_plugins(t))
                .and_then(|plugins| {
                    plugins
                        .iter()
                        .find(|p| p.environment_plugin_grant_id == grant_id)
                }) {
                Some(p) => p.clone(),
                None => continue,
            };

            let (confirmed, last_batch_start) = match self.plugin_state.get(&grant_id) {
                Some(s) => (s.confirmed_up_to, s.last_batch_start),
                None => continue,
            };

            // Recompute the idempotency key for the last confirmed batch
            if last_batch_start == confirmed {
                // No batch has been confirmed yet (sentinel value), skip
                tracing::debug!(
                    plugin = %grant_id,
                    "Locality recovery: no confirmed batch yet, skipping"
                );
                continue;
            }

            let last_key = oplog_processor_idempotency_key(
                &self.initial_worker_metadata.agent_id,
                &grant_id,
                last_batch_start,
                confirmed,
            );

            match self
                .oplog_plugins
                .lookup_invocation_status(
                    environment_id,
                    &plugin,
                    &old_target,
                    self.initial_worker_metadata.created_by,
                    &last_key,
                )
                .await
            {
                Ok(InvocationStatus::Complete | InvocationStatus::Pending) => {
                    // Old target has received the batch (complete or queued), proceed with migration
                }
                Ok(status) => {
                    tracing::debug!(
                        plugin = %grant_id,
                        old_target = %old_target,
                        ?status,
                        "Locality recovery: old target has not received last batch yet, skipping"
                    );
                    continue;
                }
                Err(err) => {
                    tracing::warn!(
                        plugin = %grant_id,
                        old_target = %old_target,
                        error = %err,
                        "Locality recovery: failed to query old target status"
                    );
                    continue;
                }
            }

            // Migrate to a new local worker
            let new_target = match self
                .oplog_plugins
                .resolve_target(environment_id, &plugin)
                .await
            {
                Ok(t) => t,
                Err(err) => {
                    tracing::warn!(
                        plugin = %grant_id,
                        error = %err,
                        "Locality recovery: failed to resolve new local target"
                    );
                    continue;
                }
            };

            // Don't persist a no-op migration or migrate to another non-local target
            if new_target == old_target {
                tracing::debug!(
                    plugin = %grant_id,
                    target = %old_target,
                    "Locality recovery: resolved target is unchanged, skipping"
                );
                continue;
            }
            match self.oplog_plugins.is_local(&new_target).await {
                Ok(true) => {}
                Ok(false) => {
                    tracing::debug!(
                        plugin = %grant_id,
                        new_target = %new_target,
                        "Locality recovery: resolved target is still non-local, skipping"
                    );
                    continue;
                }
                Err(err) => {
                    tracing::warn!(
                        plugin = %grant_id,
                        new_target = %new_target,
                        error = %err,
                        "Locality recovery: failed to verify locality of new target"
                    );
                    continue;
                }
            }

            self.write_checkpoint(
                grant_id,
                &new_target,
                confirmed,
                confirmed,
                last_batch_start,
            )
            .await;
            if let Some(s) = self.plugin_state.get_mut(&grant_id) {
                s.target_agent_id = Some(new_target.clone());
            }
            tracing::info!(
                source_worker = %self.initial_worker_metadata.agent_id,
                plugin = %grant_id,
                old_target = %old_target,
                new_target = %new_target,
                confirmed_up_to = confirmed.as_u64(),
                last_batch_start = last_batch_start.as_u64(),
                "Locality recovery: migrated plugin to new local target"
            );
        }
    }
}

/// Chooses the next batch `(start, end, is_retry)` for one plugin, or `None` when there
/// is nothing to send. Every batch holds at most `max_batch_entries` entries (at least 1).
///
/// - RETRY (`sending_up_to > confirmed_up_to`): the unconfirmed range is resent exactly —
///   never widened. If it is longer than the cap (left behind by an earlier, uncapped
///   build) it is NARROWED to its first `max_batch_entries` entries and returned as a NEW
///   send (`is_retry = false`): the caller then checkpoints the narrowed range
///   (`sending_up_to` = narrowed end, `last_batch_start` = start) before sending, so the
///   batch gets a fresh identity — its idempotency key and every later status check use
///   `(start, narrowed end)`, never the original over-long range — and the remainder is
///   sent as later NEW batches once it is confirmed. Narrowing is safe for at-least-once
///   delivery: the plugin may see a prefix of the original range again.
/// - NEW: `confirmed_up_to + 1 ..= min(committed_tail, confirmed_up_to + max_batch_entries)`.
fn select_batch(
    confirmed_up_to: OplogIndex,
    sending_up_to: OplogIndex,
    committed_tail: OplogIndex,
    max_batch_entries: u64,
) -> Option<(OplogIndex, OplogIndex, bool)> {
    let start = confirmed_up_to.next();
    let cap_end = OplogIndex::from_u64(confirmed_up_to.as_u64() + max_batch_entries.max(1));
    if sending_up_to > confirmed_up_to {
        if sending_up_to <= cap_end {
            Some((start, sending_up_to, true))
        } else {
            Some((start, cap_end, false))
        }
    } else if confirmed_up_to < committed_tail {
        Some((start, committed_tail.min(cap_end), false))
    } else {
        None
    }
}

const OPLOG_PROC_NS: Uuid = uuid!("A7E3F1B2-8C4D-5E6F-9A0B-1C2D3E4F5A6B");

fn oplog_processor_idempotency_key(
    source_agent_id: &AgentId,
    plugin_installation_id: &EnvironmentPluginGrantId,
    batch_first_index: OplogIndex,
    batch_last_index: OplogIndex,
) -> IdempotencyKey {
    let mut buf = Vec::with_capacity(128);
    buf.extend_from_slice(b"oplog-proc-v1\0");
    buf.extend_from_slice(source_agent_id.component_id.0.as_bytes());
    let worker_name = source_agent_id.agent_id.as_bytes();
    buf.extend_from_slice(&(worker_name.len() as u32).to_be_bytes());
    buf.extend_from_slice(worker_name);
    buf.extend_from_slice(plugin_installation_id.0.as_bytes());
    buf.extend_from_slice(&batch_first_index.as_u64().to_be_bytes());
    buf.extend_from_slice(&batch_last_index.as_u64().to_be_bytes());
    IdempotencyKey::from_uuid(Uuid::new_v5(&OPLOG_PROC_NS, &buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::golem_config::DEFAULT_PLUGIN_MAX_BATCH_ENTRIES;
    use golem_common::base_model::component_metadata::KnownExports;
    use golem_common::model::account::AccountId;
    use golem_common::model::application::ApplicationId;
    use golem_common::model::component::{
        ComponentId, ComponentName, ComponentRevision, InstalledPlugin,
    };
    use golem_common::model::component_metadata::ComponentMetadata;
    use golem_common::model::diff;
    use golem_common::model::environment::EnvironmentId;
    use golem_common::model::environment_plugin_grant::EnvironmentPluginGrantId;
    use golem_common::model::oplog::PersistenceLevel;
    use golem_common::model::plugin_registration::PluginRegistrationId;
    use golem_common::model::{AgentFingerprint, Timestamp};
    use golem_common::read_only_lock;
    use golem_service_base::model::component::Component;
    use test_r::test;

    // --------------------------------------------------------------------------
    // U1: Deterministic idempotency key stability
    // --------------------------------------------------------------------------

    #[test]
    fn deterministic_idempotency_key_same_inputs_produce_same_key() {
        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "TestPlugin(test-worker)".to_string(),
        };
        let plugin_id = EnvironmentPluginGrantId::new();
        let first = OplogIndex::from_u64(10);
        let last = OplogIndex::from_u64(20);

        let key1 = oplog_processor_idempotency_key(&agent_id, &plugin_id, first, last);
        let key2 = oplog_processor_idempotency_key(&agent_id, &plugin_id, first, last);

        assert_eq!(
            key1, key2,
            "Same inputs must produce the same idempotency key"
        );
    }

    #[test]
    fn deterministic_idempotency_key_different_batch_range_produces_different_key() {
        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "TestPlugin(test-worker)".to_string(),
        };
        let plugin_id = EnvironmentPluginGrantId::new();

        let key1 = oplog_processor_idempotency_key(
            &agent_id,
            &plugin_id,
            OplogIndex::from_u64(10),
            OplogIndex::from_u64(20),
        );
        let key2 = oplog_processor_idempotency_key(
            &agent_id,
            &plugin_id,
            OplogIndex::from_u64(21),
            OplogIndex::from_u64(30),
        );

        assert_ne!(
            key1, key2,
            "Different batch ranges must produce different keys"
        );
    }

    #[test]
    fn deterministic_idempotency_key_different_worker_produces_different_key() {
        let component_id = ComponentId::new();
        let agent_id1 = AgentId {
            component_id,
            agent_id: "worker-a".to_string(),
        };
        let agent_id2 = AgentId {
            component_id,
            agent_id: "worker-b".to_string(),
        };
        let plugin_id = EnvironmentPluginGrantId::new();
        let first = OplogIndex::from_u64(1);
        let last = OplogIndex::from_u64(5);

        let key1 = oplog_processor_idempotency_key(&agent_id1, &plugin_id, first, last);
        let key2 = oplog_processor_idempotency_key(&agent_id2, &plugin_id, first, last);

        assert_ne!(key1, key2, "Different workers must produce different keys");
    }

    #[test]
    fn deterministic_idempotency_key_different_plugin_produces_different_key() {
        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "TestPlugin(test-worker)".to_string(),
        };
        let plugin_id1 = EnvironmentPluginGrantId::new();
        let plugin_id2 = EnvironmentPluginGrantId::new();
        let first = OplogIndex::from_u64(1);
        let last = OplogIndex::from_u64(5);

        let key1 = oplog_processor_idempotency_key(&agent_id, &plugin_id1, first, last);
        let key2 = oplog_processor_idempotency_key(&agent_id, &plugin_id2, first, last);

        assert_ne!(key1, key2, "Different plugins must produce different keys");
    }

    // --------------------------------------------------------------------------
    // Helpers: recording mock for OplogProcessorPlugin and fake ComponentService
    // --------------------------------------------------------------------------

    /// Records all `send()` calls for verification
    struct RecordingOplogProcessorPlugin {
        sends: async_lock::Mutex<Vec<RecordedSend>>,
        lookups: async_lock::Mutex<Vec<RecordedLookup>>,
        /// Status returned by `lookup_invocation_status`. Defaults to `Unknown` (matches
        /// the previous hardcoded behavior); override via `set_status` per test.
        status: async_lock::Mutex<InvocationStatus>,
        /// What `resolve_target` returns: the plugin worker this executor currently runs.
        current_target: AgentId,
        /// Targets `is_local` reports as owned by another executor.
        remote_targets: HashSet<AgentId>,
    }

    #[derive(Debug, Clone)]
    #[allow(dead_code)]
    struct RecordedSend {
        target_agent_id: AgentId,
        initial_oplog_index: OplogIndex,
        entry_count: usize,
    }

    #[derive(Debug, Clone)]
    struct RecordedLookup {
        caller_account_id: AccountId,
        idempotency_key: IdempotencyKey,
    }

    impl RecordingOplogProcessorPlugin {
        fn new() -> Self {
            Self {
                sends: async_lock::Mutex::new(Vec::new()),
                lookups: async_lock::Mutex::new(Vec::new()),
                status: async_lock::Mutex::new(InvocationStatus::Unknown),
                current_target: AgentId {
                    component_id: ComponentId::new(),
                    agent_id: "mock-target".to_string(),
                },
                remote_targets: HashSet::new(),
            }
        }

        fn with_remote_target(mut self, target: AgentId) -> Self {
            self.remote_targets.insert(target);
            self
        }

        async fn send_count(&self) -> usize {
            self.sends.lock().await.len()
        }

        async fn sends(&self) -> Vec<RecordedSend> {
            self.sends.lock().await.clone()
        }

        async fn lookups(&self) -> Vec<RecordedLookup> {
            self.lookups.lock().await.clone()
        }

        async fn set_status(&self, status: InvocationStatus) {
            *self.status.lock().await = status;
        }
    }

    #[async_trait]
    impl OplogProcessorPlugin for RecordingOplogProcessorPlugin {
        async fn resolve_target(
            &self,
            _environment_id: EnvironmentId,
            _plugin: &InstalledPlugin,
        ) -> Result<AgentId, WorkerExecutorError> {
            Ok(self.current_target.clone())
        }

        async fn send(
            &self,
            _worker_metadata: AgentMetadata,
            _plugin: &InstalledPlugin,
            target_agent_id: &AgentId,
            initial_oplog_index: OplogIndex,
            entries: Vec<OplogEntry>,
        ) -> Result<(), WorkerExecutorError> {
            self.sends.lock().await.push(RecordedSend {
                target_agent_id: target_agent_id.clone(),
                initial_oplog_index,
                entry_count: entries.len(),
            });
            Ok(())
        }

        async fn invalidate_target(
            &self,
            _environment_id: EnvironmentId,
            _plugin: &InstalledPlugin,
            _failed_target: &AgentId,
        ) {
            // no-op in tests
        }

        async fn is_local(&self, agent_id: &AgentId) -> Result<bool, WorkerExecutorError> {
            Ok(!self.remote_targets.contains(agent_id))
        }

        async fn on_shard_assignment_changed(&self) -> Result<(), WorkerExecutorError> {
            Ok(())
        }

        async fn lookup_invocation_status(
            &self,
            _environment_id: EnvironmentId,
            _plugin: &InstalledPlugin,
            _target_agent_id: &AgentId,
            caller_account_id: AccountId,
            idempotency_key: &IdempotencyKey,
        ) -> Result<InvocationStatus, WorkerExecutorError> {
            self.lookups.lock().await.push(RecordedLookup {
                caller_account_id,
                idempotency_key: idempotency_key.clone(),
            });
            Ok(*self.status.lock().await)
        }
    }

    /// A fake ComponentService that returns a component with one installed oplog processor plugin
    struct FakeComponentService {
        installed_plugins: Vec<InstalledPlugin>,
    }

    impl FakeComponentService {
        fn with_one_oplog_processor_plugin(grant_id: EnvironmentPluginGrantId) -> Self {
            Self {
                installed_plugins: vec![InstalledPlugin {
                    environment_plugin_grant_id: grant_id,
                    priority: golem_common::model::component::PluginPriority(0),
                    parameters: BTreeMap::new(),
                    plugin_registration_id: PluginRegistrationId::new(),
                    plugin_name: "test-oplog-plugin".to_string(),
                    plugin_version: "1.0.0".to_string(),
                    oplog_processor_component_id: Some(ComponentId::new()),
                    oplog_processor_component_revision: Some(ComponentRevision::INITIAL),
                }],
            }
        }

        fn empty() -> Self {
            Self {
                installed_plugins: Vec::new(),
            }
        }
    }

    #[async_trait]
    impl ComponentService for FakeComponentService {
        async fn get(
            &self,
            _engine: &wasmtime::Engine,
            _component_id: ComponentId,
            _component_revision: ComponentRevision,
        ) -> Result<(wasmtime::component::Component, Component), WorkerExecutorError> {
            unimplemented!("not needed for forwarding oplog tests")
        }

        async fn get_metadata(
            &self,
            component_id: ComponentId,
            _forced_revision: Option<ComponentRevision>,
        ) -> Result<Component, WorkerExecutorError> {
            use golem_common::base_model::component_metadata::AgentTypeProvisionConfig;
            use golem_common::model::agent::AgentTypeName;
            use std::collections::BTreeMap;

            let provision_configs = if self.installed_plugins.is_empty() {
                BTreeMap::new()
            } else {
                BTreeMap::from([(
                    AgentTypeName("TestPlugin".to_string()),
                    AgentTypeProvisionConfig {
                        plugins: self.installed_plugins.clone(),
                        ..Default::default()
                    },
                )])
            };

            Ok(Component {
                id: component_id,
                revision: ComponentRevision::INITIAL,
                environment_id: EnvironmentId::new(),
                component_name: ComponentName("test-component".to_string()),
                hash: diff::Hash::empty(),
                application_id: ApplicationId::new(),
                account_id: AccountId::new(),
                component_size: 100,
                metadata: ComponentMetadata::from_parts(
                    KnownExports::default(),
                    vec![],
                    None,
                    None,
                    vec![],
                    provision_configs,
                ),
                created_at: chrono::Utc::now(),
                wasm_hash: diff::Hash::empty(),
                object_store_key: String::new(),
            })
        }

        async fn resolve_component(
            &self,
            _component_reference: String,
            _resolving_environment: EnvironmentId,
            _resolving_application: ApplicationId,
            _resolving_account: AccountId,
        ) -> Result<Option<ComponentId>, WorkerExecutorError> {
            Ok(None)
        }

        async fn all_cached_metadata(&self) -> Vec<Component> {
            Vec::new()
        }

        async fn invalidate_all_metadata_for_environment(&self, _environment_id: EnvironmentId) {}
    }

    /// A minimal in-memory oplog for testing ForwardingOplog behavior
    #[allow(dead_code)]
    struct InMemoryOplog {
        entries: async_lock::Mutex<Vec<OplogEntry>>,
        current_idx: async_lock::Mutex<OplogIndex>,
    }

    #[allow(dead_code)]
    impl InMemoryOplog {
        fn new() -> Self {
            Self {
                entries: async_lock::Mutex::new(Vec::new()),
                current_idx: async_lock::Mutex::new(OplogIndex::INITIAL),
            }
        }
    }

    impl Debug for InMemoryOplog {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("InMemoryOplog").finish()
        }
    }

    #[async_trait]
    impl Oplog for InMemoryOplog {
        async fn add(&self, entry: OplogEntry) -> OplogIndex {
            let mut entries = self.entries.lock().await;
            let mut idx = self.current_idx.lock().await;
            *idx = idx.next();
            entries.push(entry);
            *idx
        }

        async fn drop_prefix(&self, _last_dropped_id: OplogIndex) -> u64 {
            0
        }

        async fn commit(&self, _level: CommitLevel) -> BTreeMap<OplogIndex, OplogEntry> {
            BTreeMap::new()
        }

        async fn current_oplog_index(&self) -> OplogIndex {
            *self.current_idx.lock().await
        }

        async fn last_added_non_hint_entry(&self) -> Option<OplogIndex> {
            None
        }

        async fn wait_for_replicas(&self, _replicas: u8, _timeout: Duration) -> bool {
            true
        }

        async fn read(&self, oplog_index: OplogIndex) -> OplogEntry {
            let entries = self.entries.lock().await;
            let idx: u64 = oplog_index.into();
            entries[(idx - 1) as usize].clone()
        }

        async fn read_many(
            &self,
            oplog_index: OplogIndex,
            n: u64,
        ) -> BTreeMap<OplogIndex, OplogEntry> {
            let entries = self.entries.lock().await;
            let start: u64 = oplog_index.into();
            let mut result = BTreeMap::new();
            for i in start..(start + n) {
                if let Some(entry) = entries.get((i - 1) as usize) {
                    result.insert(OplogIndex::from_u64(i), entry.clone());
                }
            }
            result
        }

        async fn length(&self) -> u64 {
            self.entries.lock().await.len() as u64
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

        async fn switch_persistence_level(&self, _mode: PersistenceLevel) {}
    }

    fn test_worker_metadata(
        active_plugins: HashSet<EnvironmentPluginGrantId>,
    ) -> (
        AgentMetadata,
        read_only_lock::tokio::ReadOnlyLock<AgentStatusRecord>,
    ) {
        let agent_id = AgentId {
            component_id: ComponentId::new(),
            agent_id: "TestPlugin(test-worker)".to_string(),
        };
        let environment_id = EnvironmentId::new();
        let account_id = AccountId::new();
        let status = AgentStatusRecord {
            active_plugins,
            ..Default::default()
        };

        let metadata = AgentMetadata {
            agent_id,
            env: vec![],
            environment_id,
            created_by: account_id,
            config: Vec::new(),
            created_at: Timestamp::now_utc(),
            parent: None,
            last_known_status: status.clone(),
            original_phantom_id: None,
            fingerprint: AgentFingerprint::new(),
            agent_mode: AgentMode::Durable,
        };

        let status_lock =
            read_only_lock::tokio::ReadOnlyLock::new(Arc::new(tokio::sync::RwLock::new(status)));

        (metadata, status_lock)
    }

    // --------------------------------------------------------------------------
    // U6: Empty buffer → no checkpoint written, no invoke called
    // --------------------------------------------------------------------------

    #[test]
    async fn no_committed_entries_no_send() {
        let grant_id = EnvironmentPluginGrantId::new();
        let (metadata, status_lock) = test_worker_metadata(HashSet::from([grant_id]));
        let recording_plugin = Arc::new(RecordingOplogProcessorPlugin::new());
        let components: Arc<dyn ComponentService> = Arc::new(
            FakeComponentService::with_one_oplog_processor_plugin(grant_id),
        );
        let inner: Arc<dyn Oplog> = Arc::new(InMemoryOplog::new());

        let mut state = ForwardingOplogState {
            buffer: VecDeque::new(),
            buffer_start_idx: OplogIndex::INITIAL,
            commit_count: 0,
            last_send: Instant::now(),
            oplog_plugins: recording_plugin.clone(),
            initial_worker_metadata: metadata,
            last_known_status: status_lock,
            last_oplog_idx: OplogIndex::NONE,
            last_committed_idx: OplogIndex::NONE,
            components,
            inner,
            plugin_state: HashMap::from([(
                grant_id,
                LivePluginState {
                    target_agent_id: None,
                    confirmed_up_to: OplogIndex::NONE,
                    sending_up_to: OplogIndex::NONE,
                    send_in_progress: false,
                    last_batch_start: OplogIndex::NONE,
                    sending_since: None,
                },
            )]),
            pending_direct_commits: BTreeMap::new(),
            worker_event_service: None,
            max_batch_entries: DEFAULT_PLUGIN_MAX_BATCH_ENTRIES,
        };

        // No committed entries (last_committed_idx = NONE) — try_flush should be a no-op
        state.try_flush().await;

        assert_eq!(
            recording_plugin.send_count().await,
            0,
            "No committed entries should not trigger any plugin sends"
        );
    }

    // --------------------------------------------------------------------------
    // U5 (partial): No active plugins → no send even with entries in buffer
    // --------------------------------------------------------------------------

    #[test]
    async fn no_active_plugins_no_send() {
        let (metadata, status_lock) = test_worker_metadata(HashSet::new());
        let recording_plugin = Arc::new(RecordingOplogProcessorPlugin::new());
        let components: Arc<dyn ComponentService> = Arc::new(FakeComponentService::empty());
        let inner: Arc<dyn Oplog> = Arc::new(InMemoryOplog::new());

        let mut state = ForwardingOplogState {
            buffer: VecDeque::from([OplogEntry::GrowMemory {
                timestamp: Timestamp::now_utc(),
                delta: 100,
            }]),
            buffer_start_idx: OplogIndex::INITIAL,
            commit_count: 0,
            last_send: Instant::now(),
            oplog_plugins: recording_plugin.clone(),
            initial_worker_metadata: metadata,
            last_known_status: status_lock,
            last_oplog_idx: OplogIndex::from_u64(1),
            last_committed_idx: OplogIndex::from_u64(1),
            components,
            inner,
            plugin_state: HashMap::new(),
            pending_direct_commits: BTreeMap::new(),
            worker_event_service: None,
            max_batch_entries: DEFAULT_PLUGIN_MAX_BATCH_ENTRIES,
        };

        state.try_flush().await;

        assert_eq!(
            recording_plugin.send_count().await,
            0,
            "No active plugins should not trigger any plugin sends"
        );
    }

    // --------------------------------------------------------------------------
    // Basic send verification: active plugin + entries → plugin receives them
    // --------------------------------------------------------------------------

    #[test]
    async fn active_plugin_receives_buffered_entries() {
        let grant_id = EnvironmentPluginGrantId::new();
        let (metadata, status_lock) = test_worker_metadata(HashSet::from([grant_id]));
        let recording_plugin = Arc::new(RecordingOplogProcessorPlugin::new());
        let components: Arc<dyn ComponentService> = Arc::new(
            FakeComponentService::with_one_oplog_processor_plugin(grant_id),
        );
        let inner: Arc<dyn Oplog> = Arc::new(InMemoryOplog::new());

        // Pre-populate the inner oplog so read_many can return entries
        let entry1 = OplogEntry::GrowMemory {
            timestamp: Timestamp::now_utc(),
            delta: 100,
        };
        let entry2 = OplogEntry::GrowMemory {
            timestamp: Timestamp::now_utc(),
            delta: 200,
        };
        inner.add(entry1.clone()).await;
        inner.add(entry2.clone()).await;

        let mut state = ForwardingOplogState {
            buffer: VecDeque::from([entry1, entry2]),
            buffer_start_idx: OplogIndex::INITIAL,
            commit_count: 0,
            last_send: Instant::now(),
            oplog_plugins: recording_plugin.clone(),
            initial_worker_metadata: metadata,
            last_known_status: status_lock,
            last_oplog_idx: OplogIndex::from_u64(2),
            last_committed_idx: OplogIndex::from_u64(2),
            components,
            inner,
            plugin_state: HashMap::from([(
                grant_id,
                LivePluginState {
                    target_agent_id: None,
                    confirmed_up_to: OplogIndex::NONE,
                    sending_up_to: OplogIndex::NONE,
                    send_in_progress: false,
                    last_batch_start: OplogIndex::NONE,
                    sending_since: None,
                },
            )]),
            pending_direct_commits: BTreeMap::new(),
            worker_event_service: None,
            max_batch_entries: DEFAULT_PLUGIN_MAX_BATCH_ENTRIES,
        };

        state.try_flush().await;

        let sends = recording_plugin.sends().await;
        assert_eq!(sends.len(), 1, "Should have sent exactly one batch");
        assert_eq!(sends[0].entry_count, 2, "Batch should contain 2 entries");
    }

    /// Shared setup for the unconfirmed-batch tests below: one committed entry, one
    /// active plugin, nothing sent yet.
    async fn unconfirmed_batch_test_state(
        grant_id: EnvironmentPluginGrantId,
        recording_plugin: Arc<RecordingOplogProcessorPlugin>,
    ) -> ForwardingOplogState {
        let (metadata, status_lock) = test_worker_metadata(HashSet::from([grant_id]));
        let components: Arc<dyn ComponentService> = Arc::new(
            FakeComponentService::with_one_oplog_processor_plugin(grant_id),
        );
        let inner: Arc<dyn Oplog> = Arc::new(InMemoryOplog::new());
        let entry = OplogEntry::GrowMemory {
            timestamp: Timestamp::now_utc(),
            delta: 100,
        };
        inner.add(entry.clone()).await;

        ForwardingOplogState {
            buffer: VecDeque::from([entry]),
            buffer_start_idx: OplogIndex::INITIAL,
            commit_count: 0,
            last_send: Instant::now(),
            oplog_plugins: recording_plugin,
            initial_worker_metadata: metadata,
            last_known_status: status_lock,
            last_oplog_idx: OplogIndex::from_u64(1),
            last_committed_idx: OplogIndex::from_u64(1),
            components,
            inner,
            plugin_state: HashMap::from([(
                grant_id,
                LivePluginState {
                    target_agent_id: None,
                    confirmed_up_to: OplogIndex::NONE,
                    sending_up_to: OplogIndex::NONE,
                    send_in_progress: false,
                    last_batch_start: OplogIndex::NONE,
                    sending_since: None,
                },
            )]),
            pending_direct_commits: BTreeMap::new(),
            worker_event_service: None,
            max_batch_entries: DEFAULT_PLUGIN_MAX_BATCH_ENTRIES,
        }
    }

    #[test]
    async fn unconfirmed_batch_status_checked_as_original_worker_owner() {
        let grant_id = EnvironmentPluginGrantId::new();
        let recording_plugin = Arc::new(RecordingOplogProcessorPlugin::new());
        let mut state = unconfirmed_batch_test_state(grant_id, recording_plugin.clone()).await;
        let worker_owner = state.initial_worker_metadata.created_by;

        // First tick: nothing sent yet, so this just enqueues — no status check.
        state.try_flush().await;
        assert_eq!(recording_plugin.send_count().await, 1);
        assert!(
            recording_plugin.lookups().await.is_empty(),
            "First send has nothing prior to check the status of"
        );

        // Second tick: the first batch is still unconfirmed, so this checks its status
        // before considering anything else.
        state.try_flush().await;
        let lookups = recording_plugin.lookups().await;
        assert_eq!(lookups.len(), 1, "Expected exactly one status lookup");
        assert_eq!(lookups[0].caller_account_id, worker_owner);
    }

    /// Regression test for the checkpoint-advance-on-enqueue bug: previously
    /// `confirmed_up_to` advanced the moment `send()` returned `Ok(())` (meaning only
    /// that the batch was accepted into the target plugin worker's queue, not that it
    /// ran) — so a batch whose invocation never actually completed (crash, eviction, a
    /// wedged shared plugin worker) was confirmed and therefore never retried,
    /// permanently and silently losing that range's telemetry. This asserts the
    /// opposite: an unconfirmed batch whose status comes back anything other than
    /// `Complete` gets resent.
    #[test]
    async fn unconfirmed_batch_is_retried_when_status_is_not_complete() {
        let grant_id = EnvironmentPluginGrantId::new();
        let recording_plugin = Arc::new(RecordingOplogProcessorPlugin::new());
        recording_plugin.set_status(InvocationStatus::Unknown).await;
        let mut state = unconfirmed_batch_test_state(grant_id, recording_plugin.clone()).await;

        state.try_flush().await; // original send
        state.try_flush().await; // status check (Unknown) -> immediate retry

        let sends = recording_plugin.sends().await;
        assert_eq!(
            sends.len(),
            2,
            "A batch whose status is Unknown must be resent, not silently dropped"
        );
        let live = state.plugin_state.get(&grant_id).unwrap();
        assert_eq!(
            live.confirmed_up_to,
            OplogIndex::NONE,
            "Must not be confirmed until status is actually observed as Complete"
        );
    }

    /// The mirror-image case: once the plugin actually reports `Complete`, the batch
    /// must be confirmed and must NOT be resent on the next tick.
    #[test]
    async fn unconfirmed_batch_is_confirmed_and_not_resent_when_complete() {
        let grant_id = EnvironmentPluginGrantId::new();
        let recording_plugin = Arc::new(RecordingOplogProcessorPlugin::new());
        let mut state = unconfirmed_batch_test_state(grant_id, recording_plugin.clone()).await;

        state.try_flush().await; // original send
        recording_plugin
            .set_status(InvocationStatus::Complete)
            .await;
        state.try_flush().await; // status check (Complete) -> confirm, no resend

        assert_eq!(
            recording_plugin.send_count().await,
            1,
            "A confirmed batch must not be resent"
        );
        let live = state.plugin_state.get(&grant_id).unwrap();
        assert_eq!(
            live.confirmed_up_to, live.sending_up_to,
            "Must be confirmed once status is observed as Complete"
        );
    }

    /// A genuinely still-in-flight batch (`Pending`, within the grace window) must be
    /// left alone rather than eagerly resent on every tick.
    #[test]
    async fn unconfirmed_batch_not_resent_while_genuinely_pending() {
        let grant_id = EnvironmentPluginGrantId::new();
        let recording_plugin = Arc::new(RecordingOplogProcessorPlugin::new());
        let mut state = unconfirmed_batch_test_state(grant_id, recording_plugin.clone()).await;

        state.try_flush().await; // original send
        recording_plugin.set_status(InvocationStatus::Pending).await;
        state.try_flush().await; // status check (Pending, fresh) -> wait, no resend

        assert_eq!(
            recording_plugin.send_count().await,
            1,
            "A genuinely pending batch within the grace window must not be resent yet"
        );
        let live = state.plugin_state.get(&grant_id).unwrap();
        assert_eq!(live.confirmed_up_to, OplogIndex::NONE);
    }

    // --------------------------------------------------------------------------
    // Batch size cap: a backlog is shipped in bounded batches, never as one range
    // --------------------------------------------------------------------------

    /// Extends `unconfirmed_batch_test_state` to `total` committed entries with a batch
    /// cap of `max_batch_entries`.
    async fn backlog_test_state(
        grant_id: EnvironmentPluginGrantId,
        recording_plugin: Arc<RecordingOplogProcessorPlugin>,
        total: u64,
        max_batch_entries: u64,
    ) -> ForwardingOplogState {
        let mut state = unconfirmed_batch_test_state(grant_id, recording_plugin).await;
        for _ in 1..total {
            let entry = OplogEntry::GrowMemory {
                timestamp: Timestamp::now_utc(),
                delta: 100,
            };
            state.inner.add(entry.clone()).await;
            state.buffer.push_back(entry);
        }
        state.last_oplog_idx = OplogIndex::from_u64(total);
        state.last_committed_idx = OplogIndex::from_u64(total);
        state.max_batch_entries = max_batch_entries;
        state
    }

    fn last_checkpoint(
        entries: BTreeMap<OplogIndex, OplogEntry>,
    ) -> Option<(OplogIndex, OplogIndex, OplogIndex)> {
        entries.into_values().rev().find_map(|e| match e {
            OplogEntry::OplogProcessorCheckpoint {
                confirmed_up_to,
                sending_up_to,
                last_batch_start,
                ..
            } => Some((confirmed_up_to, sending_up_to, last_batch_start)),
            _ => None,
        })
    }

    /// Regression test for the OOM crash loop: a source agent whose plugin fell behind
    /// had ~18,700 committed entries past `confirmed_up_to`; the next NEW batch shipped
    /// the whole range at once (and ~100 agents did the same within seconds), so the
    /// executor ran out of memory. A NEW batch must be capped at `max_batch_entries`.
    #[test]
    async fn new_batch_over_long_backlog_is_capped() {
        let grant_id = EnvironmentPluginGrantId::new();
        let recording_plugin = Arc::new(RecordingOplogProcessorPlugin::new());
        let mut state = backlog_test_state(grant_id, recording_plugin.clone(), 10, 4).await;

        state.try_flush().await;

        let sends = recording_plugin.sends().await;
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].initial_oplog_index, OplogIndex::from_u64(1));
        assert_eq!(sends[0].entry_count, 4, "NEW batch must be capped");
        let live = state.plugin_state.get(&grant_id).unwrap();
        assert_eq!(live.sending_up_to, OplogIndex::from_u64(4));
        assert_eq!(live.last_batch_start, OplogIndex::from_u64(1));

        // Once confirmed, the rest follows in further capped batches.
        recording_plugin
            .set_status(InvocationStatus::Complete)
            .await;
        state.try_flush().await;
        state.try_flush().await;
        let sends = recording_plugin.sends().await;
        assert_eq!(sends.len(), 3);
        assert_eq!(sends[1].initial_oplog_index, OplogIndex::from_u64(5));
        assert_eq!(sends[1].entry_count, 4);
        assert_eq!(sends[2].initial_oplog_index, OplogIndex::from_u64(9));
        assert!(sends[2].entry_count <= 4);
    }

    /// Regression test for the OOM crash loop: an over-long range left unconfirmed by an
    /// earlier (uncapped) build is RETRIED (e.g. after re-targeting to a new plugin
    /// worker). The retry must be narrowed to its first `max_batch_entries` entries, the
    /// narrowing must be checkpointed, the narrowed batch's status must be checked under
    /// its own (narrowed) identity, and the remainder must follow as NEW batches.
    #[test]
    async fn retry_of_over_long_unconfirmed_range_is_narrowed() {
        let grant_id = EnvironmentPluginGrantId::new();
        let recording_plugin = Arc::new(RecordingOplogProcessorPlugin::new());
        recording_plugin.set_status(InvocationStatus::Unknown).await;
        let mut state = backlog_test_state(grant_id, recording_plugin.clone(), 10, 4).await;
        // As seeded from a checkpoint: batch [1..10] sent, unconfirmed.
        let live = state.plugin_state.get_mut(&grant_id).unwrap();
        live.target_agent_id = Some(recording_plugin.current_target.clone());
        live.sending_up_to = OplogIndex::from_u64(10);
        live.last_batch_start = OplogIndex::from_u64(1);

        state.try_flush().await; // status Unknown -> retry, narrowed

        let sends = recording_plugin.sends().await;
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].initial_oplog_index, OplogIndex::from_u64(1));
        assert_eq!(sends[0].entry_count, 4, "RETRY must be narrowed to the cap");
        let live = state.plugin_state.get(&grant_id).unwrap();
        assert_eq!(live.confirmed_up_to, OplogIndex::NONE);
        assert_eq!(live.sending_up_to, OplogIndex::from_u64(4));
        assert_eq!(live.last_batch_start, OplogIndex::from_u64(1));
        assert_eq!(
            last_checkpoint(state.inner.read_many(OplogIndex::INITIAL, 100).await),
            Some((
                OplogIndex::NONE,
                OplogIndex::from_u64(4),
                OplogIndex::from_u64(1)
            )),
            "The narrowing must be persisted"
        );

        // The next status check refers to the narrowed batch, then the rest follows.
        recording_plugin
            .set_status(InvocationStatus::Complete)
            .await;
        state.try_flush().await;
        let lookups = recording_plugin.lookups().await;
        assert_eq!(
            lookups.last().unwrap().idempotency_key,
            oplog_processor_idempotency_key(
                &state.initial_worker_metadata.agent_id,
                &grant_id,
                OplogIndex::from_u64(1),
                OplogIndex::from_u64(4),
            )
        );
        let sends = recording_plugin.sends().await;
        assert_eq!(sends.len(), 2);
        assert_eq!(sends[1].initial_oplog_index, OplogIndex::from_u64(5));
        assert_eq!(sends[1].entry_count, 4);
        assert_eq!(
            state.plugin_state.get(&grant_id).unwrap().confirmed_up_to,
            OplogIndex::from_u64(4)
        );
    }

    /// A retry within the cap is resent exactly (never widened, never narrowed).
    #[test]
    async fn retry_within_cap_is_resent_unchanged() {
        let grant_id = EnvironmentPluginGrantId::new();
        let recording_plugin = Arc::new(RecordingOplogProcessorPlugin::new());
        recording_plugin.set_status(InvocationStatus::Unknown).await;
        let mut state = backlog_test_state(grant_id, recording_plugin.clone(), 10, 4).await;
        let live = state.plugin_state.get_mut(&grant_id).unwrap();
        live.target_agent_id = Some(recording_plugin.current_target.clone());
        live.sending_up_to = OplogIndex::from_u64(3);
        live.last_batch_start = OplogIndex::from_u64(1);

        state.try_flush().await;

        let sends = recording_plugin.sends().await;
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].initial_oplog_index, OplogIndex::from_u64(1));
        assert_eq!(sends[0].entry_count, 3);
        assert_eq!(
            state.plugin_state.get(&grant_id).unwrap().sending_up_to,
            OplogIndex::from_u64(3)
        );
    }

    /// A batch below the cap is sent whole.
    #[test]
    async fn small_new_batch_is_not_capped() {
        let grant_id = EnvironmentPluginGrantId::new();
        let recording_plugin = Arc::new(RecordingOplogProcessorPlugin::new());
        let mut state = backlog_test_state(grant_id, recording_plugin.clone(), 3, 4).await;

        state.try_flush().await;

        let sends = recording_plugin.sends().await;
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].entry_count, 3);
    }

    // --------------------------------------------------------------------------
    // Plugin worker identity: stable across restarts, distinct per identity
    // --------------------------------------------------------------------------

    fn plugin_identity() -> PluginWorkerIdentity {
        PluginWorkerIdentity {
            environment_id: EnvironmentId::new(),
            plugin_registration_id: PluginRegistrationId::new(),
            component_revision: ComponentRevision::INITIAL,
            generation: 0,
        }
    }

    /// Regression test for the orphaned-exporter bug: the plugin worker name used to be a
    /// random UUID per process, so every restart created a new plugin worker while the
    /// previous one (and its oplog) stayed behind, still targeted by source agents.
    #[test]
    fn plugin_worker_id_is_stable_for_the_same_identity_and_shards() {
        let component_id = ComponentId::new();
        let identity = plugin_identity();
        let shards = HashSet::from([ShardId::new(3), ShardId::new(7)]);

        let first = generate_local_plugin_agent_id(component_id, &identity, &shards, 16);
        let after_restart =
            generate_local_plugin_agent_id(component_id, &identity.clone(), &shards, 16);

        assert_eq!(first, after_restart);
        assert!(shards.contains(&ShardId::from_agent_id(&first, 16)));
    }

    #[test]
    fn plugin_worker_id_differs_per_identity_field() {
        let component_id = ComponentId::new();
        let base = plugin_identity();
        let shards = HashSet::new();
        let id_of = |identity: &PluginWorkerIdentity| {
            generate_local_plugin_agent_id(component_id, identity, &shards, 0)
        };

        let variants = [
            PluginWorkerIdentity {
                environment_id: EnvironmentId::new(),
                ..base.clone()
            },
            PluginWorkerIdentity {
                plugin_registration_id: PluginRegistrationId::new(),
                ..base.clone()
            },
            PluginWorkerIdentity {
                component_revision: ComponentRevision::INITIAL.next().unwrap(),
                ..base.clone()
            },
            PluginWorkerIdentity {
                generation: 1,
                ..base.clone()
            },
        ];
        for variant in &variants {
            assert_ne!(id_of(&base), id_of(variant), "{variant:?}");
        }
    }

    #[test]
    fn plugin_worker_id_lands_in_an_owned_shard() {
        let component_id = ComponentId::new();
        let identity = plugin_identity();
        for owned in 0..8 {
            let shards = HashSet::from([ShardId::new(owned)]);
            let agent_id = generate_local_plugin_agent_id(component_id, &identity, &shards, 8);
            assert_eq!(ShardId::from_agent_id(&agent_id, 8), ShardId::new(owned));
        }
    }

    #[test]
    fn plugin_worker_id_without_an_owned_shard_in_range_uses_first_candidate() {
        let component_id = ComponentId::new();
        let identity = plugin_identity();
        let out_of_range = HashSet::from([ShardId::new(42)]);

        assert_eq!(
            generate_local_plugin_agent_id(component_id, &identity, &out_of_range, 8),
            generate_local_plugin_agent_id(component_id, &identity, &HashSet::new(), 8)
        );
    }

    // --------------------------------------------------------------------------
    // Stale recorded targets
    // --------------------------------------------------------------------------

    fn stale_target() -> AgentId {
        AgentId {
            component_id: ComponentId::new(),
            agent_id: "previous-plugin-worker".to_string(),
        }
    }

    /// Regression test: after a restart, a source agent's checkpoint still names the plugin
    /// worker of the previous process with an unconfirmed batch. That worker is local but
    /// no longer the current one; the batch must be re-sent to the current plugin worker
    /// instead of being retried against the stale one forever.
    #[test]
    async fn unconfirmed_batch_to_stale_local_target_is_resent_to_current_target() {
        let grant_id = EnvironmentPluginGrantId::new();
        let recording_plugin = Arc::new(RecordingOplogProcessorPlugin::new());
        recording_plugin.set_status(InvocationStatus::Pending).await;
        let mut state = unconfirmed_batch_test_state(grant_id, recording_plugin.clone()).await;
        // As seeded from a checkpoint after a restart: batch [1..1] sent, unconfirmed.
        let live = state.plugin_state.get_mut(&grant_id).unwrap();
        live.target_agent_id = Some(stale_target());
        live.sending_up_to = OplogIndex::from_u64(1);
        live.last_batch_start = OplogIndex::from_u64(1);

        state.try_flush().await;

        let sends = recording_plugin.sends().await;
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].target_agent_id, recording_plugin.current_target);
        assert_eq!(sends[0].initial_oplog_index, OplogIndex::from_u64(1));
        let live = state.plugin_state.get(&grant_id).unwrap();
        assert_eq!(
            live.target_agent_id.as_ref(),
            Some(&recording_plugin.current_target)
        );
        assert_eq!(live.confirmed_up_to, OplogIndex::NONE);
        let written = state.inner.read_many(OplogIndex::INITIAL, 100).await;
        let checkpoint_target = written.values().find_map(|e| match e {
            OplogEntry::OplogProcessorCheckpoint {
                target_agent_id,
                sending_up_to,
                ..
            } => Some((target_agent_id.clone(), *sending_up_to)),
            _ => None,
        });
        assert_eq!(
            checkpoint_target,
            Some((
                recording_plugin.current_target.clone(),
                OplogIndex::from_u64(1)
            )),
            "The new target must be checkpointed with the unconfirmed range kept"
        );
    }

    #[test]
    async fn new_batch_after_stale_local_target_goes_to_current_target() {
        let grant_id = EnvironmentPluginGrantId::new();
        let recording_plugin = Arc::new(RecordingOplogProcessorPlugin::new());
        let mut state = unconfirmed_batch_test_state(grant_id, recording_plugin.clone()).await;
        state
            .plugin_state
            .get_mut(&grant_id)
            .unwrap()
            .target_agent_id = Some(stale_target());

        state.try_flush().await;

        let sends = recording_plugin.sends().await;
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].target_agent_id, recording_plugin.current_target);
    }

    /// A target owned by another executor is not stale: delivery keeps going there (no
    /// duplicate delivery to two plugin workers) until locality recovery migrates it.
    #[test]
    async fn remote_recorded_target_is_kept() {
        let grant_id = EnvironmentPluginGrantId::new();
        let remote = stale_target();
        let recording_plugin =
            Arc::new(RecordingOplogProcessorPlugin::new().with_remote_target(remote.clone()));
        let mut state = unconfirmed_batch_test_state(grant_id, recording_plugin.clone()).await;
        state
            .plugin_state
            .get_mut(&grant_id)
            .unwrap()
            .target_agent_id = Some(remote.clone());

        state.try_flush().await;

        let sends = recording_plugin.sends().await;
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].target_agent_id, remote);
    }

    #[test]
    async fn current_recorded_target_is_kept() {
        let grant_id = EnvironmentPluginGrantId::new();
        let recording_plugin = Arc::new(RecordingOplogProcessorPlugin::new());
        let mut state = unconfirmed_batch_test_state(grant_id, recording_plugin.clone()).await;
        state
            .plugin_state
            .get_mut(&grant_id)
            .unwrap()
            .target_agent_id = Some(recording_plugin.current_target.clone());

        state.try_flush().await;

        let sends = recording_plugin.sends().await;
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].target_agent_id, recording_plugin.current_target);
        let checkpoints = state
            .inner
            .read_many(OplogIndex::INITIAL, 100)
            .await
            .into_values()
            .filter(|e| matches!(e, OplogEntry::OplogProcessorCheckpoint { .. }))
            .count();
        assert_eq!(
            checkpoints, 1,
            "Only the pre-send checkpoint, no re-target checkpoint for an unchanged target"
        );
    }
}
