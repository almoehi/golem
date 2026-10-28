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

//! Manual (snapshot-based) updates under memory pressure: a memory grow the
//! executor refuses while an update loads its snapshot is waited out rather than
//! failing the update, and an in-place restart's regrowth is not charged twice.

use crate::Tracing;
use golem_common::model::AgentId;
use golem_common::model::component::ComponentRevision;
use golem_common::model::worker::{AgentMetadataDto, UpdateRecord};
use golem_common::{agent_id, data_value};
use golem_test_framework::dsl::{TestDsl, update_counts};
use golem_worker_executor_test_utils::{
    LastUniqueId, MemoryGrowRefusal, PrecompiledComponent, TestContext, TestExecutorOverrides,
    TestWorkerExecutor, WorkerExecutorTestDependencies, start_with_overrides,
};
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use test_r::{inherit_test_dep, test};

inherit_test_dep!(WorkerExecutorTestDependencies);
inherit_test_dep!(LastUniqueId);
inherit_test_dep!(
    #[tagged_as("agent_counters")]
    PrecompiledComponent
);
inherit_test_dep!(Tracing);

/// Polls the agent's metadata until `done` holds, failing after `timeout`.
async fn await_metadata(
    executor: &TestWorkerExecutor,
    worker_id: &AgentId,
    timeout: Duration,
    what: &str,
    done: impl Fn(&AgentMetadataDto) -> bool,
) -> anyhow::Result<AgentMetadataDto> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let metadata = executor.get_worker_metadata(worker_id).await?;
        if done(&metadata) {
            return Ok(metadata);
        }
        if tokio::time::Instant::now() > deadline {
            anyhow::bail!("timed out waiting for {what}; last metadata: {metadata:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Waits until `condition` holds, failing after `timeout`.
async fn await_condition(
    timeout: Duration,
    what: &str,
    condition: impl Fn() -> bool,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    while !condition() {
        if tokio::time::Instant::now() > deadline {
            anyhow::bail!("timed out waiting for {what}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

/// A memory grow the executor refuses while a manual update loads its snapshot
/// is temporary: the executor is out of memory right now, not the agent broken.
/// The update has to stay pending, retry once memory is back, and succeed —
/// recording `FailedUpdate` for it made the agent stay on the old revision for
/// good although the same snapshot loads fine a moment later.
#[test]
#[tracing::instrument]
async fn manual_update_waits_for_memory_to_load_its_snapshot(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_counters")] agent_counters: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let refusal = MemoryGrowRefusal::default();
    let executor = start_with_overrides(
        deps,
        &context,
        TestExecutorOverrides {
            memory_grow_refusal: Some(refusal.clone()),
            ..Default::default()
        },
    )
    .await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_counters)
        .store()
        .await?;
    let counter_id = agent_id!("SnapshotCounter", "waits-for-memory");
    let worker_id = executor
        .start_agent(&component.id, counter_id.clone())
        .await?;
    for _ in 0..3 {
        executor
            .invoke_and_await_agent(&component, &counter_id, "increment", data_value!())
            .await?;
    }

    let target = executor
        .update_component(&component.id, "it_agent_counters_v2_release")
        .await?;
    refusal.refuse_while_snapshotting(target.revision);
    executor
        .manual_update_worker(&worker_id, target.revision, false)
        .await?;

    // Refused more than once: the load was retried rather than given up.
    await_condition(
        Duration::from_secs(60),
        "two refused snapshot loads",
        || refusal.refusals() >= 2,
    )
    .await?;
    let while_waiting = executor.get_worker_metadata(&worker_id).await?;

    refusal.allow();
    let after = executor
        .invoke_and_await_agent(&component, &counter_id, "get", data_value!())
        .await?;
    let metadata = executor.get_worker_metadata(&worker_id).await?;

    executor.check_oplog_is_queryable(&worker_id).await?;
    drop(executor);

    assert_eq!(update_counts(&while_waiting), (1, 0, 0));
    assert_eq!(after, data_value!(3u32));
    assert_eq!(metadata.component_revision, target.revision);
    assert_eq!(update_counts(&metadata), (0, 1, 0));

    Ok(())
}

/// The wait for memory is bounded: an update still refused memory once
/// `update_oom_retry_timeout` has passed is recorded as failed, saying it ran
/// out of memory, and the agent keeps running on its old revision.
#[test]
#[tracing::instrument]
async fn manual_update_fails_when_memory_does_not_come_back(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_counters")] agent_counters: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let refusal = MemoryGrowRefusal::default();
    let executor = start_with_overrides(
        deps,
        &context,
        TestExecutorOverrides {
            memory_grow_refusal: Some(refusal.clone()),
            configure: Some(Arc::new(|config| {
                config.memory.update_oom_retry_timeout = Duration::from_secs(3);
            })),
            ..Default::default()
        },
    )
    .await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_counters)
        .store()
        .await?;
    let counter_id = agent_id!("SnapshotCounter", "memory-never-back");
    let worker_id = executor
        .start_agent(&component.id, counter_id.clone())
        .await?;
    for _ in 0..3 {
        executor
            .invoke_and_await_agent(&component, &counter_id, "increment", data_value!())
            .await?;
    }

    let target = executor
        .update_component(&component.id, "it_agent_counters_v2_release")
        .await?;
    refusal.refuse_while_snapshotting(target.revision);
    executor
        .manual_update_worker(&worker_id, target.revision, false)
        .await?;

    let metadata = await_metadata(
        &executor,
        &worker_id,
        Duration::from_secs(90),
        "the update to be recorded as failed",
        |metadata| update_counts(metadata).2 == 1,
    )
    .await?;
    let refusals = refusal.refusals();
    let after = executor
        .invoke_and_await_agent(&component, &counter_id, "get", data_value!())
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;
    drop(executor);

    let details = metadata
        .updates
        .iter()
        .find_map(|update| match update {
            UpdateRecord::FailedUpdate(failed) => failed.details.clone(),
            _ => None,
        })
        .unwrap_or_default();
    assert!(
        refusals >= 2,
        "expected retries before giving up, got {refusals} refusals"
    );
    assert!(
        details.contains("out of memory") && details.contains("retries"),
        "unexpected failure details: {details}"
    );
    assert_eq!(update_counts(&metadata), (0, 0, 1));
    assert_eq!(metadata.component_revision, ComponentRevision::INITIAL);
    assert_eq!(after, data_value!(3u32));

    Ok(())
}

/// A manual update restarts the agent in place to load its snapshot, keeping
/// the memory grant the agent already holds. The load's memory grows are covered
/// by that grant and must not be charged to the executor again: with the pool
/// otherwise full, the update still completes.
#[test]
#[tracing::instrument]
async fn manual_update_loads_its_snapshot_within_the_agents_memory_grant(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_counters")] agent_counters: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    const LIMIT: u64 = 1024 * 1024 * 1024;
    let context = TestContext::new(last_unique_id);
    let measured_usage = Arc::new(AtomicU64::new(0));
    let executor = start_with_overrides(
        deps,
        &context,
        TestExecutorOverrides {
            measured_memory_usage: Some(measured_usage.clone()),
            configure: Some(Arc::new(|config| {
                config.memory.system_memory_override = Some(LIMIT);
                config.memory.enable_measured_admission = true;
            })),
            ..Default::default()
        },
    )
    .await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_counters)
        .store()
        .await?;
    let counter_id = agent_id!("SnapshotCounter", "within-grant");
    let worker_id = executor
        .start_agent(&component.id, counter_id.clone())
        .await?;
    for _ in 0..3 {
        executor
            .invoke_and_await_agent(&component, &counter_id, "increment", data_value!())
            .await?;
    }

    let target = executor
        .update_component(&component.id, "it_agent_counters_v2_release")
        .await?;
    // From here on the gate admits nothing new: every byte it reserves now has to
    // come out of grants already held.
    measured_usage.store(LIMIT, Ordering::SeqCst);
    executor
        .manual_update_worker(&worker_id, target.revision, false)
        .await?;

    let metadata = await_metadata(
        &executor,
        &worker_id,
        Duration::from_secs(60),
        "the update to finish",
        |metadata| update_counts(metadata).0 == 0,
    )
    .await?;
    let after = executor
        .invoke_and_await_agent(&component, &counter_id, "get", data_value!())
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;
    drop(executor);

    assert_eq!(update_counts(&metadata), (0, 1, 0));
    assert_eq!(metadata.component_revision, target.revision);
    assert_eq!(after, data_value!(3u32));

    Ok(())
}

/// A second manual update that has to wait for memory still replays to live on
/// each retry: the pending update's snapshot override has to survive the
/// retries on top of the first update's folded-in region, so the agent ends on
/// the second revision with its state intact.
#[test]
#[tracing::instrument]
async fn second_manual_update_waits_for_memory_to_load_its_snapshot(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_counters")] agent_counters: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let refusal = MemoryGrowRefusal::default();
    let executor = start_with_overrides(
        deps,
        &context,
        TestExecutorOverrides {
            memory_grow_refusal: Some(refusal.clone()),
            ..Default::default()
        },
    )
    .await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_counters)
        .store()
        .await?;
    let counter_id = agent_id!("SnapshotCounter", "second-waits-for-memory");
    let worker_id = executor
        .start_agent(&component.id, counter_id.clone())
        .await?;
    for _ in 0..3 {
        executor
            .invoke_and_await_agent(&component, &counter_id, "increment", data_value!())
            .await?;
    }

    let first = executor
        .update_component(&component.id, "it_agent_counters_v2_release")
        .await?;
    executor
        .manual_update_worker(&worker_id, first.revision, false)
        .await?;
    executor
        .invoke_and_await_agent(&component, &counter_id, "increment", data_value!())
        .await?;

    let second = executor
        .update_component(&component.id, "it_agent_counters_v2_release")
        .await?;
    refusal.refuse_while_snapshotting(second.revision);
    executor
        .manual_update_worker(&worker_id, second.revision, false)
        .await?;
    await_condition(
        Duration::from_secs(60),
        "two refused snapshot loads",
        || refusal.refusals() >= 2,
    )
    .await?;

    refusal.allow();
    let after = executor
        .invoke_and_await_agent(&component, &counter_id, "get", data_value!())
        .await?;
    let metadata = executor.get_worker_metadata(&worker_id).await?;

    executor.check_oplog_is_queryable(&worker_id).await?;
    drop(executor);

    assert_eq!(after, data_value!(4u32));
    assert_eq!(metadata.component_revision, second.revision);
    assert_eq!(update_counts(&metadata), (0, 2, 0));

    Ok(())
}
