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

use crate::Tracing;
use axum::extract::Query;
use axum::response::Response;
use axum::routing::get;
use axum::{BoxError, Router};
use bytes::Bytes;
use futures::{StreamExt, stream};
use golem_api_grpc::proto::golem::worker::LogEvent;
use golem_common::model::AgentEvent;
use golem_common::model::oplog::{
    MultipartPartData, OplogIndex, PublicOplogEntry, PublicSnapshotData,
};
use golem_common::{agent_id, data_value};
use golem_test_framework::dsl::TestDsl;
use golem_wasm::Value;
use golem_worker_executor::services::golem_config::SnapshotPolicy;
use golem_worker_executor_test_utils::{
    LastUniqueId, PrecompiledComponent, TestContext, WorkerExecutorTestDependencies, start,
    start_with_snapshot_policy,
};
use http::StatusCode;
use pretty_assertions::assert_eq;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use test_r::{inherit_test_dep, test};
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::Instrument;

inherit_test_dep!(WorkerExecutorTestDependencies);
inherit_test_dep!(LastUniqueId);
inherit_test_dep!(
    #[tagged_as("host_api_tests")]
    PrecompiledComponent
);
inherit_test_dep!(
    #[tagged_as("agent_counters")]
    PrecompiledComponent
);
inherit_test_dep!(
    #[tagged_as("constructor_parameter_echo")]
    PrecompiledComponent
);
inherit_test_dep!(Tracing);

async fn assert_snapshot_recovery_loaded(events: &mut UnboundedReceiver<LogEvent>) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(event) = events.recv().await {
            match AgentEvent::try_from(event) {
                Ok(AgentEvent::SnapshotRecoverySucceeded { .. }) => return,
                Ok(AgentEvent::SnapshotRecoveryFailed {
                    snapshot_index,
                    error,
                    ..
                }) => {
                    panic!("Snapshot recovery from {snapshot_index} failed: {error}");
                }
                _ => {}
            }
        }
        panic!("Worker event stream ended before snapshot recovery event");
    })
    .await
    .expect("Timed out waiting for snapshot recovery event");
}

#[test]
#[tracing::instrument]
async fn custom_durability_1(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("host_api_tests")] host_api_tests: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let response = Arc::new(AtomicU32::new(0));
    let response_clone = response.clone();

    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();

    let host_http_port = listener.local_addr().unwrap().port();

    #[derive(Deserialize)]
    struct QueryParams {
        payload: String,
    }

    let http_server = tokio::spawn(
        async move {
            let route = Router::new().route(
                "/callback",
                get(move |query: Query<QueryParams>| async move {
                    let result = format!(
                        "{}-{}",
                        response_clone.fetch_add(1, Ordering::AcqRel),
                        query.payload
                    );
                    tracing::info!("responding to callback: {result}");
                    result
                }),
            );

            axum::serve(listener, route).await.unwrap();
        }
        .in_current_span(),
    );

    let component = executor
        .component_dep(&context.default_environment_id, host_api_tests)
        .store()
        .await?;
    let agent_id = agent_id!("CustomDurability", "custom-durability-1");
    let mut env = HashMap::new();
    env.insert("PORT".to_string(), host_http_port.to_string());

    let worker_id = executor
        .start_agent_with(&component.id, agent_id.clone(), env, Vec::new())
        .await?;

    let result1 = executor
        .invoke_and_await_agent(&component, &agent_id, "callback", data_value!("a"))
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    drop(executor);
    let executor = start(deps, &context).await?;

    let result2 = executor
        .invoke_and_await_agent(&component, &agent_id, "callback", data_value!("b"))
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    drop(executor);
    http_server.abort();

    assert_eq!(
        result1.into_return_value(),
        Some(Value::String("0-a".to_string()))
    );
    assert_eq!(
        result2.into_return_value(),
        Some(Value::String("1-b".to_string()))
    );
    Ok(())
}

#[test]
#[tracing::instrument]
async fn lazy_pollable(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("host_api_tests")] host_api_tests: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();

    let host_http_port = listener.local_addr().unwrap().port();

    #[derive(Deserialize)]
    struct QueryParams {
        idx: u32,
    }

    let (signal_tx, signal_rx) = tokio::sync::mpsc::unbounded_channel();
    let signal_rx = Arc::new(Mutex::new(signal_rx));

    let http_server = tokio::spawn(
        async move {
            let route = Router::new().route(
                "/fetch",
                get(move |query: Query<QueryParams>| async move {
                    let idx = query.idx;
                    tracing::info!("fetch called with: {}", idx);

                    let stream = stream::iter(0..3).then(move |i| {
                        let signal_rx = signal_rx.clone();
                        async move {
                            tracing::info!("fetch awaiting signal");
                            signal_rx.lock().await.recv().await;
                            let fragment_str = format!("chunk-{idx}-{i}\n");
                            tracing::info!("emitting response fragment: {fragment_str}");
                            let fragment = Bytes::from(fragment_str);
                            Ok::<Bytes, BoxError>(fragment)
                        }
                    });

                    Response::builder()
                        .status(StatusCode::OK)
                        .body(axum::body::Body::from_stream(stream))
                        .unwrap()
                }),
            );

            axum::serve(listener, route).await.unwrap();
        }
        .in_current_span(),
    );

    let component = executor
        .component_dep(&context.default_environment_id, host_api_tests)
        .store()
        .await?;
    let agent_id = agent_id!("CustomDurability", "lazy-pollable-1");
    let mut env = HashMap::new();
    env.insert("PORT".to_string(), host_http_port.to_string());

    let worker_id = executor
        .start_agent_with(&component.id, agent_id.clone(), env, Vec::new())
        .await?;

    signal_tx.send(()).unwrap();

    executor
        .invoke_and_await_agent(&component, &agent_id, "lazy_pollable_init", data_value!())
        .await?;

    let s1 = executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "lazy_pollable_test",
            data_value!(1u32),
        )
        .await?;

    signal_tx.send(()).unwrap();

    let s2 = executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "lazy_pollable_test",
            data_value!(2u32),
        )
        .await?;

    signal_tx.send(()).unwrap();

    let s3 = executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "lazy_pollable_test",
            data_value!(3u32),
        )
        .await?;

    signal_tx.send(()).unwrap();

    drop(executor);
    let executor = start(deps, &context).await?;

    signal_tx.send(()).unwrap();

    let s4 = executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "lazy_pollable_test",
            data_value!(3u32),
        )
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;
    http_server.abort();

    assert_eq!(
        s1.into_return_value(),
        Some(Value::String("chunk-1-0\n".to_string()))
    );
    assert_eq!(
        s2.into_return_value(),
        Some(Value::String("chunk-1-1\n".to_string()))
    );
    assert_eq!(
        s3.into_return_value(),
        Some(Value::String("chunk-1-2\n".to_string()))
    );
    assert_eq!(
        s4.into_return_value(),
        Some(Value::String("chunk-3-0\n".to_string()))
    );
    Ok(())
}

/// End-to-end regression test for the seq-based `IoPollReady` replay fix
/// (`durable_host/io/poll.rs::pollable_seq`). Matches the production `scene_plates` trap: a
/// worker with 4 concurrently in-flight promise-backed pollables — the same
/// create_promise()/await_promise() primitive `workflowToolStart`/`Finish` uses for concurrent
/// render waits, no persist-nothing wrapping — only some resolve before the worker is evicted
/// and its oplog replayed from scratch on a real wasmtime instance (not a simulated
/// `DeletedRegions` unit test). Verifies every pollable resolves to its own, correctly-
/// attributed result after replay reconstructs the partially-resolved concurrent state — no
/// cross-pollable theft, and no reliance on the removed rep-mismatch fallback (this component
/// never triggers a snapshot, so if the fallback were still load-bearing for ordinary replay
/// this test would catch it via a wrong ordering).
#[test]
#[tracing::instrument]
async fn concurrent_pollables_survive_worker_replay(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("host_api_tests")] host_api_tests: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, host_api_tests)
        .store()
        .await?;
    let agent_id = agent_id!("CustomDurability", "concurrent-pollables-1");

    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "concurrent_promise_init",
            data_value!(),
        )
        .await?;

    // Complete exactly 2 of the 4 concurrently-pending promises, each as its OWN separate
    // invocation — mirroring a different agent (e.g. WorkflowAgent) calling completePromise()
    // asynchronously, outside the poll loop.
    executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "concurrent_promise_complete",
            data_value!(0u32),
        )
        .await?;
    executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "concurrent_promise_complete",
            data_value!(2u32),
        )
        .await?;

    // A single poll() round is only guaranteed to surface AT LEAST one newly-ready pollable,
    // not necessarily all of them — call repeatedly (bounded) until both completed slots (0, 2)
    // are resolved. Each call is its own external invocation, so this still exercises separate
    // rounds of the seq-matching code path, just possibly more than one before both land.
    let mut round1_parts: Vec<String> = vec!["-".to_string(); 4];
    for _ in 0..4 {
        if round1_parts[0] != "-" && round1_parts[2] != "-" {
            break;
        }
        let round = executor
            .invoke_and_await_agent(
                &component,
                &agent_id,
                "concurrent_promise_test",
                data_value!(),
            )
            .await?;
        let round_str = match round.into_return_value() {
            Some(Value::String(s)) => s,
            other => panic!("Expected string from concurrent_promise_test, got {:?}", other),
        };
        round1_parts = round_str.split('|').map(|s| s.to_string()).collect();
        assert_eq!(round1_parts.len(), 4);
    }
    for idx in [0usize, 2] {
        assert_eq!(
            round1_parts[idx],
            format!("promise-{idx}"),
            "Resolved slot {idx} must contain its OWN promise's payload, not another \
             pollable's — cross-pollable theft would show a mismatched idx here \
             (full state: {round1_parts:?})"
        );
    }
    for idx in [1usize, 3] {
        assert_eq!(
            round1_parts[idx], "-",
            "Slot {idx} was never completed and must still be unresolved \
             (full state: {round1_parts:?})"
        );
    }

    // Force a genuine worker eviction + full oplog replay on a fresh executor/instance —
    // reconstructing promise creation (4x), the 2 completions, and round 1's partial poll()
    // resolution from raw oplog, exercising the exact ready()/poll() seq-matching code path
    // this fix changed.
    drop(executor);
    let executor = start(deps, &context).await?;

    executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "concurrent_promise_complete",
            data_value!(1u32),
        )
        .await?;
    executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "concurrent_promise_complete",
            data_value!(3u32),
        )
        .await?;

    let mut round2_parts: Vec<String> = vec!["-".to_string(); 4];
    for _ in 0..4 {
        if round2_parts.iter().all(|p| p != "-") {
            break;
        }
        let round = executor
            .invoke_and_await_agent(
                &component,
                &agent_id,
                "concurrent_promise_test",
                data_value!(),
            )
            .await?;
        let round_str = match round.into_return_value() {
            Some(Value::String(s)) => s,
            other => panic!("Expected string from concurrent_promise_test, got {:?}", other),
        };
        round2_parts = round_str.split('|').map(|s| s.to_string()).collect();
        assert_eq!(round2_parts.len(), 4);
    }

    executor.check_oplog_is_queryable(&worker_id).await?;

    // All 4 must now be resolved, each correctly attributed to its own idx — the core
    // assertion: no cross-pollable theft survived the eviction + replay + continuation.
    for (idx, part) in round2_parts.iter().enumerate() {
        assert_eq!(
            *part,
            format!("promise-{idx}"),
            "Slot {idx} must resolve to its own promise's payload after replay + completion — \
             a mismatch here means a different pollable's entry was wrongly consumed \
             (Bug #2/#3 class)"
        );
    }

    Ok(())
}

const SNAPSHOT_TEST_INVOCATIONS: usize = 10;

#[test]
#[tracing::instrument]
async fn automatic_snapshot_disabled(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_counters")] agent_counters: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start_with_snapshot_policy(deps, &context, SnapshotPolicy::Disabled).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_counters)
        .store()
        .await?;
    let agent_id = agent_id!("JsonSnapshotCounter", "disabled");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    for _ in 0..SNAPSHOT_TEST_INVOCATIONS {
        executor
            .invoke_and_await_agent(&component, &agent_id, "increment", data_value!())
            .await?;
    }

    let oplog = executor.get_oplog(&worker_id, OplogIndex::INITIAL).await?;
    let snapshot_count = oplog
        .iter()
        .filter(|entry| matches!(&entry.entry, PublicOplogEntry::Snapshot(_)))
        .count();

    drop(executor);

    assert_eq!(
        snapshot_count, 0,
        "Expected no snapshots with disabled policy"
    );
    Ok(())
}

#[test]
#[tracing::instrument]
async fn automatic_snapshot_every_2nd_invocation(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_counters")] agent_counters: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start_with_snapshot_policy(
        deps,
        &context,
        SnapshotPolicy::EveryNInvocation { count: 2 },
    )
    .await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_counters)
        .store()
        .await?;
    let agent_id = agent_id!("JsonSnapshotCounter", "every-2nd");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    for _ in 0..SNAPSHOT_TEST_INVOCATIONS {
        executor
            .invoke_and_await_agent(&component, &agent_id, "increment", data_value!())
            .await?;
    }

    let oplog = executor.get_oplog(&worker_id, OplogIndex::INITIAL).await?;
    let snapshot_count = oplog
        .iter()
        .filter(|entry| matches!(&entry.entry, PublicOplogEntry::Snapshot(_)))
        .count();

    assert_eq!(
        snapshot_count,
        SNAPSHOT_TEST_INVOCATIONS / 2,
        "Expected a snapshot every 2 invocations"
    );

    drop(executor);
    let executor = start_with_snapshot_policy(
        deps,
        &context,
        SnapshotPolicy::EveryNInvocation { count: 2 },
    )
    .await?;
    let mut events = executor.capture_output(&worker_id).await?;

    let result_after_restart = executor
        .invoke_and_await_agent(&component, &agent_id, "get", data_value!())
        .await?;
    assert_snapshot_recovery_loaded(&mut events).await;

    assert_eq!(
        result_after_restart.into_return_value(),
        Some(Value::U32(SNAPSHOT_TEST_INVOCATIONS as u32)),
        "Counter should be restored from the automatic snapshot after restart"
    );

    drop(executor);
    Ok(())
}

#[test]
#[tracing::instrument]
async fn automatic_snapshot_periodic(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_counters")] agent_counters: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start_with_snapshot_policy(
        deps,
        &context,
        SnapshotPolicy::Periodic {
            period: Duration::from_secs(2),
        },
    )
    .await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_counters)
        .store()
        .await?;
    let agent_id = agent_id!("JsonSnapshotCounter", "periodic");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    for _ in 0..SNAPSHOT_TEST_INVOCATIONS {
        executor
            .invoke_and_await_agent(&component, &agent_id, "increment", data_value!())
            .await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    let oplog = executor.get_oplog(&worker_id, OplogIndex::INITIAL).await?;
    let snapshot_count = oplog
        .iter()
        .filter(|entry| matches!(&entry.entry, PublicOplogEntry::Snapshot(_)))
        .count();

    assert!(
        snapshot_count >= 1,
        "Expected at least 1 snapshot with periodic policy (every 2s over ~5s of invocations), got {snapshot_count}"
    );
    assert!(
        snapshot_count <= SNAPSHOT_TEST_INVOCATIONS,
        "Expected at most {SNAPSHOT_TEST_INVOCATIONS} snapshots, got {snapshot_count}"
    );

    drop(executor);
    let executor = start_with_snapshot_policy(
        deps,
        &context,
        SnapshotPolicy::Periodic {
            period: Duration::from_secs(2),
        },
    )
    .await?;
    let mut events = executor.capture_output(&worker_id).await?;

    let result_after_restart = executor
        .invoke_and_await_agent(&component, &agent_id, "get", data_value!())
        .await?;
    assert_snapshot_recovery_loaded(&mut events).await;

    assert_eq!(
        result_after_restart.into_return_value(),
        Some(Value::U32(SNAPSHOT_TEST_INVOCATIONS as u32)),
        "Counter should be restored from the automatic snapshot after restart"
    );

    drop(executor);
    Ok(())
}

#[test]
#[tracing::instrument]
async fn periodic_snapshot_recovery_survives_a_second_snapshot_generation(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_counters")] agent_counters: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let snapshot_policy = SnapshotPolicy::Periodic {
        period: Duration::from_secs(1),
    };
    let executor = start_with_snapshot_policy(deps, &context, snapshot_policy.clone()).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_counters)
        .store()
        .await?;
    let agent_id = agent_id!("SnapshotCounter", "periodic-two-generations");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    for _ in 0..10 {
        executor
            .invoke_and_await_agent(&component, &agent_id, "increment", data_value!())
            .await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    let first_generation = executor.get_oplog(&worker_id, OplogIndex::INITIAL).await?;
    assert!(
        first_generation
            .iter()
            .any(|entry| matches!(entry.entry, PublicOplogEntry::Snapshot(_))),
        "Expected a snapshot before the first restart"
    );

    drop(executor);
    let executor = start_with_snapshot_policy(deps, &context, snapshot_policy.clone()).await?;
    let mut events = executor.capture_output(&worker_id).await?;
    let result = executor
        .invoke_and_await_agent(&component, &agent_id, "get", data_value!())
        .await?;
    assert_snapshot_recovery_loaded(&mut events).await;
    assert_eq!(result.into_return_value(), Some(Value::U32(10)));

    for _ in 0..10 {
        executor
            .invoke_and_await_agent(&component, &agent_id, "increment", data_value!())
            .await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    let second_generation = executor.get_oplog(&worker_id, OplogIndex::INITIAL).await?;
    assert!(
        second_generation
            .iter()
            .filter(|entry| matches!(entry.entry, PublicOplogEntry::Snapshot(_)))
            .count()
            >= 2,
        "Expected a second snapshot generation before the second restart"
    );

    drop(executor);
    let executor = start_with_snapshot_policy(deps, &context, snapshot_policy).await?;
    let mut events = executor.capture_output(&worker_id).await?;
    let result = executor
        .invoke_and_await_agent(&component, &agent_id, "get", data_value!())
        .await?;
    assert_snapshot_recovery_loaded(&mut events).await;

    assert_eq!(result.into_return_value(), Some(Value::U32(20)));

    Ok(())
}

#[test]
#[tracing::instrument]
async fn snapshot_based_recovery(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_counters")] agent_counters: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start_with_snapshot_policy(
        deps,
        &context,
        SnapshotPolicy::EveryNInvocation { count: 3 },
    )
    .await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_counters)
        .store()
        .await?;
    let agent_id = agent_id!("SnapshotCounter", "recovery");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    for _ in 0..105 {
        executor
            .invoke_and_await_agent(&component, &agent_id, "increment", data_value!())
            .await?;
    }

    let result_before = executor
        .invoke_and_await_agent(&component, &agent_id, "get", data_value!())
        .await?;

    let oplog = executor.get_oplog(&worker_id, OplogIndex::INITIAL).await?;
    let snapshot_count = oplog
        .iter()
        .filter(|entry| matches!(&entry.entry, PublicOplogEntry::Snapshot(_)))
        .count();
    assert!(
        snapshot_count >= 1,
        "Expected at least one snapshot before restart, got {snapshot_count}"
    );

    drop(executor);
    let executor = start_with_snapshot_policy(
        deps,
        &context,
        SnapshotPolicy::EveryNInvocation { count: 3 },
    )
    .await?;
    let mut events = executor.capture_output(&worker_id).await?;

    let result_after = executor
        .invoke_and_await_agent(&component, &agent_id, "get", data_value!())
        .await?;
    assert_snapshot_recovery_loaded(&mut events).await;

    assert_eq!(
        result_before, result_after,
        "Worker state should be preserved across restart via snapshot recovery"
    );

    let increment_after = executor
        .invoke_and_await_agent(&component, &agent_id, "increment", data_value!())
        .await?;

    assert_eq!(
        increment_after.into_return_value(),
        Some(Value::U32(106)),
        "Counter should continue from 106 after snapshot recovery"
    );

    drop(executor);
    Ok(())
}

#[test]
#[tracing::instrument]
async fn snapshot_based_recovery_preserves_state_across_multiple_restarts(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_counters")] agent_counters: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);

    let executor = start_with_snapshot_policy(
        deps,
        &context,
        SnapshotPolicy::EveryNInvocation { count: 1 },
    )
    .await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_counters)
        .store()
        .await?;
    let agent_id = agent_id!("SnapshotCounter", "multi-restart");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    for _ in 0..105 {
        executor
            .invoke_and_await_agent(&component, &agent_id, "increment", data_value!())
            .await?;
    }

    drop(executor);
    let executor = start_with_snapshot_policy(
        deps,
        &context,
        SnapshotPolicy::EveryNInvocation { count: 1 },
    )
    .await?;

    for _ in 0..3 {
        executor
            .invoke_and_await_agent(&component, &agent_id, "increment", data_value!())
            .await?;
    }

    drop(executor);
    let executor = start_with_snapshot_policy(
        deps,
        &context,
        SnapshotPolicy::EveryNInvocation { count: 1 },
    )
    .await?;
    let mut events = executor.capture_output(&worker_id).await?;

    let result = executor
        .invoke_and_await_agent(&component, &agent_id, "get", data_value!())
        .await?;
    assert_snapshot_recovery_loaded(&mut events).await;

    assert_eq!(
        result.into_return_value(),
        Some(Value::U32(108)),
        "Counter should be 108 after 105 increments, restart, then 3 more increments"
    );

    drop(executor);
    Ok(())
}

#[test]
#[tracing::instrument]
async fn ts_default_json_snapshot_recovery(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("constructor_parameter_echo")] constructor_parameter_echo: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, constructor_parameter_echo)
        .store()
        .await?;
    let agent_id = agent_id!("SnapshotCounterAgent", "ts-recovery");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    for _ in 0..5 {
        executor
            .invoke_and_await_agent(&component, &agent_id, "increment", data_value!())
            .await?;
    }

    let result_before = executor
        .invoke_and_await_agent(&component, &agent_id, "get", data_value!())
        .await?;

    assert_eq!(
        result_before.clone().into_return_value(),
        Some(Value::F64(5.0)),
        "Counter should be 5 after 5 increments"
    );

    let oplog = executor.get_oplog(&worker_id, OplogIndex::INITIAL).await?;
    let snapshots: Vec<_> = oplog
        .iter()
        .filter_map(|entry| match &entry.entry {
            PublicOplogEntry::Snapshot(params) => Some(params.clone()),
            _ => None,
        })
        .collect();
    assert!(
        !snapshots.is_empty(),
        "Expected at least one snapshot before restart, got 0"
    );

    for (i, snapshot) in snapshots.iter().enumerate() {
        match &snapshot.data {
            PublicSnapshotData::Json(json_data) => {
                let state = json_data
                    .data
                    .get("state")
                    .unwrap_or_else(|| panic!("Snapshot {i} JSON missing 'state' field"));
                assert!(
                    state.get("count").is_some(),
                    "Snapshot {i} JSON state missing 'count' field"
                );
                let count = state["count"].as_f64().unwrap_or_else(|| {
                    panic!("Snapshot {i} 'count' is not a number: {:?}", state["count"])
                });
                assert!(
                    (0.0..=5.0).contains(&count),
                    "Snapshot {i} count should be between 0 and 5, got {count}"
                );
            }
            other => {
                panic!(
                    "Expected JSON snapshot but got {:?}",
                    std::mem::discriminant(other)
                );
            }
        }
    }

    drop(executor);
    let executor = start(deps, &context).await?;
    let mut events = executor.capture_output(&worker_id).await?;

    let result_after = executor
        .invoke_and_await_agent(&component, &agent_id, "get", data_value!())
        .await?;
    assert_snapshot_recovery_loaded(&mut events).await;

    assert_eq!(
        result_before, result_after,
        "TS agent state should be preserved across restart via default JSON snapshot recovery"
    );

    let increment_after = executor
        .invoke_and_await_agent(&component, &agent_id, "increment", data_value!())
        .await?;

    assert_eq!(
        increment_after.into_return_value(),
        Some(Value::F64(6.0)),
        "Counter should continue from 6 after snapshot recovery"
    );

    executor.check_oplog_is_queryable(&worker_id).await?;

    drop(executor);
    Ok(())
}

#[test]
#[tracing::instrument]
async fn ts_default_json_snapshot_recovery_across_multiple_restarts(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("constructor_parameter_echo")] constructor_parameter_echo: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);

    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, constructor_parameter_echo)
        .store()
        .await?;
    let agent_id = agent_id!("SnapshotCounterAgent", "ts-multi-restart");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    for _ in 0..3 {
        executor
            .invoke_and_await_agent(&component, &agent_id, "increment", data_value!())
            .await?;
    }

    let oplog1 = executor.get_oplog(&worker_id, OplogIndex::INITIAL).await?;
    let snapshots1: Vec<_> = oplog1
        .iter()
        .filter_map(|entry| match &entry.entry {
            PublicOplogEntry::Snapshot(params) => Some(params.clone()),
            _ => None,
        })
        .collect();
    assert!(
        !snapshots1.is_empty(),
        "Expected at least one snapshot after first round of increments"
    );

    drop(executor);
    let executor = start(deps, &context).await?;

    for _ in 0..3 {
        executor
            .invoke_and_await_agent(&component, &agent_id, "increment", data_value!())
            .await?;
    }

    let oplog2 = executor.get_oplog(&worker_id, OplogIndex::INITIAL).await?;
    let snapshots2: Vec<_> = oplog2
        .iter()
        .filter_map(|entry| match &entry.entry {
            PublicOplogEntry::Snapshot(params) => Some(params.clone()),
            _ => None,
        })
        .collect();
    assert!(
        snapshots2.len() > snapshots1.len(),
        "Expected more snapshots after second round of increments"
    );

    for (i, snapshot) in snapshots2.iter().enumerate() {
        match &snapshot.data {
            PublicSnapshotData::Json(json_data) => {
                let state = json_data
                    .data
                    .get("state")
                    .unwrap_or_else(|| panic!("Snapshot {i} JSON missing 'state' field"));
                assert!(
                    state.get("count").is_some(),
                    "Snapshot {i} JSON state missing 'count' field"
                );
                let count = state["count"].as_f64().unwrap_or_else(|| {
                    panic!("Snapshot {i} 'count' is not a number: {:?}", state["count"])
                });
                assert!(
                    (0.0..=6.0).contains(&count),
                    "Snapshot {i} count should be between 0 and 6, got {count}"
                );
            }
            other => {
                panic!(
                    "Expected JSON snapshot but got {:?}",
                    std::mem::discriminant(other)
                );
            }
        }
    }

    drop(executor);
    let executor = start(deps, &context).await?;
    let mut events = executor.capture_output(&worker_id).await?;

    let result = executor
        .invoke_and_await_agent(&component, &agent_id, "get", data_value!())
        .await?;
    assert_snapshot_recovery_loaded(&mut events).await;

    assert_eq!(
        result.into_return_value(),
        Some(Value::F64(6.0)),
        "Counter should be 6 after two rounds of 3 increments across restarts"
    );

    drop(executor);
    Ok(())
}

#[test]
#[tracing::instrument]
async fn rust_default_json_snapshot_recovery(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_counters")] agent_counters: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start_with_snapshot_policy(
        deps,
        &context,
        SnapshotPolicy::EveryNInvocation { count: 1 },
    )
    .await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_counters)
        .store()
        .await?;
    let agent_id = agent_id!("JsonSnapshotCounter", "rust-recovery");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    for _ in 0..5 {
        executor
            .invoke_and_await_agent(&component, &agent_id, "increment", data_value!())
            .await?;
    }

    let result_before = executor
        .invoke_and_await_agent(&component, &agent_id, "get", data_value!())
        .await?;

    assert_eq!(
        result_before.clone().into_return_value(),
        Some(Value::U32(5)),
        "Counter should be 5 after 5 increments"
    );

    let oplog = executor.get_oplog(&worker_id, OplogIndex::INITIAL).await?;
    let snapshots: Vec<_> = oplog
        .iter()
        .filter_map(|entry| match &entry.entry {
            PublicOplogEntry::Snapshot(params) => Some(params.clone()),
            _ => None,
        })
        .collect();
    assert!(
        !snapshots.is_empty(),
        "Expected at least one snapshot before restart, got 0"
    );

    for (i, snapshot) in snapshots.iter().enumerate() {
        match &snapshot.data {
            PublicSnapshotData::Json(json_data) => {
                let state = json_data
                    .data
                    .get("state")
                    .unwrap_or_else(|| panic!("Snapshot {i} JSON missing 'state' field"));
                assert!(
                    state.get("count").is_some(),
                    "Snapshot {i} JSON state missing 'count' field"
                );
                let count = state["count"].as_u64().unwrap_or_else(|| {
                    panic!("Snapshot {i} 'count' is not a number: {:?}", state["count"])
                });
                assert!(
                    (0..=5).contains(&count),
                    "Snapshot {i} count should be between 0 and 5, got {count}"
                );
            }
            other => {
                panic!(
                    "Expected JSON snapshot but got {:?}",
                    std::mem::discriminant(other)
                );
            }
        }
    }

    drop(executor);
    let executor = start_with_snapshot_policy(
        deps,
        &context,
        SnapshotPolicy::EveryNInvocation { count: 1 },
    )
    .await?;
    let mut events = executor.capture_output(&worker_id).await?;

    let result_after = executor
        .invoke_and_await_agent(&component, &agent_id, "get", data_value!())
        .await?;
    assert_snapshot_recovery_loaded(&mut events).await;

    assert_eq!(
        result_before, result_after,
        "Rust agent state should be preserved across restart via default JSON snapshot recovery"
    );

    let increment_after = executor
        .invoke_and_await_agent(&component, &agent_id, "increment", data_value!())
        .await?;

    assert_eq!(
        increment_after.into_return_value(),
        Some(Value::U32(6)),
        "Counter should continue from 6 after snapshot recovery"
    );

    executor.check_oplog_is_queryable(&worker_id).await?;

    drop(executor);
    Ok(())
}

#[test]
#[tracing::instrument]
async fn rust_default_json_snapshot_recovery_across_multiple_restarts(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_counters")] agent_counters: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);

    let executor = start_with_snapshot_policy(
        deps,
        &context,
        SnapshotPolicy::EveryNInvocation { count: 1 },
    )
    .await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_counters)
        .store()
        .await?;
    let agent_id = agent_id!("JsonSnapshotCounter", "rust-multi-restart");
    let _worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    for _ in 0..3 {
        executor
            .invoke_and_await_agent(&component, &agent_id, "increment", data_value!())
            .await?;
    }

    let oplog1 = executor.get_oplog(&_worker_id, OplogIndex::INITIAL).await?;
    let snapshots1: Vec<_> = oplog1
        .iter()
        .filter_map(|entry| match &entry.entry {
            PublicOplogEntry::Snapshot(params) => Some(params.clone()),
            _ => None,
        })
        .collect();
    assert!(
        !snapshots1.is_empty(),
        "Expected at least one snapshot after first round of increments"
    );

    drop(executor);
    let executor = start_with_snapshot_policy(
        deps,
        &context,
        SnapshotPolicy::EveryNInvocation { count: 1 },
    )
    .await?;

    for _ in 0..3 {
        executor
            .invoke_and_await_agent(&component, &agent_id, "increment", data_value!())
            .await?;
    }

    let oplog2 = executor.get_oplog(&_worker_id, OplogIndex::INITIAL).await?;
    let snapshots2: Vec<_> = oplog2
        .iter()
        .filter_map(|entry| match &entry.entry {
            PublicOplogEntry::Snapshot(params) => Some(params.clone()),
            _ => None,
        })
        .collect();
    assert!(
        snapshots2.len() > snapshots1.len(),
        "Expected more snapshots after second round of increments"
    );

    for (i, snapshot) in snapshots2.iter().enumerate() {
        match &snapshot.data {
            PublicSnapshotData::Json(json_data) => {
                let state = json_data
                    .data
                    .get("state")
                    .unwrap_or_else(|| panic!("Snapshot {i} JSON missing 'state' field"));
                assert!(
                    state.get("count").is_some(),
                    "Snapshot {i} JSON state missing 'count' field"
                );
                let count = state["count"].as_u64().unwrap_or_else(|| {
                    panic!("Snapshot {i} 'count' is not a number: {:?}", state["count"])
                });
                assert!(
                    (0..=6).contains(&count),
                    "Snapshot {i} count should be between 0 and 6, got {count}"
                );
            }
            other => {
                panic!(
                    "Expected JSON snapshot but got {:?}",
                    std::mem::discriminant(other)
                );
            }
        }
    }

    drop(executor);
    let executor = start_with_snapshot_policy(
        deps,
        &context,
        SnapshotPolicy::EveryNInvocation { count: 1 },
    )
    .await?;
    let mut events = executor.capture_output(&_worker_id).await?;

    let result = executor
        .invoke_and_await_agent(&component, &agent_id, "get", data_value!())
        .await?;
    assert_snapshot_recovery_loaded(&mut events).await;

    assert_eq!(
        result.into_return_value(),
        Some(Value::U32(6)),
        "Counter should be 6 after two rounds of 3 increments across restarts (rust)"
    );

    drop(executor);
    Ok(())
}

#[test]
#[tracing::instrument]
async fn ts_sqlite_multipart_snapshot_recovery(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("constructor_parameter_echo")] constructor_parameter_echo: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, constructor_parameter_echo)
        .store()
        .await?;
    let agent_id = agent_id!("SqliteSnapshotAgent", "sqlite-recovery");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    // Insert data into both databases and set a label
    executor
        .invoke_and_await_agent(&component, &agent_id, "addItem", data_value!("apple"))
        .await?;
    executor
        .invoke_and_await_agent(&component, &agent_id, "addItem", data_value!("banana"))
        .await?;
    executor
        .invoke_and_await_agent(&component, &agent_id, "addLog", data_value!("started"))
        .await?;
    executor
        .invoke_and_await_agent(&component, &agent_id, "setLabel", data_value!("after-init"))
        .await?;

    let state_before = executor
        .invoke_and_await_agent(&component, &agent_id, "getState", data_value!())
        .await?;

    let state_before_str = match state_before.clone().into_return_value() {
        Some(Value::String(s)) => s,
        other => panic!("Expected string from getState, got {:?}", other),
    };
    let state_before_json: serde_json::Value = serde_json::from_str(&state_before_str)?;
    assert_eq!(state_before_json["label"], "after-init");
    assert_eq!(
        state_before_json["items"],
        serde_json::json!(["apple", "banana"])
    );
    assert_eq!(state_before_json["logs"], serde_json::json!(["started"]));

    // Verify multipart snapshots exist in the oplog
    let oplog = executor.get_oplog(&worker_id, OplogIndex::INITIAL).await?;
    let snapshots: Vec<_> = oplog
        .iter()
        .filter_map(|entry| match &entry.entry {
            PublicOplogEntry::Snapshot(params) => Some(params.clone()),
            _ => None,
        })
        .collect();
    assert!(
        !snapshots.is_empty(),
        "Expected at least one snapshot before restart"
    );

    // Verify snapshots are multipart with the expected structure
    let last_snapshot = snapshots.last().unwrap();
    match &last_snapshot.data {
        PublicSnapshotData::Multipart(multipart) => {
            assert!(
                multipart.mime_type.starts_with("multipart/mixed"),
                "Expected multipart/mixed mime type, got '{}'",
                multipart.mime_type
            );

            // Should have a state part (JSON) and two db parts
            let state_parts: Vec<_> = multipart
                .parts
                .iter()
                .filter(|p| p.name == "state")
                .collect();
            assert_eq!(state_parts.len(), 1, "Expected exactly one 'state' part");
            match &state_parts[0].data {
                MultipartPartData::Json(json) => {
                    let state = json
                        .data
                        .get("state")
                        .expect("State JSON should contain 'state' envelope");
                    assert!(
                        state.get("label").is_some(),
                        "State JSON 'state' should contain 'label'"
                    );
                }
                other => panic!("Expected JSON data for state part, got {:?}", other),
            }

            let db_parts: Vec<_> = multipart
                .parts
                .iter()
                .filter(|p| p.name.starts_with("db:"))
                .collect();
            assert_eq!(
                db_parts.len(),
                2,
                "Expected 2 database parts (memDb and fileDb), got {}",
                db_parts.len()
            );

            for db_part in &db_parts {
                assert_eq!(db_part.content_type, "application/x-sqlite3");
                match &db_part.data {
                    MultipartPartData::Raw(raw) => {
                        assert!(
                            !raw.data.is_empty(),
                            "Database part '{}' should not be empty",
                            db_part.name
                        );
                    }
                    other => panic!(
                        "Expected Raw data for db part '{}', got {:?}",
                        db_part.name, other
                    ),
                }
            }
        }
        other => panic!(
            "Expected Multipart snapshot but got {:?}",
            std::mem::discriminant(other)
        ),
    }

    // Restart the executor — this triggers snapshot-based recovery
    drop(executor);
    let executor = start(deps, &context).await?;
    let mut events = executor.capture_output(&worker_id).await?;

    // Verify state is preserved after recovery
    let state_after = executor
        .invoke_and_await_agent(&component, &agent_id, "getState", data_value!())
        .await?;
    assert_snapshot_recovery_loaded(&mut events).await;

    assert_eq!(
        state_before, state_after,
        "Agent state (including SQLite databases) should be preserved across restart"
    );

    // Add more data after recovery to verify databases are functional
    executor
        .invoke_and_await_agent(&component, &agent_id, "addItem", data_value!("cherry"))
        .await?;
    executor
        .invoke_and_await_agent(&component, &agent_id, "addLog", data_value!("recovered"))
        .await?;

    let state_after_more = executor
        .invoke_and_await_agent(&component, &agent_id, "getState", data_value!())
        .await?;

    let state_after_str = match state_after_more.into_return_value() {
        Some(Value::String(s)) => s,
        other => panic!("Expected string from getState, got {:?}", other),
    };
    let state_after_json: serde_json::Value = serde_json::from_str(&state_after_str)?;
    assert_eq!(state_after_json["label"], "after-init");
    assert_eq!(
        state_after_json["items"],
        serde_json::json!(["apple", "banana", "cherry"])
    );
    assert_eq!(
        state_after_json["logs"],
        serde_json::json!(["started", "recovered"])
    );

    executor.check_oplog_is_queryable(&worker_id).await?;

    drop(executor);
    Ok(())
}
