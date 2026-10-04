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
use axum::Router;
use axum::routing::post;
use bytes::Bytes;
use golem_common::model::IdempotencyKey;
use golem_common::{agent_id, data_value};
use golem_test_framework::dsl::TestDsl;
use golem_worker_executor_test_utils::{
    LastUniqueId, PrecompiledComponent, TestContext, WorkerExecutorTestDependencies, start,
};
use http::HeaderMap;
use pretty_assertions::assert_eq;
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use test_r::{inherit_test_dep, test};
use tokio::spawn;
use tracing::Instrument;

inherit_test_dep!(WorkerExecutorTestDependencies);
inherit_test_dep!(LastUniqueId);
inherit_test_dep!(Tracing);
inherit_test_dep!(
    #[tagged_as("http_tests")]
    PrecompiledComponent
);

#[test]
#[tracing::instrument]
async fn http_client(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    _tracing: &Tracing,
    #[tagged_as("http_tests")] http_tests: &PrecompiledComponent,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let host_http_port = listener.local_addr().unwrap().port();

    let http_server = spawn(
        async move {
            let route = Router::new().route(
                "/",
                post(move |headers: HeaderMap, body: Bytes| async move {
                    let header = headers.get("X-Test").unwrap().to_str().unwrap();
                    let body = String::from_utf8(body.to_vec()).unwrap();
                    format!("response is {header} {body}")
                }),
            );

            axum::serve(listener, route).await.unwrap();
        }
        .in_current_span(),
    );

    let component = executor
        .component_dep(&context.default_environment_id, http_tests)
        .store()
        .await?;
    let mut env = HashMap::new();
    env.insert("PORT".to_string(), host_http_port.to_string());
    env.insert("RUST_BACKTRACE".to_string(), "full".to_string());

    let agent_id = agent_id!("HttpClient");
    let worker_id = executor
        .start_agent_with(&component.id, agent_id.clone(), env, Vec::new())
        .await?;
    let rx = executor.capture_output(&worker_id).await?;

    let result = executor
        .invoke_and_await_agent(&component, &agent_id, "run", data_value!())
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    drop(executor);
    drop(rx);
    http_server.abort();

    assert_eq!(result, data_value!("200 response is test-header test-body"));
    Ok(())
}

#[test]
#[tracing::instrument]
async fn http_client_using_reqwest(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    _tracing: &Tracing,
    #[tagged_as("http_tests")] http_tests: &PrecompiledComponent,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let captured_body: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let captured_body_clone = captured_body.clone();

    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let host_http_port = listener.local_addr().unwrap().port();

    let http_server = spawn(
        async move {
            let route = Router::new().route(
                "/post-example",
                post(move |headers: HeaderMap, body: Bytes| async move {
                    let header = headers
                        .get("X-Test")
                        .map(|h| h.to_str().unwrap().to_string())
                        .unwrap_or("no X-Test header".to_string());
                    let body = String::from_utf8(body.to_vec()).unwrap();
                    {
                        let mut capture = captured_body_clone.lock().unwrap();
                        *capture = Some(body.clone());
                    }
                    format!(
                        "{{ \"percentage\" : 0.25, \"message\": \"response message {header}\" }}"
                    )
                }),
            );

            axum::serve(listener, route).await.unwrap();
        }
        .in_current_span(),
    );

    let component = executor
        .component_dep(&context.default_environment_id, http_tests)
        .store()
        .await?;
    let mut env = HashMap::new();
    env.insert("PORT".to_string(), host_http_port.to_string());

    let agent_id = agent_id!("HttpClient2");
    let worker_id = executor
        .start_agent_with(&component.id, agent_id.clone(), env, Vec::new())
        .await?;

    let result = executor
        .invoke_and_await_agent(&component, &agent_id, "run", data_value!())
        .await?;

    let captured_body = captured_body.lock().unwrap().clone().unwrap();

    executor.check_oplog_is_queryable(&worker_id).await?;

    drop(executor);
    http_server.abort();

    assert_eq!(
        result,
        data_value!(
            "200 ExampleResponse { percentage: 0.25, message: Some(\"response message Golem\") }"
        )
    );
    assert_eq!(
        captured_body,
        "{\"name\":\"Something\",\"amount\":42,\"comments\":[\"Hello\",\"World\"]}".to_string()
    );
    Ok(())
}

#[test]
#[tracing::instrument]
async fn http_client_using_reqwest_async(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    _tracing: &Tracing,
    #[tagged_as("http_tests")] http_tests: &PrecompiledComponent,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let captured_body: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let captured_body_clone = captured_body.clone();

    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let host_http_port = listener.local_addr().unwrap().port();

    let http_server = spawn(
        async move {
            let route = Router::new().route(
                "/post-example",
                post(move |headers: HeaderMap, body: Bytes| async move {
                    let header = headers
                        .get("X-Test")
                        .map(|h| h.to_str().unwrap().to_string())
                        .unwrap_or("no X-Test header".to_string());
                    let body = String::from_utf8(body.to_vec()).unwrap();
                    {
                        let mut capture = captured_body_clone.lock().unwrap();
                        *capture = Some(body.clone());
                    }
                    format!(
                        "{{ \"percentage\" : 0.25, \"message\": \"response message {header}\" }}"
                    )
                }),
            );

            axum::serve(listener, route).await.unwrap();
        }
        .in_current_span(),
    );

    let component = executor
        .component_dep(&context.default_environment_id, http_tests)
        .store()
        .await?;
    let mut env = HashMap::new();
    env.insert("PORT".to_string(), host_http_port.to_string());

    let agent_id = agent_id!("HttpClient3");
    let worker_id = executor
        .start_agent_with(&component.id, agent_id.clone(), env, Vec::new())
        .await?;

    let result = executor
        .invoke_and_await_agent(&component, &agent_id, "run", data_value!())
        .await?;
    let captured_body = captured_body.lock().unwrap().clone().unwrap();

    executor.check_oplog_is_queryable(&worker_id).await?;

    drop(executor);
    http_server.abort();

    assert_eq!(
        result,
        data_value!(
            "200 ExampleResponse { percentage: 0.25, message: Some(\"response message Golem\") }"
        )
    );
    assert_eq!(
        captured_body,
        "{\"name\":\"Something\",\"amount\":42,\"comments\":[\"Hello\",\"World\"]}".to_string()
    );

    Ok(())
}

#[test]
#[tracing::instrument]
async fn http_client_using_reqwest_async_parallel(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    _tracing: &Tracing,
    #[tagged_as("http_tests")] http_tests: &PrecompiledComponent,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;
    let captured_body: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_body_clone = captured_body.clone();

    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let host_http_port = listener.local_addr().unwrap().port();

    let http_server = spawn(
        async move {
            let route = Router::new().route(
                "/post-example",
                post(move |headers: HeaderMap, body: Bytes| async move {
                    let header = headers
                        .get("X-Test")
                        .map(|h| h.to_str().unwrap().to_string())
                        .unwrap_or("no X-Test header".to_string());
                    let body = String::from_utf8(body.to_vec()).unwrap();
                    {
                        let mut capture = captured_body_clone.lock().unwrap();
                        capture.push(body.clone());
                    }
                    format!(
                        "{{ \"percentage\" : 0.25, \"message\": \"response message {header}\" }}"
                    )
                }),
            );

            axum::serve(listener, route).await.unwrap();
        }
        .in_current_span(),
    );

    let component = executor
        .component_dep(&context.default_environment_id, http_tests)
        .store()
        .await?;
    let mut env = HashMap::new();
    env.insert("PORT".to_string(), host_http_port.to_string());

    let agent_id = agent_id!("HttpClient3");
    let worker_id = executor
        .start_agent_with(&component.id, agent_id.clone(), env, Vec::new())
        .await?;

    let result = executor
        .invoke_and_await_agent(&component, &agent_id, "run_parallel", data_value!(32u16))
        .await?;
    let mut captured_body = captured_body.lock().unwrap().clone();
    captured_body.sort();

    executor.check_oplog_is_queryable(&worker_id).await?;

    drop(executor);
    http_server.abort();

    let return_value = result.into_return_value().expect("Expected a return value");
    let golem_wasm::Value::List(lst) = &return_value else {
        panic!("Expected List, got {:?}", return_value)
    };
    assert_eq!(lst.len(), 32);
    assert_eq!(
        captured_body,
        vec![
            r#"{"name":"Something","amount":0,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":1,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":10,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":11,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":12,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":13,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":14,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":15,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":16,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":17,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":18,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":19,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":2,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":20,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":21,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":22,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":23,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":24,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":25,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":26,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":27,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":28,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":29,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":3,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":30,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":31,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":4,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":5,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":6,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":7,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":8,"comments":["Hello","World"]}"#.to_string(),
            r#"{"name":"Something","amount":9,"comments":["Hello","World"]}"#.to_string(),
        ]
    );

    Ok(())
}

#[test]
#[tracing::instrument]
async fn outgoing_http_contains_idempotency_key(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    _tracing: &Tracing,
    #[tagged_as("http_tests")] http_tests: &PrecompiledComponent,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let host_http_port = listener.local_addr().unwrap().port();

    let http_server = spawn(
        async move {
            let route = Router::new().route(
                "/post-example",
                post(move |headers: HeaderMap| async move {
                    let idempotency_key = headers
                        .get("idempotency-key")
                        .map(|h| h.to_str().unwrap().to_string());
                    let idempotency_key_str = idempotency_key.map(|i| i.to_string());
                    json!({
                        "percentage": 0.0,
                        "message": idempotency_key_str
                    })
                    .to_string()
                }),
            );

            axum::serve(listener, route).await.unwrap();
        }
        .in_current_span(),
    );

    let component = executor
        .component_dep(&context.default_environment_id, http_tests)
        .store()
        .await?;
    let mut env = HashMap::new();
    env.insert("PORT".to_string(), host_http_port.to_string());

    let agent_id = agent_id!("HttpClient2");
    let worker_id = executor
        .start_agent_with(&component.id, agent_id.clone(), env, Vec::new())
        .await?;

    let key = IdempotencyKey::new("177db03d-3234-4a04-8d03-e8d042348abd".to_string());
    let result = executor
        .invoke_and_await_agent_with_key(&component, &agent_id, &key, "run", data_value!())
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    drop(executor);
    http_server.abort();

    assert_eq!(
        result,
        data_value!(
            "200 ExampleResponse { percentage: 0.0, message: Some(\"8c25adee-7935-5315-a99b-457f41180bc1\") }"
        )
    );
    Ok(())
}

/// Regression test for the `io::poll::poll` replay-order bug (`durable_host/io/poll.rs`,
/// `map_recorded_poll_ready`; GOLEM_IO_POLL_BUG.md "Fifth Bug").
///
/// Golem used to replay a recorded `poll()` answer POSITIONALLY: the recorded ready index was
/// handed back as-is. But a guest's target-list order need not be replay-stable — wstd's reactor
/// (and the `PollOrderClient` test agent, which mirrors it) builds the list by iterating a
/// `HashMap` keyed on a process-wide counter, so a fresh instance (here: one resumed from a
/// snapshot) presents the same pollables in a different order than the live run did. The
/// replayed index then names a different pollable — e.g. the never-firing timeout instead of the
/// HTTP response — and a guest that trusts `poll()` diverges from what it did live.
///
/// Setup: `warm_up` advances the agent's process-wide wait counter in the live instance and a
/// snapshot lands right after it; `racing_fetches` then runs GETs (response + timeout:
/// 2-pollable polls) and large POSTs (body-write backpressure + response + timeout: 3-pollable
/// polls) AFTER the snapshot. A cold restart resumes from the snapshot — with the counter back at
/// 0 — and must replay `racing_fetches` to rebuild the agent state: every result must come back
/// exactly as recorded, and no request may be re-sent.
#[test]
#[tracing::instrument]
async fn poll_replay_after_snapshot_restore_maps_ready_set_by_pollable_identity(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    _tracing: &Tracing,
    #[tagged_as("http_tests")] http_tests: &PrecompiledComponent,
) -> anyhow::Result<()> {
    use axum::body::Body;
    use axum::extract::Path;
    use axum::routing::get;
    use futures::StreamExt;
    use golem_common::model::oplog::PublicOplogEntry;
    use golem_common::model::{AgentStatus, OplogIndex};
    use golem_wasm::Value;
    use golem_worker_executor::services::golem_config::SnapshotPolicy;
    use golem_worker_executor_test_utils::start_with_snapshot_policy;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    const ROUNDS: u32 = 6;
    const UPLOAD_SIZE: usize = 2 * 1024 * 1024; // keep in sync with poll_order_client.rs

    let context = TestContext::new(last_unique_id);
    // Every 2nd invocation (the constructor counts as one): the constructor + three `warm_up`
    // calls end on a snapshot, the single `racing_fetches` call after them does not — so it is
    // exactly the region replayed after the restart.
    let snapshot_policy = SnapshotPolicy::EveryNInvocation { count: 2 };
    let executor = start_with_snapshot_policy(deps, &context, snapshot_policy.clone()).await?;

    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let host_http_port = listener.local_addr().unwrap().port();
    let requests_served = Arc::new(AtomicUsize::new(0));
    let served = requests_served.clone();
    let served_upload = requests_served.clone();
    let http_server = spawn(
        async move {
            let route = Router::new()
                .route(
                    "/delayed/{round}",
                    get(move |Path(round): Path<u32>| {
                        let served = served.clone();
                        async move {
                            // Long enough that the guest has to poll (response + timeout).
                            tokio::time::sleep(Duration::from_millis(150)).await;
                            served.fetch_add(1, Ordering::SeqCst);
                            format!("get-{round}")
                        }
                    }),
                )
                .route(
                    "/upload/{round}",
                    post(move |Path(round): Path<u32>, body: Body| {
                        let served = served_upload.clone();
                        async move {
                            // Read slowly so the guest's body writes hit backpressure.
                            let mut stream = body.into_data_stream();
                            let mut len = 0usize;
                            while let Some(chunk) = stream.next().await {
                                len += chunk.unwrap().len();
                                tokio::time::sleep(Duration::from_millis(2)).await;
                            }
                            served.fetch_add(1, Ordering::SeqCst);
                            format!("post-{round}-{len}")
                        }
                    }),
                );
            axum::serve(listener, route).await.unwrap();
        }
        .in_current_span(),
    );

    let component = executor
        .component_dep(&context.default_environment_id, http_tests)
        .store()
        .await?;
    let mut env = HashMap::new();
    env.insert("PORT".to_string(), host_http_port.to_string());
    let agent_id = agent_id!("PollOrderClient", "snapshot-restore");
    let worker_id = executor
        .start_agent_with(&component.id, agent_id.clone(), env, Vec::new())
        .await?;

    for _ in 0..3 {
        executor
            .invoke_and_await_agent(&component, &agent_id, "warm_up", data_value!(1013u64))
            .await?;
    }
    let snapshots = |oplog: &[golem_common::model::oplog::PublicOplogEntryWithIndex]| {
        oplog
            .iter()
            .filter(|entry| matches!(&entry.entry, PublicOplogEntry::Snapshot(_)))
            .count()
    };
    let snapshots_before = snapshots(&executor.get_oplog(&worker_id, OplogIndex::INITIAL).await?);
    assert!(
        snapshots_before >= 1,
        "test setup invalid: expected a snapshot after the warm-up invocations"
    );

    let expected: Vec<Value> = (0..ROUNDS)
        .flat_map(|round| {
            [
                Value::String(format!("200:get-{round}")),
                Value::String(format!("200:post-{round}-{UPLOAD_SIZE}")),
            ]
        })
        .collect();

    let live = executor
        .invoke_and_await_agent(&component, &agent_id, "racing_fetches", data_value!(ROUNDS))
        .await?
        .into_return_value();
    assert_eq!(live, Some(Value::List(expected.clone())), "live run");
    assert_eq!(
        snapshots(&executor.get_oplog(&worker_id, OplogIndex::INITIAL).await?),
        snapshots_before,
        "test setup invalid: racing_fetches must not be covered by a snapshot, or the restart \
         would not replay it"
    );
    let served_live = requests_served.load(Ordering::SeqCst);
    assert_eq!(served_live, 2 * ROUNDS as usize);

    // Cold restart: the next invocation resumes from the snapshot in a fresh instance (wait
    // counter back at 0, so different target-list orders) and replays racing_fetches.
    drop(executor);
    let executor = start_with_snapshot_policy(deps, &context, snapshot_policy).await?;

    let recovered = executor
        .invoke_and_await_agent(&component, &agent_id, "results", data_value!())
        .await?
        .into_return_value();
    let metadata = executor
        .wait_for_statuses(
            &worker_id,
            &[AgentStatus::Idle, AgentStatus::Failed],
            Duration::from_secs(30),
        )
        .await?;

    drop(executor);
    http_server.abort();

    assert_eq!(
        recovered,
        Some(Value::List(expected)),
        "replay after the snapshot restore must hand every poll() answer to the same pollable \
         as live — a `timeout` here is the replayed ready index waking the timer"
    );
    assert_eq!(metadata.status, AgentStatus::Idle);
    assert_eq!(
        requests_served.load(Ordering::SeqCst),
        served_live,
        "replay must not re-send any request"
    );
    Ok(())
}
