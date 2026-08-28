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
use golem_common::base_model::agent::{ComponentModelElementValue, ElementValue};
use golem_common::model::agent::{DataValue, ElementValues};
use golem_common::model::oplog::OplogIndex;
use golem_common::model::{AgentStatus, PromiseId};
use golem_common::{agent_id, data_value};
use golem_test_framework::dsl::TestDsl;
use golem_wasm::analysis::analysed_type;
use golem_wasm::{FromValue, IntoValue, IntoValueAndType, UuidRecord, Value, ValueAndType};
use golem_worker_executor_test_utils::{
    LastUniqueId, PrecompiledComponent, TestContext, WorkerExecutorTestDependencies, start,
};
use pretty_assertions::assert_eq;
use std::time::Duration;
use test_r::{inherit_test_dep, test};
use tracing::Instrument;

inherit_test_dep!(WorkerExecutorTestDependencies);
inherit_test_dep!(LastUniqueId);
inherit_test_dep!(
    #[tagged_as("agent_rpc_rust")]
    PrecompiledComponent
);
inherit_test_dep!(
    #[tagged_as("agent_rpc")]
    PrecompiledComponent
);
inherit_test_dep!(
    #[tagged_as("agent_counters")]
    PrecompiledComponent
);
inherit_test_dep!(Tracing);

#[test]
#[tracing::instrument]
async fn rust_rpc_with_payload(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc_rust")] agent_rpc_rust: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc_rust)
        .store()
        .await?;

    let parent_agent_id = agent_id!("RustParent", "rust_rpc_with_payload");
    let parent = executor
        .start_agent(&component.id, parent_agent_id.clone())
        .await?;

    executor.log_output(&parent).await?;

    let spawn_result = executor
        .invoke_and_await_agent(
            &component,
            &parent_agent_id,
            "spawn_child",
            data_value!("hello world"),
        )
        .await?;

    let uuid_as_value = spawn_result
        .into_return_value()
        .expect("Expected a single return value");

    let uuid = UuidRecord::from_value(uuid_as_value.clone()).expect("UUID expected");

    let child_agent_id = agent_id!("RustChild", uuid);

    let get_result = executor
        .invoke_and_await_agent(&component, &child_agent_id, "get", data_value!())
        .await?;

    let option_payload_as_value = get_result
        .into_return_value()
        .expect("Expected a single return value");

    executor.check_oplog_is_queryable(&parent).await?;

    assert_eq!(
        option_payload_as_value,
        Value::Option(Some(Box::new(Value::Record(vec![
            Value::String("hello world".to_string()),
            uuid_as_value.clone(),
            Value::Enum(0)
        ]))))
    );
    Ok(())
}

#[test]
#[tracing::instrument]
async fn rust_rpc_missing_target(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc_rust")] agent_rpc_rust: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc_rust)
        .store()
        .await?;

    let parent_agent_id = agent_id!("RustParent", "rust_rpc_with_payload");
    let parent = executor
        .start_agent(&component.id, parent_agent_id.clone())
        .await?;

    executor.log_output(&parent).await?;

    let call_result = executor
        .invoke_and_await_agent(
            &component,
            &parent_agent_id,
            "call_ts_agent",
            data_value!("example"),
        )
        .await;

    assert!(
        call_result
            .err()
            .unwrap()
            .to_string()
            .contains("Agent type not registered")
    );

    Ok(())
}

#[test]
#[tracing::instrument]
async fn counter_resource_test_1(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc_rust")] agent_rpc_rust: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc_rust)
        .store()
        .await?;

    let agent_id = agent_id!("RpcCaller", "counter_resource_test_1");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    let result = executor
        .invoke_and_await_agent(&component, &agent_id, "test1", data_value!())
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    let result_value = result
        .into_return_value()
        .expect("Expected a single return value");

    assert_eq!(
        result_value,
        Value::List(vec![
            Value::Tuple(vec![
                Value::String("counter_resource_test_1_test1_counter3".to_string()),
                Value::U64(3)
            ]),
            Value::Tuple(vec![
                Value::String("counter_resource_test_1_test1_counter2".to_string()),
                Value::U64(3)
            ]),
            Value::Tuple(vec![
                Value::String("counter_resource_test_1_test1_counter1".to_string()),
                Value::U64(3)
            ])
        ])
    );

    Ok(())
}

#[test]
#[tracing::instrument]
async fn counter_resource_test_2(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc_rust")] agent_rpc_rust: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc_rust)
        .store()
        .await?;

    let agent_id = agent_id!("RpcCaller", "counter_resource_test_2");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    let result1 = executor
        .invoke_and_await_agent(&component, &agent_id, "test2", data_value!())
        .await?;

    let result2 = executor
        .invoke_and_await_agent(&component, &agent_id, "test2", data_value!())
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    let result_value1 = result1
        .into_return_value()
        .expect("Expected a single return value");
    let result_value2 = result2
        .into_return_value()
        .expect("Expected a single return value");

    assert_eq!(result_value1, Value::U64(1));
    assert_eq!(result_value2, Value::U64(2));

    Ok(())
}

#[test]
#[tracing::instrument]
async fn counter_resource_test_2_with_restart(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc_rust")] agent_rpc_rust: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc_rust)
        .store()
        .await?;

    let agent_id = agent_id!("RpcCaller", "counter_resource_test_2_with_restart");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    let result1 = executor
        .invoke_and_await_agent(&component, &agent_id, "test2", data_value!())
        .await?;

    drop(executor);
    let executor = start(deps, &context).await?;

    let result2 = executor
        .invoke_and_await_agent(&component, &agent_id, "test2", data_value!())
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    let result_value1 = result1
        .into_return_value()
        .expect("Expected a single return value");
    let result_value2 = result2
        .into_return_value()
        .expect("Expected a single return value");

    assert_eq!(result_value1, Value::U64(1));
    assert_eq!(result_value2, Value::U64(2));

    Ok(())
}

#[test]
#[tracing::instrument]
async fn counter_resource_test_3(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc_rust")] agent_rpc_rust: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc_rust)
        .store()
        .await?;

    let agent_id = agent_id!("RpcCaller", "counter_resource_test_3");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    let result1 = executor
        .invoke_and_await_agent(&component, &agent_id, "test3", data_value!())
        .await?;

    let result2 = executor
        .invoke_and_await_agent(&component, &agent_id, "test3", data_value!())
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    let result_value1 = result1
        .into_return_value()
        .expect("Expected a single return value");
    let result_value2 = result2
        .into_return_value()
        .expect("Expected a single return value");

    assert_eq!(result_value1, Value::U64(1));
    assert_eq!(result_value2, Value::U64(2));

    Ok(())
}

#[test]
#[tracing::instrument]
async fn counter_resource_test_3_with_restart(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc_rust")] agent_rpc_rust: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc_rust)
        .store()
        .await?;

    let agent_id = agent_id!("RpcCaller", "counter_resource_test_3_with_restart");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    let result1 = executor
        .invoke_and_await_agent(&component, &agent_id, "test3", data_value!())
        .await?;

    drop(executor);
    let executor = start(deps, &context).await?;

    let result2 = executor
        .invoke_and_await_agent(&component, &agent_id, "test3", data_value!())
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    let result_value1 = result1
        .into_return_value()
        .expect("Expected a single return value");
    let result_value2 = result2
        .into_return_value()
        .expect("Expected a single return value");

    assert_eq!(result_value1, Value::U64(1));
    assert_eq!(result_value2, Value::U64(2));

    Ok(())
}

#[test]
#[tracing::instrument]
async fn context_inheritance(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc_rust")] agent_rpc_rust: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc_rust)
        .store()
        .await?;

    let agent_id = agent_id!("RpcCaller", "context_inheritance");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    let result = executor
        .invoke_and_await_agent(&component, &agent_id, "test4", data_value!())
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    let result_value = result
        .into_return_value()
        .expect("Expected a single return value");

    let result_tuple = match &result_value {
        Value::Tuple(result) => result,
        _ => panic!("Unexpected result: {result_value:?}"),
    };
    let args = match &result_tuple[0] {
        Value::List(args) => args.clone(),
        _ => panic!("Unexpected result: {result_value:?}"),
    };
    let mut env = match &result_tuple[1] {
        Value::List(env) => env
            .clone()
            .into_iter()
            .map(|value| match value {
                Value::Tuple(tuple) => match (&tuple[0], &tuple[1]) {
                    (Value::String(key), Value::String(value)) => (key.clone(), value.clone()),
                    _ => panic!("Unexpected result: {result_value:?}"),
                },
                _ => panic!("Unexpected result: {result_value:?}"),
            })
            .collect::<Vec<_>>(),
        _ => panic!("Unexpected result: {result_value:?}"),
    };
    env.sort_by_key(|(k, _v)| k.clone());

    assert_eq!(args, vec![] as Vec<Value>);

    let env_keys: Vec<&str> = env.iter().map(|(k, _)| k.as_str()).collect();
    assert!(
        env_keys.contains(&"GOLEM_AGENT_ID"),
        "Expected GOLEM_AGENT_ID in env, got: {env:?}"
    );
    assert!(
        env_keys.contains(&"GOLEM_WORKER_NAME"),
        "Expected GOLEM_WORKER_NAME in env, got: {env:?}"
    );
    assert!(
        env_keys.contains(&"GOLEM_COMPONENT_ID"),
        "Expected GOLEM_COMPONENT_ID in env, got: {env:?}"
    );
    assert!(
        env_keys.contains(&"GOLEM_COMPONENT_REVISION"),
        "Expected GOLEM_COMPONENT_REVISION in env, got: {env:?}"
    );
    assert!(
        env_keys.contains(&"GOLEM_AGENT_TYPE"),
        "Expected GOLEM_AGENT_TYPE in env, got: {env:?}"
    );

    Ok(())
}

#[test]
#[tracing::instrument]
async fn counter_resource_test_5(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc_rust")] agent_rpc_rust: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc_rust)
        .store()
        .await?;

    let agent_id = agent_id!("RpcCaller", "counter_resource_test_5");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    let result = executor
        .invoke_and_await_agent(&component, &agent_id, "test5", data_value!())
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    let result_value = result
        .into_return_value()
        .expect("Expected a single return value");

    assert_eq!(
        result_value,
        Value::List(vec![Value::U64(3), Value::U64(3), Value::U64(3),])
    );

    Ok(())
}

#[test]
#[tracing::instrument]
async fn wasm_rpc_bug_32_test(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc_rust")] agent_rpc_rust: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc_rust)
        .store()
        .await?;

    let agent_id = agent_id!("RpcCaller", "wasm_rpc_bug_32_test");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    let input_vat = ValueAndType {
        value: Value::Enum(0),
        typ: analysed_type::r#enum(&["leaf"]),
    };

    let result = executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "bug_wasm_rpc_i32",
            DataValue::Tuple(ElementValues {
                elements: vec![ElementValue::ComponentModel(ComponentModelElementValue {
                    value: input_vat,
                })],
            }),
        )
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    let result_value = result
        .into_return_value()
        .expect("Expected a single return value");

    assert_eq!(result_value, Value::Enum(0));

    Ok(())
}

#[test]
#[tracing::instrument]
async fn golem_bug_1265_test(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc_rust")] agent_rpc_rust: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc_rust)
        .store()
        .await?;

    let agent_id = agent_id!("RpcCaller", "golem_bug_1265_test");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    let result = executor
        .invoke_and_await_agent(&component, &agent_id, "bug_golem1265", data_value!("test"))
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    let result_value = result
        .into_return_value()
        .expect("Expected a single return value");

    assert_eq!(result_value, Value::Result(Ok(None)));

    Ok(())
}

#[test]
#[tracing::instrument]
async fn ephemeral_worker_invocation_via_rpc1(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_counters")] agent_counters: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_counters)
        .store()
        .await?;
    let agent_id = agent_id!("Counter", "ephemeral_worker_invocation_via_rpc1");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    let _ = executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "increment_through_rpc_to_ephemeral",
            data_value!(),
        )
        .await?;
    let result = executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "increment_through_rpc_to_ephemeral",
            data_value!(),
        )
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;
    drop(executor);

    let value = result
        .into_return_value()
        .expect("Expected a single return value");
    assert_eq!(value, Value::U32(1));

    Ok(())
}

#[test]
#[tracing::instrument]
async fn ephemeral_worker_invocation_via_rpc2(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_counters")] agent_counters: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_counters)
        .store()
        .await?;
    let agent_id = agent_id!("Counter", "ephemeral_worker_invocation_via_rpc2");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    let _ = executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "increment_through_rpc_to_ephemeral_phantom",
            data_value!(),
        )
        .await;
    let result = executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "increment_through_rpc_to_ephemeral_phantom",
            data_value!(),
        )
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;
    drop(executor);

    let value = result
        .into_return_value()
        .expect("Expected a single return value");
    assert_eq!(value, Value::U32(1));

    Ok(())
}

#[test]
#[tracing::instrument]
async fn cancel_pending_async_rpc_returns_error(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc_rust")] agent_rpc_rust: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc_rust)
        .store()
        .await?;

    let agent_id = agent_id!("CancelTester", "cancel_pending_test");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    // Call test_cancel_before_await - initiates async RPC to inc_by, then cancels
    executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "test_cancel_before_await",
            data_value!("cancel_pending_counter"),
        )
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    // The test verifies that cancel() doesn't panic/trap and completes successfully.
    // Cancel is "best-effort" — the remote invocation may or may not have already
    // executed by the time cancel is processed.

    Ok(())
}

#[test]
#[tracing::instrument]
async fn cancel_completed_async_rpc_is_noop(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc_rust")] agent_rpc_rust: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc_rust)
        .store()
        .await?;

    let agent_id = agent_id!("CancelTester", "cancel_completed_test");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    let result = executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "test_cancel_completed",
            data_value!("cancel_completed_counter"),
        )
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    let result_value = result
        .into_return_value()
        .expect("Expected a single return value");

    // The counter was incremented by 5, so get_value should return 5
    assert_eq!(result_value, Value::U64(5));

    Ok(())
}

#[test]
#[tracing::instrument]
async fn ts_abort_before_await_returns_aborted(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc")] agent_rpc: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc)
        .store()
        .await?;

    let agent_id = agent_id!("TsCancelTester", "ts_abort_test1");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    let result = executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "testAbortBeforeAwait",
            data_value!("ts_abort_counter1"),
        )
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    let result_value = result
        .into_return_value()
        .expect("Expected a single return value");

    assert_eq!(result_value, Value::String("aborted".to_string()));

    Ok(())
}

#[test]
#[tracing::instrument]
async fn ts_abort_after_complete_is_noop(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc")] agent_rpc: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc)
        .store()
        .await?;

    let agent_id = agent_id!("TsCancelTester", "ts_abort_test2");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    let result = executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "testAbortAfterComplete",
            data_value!("ts_abort_counter2"),
        )
        .await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    let result_value = result
        .into_return_value()
        .expect("Expected a single return value");

    // The counter was incremented by 5, so getValue should return 5.0
    assert_eq!(result_value, Value::F64(5.0));

    Ok(())
}

fn extract_oplog_idx_from_promise_id(promise_id_value: &Value) -> OplogIndex {
    let Value::Record(fields) = promise_id_value else {
        panic!("Expected a record for PromiseId");
    };
    let Value::U64(oplog_idx) = fields[1] else {
        panic!("Expected u64 oplog-idx field");
    };
    OplogIndex::from_u64(oplog_idx)
}

#[test]
#[tracing::instrument]
async fn ts_cancel_unblocks_caller_while_callee_blocked(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc")] agent_rpc: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc)
        .store()
        .await?;

    // Start agent B (TsBlockingAgent) and prepare a promise
    let b_name = "cancel_unblocks_b";
    let b_agent_id = agent_id!("TsBlockingAgent", b_name);
    let b_worker_id = executor
        .start_agent(&component.id, b_agent_id.clone())
        .await?;

    let prepare_result = executor
        .invoke_and_await_agent(&component, &b_agent_id, "prepareBlock", data_value!())
        .await?;

    let promise_id_value = prepare_result
        .into_return_value()
        .expect("Expected a single return value from prepareBlock");

    let oplog_idx = extract_oplog_idx_from_promise_id(&promise_id_value);

    // Start agent A (TsCancelCallerAgent)
    let a_name = "cancel_unblocks_a";
    let a_agent_id = agent_id!("TsCancelCallerAgent", a_name);
    let _a_worker_id = executor
        .start_agent(&component.id, a_agent_id.clone())
        .await?;

    // Spawn fiber: A.callAndAbort(bName, 3000ms delay before abort)
    let executor_clone = executor.clone();
    let component_clone = component.clone();
    let a_agent_id_clone = a_agent_id.clone();

    let mut fiber = tokio::spawn(
        async move {
            executor_clone
                .invoke_and_await_agent(
                    &component_clone,
                    &a_agent_id_clone,
                    "callAndAbort",
                    data_value!(b_name, 3000.0),
                )
                .await
        }
        .in_current_span(),
    );

    // Wait for B to suspend on the promise
    tokio::select! {
        result = &mut fiber => {
            let invoke_result = result??;
            return Err(anyhow::anyhow!("callAndAbort returned before B suspended: {:?}", invoke_result));
        }
        status = executor.wait_for_status(&b_worker_id, AgentStatus::Suspended, Duration::from_secs(30)) => {
            status?;
        }
    }

    // Now wait for A's result (abort fires at 3s, so A should complete relatively soon)
    let a_result = fiber.await??;
    let a_value = a_result
        .into_return_value()
        .expect("Expected a single return value from callAndAbort");
    assert_eq!(a_value, Value::String("aborted".to_string()));

    // B should still be suspended (cancel unblocked caller but NOT callee)
    let b_status = executor.get_worker_metadata(&b_worker_id).await?.status;
    assert_eq!(b_status, AgentStatus::Suspended);

    // Complete the promise to unblock B
    executor
        .complete_promise(
            &PromiseId {
                agent_id: b_worker_id.clone(),
                oplog_idx,
            },
            vec![],
        )
        .await?;

    // Wait for B to return to Idle
    executor
        .wait_for_status(&b_worker_id, AgentStatus::Idle, Duration::from_secs(10))
        .await?;

    // Verify B processed the call
    let count_result = executor
        .invoke_and_await_agent(&component, &b_agent_id, "getCompletedCount", data_value!())
        .await?;

    let count_value = count_result
        .into_return_value()
        .expect("Expected a single return value from getCompletedCount");
    assert_eq!(count_value, Value::F64(1.0));

    Ok(())
}

#[test]
#[tracing::instrument]
async fn ts_cancel_survives_executor_restart(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc")] agent_rpc: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc)
        .store()
        .await?;

    // Start agent B (TsBlockingAgent) and prepare a promise
    let b_name = "cancel_restart_b";
    let b_agent_id = agent_id!("TsBlockingAgent", b_name);
    let b_worker_id = executor
        .start_agent(&component.id, b_agent_id.clone())
        .await?;

    let prepare_result = executor
        .invoke_and_await_agent(&component, &b_agent_id, "prepareBlock", data_value!())
        .await?;

    let promise_id_value = prepare_result
        .into_return_value()
        .expect("Expected a single return value from prepareBlock");

    let oplog_idx = extract_oplog_idx_from_promise_id(&promise_id_value);

    // Start agent A (TsCancelCallerAgent)
    let a_name = "cancel_restart_a";
    let a_agent_id = agent_id!("TsCancelCallerAgent", a_name);
    let _a_worker_id = executor
        .start_agent(&component.id, a_agent_id.clone())
        .await?;

    // Spawn fiber: A.callAndAbort(bName, 3000ms)
    let executor_clone = executor.clone();
    let component_clone = component.clone();
    let a_agent_id_clone = a_agent_id.clone();

    let mut fiber = tokio::spawn(
        async move {
            executor_clone
                .invoke_and_await_agent(
                    &component_clone,
                    &a_agent_id_clone,
                    "callAndAbort",
                    data_value!(b_name, 3000.0),
                )
                .await
        }
        .in_current_span(),
    );

    // Wait for B to suspend on the promise
    tokio::select! {
        result = &mut fiber => {
            let invoke_result = result??;
            return Err(anyhow::anyhow!("callAndAbort returned before B suspended: {:?}", invoke_result));
        }
        status = executor.wait_for_status(&b_worker_id, AgentStatus::Suspended, Duration::from_secs(30)) => {
            status?;
        }
    }

    // Wait for A's result
    let a_result = fiber.await??;
    let a_value = a_result
        .into_return_value()
        .expect("Expected a single return value from callAndAbort");
    assert_eq!(a_value, Value::String("aborted".to_string()));

    // Restart executor
    drop(executor);
    let executor = start(deps, &context).await?;

    // After restart, B should still be suspended (replayed from oplog)
    executor
        .wait_for_status(
            &b_worker_id,
            AgentStatus::Suspended,
            Duration::from_secs(30),
        )
        .await?;

    // Verify A's state survived restart
    let outcome_result = executor
        .invoke_and_await_agent(&component, &a_agent_id, "getLastOutcome", data_value!())
        .await?;

    let outcome_value = outcome_result
        .into_return_value()
        .expect("Expected a single return value from getLastOutcome");
    assert_eq!(outcome_value, Value::String("aborted".to_string()));

    // Complete the promise to unblock B
    executor
        .complete_promise(
            &PromiseId {
                agent_id: b_worker_id.clone(),
                oplog_idx,
            },
            vec![],
        )
        .await?;

    // Wait for B to return to Idle
    executor
        .wait_for_status(&b_worker_id, AgentStatus::Idle, Duration::from_secs(10))
        .await?;

    // Verify B processed the call
    let count_result = executor
        .invoke_and_await_agent(&component, &b_agent_id, "getCompletedCount", data_value!())
        .await?;

    let count_value = count_result
        .into_return_value()
        .expect("Expected a single return value from getCompletedCount");
    assert_eq!(count_value, Value::F64(1.0));

    Ok(())
}

/// Round nine: real two-agent-RPC variant of round eight's
/// `sequential_atomic_rpc_region_survives_cold_replay_after_suspend` (see
/// `ROUND_EIGHT_FINDINGS.md`), which used a plain `atomically()`-wrapped WASI HTTP call and
/// passed clean, falsifying the generic "atomic region + cold replay-after-suspend" hypothesis.
/// This test swaps the atomic region's content for a REAL cross-agent RPC call — `RpcCaller`
/// (this file's `agent_rpc_rust` component) constructing a fresh `RpcCounterClient::get(...)`
/// proxy and calling `inc_by(1)` inside `atomically_async()`, matching the production
/// `WorkerAgent.workflowToolStart()` → `atomicRpcCall()` → `WorkflowAgent.get(...).run(...)`
/// shape's actual host-call sequence (`get_agent_type` x2, two tracing spans,
/// `future-invoke-result::get`) rather than the WASI-HTTP `check_write`/`write` path round
/// eight exercised. Same suspend + cold-restart-with-no-snapshot shape as round eight.
#[test]
#[tracing::instrument]
async fn sequential_atomic_rpc_call_region_survives_cold_replay_after_suspend(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc_rust")] agent_rpc_rust: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc_rust)
        .store()
        .await?;

    let agent_id = agent_id!("RpcCaller", "round9-atomic-rpc-1");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    // ONE atomically_async()-wrapped real cross-agent RPC call (RpcCounterClient::get(...).
    // inc_by(1).await), then create_promise() (not awaited yet). Matches production's
    // atomic-region shape exactly: get_agent_type x2, rpc-connection span, rpc-invocation span,
    // io::poll::poll, io::poll::pollable::ready, future-invoke-result::get, FINISH SPAN.
    let promise_id_value = executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "sequential_atomic_rpc_calls_then_promise_init",
            data_value!(1u32),
        )
        .await?
        .into_return_value()
        .ok_or_else(|| anyhow::anyhow!("expected a PromiseId return value"))?;
    let promise_id_vat = ValueAndType::new(promise_id_value.clone(), PromiseId::get_type());

    // Fire-and-forget: the promise is not complete yet, so this invocation genuinely suspends
    // (matches production's bare SUSPEND entry with no completing IoPollPoll recorded).
    executor
        .invoke_agent(
            &component,
            &agent_id,
            "sequential_atomic_rpc_await",
            DataValue::Tuple(ElementValues {
                elements: vec![ElementValue::ComponentModel(ComponentModelElementValue {
                    value: promise_id_vat.clone(),
                })],
            }),
        )
        .await?;

    // Give the suspend a moment to actually land before forcing a cold restart.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Force a genuine worker eviction + full oplog replay on a fresh executor/instance, with
    // the promise STILL incomplete across the restart — no intervening snapshot exists for this
    // component, so replay must walk the entire history (including the atomic RPC region and
    // the RpcCounter callee agent's own oplog) from scratch.
    drop(executor);
    let executor = start(deps, &context).await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    let Value::Record(fields) = &promise_id_value else {
        panic!("Expected a record for PromiseId");
    };
    let Value::U64(oplog_idx) = fields[1] else {
        panic!("Expected a u64 for oplog_idx");
    };
    executor
        .complete_promise(
            &PromiseId {
                agent_id: worker_id.clone(),
                oplog_idx: OplogIndex::from_u64(oplog_idx),
            },
            b"resumed-ok".to_vec(),
        )
        .await?;

    let result = executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "sequential_atomic_rpc_await",
            DataValue::Tuple(ElementValues {
                elements: vec![ElementValue::ComponentModel(ComponentModelElementValue {
                    value: promise_id_vat,
                })],
            }),
        )
        .await?
        .into_return_value();

    assert_eq!(
        result,
        Some(Value::List(b"resumed-ok".iter().map(|b| Value::U8(*b)).collect())),
        "worker must survive the cold replay of the atomic RPC region + suspend and resolve the \
         promise correctly — a mismatch or a trap during the preceding atomic RPC call's replay \
         means the io::poll::poll/pollable::ready replay divergence reproduced"
    );

    Ok(())
}

/// Round nine, single-invocation variant: the other untested hypothesis from
/// `ROUND_EIGHT_FINDINGS.md` — collapse the atomic RPC region and the suspend-inducing await
/// into ONE external invocation (`sequential_atomic_rpc_calls_then_await`), rather than the
/// two-invocation split `sequential_atomic_rpc_call_region_survives_cold_replay_after_suspend`
/// used. Matches production even more closely: `WorkerAgent.advanceReasoningLoop()`'s one round
/// does the atomic `workflowToolStart()` RPC dispatches AND the `awaitPromise()` suspend
/// together, never across a separate invocation boundary. The promise itself is still created
/// via a cheap, separate, non-suspending call (`sequential_atomic_rpc_calls_then_promise_init`
/// with `n=0` — zero atomic calls, just `create_promise()`) purely so the test can learn its id
/// to complete it externally later; promise creation is not part of either hypothesis.
#[test]
#[tracing::instrument]
async fn sequential_atomic_rpc_call_and_suspend_single_invocation_survives_cold_replay(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc_rust")] agent_rpc_rust: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc_rust)
        .store()
        .await?;

    let agent_id = agent_id!("RpcCaller", "round9b-atomic-rpc-1");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    // Zero atomic calls — just create_promise(), so the test learns the id up front.
    let promise_id_value = executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "sequential_atomic_rpc_calls_then_promise_init",
            data_value!(0u32),
        )
        .await?
        .into_return_value()
        .ok_or_else(|| anyhow::anyhow!("expected a PromiseId return value"))?;

    let Value::Record(fields) = &promise_id_value else {
        panic!("Expected a record for PromiseId");
    };
    let Value::U64(oplog_idx) = fields[1] else {
        panic!("Expected a u64 for oplog_idx");
    };
    let promise_id = PromiseId {
        agent_id: worker_id.clone(),
        oplog_idx: OplogIndex::from_u64(oplog_idx),
    };
    let promise_id_vat = ValueAndType::new(promise_id_value, PromiseId::get_type());

    // ONE invocation: 1 atomically_async()-wrapped RPC call THEN blocking_await_promise —
    // matches production's single-round shape. Fire-and-forget since it will suspend.
    executor
        .invoke_agent(
            &component,
            &agent_id,
            "sequential_atomic_rpc_calls_then_await",
            DataValue::Tuple(ElementValues {
                elements: vec![
                    ElementValue::ComponentModel(ComponentModelElementValue {
                        value: 1u32.into_value_and_type(),
                    }),
                    ElementValue::ComponentModel(ComponentModelElementValue {
                        value: promise_id_vat,
                    }),
                ],
            }),
        )
        .await?;

    // Give the suspend a moment to actually land before forcing a cold restart.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Force a genuine worker eviction + full oplog replay on a fresh executor/instance, with
    // the promise STILL incomplete across the restart and the atomic RPC region + suspend both
    // inside the SAME not-yet-completed invocation's history.
    drop(executor);
    let executor = start(deps, &context).await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    executor
        .complete_promise(&promise_id, b"resumed-ok".to_vec())
        .await?;

    // If replay trapped, this will time out (worker stuck Failed/never reaching Idle) or the
    // eventual invoke below will error.
    executor
        .wait_for_status(&worker_id, AgentStatus::Idle, Duration::from_secs(20))
        .await?;

    // Confirm the worker is still genuinely functional post-replay (not just reporting Idle
    // while wedged) by successfully completing a fresh, independent round trip.
    let followup = executor
        .invoke_and_await_agent(&component, &agent_id, "test3", data_value!())
        .await?
        .into_return_value();
    assert_eq!(
        followup,
        Some(Value::U64(1)),
        "worker must remain genuinely functional after the cold replay of the single-invocation \
         atomic RPC region + suspend — a trap during replay would leave it Failed/wedged instead \
         of able to complete this independent follow-up RPC call"
    );

    Ok(())
}

/// Round nine, n=4 single-invocation variant: identical to
/// `sequential_atomic_rpc_call_and_suspend_single_invocation_survives_cold_replay` but with
/// `n=4` instead of `n=1` — matching the ACTUAL byte-traced production `scene_plates` oplog
/// exactly (4 sequential atomically()-wrapped RPC regions, not 1). The "1 region is sufficient"
/// simplification used by the other round-nine variants was inferred from a summary of the
/// SEPARATE `character_sheet` oplog (a different trap shape entirely — a timer-vs-HTTP race,
/// not an atomic RPC region) rather than personally re-traced from `scene_plates` itself, so it
/// is untested until this variant.
#[test]
#[tracing::instrument]
async fn sequential_atomic_rpc_call_and_suspend_single_invocation_n4_survives_cold_replay(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc_rust")] agent_rpc_rust: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc_rust)
        .store()
        .await?;

    let agent_id = agent_id!("RpcCaller", "round9c-atomic-rpc-n4");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    let promise_id_value = executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "sequential_atomic_rpc_calls_then_promise_init",
            data_value!(0u32),
        )
        .await?
        .into_return_value()
        .ok_or_else(|| anyhow::anyhow!("expected a PromiseId return value"))?;

    let Value::Record(fields) = &promise_id_value else {
        panic!("Expected a record for PromiseId");
    };
    let Value::U64(oplog_idx) = fields[1] else {
        panic!("Expected a u64 for oplog_idx");
    };
    let promise_id = PromiseId {
        agent_id: worker_id.clone(),
        oplog_idx: OplogIndex::from_u64(oplog_idx),
    };
    let promise_id_vat = ValueAndType::new(promise_id_value, PromiseId::get_type());

    executor
        .invoke_agent(
            &component,
            &agent_id,
            "sequential_atomic_rpc_calls_then_await",
            DataValue::Tuple(ElementValues {
                elements: vec![
                    ElementValue::ComponentModel(ComponentModelElementValue {
                        value: 4u32.into_value_and_type(),
                    }),
                    ElementValue::ComponentModel(ComponentModelElementValue {
                        value: promise_id_vat,
                    }),
                ],
            }),
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    drop(executor);
    let executor = start(deps, &context).await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    executor
        .complete_promise(&promise_id, b"resumed-ok".to_vec())
        .await?;

    executor
        .wait_for_status(&worker_id, AgentStatus::Idle, Duration::from_secs(20))
        .await?;

    let followup = executor
        .invoke_and_await_agent(&component, &agent_id, "test3", data_value!())
        .await?
        .into_return_value();
    assert_eq!(
        followup,
        Some(Value::U64(1)),
        "worker must remain genuinely functional after the cold replay of 4 sequential atomic \
         RPC regions + suspend, matching the byte-traced production scene_plates shape exactly"
    );

    Ok(())
}

/// Round ten: real cross-agent RPC variant of `atomic_double_ready_call_then_promise_init`
/// (durability.rs) — drives the RAW `WasmRpc`/`future-invoke-result` API manually with the
/// production-matching `ready()` (non-blocking) → `poll()` (blocking) → `ready()` (confirm)
/// shape, on the actual `golem::rpc::future-invoke-result` pollable (not an HTTP input-stream
/// pollable). See `/Users/hannes/work/golem/oplog-backups/README.md` "Sixth capture" for the
/// live-instrumented production trace this reproduces.
#[test]
#[tracing::instrument]
async fn atomic_double_ready_rpc_call_survives_cold_replay_after_suspend(
    last_unique_id: &LastUniqueId,
    deps: &WorkerExecutorTestDependencies,
    #[tagged_as("agent_rpc_rust")] agent_rpc_rust: &PrecompiledComponent,
    _tracing: &Tracing,
) -> anyhow::Result<()> {
    let context = TestContext::new(last_unique_id);
    let executor = start(deps, &context).await?;

    let component = executor
        .component_dep(&context.default_environment_id, agent_rpc_rust)
        .store()
        .await?;

    let agent_id = agent_id!("RpcCaller", "round10-double-ready-rpc-1");
    let worker_id = executor
        .start_agent(&component.id, agent_id.clone())
        .await?;

    let promise_id_value = executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "atomic_double_ready_rpc_call_then_promise_init",
            data_value!("round10_double_ready_rpc_counter".to_string()),
        )
        .await?
        .into_return_value()
        .ok_or_else(|| anyhow::anyhow!("expected a PromiseId return value"))?;
    let promise_id_vat = ValueAndType::new(promise_id_value.clone(), PromiseId::get_type());

    executor
        .invoke_agent(
            &component,
            &agent_id,
            "sequential_atomic_rpc_await",
            DataValue::Tuple(ElementValues {
                elements: vec![ElementValue::ComponentModel(ComponentModelElementValue {
                    value: promise_id_vat.clone(),
                })],
            }),
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    drop(executor);
    let executor = start(deps, &context).await?;

    executor.check_oplog_is_queryable(&worker_id).await?;

    let Value::Record(fields) = &promise_id_value else {
        panic!("Expected a record for PromiseId");
    };
    let Value::U64(oplog_idx) = fields[1] else {
        panic!("Expected a u64 for oplog_idx");
    };
    executor
        .complete_promise(
            &PromiseId {
                agent_id: worker_id.clone(),
                oplog_idx: OplogIndex::from_u64(oplog_idx),
            },
            b"resumed-ok".to_vec(),
        )
        .await?;

    let result = executor
        .invoke_and_await_agent(
            &component,
            &agent_id,
            "sequential_atomic_rpc_await",
            DataValue::Tuple(ElementValues {
                elements: vec![ElementValue::ComponentModel(ComponentModelElementValue {
                    value: promise_id_vat,
                })],
            }),
        )
        .await?
        .into_return_value();

    assert_eq!(
        result,
        Some(Value::List(b"resumed-ok".iter().map(|b| Value::U8(*b)).collect())),
        "worker must survive the cold replay of the atomic RPC region + suspend and resolve the \
         promise correctly — a mismatch or a trap during replay of the preceding double-ready() \
         atomic RPC call means the io::poll::poll/pollable::ready replay divergence reproduced"
    );

    Ok(())
}
