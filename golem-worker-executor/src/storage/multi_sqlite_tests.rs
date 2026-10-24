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

//! Resource-bound regression tests for the per-agent ("multi") SQLite storages used by the
//! single-executable `golem server run`.
//!
//! Failure mode (live, 2026-10-24): every agent id - ephemeral phantoms included - got five
//! SQLite files (durable + ephemeral oplog, durable + ephemeral compressed oplog, kv-worker), each
//! with an eagerly connected read + write pool kept in two 1024-entry caches with a 6 h idle TTL.
//! sqlx-sqlite runs one OS thread per connection, so the process grew by ~20 threads per LLM call
//! until it was OOM-killed. These tests drive the storages with thousands of agent ids and assert
//! that files, cached pools and connections stay within hard caps, without waiting for any timer.

use crate::storage::indexed::multi_sqlite::MultiSqliteIndexedStorage;
use crate::storage::indexed::sqlite::SqliteIndexedStorage;
use crate::storage::indexed::{
    IndexedStorage, IndexedStorageMetaNamespace, IndexedStorageNamespace,
};
use crate::storage::keyvalue::multi_sqlite::MultiSqliteKeyValueStorage;
use crate::storage::keyvalue::{KeyValueStorage, KeyValueStorageNamespace};
use crate::storage::sqlite_files::{MAX_CACHED_DATABASES, agent_db_hash};
use golem_common::config::DbSqliteConfig;
use golem_common::model::AgentId;
use golem_common::model::agent::AgentMode;
use golem_common::model::component::ComponentId;
use std::path::Path;
use test_r::test;

const SVC: &str = "test";
const API: &str = "test";
const ENTITY: &str = "entity";

fn agent(component_id: ComponentId, i: usize) -> AgentId {
    AgentId {
        component_id,
        agent_id: format!("agent-{i}"),
    }
}

fn oplog(agent_id: &AgentId, agent_mode: AgentMode) -> IndexedStorageNamespace {
    IndexedStorageNamespace::OpLog {
        agent_id: agent_id.clone(),
        agent_mode,
    }
}

fn compressed(agent_id: &AgentId, agent_mode: AgentMode) -> IndexedStorageNamespace {
    IndexedStorageNamespace::CompressedOpLog {
        agent_id: agent_id.clone(),
        agent_mode,
        level: 1,
    }
}

/// Names of the SQLite main database files in `dir` (WAL / SHM side files excluded).
fn db_files(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| !n.ends_with("-wal") && !n.ends_with("-shm"))
        .collect();
    names.sort();
    names
}

/// One ephemeral invocation as the executor performs it against storage: the agent-mode probe of
/// both oplog namespaces, the status probe, the last-index probe, the archive write of its oplog
/// and a read-back.
async fn ephemeral_lifecycle(
    is: &MultiSqliteIndexedStorage,
    kv: &MultiSqliteKeyValueStorage,
    agent_id: &AgentId,
    key: &str,
) {
    assert!(
        !is.exists(SVC, API, oplog(agent_id, AgentMode::Durable), key)
            .await
            .unwrap()
    );
    assert!(
        !is.exists(SVC, API, oplog(agent_id, AgentMode::Ephemeral), key)
            .await
            .unwrap()
    );
    assert!(
        kv.get(
            SVC,
            API,
            ENTITY,
            KeyValueStorageNamespace::AgentStatus {
                agent_id: agent_id.clone()
            },
            "core"
        )
        .await
        .unwrap()
        .is_none()
    );
    assert_eq!(
        is.length(SVC, API, compressed(agent_id, AgentMode::Durable), key)
            .await
            .unwrap(),
        0
    );
    is.append(
        SVC,
        API,
        ENTITY,
        compressed(agent_id, AgentMode::Ephemeral),
        key,
        1,
        key.as_bytes().to_vec(),
    )
    .await
    .unwrap();
    is.append(
        SVC,
        API,
        ENTITY,
        oplog(agent_id, AgentMode::Ephemeral),
        key,
        1,
        key.as_bytes().to_vec(),
    )
    .await
    .unwrap();
    let last = is
        .last(
            SVC,
            API,
            ENTITY,
            compressed(agent_id, AgentMode::Ephemeral),
            key,
        )
        .await
        .unwrap();
    assert_eq!(last, Some((1, key.as_bytes().to_vec())));
}

#[test]
async fn ephemeral_agents_get_no_per_agent_files_and_pools_stay_capped() {
    let dir = tempfile::tempdir().unwrap();
    let is = MultiSqliteIndexedStorage::new(dir.path(), 4, false);
    let kv = MultiSqliteKeyValueStorage::new(dir.path(), 4, false);
    let component_id = ComponentId::new();

    for i in 0..2000 {
        let agent_id = agent(component_id, i);
        ephemeral_lifecycle(&is, &kv, &agent_id, &format!("key-{i}")).await;
    }

    let files = db_files(dir.path());
    assert!(
        files
            .iter()
            .all(|f| !f.starts_with("durable-") && !f.starts_with("kv-worker-")),
        "ephemeral agents must not create durable or kv-worker files: {:?}",
        files.iter().take(5).collect::<Vec<_>>()
    );
    assert!(
        files.len() <= 4,
        "expected only the shared ephemeral databases, got {}",
        files.len()
    );

    // Every ephemeral oplog is still readable from the shared databases.
    for i in (0..2000).step_by(97) {
        let agent_id = agent(component_id, i);
        let key = format!("key-{i}");
        assert!(
            is.exists(SVC, API, oplog(&agent_id, AgentMode::Ephemeral), &key)
                .await
                .unwrap()
        );
    }

    assert!(is.open_databases().await <= 4);
    assert!(kv.open_databases().await == 0);
}

#[test]
async fn durable_agents_pool_cache_is_capped_and_data_survives_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let is = MultiSqliteIndexedStorage::new(dir.path(), 4, false);
    let kv = MultiSqliteKeyValueStorage::new(dir.path(), 4, false);
    let component_id = ComponentId::new();
    let agents = 3 * MAX_CACHED_DATABASES;

    for i in 0..agents {
        let agent_id = agent(component_id, i);
        let key = format!("key-{i}");
        for idx in 1..=3u64 {
            is.append(
                SVC,
                API,
                ENTITY,
                oplog(&agent_id, AgentMode::Durable),
                &key,
                idx,
                vec![idx as u8],
            )
            .await
            .unwrap();
        }
        kv.set(
            SVC,
            API,
            ENTITY,
            KeyValueStorageNamespace::AgentStatus {
                agent_id: agent_id.clone(),
            },
            "core",
            b"status",
        )
        .await
        .unwrap();

        assert!(is.open_databases().await <= MAX_CACHED_DATABASES);
        assert!(kv.open_databases().await <= MAX_CACHED_DATABASES);
        // read pool (1) + write pool (1) per cached per-agent database
        assert!(is.open_connections().await <= 2 * MAX_CACHED_DATABASES as u32);
        assert!(kv.open_connections().await <= 2 * MAX_CACHED_DATABASES as u32);
    }

    let files = db_files(dir.path());
    assert_eq!(
        files
            .iter()
            .filter(|f| f.starts_with("durable-oplog-"))
            .count(),
        agents
    );
    assert_eq!(
        files.iter().filter(|f| f.starts_with("kv-worker-")).count(),
        agents
    );
    assert!(
        files
            .iter()
            .all(|f| !f.starts_with("ephemeral-") && !f.starts_with("durable-compressed-"))
    );

    // Durability across eviction: the oldest agents' pools were evicted, their data is intact.
    for i in 0..agents {
        let agent_id = agent(component_id, i);
        let key = format!("key-{i}");
        let entries = is
            .read(
                SVC,
                API,
                ENTITY,
                oplog(&agent_id, AgentMode::Durable),
                &key,
                1,
                3,
            )
            .await
            .unwrap();
        assert_eq!(entries, vec![(1, vec![1]), (2, vec![2]), (3, vec![3])]);
        let status = kv
            .get(
                SVC,
                API,
                ENTITY,
                KeyValueStorageNamespace::AgentStatus {
                    agent_id: agent_id.clone(),
                },
                "core",
            )
            .await
            .unwrap();
        assert_eq!(status.as_deref(), Some(&b"status"[..]));
    }
}

#[test]
async fn reads_and_deletes_of_unknown_agents_create_no_files() {
    let dir = tempfile::tempdir().unwrap();
    let is = MultiSqliteIndexedStorage::new(dir.path(), 4, false);
    let kv = MultiSqliteKeyValueStorage::new(dir.path(), 4, false);
    let agent_id = agent(ComponentId::new(), 0);
    let key = "key";

    for mode in [AgentMode::Durable, AgentMode::Ephemeral] {
        for ns in [oplog(&agent_id, mode), compressed(&agent_id, mode)] {
            assert!(!is.exists(SVC, API, ns.clone(), key).await.unwrap());
            assert_eq!(is.length(SVC, API, ns.clone(), key).await.unwrap(), 0);
            assert!(
                is.read(SVC, API, ENTITY, ns.clone(), key, 1, 10)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(
                is.first(SVC, API, ENTITY, ns.clone(), key)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                is.last(SVC, API, ENTITY, ns.clone(), key)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                is.closest(SVC, API, ENTITY, ns.clone(), key, 5)
                    .await
                    .unwrap()
                    .is_none()
            );
            is.drop_prefix(SVC, API, ns.clone(), key, 5).await.unwrap();
            is.delete(SVC, API, ns.clone(), key).await.unwrap();
        }
    }
    for ns in [
        KeyValueStorageNamespace::Worker {
            agent_id: agent_id.clone(),
        },
        KeyValueStorageNamespace::AgentStatus {
            agent_id: agent_id.clone(),
        },
        KeyValueStorageNamespace::AgentStatusCheckpoint {
            agent_id: agent_id.clone(),
        },
        KeyValueStorageNamespace::Promise {
            agent_id: agent_id.clone(),
        },
    ] {
        assert!(
            kv.get(SVC, API, ENTITY, ns.clone(), key)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            kv.get_many(SVC, API, ENTITY, ns.clone(), vec![key.to_string()])
                .await
                .unwrap(),
            vec![None]
        );
        assert!(
            kv.get_all(SVC, API, ENTITY, ns.clone())
                .await
                .unwrap()
                .is_empty()
        );
        assert!(kv.keys(SVC, API, ns.clone()).await.unwrap().is_empty());
        assert!(!kv.exists(SVC, API, ns.clone(), key).await.unwrap());
        assert!(
            kv.members_of_set(SVC, API, ENTITY, ns.clone(), key)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            kv.get_sorted_set(SVC, API, ENTITY, ns.clone(), key)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            kv.query_sorted_set(SVC, API, ENTITY, ns.clone(), key, 0.0, 1.0)
                .await
                .unwrap()
                .is_empty()
        );
        kv.del(SVC, API, ns.clone(), key).await.unwrap();
        kv.del_many(SVC, API, ns.clone(), vec![key.to_string()])
            .await
            .unwrap();
        kv.remove_from_set(SVC, API, ENTITY, ns.clone(), key, b"v")
            .await
            .unwrap();
        kv.remove_from_sorted_set(SVC, API, ENTITY, ns.clone(), key, b"v")
            .await
            .unwrap();
    }

    assert_eq!(db_files(dir.path()), Vec::<String>::new());
    assert_eq!(is.open_databases().await, 0);
    assert_eq!(kv.open_databases().await, 0);
}

#[test]
async fn scan_over_many_databases_leaves_the_pool_cache_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let component_id = ComponentId::new();
    let agents = 300;
    {
        let writer = MultiSqliteIndexedStorage::new(dir.path(), 4, false);
        for i in 0..agents {
            let agent_id = agent(component_id, i);
            writer
                .append(
                    SVC,
                    API,
                    ENTITY,
                    oplog(&agent_id, AgentMode::Durable),
                    &format!("key-{i}"),
                    1,
                    vec![1],
                )
                .await
                .unwrap();
        }
    }

    let is = MultiSqliteIndexedStorage::new(dir.path(), 4, false);
    let mut keys = Vec::new();
    let mut cursor = 0;
    loop {
        let (next, mut page) = is
            .scan(
                SVC,
                API,
                IndexedStorageMetaNamespace::Oplog {
                    agent_mode: AgentMode::Durable,
                },
                None,
                cursor,
                50,
            )
            .await
            .unwrap();
        keys.append(&mut page);
        if next == 0 {
            break;
        }
        cursor = next;
    }
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), agents);
    assert_eq!(is.open_databases().await, 0);
    assert_eq!(is.open_connections().await, 0);
}

#[test]
async fn deleting_an_agents_oplog_releases_its_pool() {
    let dir = tempfile::tempdir().unwrap();
    let is = MultiSqliteIndexedStorage::new(dir.path(), 4, false);
    let agent_id = agent(ComponentId::new(), 0);
    let ns = oplog(&agent_id, AgentMode::Durable);

    is.append(SVC, API, ENTITY, ns.clone(), "key", 1, vec![1])
        .await
        .unwrap();
    assert_eq!(is.open_databases().await, 1);
    is.delete(SVC, API, ns.clone(), "key").await.unwrap();
    assert_eq!(is.open_databases().await, 0);

    // The agent can be recreated under the same id.
    is.append(SVC, API, ENTITY, ns.clone(), "key", 1, vec![2])
        .await
        .unwrap();
    assert_eq!(
        is.last(SVC, API, ENTITY, ns, "key").await.unwrap(),
        Some((1, vec![2]))
    );
}

#[test]
async fn ephemeral_oplogs_written_before_the_shared_layout_stay_readable() {
    let dir = tempfile::tempdir().unwrap();
    let agent_id = agent(ComponentId::new(), 0);
    let legacy = dir.path().join(format!(
        "ephemeral-compressed-oplog-l1-{}.db",
        agent_db_hash(&agent_id)
    ));
    let legacy_storage = SqliteIndexedStorage::configured(&DbSqliteConfig {
        database: legacy.to_string_lossy().into_owned(),
        max_connections: 1,
        foreign_keys: false,
    })
    .await
    .unwrap();
    legacy_storage
        .append(
            SVC,
            API,
            ENTITY,
            compressed(&agent_id, AgentMode::Ephemeral),
            "key",
            1,
            vec![7],
        )
        .await
        .unwrap();
    drop(legacy_storage);

    let is = MultiSqliteIndexedStorage::new(dir.path(), 4, false);
    assert_eq!(
        is.last(
            SVC,
            API,
            ENTITY,
            compressed(&agent_id, AgentMode::Ephemeral),
            "key"
        )
        .await
        .unwrap(),
        Some((1, vec![7]))
    );
    let (_, keys) = is
        .scan(
            SVC,
            API,
            IndexedStorageMetaNamespace::CompressedOplog {
                agent_mode: AgentMode::Ephemeral,
                level: 1,
            },
            None,
            0,
            10,
        )
        .await
        .unwrap();
    assert_eq!(keys, vec!["key".to_string()]);
}
