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

use crate::storage::keyvalue::sqlite::SqliteKeyValueStorage;
use crate::storage::keyvalue::{KeyValueStorage, KeyValueStorageNamespace};
use crate::storage::sqlite_files::{DatabaseScope, SqliteFileStorage, SqliteFiles, agent_db_hash};
use async_trait::async_trait;
use bytes::Bytes;
use golem_common::config::DbSqliteConfig;
use golem_service_base::db::sqlite::{SqlitePool, SqlitePoolTuning};
use std::fmt::{Debug, Formatter};
use std::path::Path;

/// KeyValueStorage implementation over multiple SQLite databases: the per-agent namespaces
/// (worker, status, status checkpoint, promises) of an agent share one database
/// (`kv-worker-<hash>.db`), the global namespaces have one database each. A per-agent database is
/// created by the agent's first write; reads of an agent that never wrote create nothing.
pub struct MultiSqliteKeyValueStorage {
    files: SqliteFiles<SqliteKeyValueStorage>,
}

impl SqliteFileStorage for SqliteKeyValueStorage {
    async fn open(config: DbSqliteConfig, tuning: SqlitePoolTuning) -> Result<Self, String> {
        SqliteKeyValueStorage::configured_with(&config, &tuning).await
    }

    fn pool(&self) -> SqlitePool {
        SqliteKeyValueStorage::pool(self)
    }
}

impl MultiSqliteKeyValueStorage {
    pub fn new(root_dir: &Path, max_connections: u32, foreign_keys: bool) -> Self {
        Self {
            files: SqliteFiles::new(root_dir, max_connections, foreign_keys, "multi-sqlite-kvs"),
        }
    }

    /// Number of open (cached) databases.
    pub async fn open_databases(&self) -> usize {
        self.files.open_databases().await
    }

    /// Number of open connections of all cached databases.
    pub async fn open_connections(&self) -> u32 {
        self.files.open_connections().await
    }

    /// The database holding `namespace`: the single place a namespace is mapped to a file.
    fn database_of(namespace: &KeyValueStorageNamespace) -> (String, DatabaseScope) {
        match namespace {
            KeyValueStorageNamespace::RunningWorkers => {
                ("kv-running_workers".to_string(), DatabaseScope::Shared)
            }
            KeyValueStorageNamespace::Worker { agent_id }
            | KeyValueStorageNamespace::AgentStatus { agent_id }
            | KeyValueStorageNamespace::AgentStatusCheckpoint { agent_id }
            | KeyValueStorageNamespace::Promise { agent_id } => (
                format!("kv-worker-{}.db", agent_db_hash(agent_id)),
                DatabaseScope::PerAgent,
            ),
            KeyValueStorageNamespace::Schedule => {
                ("kv-schedule.db".to_string(), DatabaseScope::Shared)
            }
            KeyValueStorageNamespace::UserDefined { .. } => {
                ("kv-user-defined.db".to_string(), DatabaseScope::Shared)
            }
        }
    }

    /// The database of `namespace`, created if missing. For writes.
    async fn writable(
        &self,
        namespace: &KeyValueStorageNamespace,
    ) -> Result<SqliteKeyValueStorage, String> {
        let (db, scope) = Self::database_of(namespace);
        self.files.open_or_create(&db, scope).await
    }

    /// The database of `namespace` if it exists. For reads and deletes: a missing database is
    /// empty, and is not created.
    async fn readable(
        &self,
        namespace: &KeyValueStorageNamespace,
    ) -> Result<Option<SqliteKeyValueStorage>, String> {
        let (db, scope) = Self::database_of(namespace);
        self.files.open_existing(&db, scope).await
    }
}

impl Debug for MultiSqliteKeyValueStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MultiSqliteKeyValueStorage")
    }
}

#[async_trait]
impl KeyValueStorage for MultiSqliteKeyValueStorage {
    async fn set(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        entity_name: &'static str,
        namespace: KeyValueStorageNamespace,
        key: &str,
        value: &[u8],
    ) -> Result<(), String> {
        self.writable(&namespace)
            .await?
            .set(svc_name, api_name, entity_name, namespace, key, value)
            .await
    }

    async fn set_many(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        entity_name: &'static str,
        namespace: KeyValueStorageNamespace,
        pairs: &[(&str, &[u8])],
    ) -> Result<(), String> {
        self.writable(&namespace)
            .await?
            .set_many(svc_name, api_name, entity_name, namespace, pairs)
            .await
    }

    async fn set_if_not_exists(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        entity_name: &'static str,
        namespace: KeyValueStorageNamespace,
        key: &str,
        value: &[u8],
    ) -> Result<bool, String> {
        self.writable(&namespace)
            .await?
            .set_if_not_exists(svc_name, api_name, entity_name, namespace, key, value)
            .await
    }

    async fn get(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        entity_name: &'static str,
        namespace: KeyValueStorageNamespace,
        key: &str,
    ) -> Result<Option<Bytes>, String> {
        match self.readable(&namespace).await? {
            Some(storage) => {
                storage
                    .get(svc_name, api_name, entity_name, namespace, key)
                    .await
            }
            None => Ok(None),
        }
    }

    async fn get_many(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        entity_name: &'static str,
        namespace: KeyValueStorageNamespace,
        keys: Vec<String>,
    ) -> Result<Vec<Option<Bytes>>, String> {
        match self.readable(&namespace).await? {
            Some(storage) => {
                storage
                    .get_many(svc_name, api_name, entity_name, namespace, keys)
                    .await
            }
            None => Ok(vec![None; keys.len()]),
        }
    }

    async fn get_all(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        entity_name: &'static str,
        namespace: KeyValueStorageNamespace,
    ) -> Result<Vec<(String, Bytes)>, String> {
        match self.readable(&namespace).await? {
            Some(storage) => {
                storage
                    .get_all(svc_name, api_name, entity_name, namespace)
                    .await
            }
            None => Ok(vec![]),
        }
    }

    async fn del(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        namespace: KeyValueStorageNamespace,
        key: &str,
    ) -> Result<(), String> {
        match self.readable(&namespace).await? {
            Some(storage) => storage.del(svc_name, api_name, namespace, key).await,
            None => Ok(()),
        }
    }

    async fn del_many(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        namespace: KeyValueStorageNamespace,
        keys: Vec<String>,
    ) -> Result<(), String> {
        match self.readable(&namespace).await? {
            Some(storage) => storage.del_many(svc_name, api_name, namespace, keys).await,
            None => Ok(()),
        }
    }

    async fn exists(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        namespace: KeyValueStorageNamespace,
        key: &str,
    ) -> Result<bool, String> {
        match self.readable(&namespace).await? {
            Some(storage) => storage.exists(svc_name, api_name, namespace, key).await,
            None => Ok(false),
        }
    }

    async fn keys(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        namespace: KeyValueStorageNamespace,
    ) -> Result<Vec<String>, String> {
        match self.readable(&namespace).await? {
            Some(storage) => storage.keys(svc_name, api_name, namespace).await,
            None => Ok(vec![]),
        }
    }

    async fn add_to_set(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        entity_name: &'static str,
        namespace: KeyValueStorageNamespace,
        key: &str,
        value: &[u8],
    ) -> Result<(), String> {
        self.writable(&namespace)
            .await?
            .add_to_set(svc_name, api_name, entity_name, namespace, key, value)
            .await
    }

    async fn remove_from_set(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        entity_name: &'static str,
        namespace: KeyValueStorageNamespace,
        key: &str,
        value: &[u8],
    ) -> Result<(), String> {
        match self.readable(&namespace).await? {
            Some(storage) => {
                storage
                    .remove_from_set(svc_name, api_name, entity_name, namespace, key, value)
                    .await
            }
            None => Ok(()),
        }
    }

    async fn members_of_set(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        entity_name: &'static str,
        namespace: KeyValueStorageNamespace,
        key: &str,
    ) -> Result<Vec<Bytes>, String> {
        match self.readable(&namespace).await? {
            Some(storage) => {
                storage
                    .members_of_set(svc_name, api_name, entity_name, namespace, key)
                    .await
            }
            None => Ok(vec![]),
        }
    }

    async fn add_to_sorted_set(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        entity_name: &'static str,
        namespace: KeyValueStorageNamespace,
        key: &str,
        score: f64,
        value: &[u8],
    ) -> Result<(), String> {
        self.writable(&namespace)
            .await?
            .add_to_sorted_set(
                svc_name,
                api_name,
                entity_name,
                namespace,
                key,
                score,
                value,
            )
            .await
    }

    async fn remove_from_sorted_set(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        entity_name: &'static str,
        namespace: KeyValueStorageNamespace,
        key: &str,
        value: &[u8],
    ) -> Result<(), String> {
        match self.readable(&namespace).await? {
            Some(storage) => {
                storage
                    .remove_from_sorted_set(svc_name, api_name, entity_name, namespace, key, value)
                    .await
            }
            None => Ok(()),
        }
    }

    async fn get_sorted_set(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        entity_name: &'static str,
        namespace: KeyValueStorageNamespace,
        key: &str,
    ) -> Result<Vec<(f64, Bytes)>, String> {
        match self.readable(&namespace).await? {
            Some(storage) => {
                storage
                    .get_sorted_set(svc_name, api_name, entity_name, namespace, key)
                    .await
            }
            None => Ok(vec![]),
        }
    }

    async fn query_sorted_set(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        entity_name: &'static str,
        namespace: KeyValueStorageNamespace,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<Vec<(f64, Bytes)>, String> {
        match self.readable(&namespace).await? {
            Some(storage) => {
                storage
                    .query_sorted_set(svc_name, api_name, entity_name, namespace, key, min, max)
                    .await
            }
            None => Ok(vec![]),
        }
    }
}
