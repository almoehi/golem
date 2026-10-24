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

use super::{
    IndexedStorage, IndexedStorageError, IndexedStorageMetaNamespace, IndexedStorageNamespace,
    ScanCursor,
};
use crate::storage::indexed::sqlite::SqliteIndexedStorage;
use crate::storage::sqlite_files::{DatabaseScope, SqliteFileStorage, SqliteFiles, agent_db_hash};
use async_trait::async_trait;
use golem_common::config::DbSqliteConfig;
use golem_common::model::agent::AgentMode;
use golem_service_base::db::sqlite::{SqlitePool, SqlitePoolTuning};
use std::fmt::{Debug, Formatter};
use std::path::Path;
use std::time::Duration;

/// IndexedStorage implementation over multiple SQLite databases: a durable agent's oplog and
/// compressed oplog layers each live in a database of their own (`durable-oplog-<hash>.db`,
/// `durable-compressed-oplog-l<n>-<hash>.db`), while ephemeral agents - short-lived, never
/// replayed, usually one per call - share one database per namespace
/// (`ephemeral-oplog-shared.db`, `ephemeral-compressed-oplog-l<n>-shared.db`). An ephemeral agent
/// whose per-agent database exists from before the shared layout keeps using it.
pub struct MultiSqliteIndexedStorage {
    files: SqliteFiles<SqliteIndexedStorage>,
}

/// File name stem of the database shared by all ephemeral agents of a namespace.
const SHARED_EPHEMERAL_STEM: &str = "shared";

impl SqliteFileStorage for SqliteIndexedStorage {
    async fn open(config: DbSqliteConfig, tuning: SqlitePoolTuning) -> Result<Self, String> {
        SqliteIndexedStorage::configured_with(&config, &tuning).await
    }

    fn pool(&self) -> SqlitePool {
        SqliteIndexedStorage::pool(self)
    }
}

impl MultiSqliteIndexedStorage {
    pub fn new(root_dir: &Path, max_connections: u32, foreign_keys: bool) -> Self {
        Self {
            files: SqliteFiles::new(
                root_dir,
                max_connections,
                foreign_keys,
                "multi-sqlite-indexed",
            ),
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

    /// The file name prefix of every database of a meta namespace (the scan unit).
    fn database_prefix(namespace: &IndexedStorageMetaNamespace) -> String {
        match namespace {
            IndexedStorageMetaNamespace::Oplog { agent_mode } => {
                format!("{}-oplog-", super::agent_mode_prefix(*agent_mode))
            }
            IndexedStorageMetaNamespace::CompressedOplog { agent_mode, level } => {
                format!(
                    "{}-compressed-oplog-l{level}-",
                    super::agent_mode_prefix(*agent_mode)
                )
            }
        }
    }

    /// The database holding `namespace`: the single place a namespace is mapped to a file.
    async fn database_of(
        &self,
        namespace: &IndexedStorageNamespace,
    ) -> Result<(String, DatabaseScope), IndexedStorageError> {
        let (meta, agent_id, agent_mode) = match namespace {
            IndexedStorageNamespace::OpLog {
                agent_id,
                agent_mode,
            } => (
                IndexedStorageMetaNamespace::Oplog {
                    agent_mode: *agent_mode,
                },
                agent_id,
                *agent_mode,
            ),
            IndexedStorageNamespace::CompressedOpLog {
                agent_id,
                agent_mode,
                level,
            } => (
                IndexedStorageMetaNamespace::CompressedOplog {
                    agent_mode: *agent_mode,
                    level: *level,
                },
                agent_id,
                *agent_mode,
            ),
        };
        let prefix = Self::database_prefix(&meta);
        let per_agent = format!("{prefix}{}.db", agent_db_hash(agent_id));
        match agent_mode {
            AgentMode::Durable => Ok((per_agent, DatabaseScope::PerAgent)),
            AgentMode::Ephemeral => {
                if self
                    .files
                    .is_present(&per_agent)
                    .await
                    .map_err(IndexedStorageError::Other)?
                {
                    Ok((per_agent, DatabaseScope::PerAgent))
                } else {
                    Ok((
                        format!("{prefix}{SHARED_EPHEMERAL_STEM}.db"),
                        DatabaseScope::Shared,
                    ))
                }
            }
        }
    }

    /// The database of `namespace`, created if missing. For writes.
    async fn writable(
        &self,
        namespace: &IndexedStorageNamespace,
    ) -> Result<SqliteIndexedStorage, IndexedStorageError> {
        let (db, scope) = self.database_of(namespace).await?;
        self.files
            .open_or_create(&db, scope)
            .await
            .map_err(IndexedStorageError::Other)
    }

    /// The database of `namespace` if it exists. For reads and deletes: a missing database is
    /// empty, and is not created.
    async fn readable(
        &self,
        namespace: &IndexedStorageNamespace,
    ) -> Result<Option<SqliteIndexedStorage>, IndexedStorageError> {
        let (db, scope) = self.database_of(namespace).await?;
        self.files
            .open_existing(&db, scope)
            .await
            .map_err(IndexedStorageError::Other)
    }
}

impl Debug for MultiSqliteIndexedStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MultiSqliteIndexedStorage")
    }
}

#[async_trait]
impl IndexedStorage for MultiSqliteIndexedStorage {
    async fn number_of_replicas(
        &self,
        _svc_name: &'static str,
        _api_name: &'static str,
    ) -> Result<u8, IndexedStorageError> {
        Ok(0)
    }

    async fn wait_for_replicas(
        &self,
        _svc_name: &'static str,
        _api_name: &'static str,
        _replicas: u8,
        _timeout: Duration,
    ) -> Result<u8, IndexedStorageError> {
        Ok(0)
    }

    async fn exists(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        namespace: IndexedStorageNamespace,
        key: &str,
    ) -> Result<bool, IndexedStorageError> {
        match self.readable(&namespace).await? {
            Some(storage) => storage.exists(svc_name, api_name, namespace, key).await,
            None => Ok(false),
        }
    }

    async fn scan(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        namespace: IndexedStorageMetaNamespace,
        prefix: Option<&str>,
        cursor: ScanCursor,
        count: u64,
    ) -> Result<(ScanCursor, Vec<String>), IndexedStorageError> {
        use std::fs;

        let db_prefix = Self::database_prefix(&namespace);

        // List all .db files matching the namespace prefix, sorted consistently.
        // The root_dir may not exist yet (e.g. after `golem server clean`); treat as empty.
        let read_dir_iter = match fs::read_dir(self.files.root_dir()) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok((0, vec![]));
            }
            Err(e) => {
                return Err(IndexedStorageError::Other(format!(
                    "Failed to read root directory: {:?}",
                    e
                )));
            }
        };
        let mut matching_files: Vec<_> = read_dir_iter
            .filter_map(|entry| {
                entry.ok().and_then(|e| {
                    let path = e.path();
                    let file_name = path.file_name()?.to_string_lossy().to_string();
                    if file_name.starts_with(&db_prefix) && file_name.ends_with(".db") {
                        Some(file_name)
                    } else {
                        None
                    }
                })
            })
            .collect();
        matching_files.sort();

        // Decode cursor: upper 32 bits = file index, lower 32 bits = scan cursor within file
        let file_index = (cursor >> 32) as usize;
        let file_cursor = cursor & 0xFFFFFFFF;

        let mut results = Vec::new();
        let mut current_file_cursor = file_cursor;

        for (idx, file_name) in matching_files.iter().enumerate().skip(file_index) {
            // A scan visits every database once: it must not fill (and churn) the pool cache.
            let remaining = count - results.len() as u64;
            let (next_cursor, mut file_results) = self
                .files
                .with_uncached(file_name, async |storage| {
                    storage
                        .scan(
                            svc_name,
                            api_name,
                            namespace.clone(),
                            prefix,
                            current_file_cursor,
                            remaining,
                        )
                        .await
                })
                .await
                .map_err(IndexedStorageError::Other)??;

            results.append(&mut file_results);

            if results.len() as u64 >= count {
                // Encode next cursor: file index in upper 32 bits, file cursor in lower 32 bits
                let next_combined_cursor = ((idx as u64) << 32) | (next_cursor & 0xFFFFFFFF);
                return Ok((
                    next_combined_cursor,
                    results.into_iter().take(count as usize).collect(),
                ));
            }

            current_file_cursor = 0;
        }

        Ok((0, results))
    }

    async fn append(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        entity_name: &'static str,
        namespace: IndexedStorageNamespace,
        key: &str,
        id: u64,
        value: Vec<u8>,
    ) -> Result<(), IndexedStorageError> {
        self.writable(&namespace)
            .await?
            .append(svc_name, api_name, entity_name, namespace, key, id, value)
            .await
    }

    async fn length(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        namespace: IndexedStorageNamespace,
        key: &str,
    ) -> Result<u64, IndexedStorageError> {
        match self.readable(&namespace).await? {
            Some(storage) => storage.length(svc_name, api_name, namespace, key).await,
            None => Ok(0),
        }
    }

    async fn delete(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        namespace: IndexedStorageNamespace,
        key: &str,
    ) -> Result<(), IndexedStorageError> {
        let (db, scope) = self.database_of(&namespace).await?;
        let Some(storage) = self
            .files
            .open_existing(&db, scope)
            .await
            .map_err(IndexedStorageError::Other)?
        else {
            return Ok(());
        };
        storage.delete(svc_name, api_name, namespace, key).await?;
        if scope == DatabaseScope::PerAgent {
            // The agent's only index in this database is gone: free its connections now instead
            // of when the cache evicts it. A later write reopens it.
            self.files.release(&db).await;
        }
        Ok(())
    }

    async fn read(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        entity_name: &'static str,
        namespace: IndexedStorageNamespace,
        key: &str,
        start_id: u64,
        end_id: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>, IndexedStorageError> {
        match self.readable(&namespace).await? {
            Some(storage) => {
                storage
                    .read(
                        svc_name,
                        api_name,
                        entity_name,
                        namespace,
                        key,
                        start_id,
                        end_id,
                    )
                    .await
            }
            None => Ok(vec![]),
        }
    }

    async fn first(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        entity_name: &'static str,
        namespace: IndexedStorageNamespace,
        key: &str,
    ) -> Result<Option<(u64, Vec<u8>)>, IndexedStorageError> {
        match self.readable(&namespace).await? {
            Some(storage) => {
                storage
                    .first(svc_name, api_name, entity_name, namespace, key)
                    .await
            }
            None => Ok(None),
        }
    }

    async fn last(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        entity_name: &'static str,
        namespace: IndexedStorageNamespace,
        key: &str,
    ) -> Result<Option<(u64, Vec<u8>)>, IndexedStorageError> {
        match self.readable(&namespace).await? {
            Some(storage) => {
                storage
                    .last(svc_name, api_name, entity_name, namespace, key)
                    .await
            }
            None => Ok(None),
        }
    }

    async fn closest(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        entity_name: &'static str,
        namespace: IndexedStorageNamespace,
        key: &str,
        id: u64,
    ) -> Result<Option<(u64, Vec<u8>)>, IndexedStorageError> {
        match self.readable(&namespace).await? {
            Some(storage) => {
                storage
                    .closest(svc_name, api_name, entity_name, namespace, key, id)
                    .await
            }
            None => Ok(None),
        }
    }

    async fn drop_prefix(
        &self,
        svc_name: &'static str,
        api_name: &'static str,
        namespace: IndexedStorageNamespace,
        key: &str,
        last_dropped_id: u64,
    ) -> Result<(), IndexedStorageError> {
        match self.readable(&namespace).await? {
            Some(storage) => {
                storage
                    .drop_prefix(svc_name, api_name, namespace, key, last_dropped_id)
                    .await
            }
            None => Ok(()),
        }
    }
}
