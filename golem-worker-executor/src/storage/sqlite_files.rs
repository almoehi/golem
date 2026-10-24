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

//! The set of SQLite database files behind the "multi-sqlite" storages
//! ([`crate::storage::keyvalue::multi_sqlite`], [`crate::storage::indexed::multi_sqlite`]): one
//! file per agent for durable agent data plus a few shared files.
//!
//! Every open database costs OS threads (sqlx-sqlite runs one thread per connection) and page
//! cache memory, so the number of open databases and their connections are bounded by hard caps
//! that do not depend on any clock: at most [`MAX_CACHED_DATABASES`] pools per storage, one read
//! and one write connection per per-agent database, opened lazily and closed when idle. Reads
//! never create a database file; only writes do.

use golem_common::cache::{BackgroundEvictionMode, Cache, FullCacheEvictionMode, SimpleCache};
use golem_common::config::DbSqliteConfig;
use golem_common::model::AgentId;
use golem_service_base::db::sqlite::{SqlitePool, SqlitePoolTuning};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Upper bound of open (cached) databases per multi-sqlite storage. A durable agent that is
/// active uses one key-value and up to three indexed databases; past this bound the least
/// recently used pool is dropped and reopened on its next use.
pub const MAX_CACHED_DATABASES: usize = 128;

/// A cached pool unused for this long is dropped (housekeeping only - the size cap above is the
/// bound that holds when the clock does not advance, e.g. in a suspended VM).
const CACHE_TIME_TO_IDLE: Duration = Duration::from_secs(600);
const CACHE_EVICTION_PERIOD: Duration = Duration::from_secs(60);

/// Per-agent databases: one reader suffices (an agent's storage access is sequential), the page
/// cache is small (the OS page cache still holds the file) and idle connections - and with them
/// their threads - are closed while the pool stays cached.
const PER_AGENT_READ_CONNECTIONS: u32 = 1;
const PER_AGENT_PAGE_CACHE_KIB: u32 = 256;
const PER_AGENT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// The file name component of an agent's per-agent databases. The only place it is derived.
pub fn agent_db_hash(agent_id: &AgentId) -> String {
    blake3::hash(agent_id.to_string().as_bytes()).to_string()
}

/// Whether a database belongs to one agent or is shared by many.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseScope {
    PerAgent,
    Shared,
}

impl DatabaseScope {
    fn tuning(self) -> SqlitePoolTuning {
        match self {
            DatabaseScope::PerAgent => SqlitePoolTuning {
                max_read_connections: Some(PER_AGENT_READ_CONNECTIONS),
                page_cache_kib: Some(PER_AGENT_PAGE_CACHE_KIB),
                idle_timeout: Some(PER_AGENT_IDLE_TIMEOUT),
                lazy: true,
            },
            DatabaseScope::Shared => SqlitePoolTuning::default(),
        }
    }
}

/// A storage over one migrated SQLite database.
pub trait SqliteFileStorage: Clone + Send + Sync + 'static {
    fn open(
        config: DbSqliteConfig,
        tuning: SqlitePoolTuning,
    ) -> impl Future<Output = Result<Self, String>> + Send;

    fn pool(&self) -> SqlitePool;
}

/// The open databases of one multi-sqlite storage, by file name relative to `root_dir`.
pub struct SqliteFiles<S> {
    cache: Cache<String, (), S, String>,
    root_dir: PathBuf,
    max_connections: u32,
    foreign_keys: bool,
}

impl<S: SqliteFileStorage> SqliteFiles<S> {
    pub fn new(
        root_dir: &Path,
        max_connections: u32,
        foreign_keys: bool,
        name: &'static str,
    ) -> Self {
        if !root_dir.exists() {
            std::fs::create_dir_all(root_dir)
                .expect("Failed to create root directory for sqlite storage");
        }
        Self {
            cache: Cache::new(
                Some(MAX_CACHED_DATABASES),
                FullCacheEvictionMode::LeastRecentlyUsed(1),
                BackgroundEvictionMode::OlderThan {
                    ttl: CACHE_TIME_TO_IDLE,
                    period: CACHE_EVICTION_PERIOD,
                },
                name,
            ),
            root_dir: root_dir.to_path_buf(),
            max_connections,
            foreign_keys,
        }
    }

    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    /// The database `db`, created (and migrated) if it does not exist yet. For writes.
    pub async fn open_or_create(&self, db: &str, scope: DatabaseScope) -> Result<S, String> {
        let root_dir = self.root_dir.clone();
        let config = self.config(db);
        self.cache
            .get_or_insert_simple(&db.to_string(), async move || {
                // Recreate root_dir if it was deleted after startup (e.g. by a concurrent
                // `golem server clean`).
                tokio::fs::create_dir_all(&root_dir).await.map_err(|e| {
                    format!(
                        "Failed to create sqlite storage directory {}: {e}",
                        root_dir.display()
                    )
                })?;
                S::open(config, scope.tuning()).await
            })
            .await
    }

    /// The database `db` if it exists; `None` without creating anything otherwise. For reads and
    /// deletes, whose result on a missing database is "empty".
    pub async fn open_existing(&self, db: &str, scope: DatabaseScope) -> Result<Option<S>, String> {
        if let Some(storage) = self.cache.get(&db.to_string()).await {
            return Ok(Some(storage));
        }
        if !self.file_exists(db).await? {
            return Ok(None);
        }
        self.open_or_create(db, scope).await.map(Some)
    }

    /// Whether the database `db` is open or exists on disk.
    pub async fn is_present(&self, db: &str) -> Result<bool, String> {
        Ok(self.cache.contains_key(&db.to_string()).await || self.file_exists(db).await?)
    }

    /// Runs `f` on the existing database `db` without adding it to the cache: an open pool is
    /// reused, otherwise a temporary one is opened and closed afterwards. For one-off passes over
    /// many databases (scans).
    pub async fn with_uncached<R>(
        &self,
        db: &str,
        f: impl AsyncFnOnce(&S) -> R,
    ) -> Result<R, String> {
        if let Some(storage) = self.cache.get(&db.to_string()).await {
            return Ok(f(&storage).await);
        }
        let storage = S::open(self.config(db), DatabaseScope::PerAgent.tuning()).await?;
        let result = f(&storage).await;
        storage.pool().close().await;
        Ok(result)
    }

    /// Drops the pool of `db` from the cache; its connections close once no caller holds the pool
    /// any more. The next access reopens the database.
    pub async fn release(&self, db: &str) {
        self.cache.remove(&db.to_string()).await;
    }

    /// Number of open (cached) databases.
    pub async fn open_databases(&self) -> usize {
        self.cache.iter().await.len()
    }

    /// Number of open connections of all cached databases.
    pub async fn open_connections(&self) -> u32 {
        self.cache
            .iter()
            .await
            .iter()
            .map(|(_, storage)| storage.pool().size())
            .sum()
    }

    fn config(&self, db: &str) -> DbSqliteConfig {
        DbSqliteConfig {
            database: self.root_dir.join(db).to_string_lossy().to_string(),
            max_connections: self.max_connections,
            foreign_keys: self.foreign_keys,
        }
    }

    async fn file_exists(&self, db: &str) -> Result<bool, String> {
        let path = self.root_dir.join(db);
        tokio::fs::try_exists(&path)
            .await
            .map_err(|e| format!("Failed to check sqlite database {}: {e}", path.display()))
    }
}
