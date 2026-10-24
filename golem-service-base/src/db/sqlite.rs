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

use crate::repo::RepoError;
use async_trait::async_trait;
use golem_common::config::DbSqliteConfig;
use golem_common::metrics::db::{record_db_failure, record_db_success, record_db_transaction};
use sqlx::migrate::MigrationSource;
use sqlx::query::{Query, QueryAs};
use sqlx::sqlite::{SqliteArguments, SqlitePoolOptions, SqliteQueryResult, SqliteRow};
use sqlx::{Connection, Error, FromRow, IntoArguments, Sqlite, SqliteConnection};
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

#[derive(Clone, Debug)]
pub struct SqlitePool {
    read_pool: sqlx::SqlitePool,
    write_pool: sqlx::SqlitePool,
}

impl SqlitePool {
    pub fn new(read_pool: sqlx::SqlitePool, write_pool: sqlx::SqlitePool) -> Self {
        Self {
            read_pool,
            write_pool,
        }
    }

    pub async fn configured(config: &DbSqliteConfig) -> Result<Self, anyhow::Error> {
        Self::configured_with(config, &SqlitePoolTuning::default()).await
    }

    /// Creates the read and write pools for `config`, adjusted by `tuning`.
    pub async fn configured_with(
        config: &DbSqliteConfig,
        tuning: &SqlitePoolTuning,
    ) -> Result<Self, anyhow::Error> {
        let read_connections = tuning
            .max_read_connections
            .map_or(config.max_connections, |max| {
                max.min(config.max_connections)
            })
            .max(1);
        let read_pool = tuning.connect(config, read_connections).await?;
        let write_pool = tuning.connect(config, 1).await?;

        Ok(Self::new(read_pool, write_pool))
    }

    /// Closes both pools: idle connections are closed now, borrowed ones when returned. Only for
    /// pools nobody else uses any more - a query on a closed pool fails.
    pub async fn close(&self) {
        self.read_pool.close().await;
        self.write_pool.close().await;
    }

    /// Number of open connections (idle and in use) of both pools.
    pub fn size(&self) -> u32 {
        self.read_pool.size() + self.write_pool.size()
    }

    pub fn with_ro(&self, svc_name: &'static str, api_name: &'static str) -> SqliteLabelledApi {
        SqliteLabelledApi {
            svc_name,
            api_name,
            pool: self.read_pool.clone(),
        }
    }

    pub fn with_rw(&self, svc_name: &'static str, api_name: &'static str) -> SqliteLabelledApi {
        SqliteLabelledApi {
            svc_name,
            api_name,
            pool: self.write_pool.clone(),
        }
    }
}

#[async_trait]
impl super::Pool for SqlitePool {
    type LabelledApi = SqliteLabelledApi;
    type QueryResult = SqliteQueryResult;
    type Db = Sqlite;
    type Args<'a> = SqliteArguments<'a>;

    fn with_ro(&self, svc_name: &'static str, api_name: &'static str) -> Self::LabelledApi {
        SqlitePool::with_ro(self, svc_name, api_name)
    }

    fn with_rw(&self, svc_name: &'static str, api_name: &'static str) -> Self::LabelledApi {
        SqlitePool::with_rw(self, svc_name, api_name)
    }
}

/// Resource tuning of a [`SqlitePool`] on top of its [`DbSqliteConfig`]. The default keeps
/// sqlx's / SQLite's own behaviour. Storages that keep many small databases open at once (one per
/// agent) shrink it: sqlx-sqlite runs one OS thread per connection and every connection has its own
/// page cache, so these settings bound threads and memory per open database.
#[derive(Clone, Debug, Default)]
pub struct SqlitePoolTuning {
    /// Upper bound of the read pool, below `DbSqliteConfig::max_connections` (`None`: the config).
    pub max_read_connections: Option<u32>,
    /// `PRAGMA cache_size` per connection in KiB (`None`: the SQLite default of 2 MiB).
    pub page_cache_kib: Option<u32>,
    /// Closes connections idle for this long (`None`: the sqlx default of 10 minutes).
    pub idle_timeout: Option<Duration>,
    /// Opens connections on first use instead of one per pool at creation.
    pub lazy: bool,
}

impl SqlitePoolTuning {
    async fn connect(
        &self,
        config: &DbSqliteConfig,
        max_connections: u32,
    ) -> Result<sqlx::SqlitePool, sqlx::Error> {
        let mut pool_options = SqlitePoolOptions::new()
            .min_connections(0)
            .max_connections(max_connections);
        if let Some(idle_timeout) = self.idle_timeout {
            pool_options = pool_options.idle_timeout(Some(idle_timeout));
        }
        let mut connect_options = config.connect_options();
        if let Some(kib) = self.page_cache_kib {
            // A negative cache_size is a size in KiB rather than in pages.
            connect_options = connect_options.pragma("cache_size", format!("-{kib}"));
        }
        if self.lazy {
            Ok(pool_options.connect_lazy_with(connect_options))
        } else {
            pool_options.connect_with(connect_options).await
        }
    }
}

pub struct SqliteLabelledTransaction {
    svc_name: &'static str,
    api_name: &'static str,
    tx: sqlx::Transaction<'static, Sqlite>,
    start: Instant,
}

#[async_trait]
impl super::PoolApi for SqliteLabelledTransaction {
    type QueryResult = SqliteQueryResult;
    type Row = SqliteRow;
    type Db = Sqlite;
    type Args<'a> = SqliteArguments<'a>;

    async fn execute<'a>(
        &mut self,
        query: Query<'a, Self::Db, SqliteArguments<'a>>,
    ) -> Result<SqliteQueryResult, RepoError> {
        Ok(query.execute(&mut *self.tx).await?)
    }

    async fn fetch_optional<'a, A>(
        &mut self,
        query: Query<'a, Self::Db, A>,
    ) -> Result<Option<Self::Row>, RepoError>
    where
        A: 'a + IntoArguments<'a, Self::Db>,
    {
        Ok(query.fetch_optional(&mut *self.tx).await?)
    }

    async fn fetch_optional_as<'a, O, A>(
        &mut self,
        query_as: QueryAs<'a, Self::Db, O, A>,
    ) -> Result<Option<O>, RepoError>
    where
        A: 'a + IntoArguments<'a, Self::Db>,
        O: 'a + Send + Unpin + for<'r> FromRow<'r, Self::Row>,
    {
        Ok(query_as.fetch_optional(&mut *self.tx).await?)
    }

    async fn fetch_all<'a, A>(
        &mut self,
        query: Query<'a, Self::Db, A>,
    ) -> Result<Vec<SqliteRow>, RepoError>
    where
        A: 'a + IntoArguments<'a, Self::Db>,
    {
        Ok(query.fetch_all(&mut *self.tx).await?)
    }

    async fn fetch_all_as<'a, O, A>(
        &mut self,
        query_as: QueryAs<'a, Self::Db, O, A>,
    ) -> Result<Vec<O>, RepoError>
    where
        A: 'a + IntoArguments<'a, Self::Db>,
        O: 'a + Send + Unpin + for<'r> FromRow<'r, Self::Row>,
    {
        Ok(query_as.fetch_all(&mut *self.tx).await?)
    }
}

#[async_trait]
impl super::LabelledPoolTransaction for SqliteLabelledTransaction {
    async fn commit(self) -> Result<(), RepoError> {
        let svc_name = self.svc_name;
        let api_name = self.api_name;
        let start = self.start;
        let result = self.tx.commit().await;
        record_db_transaction("sqlite", svc_name, api_name, start.elapsed());
        SqliteLabelledApi::record(svc_name, api_name, start, result)
    }

    async fn rollback(self) -> Result<(), RepoError> {
        let svc_name = self.svc_name;
        let api_name = self.api_name;
        let start = self.start;
        warn!(
            svc_name = svc_name,
            api_name = api_name,
            "DB transaction rollback",
        );
        golem_common::metrics::db::record_db_rollback("sqlite", svc_name, api_name);
        let result = self.tx.rollback().await;
        record_db_transaction("sqlite", svc_name, api_name, start.elapsed());
        SqliteLabelledApi::record(svc_name, api_name, start, result)
    }
}

pub struct SqliteLabelledApi {
    svc_name: &'static str,
    api_name: &'static str,
    pool: sqlx::SqlitePool,
}

impl SqliteLabelledApi {
    fn record<R>(
        svc_name: &'static str,
        api_name: &'static str,
        start: Instant,
        result: Result<R, Error>,
    ) -> Result<R, RepoError> {
        let end = Instant::now();
        match result {
            Ok(result) => {
                record_db_success("sqlite", svc_name, api_name, end.duration_since(start));
                Ok(result)
            }
            Err(err) => {
                error!(
                    svc_name = svc_name,
                    api_name = api_name,
                    duration = end.duration_since(start).as_millis(),
                    error = format!("{err:#}"),
                    "DB query failed",
                );
                record_db_failure("sqlite", svc_name, api_name);
                Err(err.into())
            }
        }
    }

    fn record_self<R>(&self, start: Instant, result: Result<R, Error>) -> Result<R, RepoError> {
        Self::record(self.svc_name, self.api_name, start, result)
    }
}

#[async_trait]
impl super::PoolApi for SqliteLabelledApi {
    type QueryResult = SqliteQueryResult;
    type Row = SqliteRow;
    type Db = Sqlite;
    type Args<'a> = SqliteArguments<'a>;

    async fn execute<'a>(
        &mut self,
        query: Query<'a, Self::Db, Self::Args<'a>>,
    ) -> Result<Self::QueryResult, RepoError> {
        let start = Instant::now();
        self.record_self(start, query.execute(&self.pool).await)
    }

    async fn fetch_optional<'a, A>(
        &mut self,
        query: Query<'a, Self::Db, A>,
    ) -> Result<Option<Self::Row>, RepoError>
    where
        A: 'a + IntoArguments<'a, Self::Db>,
    {
        let start = Instant::now();
        self.record_self(start, query.fetch_optional(&self.pool).await)
    }

    async fn fetch_optional_as<'a, O, A>(
        &mut self,
        query_as: QueryAs<'a, Self::Db, O, A>,
    ) -> Result<Option<O>, RepoError>
    where
        A: 'a + IntoArguments<'a, Self::Db>,
        O: 'a + Send + Unpin + for<'r> FromRow<'r, Self::Row>,
    {
        let start = Instant::now();
        self.record_self(start, query_as.fetch_optional(&self.pool).await)
    }

    async fn fetch_all<'a, A>(
        &mut self,
        query: Query<'a, Self::Db, A>,
    ) -> Result<Vec<Self::Row>, RepoError>
    where
        A: 'a + IntoArguments<'a, Self::Db>,
    {
        let start = Instant::now();
        self.record_self(start, query.fetch_all(&self.pool).await)
    }

    async fn fetch_all_as<'a, O, A>(
        &mut self,
        query_as: QueryAs<'a, Self::Db, O, A>,
    ) -> Result<Vec<O>, RepoError>
    where
        A: 'a + IntoArguments<'a, Self::Db>,
        O: 'a + Send + Unpin + for<'r> FromRow<'r, Self::Row>,
    {
        let start = Instant::now();
        self.record_self(start, query_as.fetch_all(&self.pool).await)
    }
}

#[async_trait]
impl super::LabelledPoolApi for SqliteLabelledApi {
    type LabelledTransaction = SqliteLabelledTransaction;

    async fn begin(&self) -> Result<Self::LabelledTransaction, RepoError> {
        let tx = self.pool.begin().await?;
        Ok(SqliteLabelledTransaction {
            svc_name: self.svc_name,
            api_name: self.api_name,
            tx,
            start: Instant::now(),
        })
    }
}

pub async fn migrate(
    config: &DbSqliteConfig,
    migrations: impl MigrationSource<'_>,
) -> Result<(), anyhow::Error> {
    info!("DB migration: sqlite://{}", config.database);
    let mut conn = SqliteConnection::connect_with(&config.connect_options()).await?;
    let migrator = sqlx::migrate::Migrator::new(migrations).await?;
    migrator.run_direct(&mut conn).await?;

    let _ = conn.close().await;
    Ok(())
}
