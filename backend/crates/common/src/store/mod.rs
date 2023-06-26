use std::sync::Arc;

use crate::api_types::{DivergenceInvestigationRequest, DivergenceInvestigationRequestWithUuid};
use crate::{api_types::BlockRangeInput, store::models::PoI};
use anyhow::Error;
use diesel::prelude::*;
use diesel::{
    r2d2::{self, ConnectionManager, Pool, PooledConnection},
    Connection, PgConnection,
};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use sqlx::postgres::PgListener;
use tokio::sync::Mutex;
use tracing::info;

// Provides the diesel queries, callers should handle connection pooling and transactions.
mod diesel_queries;
#[cfg(tests)]
pub use diesel_queries;
use uuid::Uuid;

use self::models::{BigIntId, IndexerRef, IntId, WritablePoI};

pub mod models;
mod schema;

#[cfg(test)]
mod tests;

macro_rules! indexer_ref {
    ( $indexer:expr ) => {{
        match $indexer {
            IndexerRef::Id(i) => indexers::table
                .select(indexers::id)
                .filter(indexers::id.eq(i))
                .into_boxed(),
            IndexerRef::Address(addr) => indexers::table
                .select(indexers::id)
                .filter(indexers::address.eq(addr))
                .into_boxed(),
        }
        .single_value()
        .assume_not_null()
    }};
}

macro_rules! get_block_id {
    ( $block_hash:expr, $network:expr ) => {{
        blocks::table
            .select(blocks::id)
            .filter(
                blocks::hash.eq($block_hash).and(
                    blocks::network_id.eq(networks::table
                        .select(networks::id)
                        .filter(networks::name.eq($network))
                        .single_value()
                        .assume_not_null()),
                ),
            )
            .single_value()
            .assume_not_null()
    }};
}

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

/// An abstraction over all database operations. It uses [`Arc`](std::sync::Arc) internally, so
/// it's cheaply cloneable.
#[derive(Clone)]
pub struct Store {
    pool: Pool<ConnectionManager<PgConnection>>,
    listener: Arc<Mutex<PgListener>>,
}

impl Store {
    pub async fn new(db_url: &str) -> anyhow::Result<Self> {
        info!("Initializing database connection pool");
        let manager = r2d2::ConnectionManager::<PgConnection>::new(db_url);
        let pool = r2d2::Builder::new().build(manager)?;
        let mut listener = PgListener::connect(db_url).await?;
        listener.listen("cross_check_reports").await?;
        let store = Self {
            pool,
            listener: Arc::new(Mutex::new(listener)),
        };
        store.run_migrations()?;
        Ok(store)
    }

    fn run_migrations(&self) -> anyhow::Result<()> {
        let mut conn = self.pool.get()?;

        // Get a lock for running migrations. Blocks until we get the lock.
        diesel::sql_query("select pg_advisory_lock(1)").execute(&mut conn)?;
        info!("Run database migrations");
        conn.run_pending_migrations(MIGRATIONS)
            .map_err(|e| anyhow::anyhow!(e))?;

        // Release the migration lock.
        diesel::sql_query("select pg_advisory_unlock(1)").execute(&mut conn)?;
        Ok(())
    }

    fn conn(&self) -> anyhow::Result<PooledConnection<ConnectionManager<PgConnection>>> {
        Ok(self.pool.get()?)
    }

    #[cfg(test)]
    pub fn test_conn(&self) -> PooledConnection<ConnectionManager<PgConnection>> {
        self.pool.get().unwrap()
    }

    /// Returns all subgraph deployments that have ever analyzed.
    pub fn sg_deployments(&self) -> anyhow::Result<Vec<String>> {
        use schema::sg_deployments as sgd;

        Ok(sgd::table
            .select(sgd::ipfs_cid)
            .order_by(sgd::ipfs_cid.asc())
            .load::<String>(&mut self.conn()?)?)
    }

    pub fn poi(&self, poi: &str) -> anyhow::Result<Option<PoI>> {
        let mut conn = self.conn()?;
        diesel_queries::poi(&mut conn, poi)
    }

    pub fn indexers(&self) -> anyhow::Result<Vec<models::Indexer>> {
        let mut conn = self.conn()?;
        Ok(diesel_queries::indexers(&mut conn)?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    /// Queries the database for proofs of indexing that refer to the specified
    /// subgraph deployments and in the given [`BlockRange`], if given.
    pub fn pois(
        &self,
        sg_deployments: &[String],
        block_range: Option<BlockRangeInput>,
        limit: Option<u16>,
    ) -> anyhow::Result<Vec<PoI>> {
        let mut conn = self.conn()?;
        diesel_queries::pois(
            &mut conn,
            None,
            Some(sg_deployments),
            block_range,
            limit,
            false,
        )
    }

    /// Like `pois`, but only returns live pois.
    pub fn live_pois(
        &self,
        indexer_name: Option<&str>,
        sg_deployments_cids: Option<&[String]>,
        block_range: Option<BlockRangeInput>,
        limit: Option<u16>,
    ) -> anyhow::Result<Vec<PoI>> {
        let mut conn = self.conn()?;
        diesel_queries::pois(
            &mut conn,
            indexer_name,
            sg_deployments_cids,
            block_range,
            limit,
            true,
        )
    }

    pub fn write_pois(&self, pois: &[impl WritablePoI], live: PoiLiveness) -> anyhow::Result<()> {
        self.conn()?
            .transaction::<_, Error, _>(|conn| diesel_queries::write_pois(conn, pois, live))
    }

    pub async fn recv_cross_check_report_request(
        &self,
    ) -> anyhow::Result<DivergenceInvestigationRequestWithUuid> {
        let notification = self.listener.lock().await.recv().await?;
        assert_eq!(notification.channel(), "cross_check_reports");
        let payload = serde_json::from_str(notification.payload())?;
        Ok(payload)
    }

    pub fn queue_cross_check_report(
        &self,
        req: DivergenceInvestigationRequest,
    ) -> anyhow::Result<Uuid> {
        let uuid = Uuid::new_v4();
        let with_uuid = DivergenceInvestigationRequestWithUuid { uuid, req };

        diesel::dsl::sql_query("SELECT pg_notify('cross_check_reports', $1);")
            .bind::<diesel::sql_types::Text, _>(serde_json::to_string(&with_uuid)?)
            .execute(&mut self.conn()?)?;

        Ok(uuid)
    }

    pub fn write_divergence_bisect_report(
        &self,
        poi1: &str,
        poi2: &str,
        divergence_block: BigIntId,
    ) -> anyhow::Result<IntId> {
        use schema::{blocks, poi_divergence_bisect_reports as reports};

        let poi1_id = self.poi(poi1)?.unwrap().id;
        let poi2_id = self.poi(poi2)?.unwrap().id;

        // Normalize pairing order to avoid duplicates.
        let (poi1_id, poi2_id) = if poi1_id < poi2_id {
            (poi1_id, poi2_id)
        } else {
            (poi2_id, poi1_id)
        };

        let id = diesel::insert_into(reports::table)
            .values((
                reports::poi1_id.eq(poi1_id),
                reports::poi2_id.eq(poi2_id),
                reports::divergence_block_id.eq(blocks::table
                    .select(blocks::id)
                    .filter(blocks::id.eq(divergence_block))
                    .single_value()),
            ))
            .returning(reports::id)
            .get_result(&mut self.conn()?)?;

        Ok(id)
    }

    pub fn write_block_cache_entry(
        &self,
        indexer: IndexerRef,
        network: &str,
        block_hash: &[u8],
        block_data_json: serde_json::Value,
    ) -> anyhow::Result<BigIntId> {
        use schema::{block_cache_entries as entries, blocks, indexers, networks};

        let id = diesel::insert_into(entries::table)
            .values((
                entries::indexer_id.eq(indexer_ref!(indexer)),
                entries::block_id.eq(get_block_id!(block_hash, network)),
                entries::block_data.eq(block_data_json),
            ))
            .returning(entries::id)
            .get_result(&mut self.conn()?)?;

        Ok(id)
    }

    pub fn write_eth_call_cache_entry(
        &self,
        indexer: IndexerRef,
        network: &str,
        block_hash: &[u8],
        eth_call_data: serde_json::Value,
        eth_call_result: serde_json::Value,
    ) -> anyhow::Result<BigIntId> {
        use schema::{blocks, eth_call_cache_entries as entries, indexers, networks};

        let id = diesel::insert_into(entries::table)
            .values((
                entries::indexer_id.eq(indexer_ref!(indexer)),
                entries::block_id.eq(get_block_id!(block_hash, network)),
                entries::eth_call_data.eq(eth_call_data),
                entries::eth_call_result.eq(eth_call_result),
            ))
            .returning(entries::id)
            .get_result(&mut self.conn()?)?;

        Ok(id)
    }

    pub fn write_entity_changes_in_block(
        &self,
        indexer: IndexerRef,
        network: &str,
        block_hash: &[u8],
        entity_changes: serde_json::Value,
    ) -> anyhow::Result<BigIntId> {
        use schema::{blocks, entity_changes_in_block as changes, indexers, networks};

        let id = diesel::insert_into(changes::table)
            .values((
                changes::indexer_id.eq(indexer_ref!(indexer)),
                changes::block_id.eq(get_block_id!(block_hash, network)),
                changes::entity_change_data.eq(entity_changes),
            ))
            .returning(changes::id)
            .get_result(&mut self.conn()?)?;

        Ok(id)
    }

    pub fn read_report_metadata(&self, _poi1: &str, _poi2: &str) -> anyhow::Result<()> {
        todo!("read_report_metadata")
    }

    // pub fn poi_divergence_bisect_reports(
    //     &self,
    //     indexer1: Filter,
    //     indexer2: Filter,
    // ) -> anyhow::Result<Vec<models::PoiDivergenceBisectReport>> {
    //     use schema::poi_divergence_bisect_reports::dsl::*;

    //     let mut query = poi_divergence_bisect_reports
    //         .filter(sql)
    //         .filter(poi1_id.eq(foo).and(poi2_id.eq(bar)))
    //         .distinct_on((block_number, indexer1, indexer2, deployment))
    //         .into_boxed();

    //     if let Some(indexer) = indexer1_s {
    //         query = query.filter(indexer1.eq(indexer));
    //     }

    //     if let Some(indexer) = indexer2_s {
    //         query = query.filter(indexer2.eq(indexer));
    //     }

    //     query = query
    //         .order_by((
    //             block_number.desc(),
    //             deployment.asc(),
    //             indexer1.asc(),
    //             indexer2.asc(),
    //         ))
    //         .limit(5000);

    //     Ok(query.load::<models::PoiCrossCheckReport>(&self.conn()?)?)
    // }

    // pub fn write_poi_cross_check_reports(
    //     &self,
    //     reports: Vec<models::PoiCrossCheckReport>,
    // ) -> anyhow::Result<()> {
    //     let len = reports.len();
    //     diesel::insert_into(schema::poi_cross_check_reports::table)
    //         .values(reports)
    //         .on_conflict_do_nothing()
    //         .execute(&self.conn()?)?;

    //     info!(%len, "Wrote POI cross check reports to database");
    //     Ok(())
    // }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PoiLiveness {
    Live,
    NotLive,
}