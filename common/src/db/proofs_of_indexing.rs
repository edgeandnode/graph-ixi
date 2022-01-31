use std::sync::Arc;
use std::time::Duration;

use chrono::prelude::Utc;
use diesel::{r2d2, PgConnection, RunQueryDsl};
use futures::{FutureExt, Stream, StreamExt, TryFutureExt};
use futures_retry::{FutureRetry, RetryPolicy};
use tracing::{info, warn};

use crate::{db::models::POICrossCheckReport, indexer::Indexer, types};

use super::{models::ProofOfIndexing, schema};

/// Write any POIs that we receive to the database.
pub fn write<S, I>(
    connection_pool: Arc<r2d2::Pool<r2d2::ConnectionManager<PgConnection>>>,
    proofs_of_indexing: S,
) where
    S: Stream<Item = types::ProofOfIndexing<I>> + Send + 'static,
    I: Indexer + Send + Sync + 'static,
{
    tokio::spawn(async move {
        proofs_of_indexing
            .ready_chunks(100)
            .for_each(move |chunk: Vec<types::ProofOfIndexing<I>>| {
                let connection_pool = connection_pool.clone();
                let mut consecutive_errors = 0;

                async move {
                    FutureRetry::new(
                        || async {
                            let pois = chunk
                                .clone()
                                .into_iter()
                                .map(|poi| ProofOfIndexing {
                                    timestamp: Utc::now().naive_utc(),
                                    indexer: poi.indexer.id().trim_start_matches("0x").into(),
                                    deployment: poi.deployment.to_string(),
                                    block_number: poi.block.number as i64,
                                    block_hash: poi.block.hash.map(|hash| hash.into()),
                                    proof_of_indexing: poi.proof_of_indexing.into(),
                                })
                                .collect::<Vec<_>>();

                            let number_of_pois = pois.len();

                            let connection = connection_pool.get()?;
                            diesel::insert_into(schema::proofs_of_indexing::table)
                                .values(pois)
                                .on_conflict_do_nothing()
                                .execute(&connection)?;

                            info!(%number_of_pois, "Wrote POIs to database");

                            Ok(()) as Result<_, anyhow::Error>
                        },
                        |e| {
                            if consecutive_errors >= 5 {
                                consecutive_errors = 0;
                                RetryPolicy::ForwardError(e)
                            } else {
                                consecutive_errors += 1;
                                RetryPolicy::WaitRetry(Duration::from_secs(1))
                            }
                        },
                    )
                    .await
                }
                .map_err(|(error, attempts)| {
                    warn!(%error, %attempts, "Failed to write POIs to database");
                })
                .map(|_| ())
            })
            .await;
    });
}

/// Write any POI cross-check reports that we receive to the database.
pub fn write_reports<S, I>(
    connection_pool: Arc<r2d2::Pool<r2d2::ConnectionManager<PgConnection>>>,
    reports: S,
) where
    S: Stream<Item = types::POICrossCheckReport<I>> + Send + 'static,
    I: Indexer + Send + Sync + 'static,
{
    tokio::spawn(async move {
        reports
            .ready_chunks(100)
            .for_each(move |chunk: Vec<types::POICrossCheckReport<I>>| {
                let connection_pool = connection_pool.clone();
                let mut consecutive_errors = 0;

                async move {
                    FutureRetry::new(
                        || async {
                            let reports = chunk
                                .clone()
                                .into_iter()
                                .map(|report| POICrossCheckReport {
                                    timestamp: Utc::now().naive_utc(),
                                    indexer1: report
                                        .poi1
                                        .indexer
                                        .id()
                                        .trim_start_matches("0x")
                                        .into(),
                                    indexer2: report
                                        .poi2
                                        .indexer
                                        .id()
                                        .trim_start_matches("0x")
                                        .into(),
                                    deployment: report.poi1.deployment.to_string(),
                                    block_hash: report.poi1.block.hash.map(|hash| hash.to_string()),
                                    block_number: report.poi1.block.number as i64,
                                    proof_of_indexing1: report.poi1.proof_of_indexing.to_string(),
                                    proof_of_indexing2: report.poi2.proof_of_indexing.to_string(),
                                })
                                .collect::<Vec<_>>();

                            let number_of_reports = reports.len();

                            let connection = connection_pool.get()?;
                            diesel::insert_into(schema::poi_cross_check_reports::table)
                                .values(reports)
                                .on_conflict_do_nothing()
                                .execute(&connection)?;

                            info!(%number_of_reports, "Wrote POI cross-check reports to database");

                            Ok(()) as Result<_, anyhow::Error>
                        },
                        |e| {
                            if consecutive_errors >= 5 {
                                consecutive_errors = 0;
                                RetryPolicy::ForwardError(e)
                            } else {
                                consecutive_errors += 1;
                                RetryPolicy::WaitRetry(Duration::from_secs(1))
                            }
                        },
                    )
                    .await
                }
                .map_err(|(error, attempts)| {
                    warn!(%error, %attempts, "Failed to write POI cross-check reports to database");
                })
                .map(|_| ())
            })
            .await;
    });
}