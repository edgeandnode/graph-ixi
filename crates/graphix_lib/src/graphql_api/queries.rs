use std::collections::BTreeMap;

use anyhow::Context as _;
use async_graphql::{Context, Object, Result};
use futures::future::try_join_all;
use graphix_common_types::*;
use graphix_store::models::ApiKeyPublicMetadata;
use uuid::Uuid;

use super::{api_types, ctx_data, require_permission_level};

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Returns the version of the Graphix instance.
    async fn version(&self) -> Result<String> {
        Ok(crate::GRAPHIX_VERSION.to_string())
    }

    /// Fetches all tracked subgraph deploymens in this Graphix instance and
    /// filters them according to some filtering rules.
    async fn deployments(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "The network name of the subgraph deployments to fetch")]
        network_name: Option<String>,
        name: Option<String>,
        ipfs_cid: Option<IpfsCid>,
        #[graphql(
            default = 100,
            validator(maximum = 250),
            desc = "Upper limit on the number of shown results."
        )]
        limit: u16,
    ) -> Result<Vec<api_types::SubgraphDeployment>> {
        let ctx_data = ctx_data(ctx);

        let filter = inputs::SgDeploymentsQuery {
            network_name,
            name,
            ipfs_cid,
            limit: Some(limit),
        };
        let deployments = ctx_data.store.sg_deployments(filter).await?;

        Ok(deployments.into_iter().map(Into::into).collect())
    }

    /// Fetches all tracked indexers in this Graphix instance and filters them
    /// according to some filtering rules.
    async fn indexers(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "The address of the indexer, encoded as a hex string with a '0x' prefix")]
        address: Option<IndexerAddress>,
        #[graphql(
            default = 100,
            validator(maximum = 250),
            desc = "Upper limit on the number of shown results."
        )]
        limit: u16,
    ) -> Result<Vec<api_types::Indexer>> {
        let ctx_data = ctx_data(ctx);

        let filter = inputs::IndexersQuery {
            address,
            limit: Some(limit),
        };
        let indexers = ctx_data.store.indexers(filter).await?;

        Ok(indexers.into_iter().map(Into::into).collect())
    }

    /// Filters through all PoIs ever collected by this Graphix
    /// instance, according to some filtering rules specified in `filter`.
    async fn proofs_of_indexing(
        &self,
        ctx: &Context<'_>,
        #[graphql(
            desc = "Restricts the query to PoIs for subgraph deployments that index the given chain name."
        )]
        network: Option<String>,
        #[graphql(
            default,
            desc = "Restricts the query to PoIs for these given subgraph deployments (by hex-encoded IPFS CID with '0x' prefix)."
        )]
        deployments: Vec<IpfsCid>,
        #[graphql(
            desc = "Restricts the query to PoIs that were collected in the given block range."
        )]
        block_range: Option<inputs::BlockRange>,
        #[graphql(
            default = 100,
            validator(maximum = 250),
            desc = "Upper limit on the number of shown results."
        )]
        limit: u16,
    ) -> Result<Vec<api_types::ProofOfIndexing>> {
        let ctx_data = ctx_data(ctx);

        let filter = inputs::PoisQuery {
            network,
            deployments,
            block_range,
            limit: Some(limit),
        };
        let pois = ctx_data
            .store
            .pois(&filter.deployments, filter.block_range, filter.limit)
            .await?;

        Ok(pois.into_iter().map(Into::into).collect())
    }

    /// A copy of the configuration file used to run Graphix.
    async fn configuration(&self, ctx: &Context<'_>) -> Result<Option<serde_json::Value>> {
        require_permission_level(ctx, ApiKeyPermissionLevel::Admin).await?;

        let ctx_data = ctx_data(ctx);
        let config = ctx_data.store.config().await?;

        Ok(config)
    }

    /// Same as [`QueryRoot::proofs_of_indexing`], but only returns PoIs that
    /// are "live" i.e. they are the most recent PoI collected for their
    /// subgraph deployment.
    async fn live_proofs_of_indexing(
        &self,
        ctx: &Context<'_>,
        filter: inputs::PoisQuery,
    ) -> Result<Vec<api_types::ProofOfIndexing>> {
        let ctx_data = ctx_data(ctx);
        let pois = ctx_data
            .store
            .live_pois(
                None,
                Some(&filter.deployments),
                filter.block_range,
                filter.limit,
            )
            .await?;

        Ok(pois.into_iter().map(Into::into).collect())
    }

    async fn api_keys(&self, ctx: &Context<'_>) -> Result<Vec<ApiKeyPublicMetadata>> {
        let ctx_data = ctx_data(ctx);
        let api_keys = ctx_data.store.api_keys().await?;

        Ok(api_keys)
    }

    async fn poi_agreement_ratios(
        &self,
        ctx: &Context<'_>,
        indexer_address: IndexerAddress,
    ) -> Result<Vec<api_types::PoiAgreementRatio>> {
        let ctx_data = ctx_data(ctx);

        // Query live POIs of a the requested indexer.
        let indexer_pois = live_pois(ctx, indexer_address).await?;

        let deployments =
            try_join_all(indexer_pois.iter().map(|poi| poi.deployment(ctx_data))).await?;

        let deployment_cids: Vec<IpfsCid> = deployments.iter().map(|d| d.cid().clone()).collect();

        // Query all live POIs for the specific deployments.
        let all_deployment_pois = ctx_data
            .store
            .live_pois(None, Some(&deployment_cids), None, None)
            .await?;

        // Convert POIs to ProofOfIndexing and group by deployment
        let mut deployment_to_pois: BTreeMap<String, Vec<api_types::ProofOfIndexing>> =
            BTreeMap::new();
        for poi in all_deployment_pois {
            let proof_of_indexing: api_types::ProofOfIndexing = poi.into();
            deployment_to_pois
                .entry(
                    proof_of_indexing
                        .deployment(ctx_data)
                        .await?
                        .cid()
                        .to_string(),
                )
                .or_default()
                .push(proof_of_indexing);
        }

        let mut agreement_ratios: Vec<api_types::PoiAgreementRatio> = Vec::new();

        for poi in indexer_pois {
            let deployment_pois = deployment_to_pois
                .get(&poi.deployment(ctx_data).await?.cid().to_string())
                .context("inconsistent pois table, no pois for deployment")?;

            let total_indexers = deployment_pois.len() as u32;

            // Calculate POI agreement by creating a map to count unique POIs and their occurrence.
            let mut poi_counts: BTreeMap<PoiBytes, u32> = BTreeMap::new();
            for dp in deployment_pois {
                *poi_counts.entry(dp.hash()).or_insert(0) += 1;
            }

            // Define consensus and agreement based on the map.
            let (max_poi, max_poi_count) = poi_counts
                .iter()
                .max_by_key(|(_, &v)| v)
                .context("inconsistent pois table, no pois")?;

            let has_consensus = *max_poi_count > total_indexers / 2;

            let n_agreeing_indexers = *poi_counts
                .get(&poi.hash())
                .context("inconsistent pois table, no matching poi")?;

            let n_disagreeing_indexers = total_indexers - n_agreeing_indexers;

            let in_consensus = has_consensus && max_poi == &poi.hash();

            let ratio = api_types::PoiAgreementRatio {
                poi_id: poi.model.id,
                total_indexers,
                n_agreeing_indexers,
                n_disagreeing_indexers,
                has_consensus,
                in_consensus,
            };

            agreement_ratios.push(ratio);
        }

        Ok(agreement_ratios)
    }

    async fn divergence_investigation_report(
        &self,
        ctx: &Context<'_>,
        #[graphql(
            desc = "The UUID of the divergence investigation report to fetch. This is the UUID that was returned by the `launchDivergenceInvestigation` mutation."
        )]
        uuid: Uuid,
    ) -> Result<Option<DivergenceInvestigationReport>> {
        let ctx_data = ctx_data(ctx);

        if let Some(report_json) = ctx_data
            .store
            .divergence_investigation_report(&uuid)
            .await?
        {
            Ok(
                serde_json::from_value(report_json)
                    .expect("Can't deserialize report from database"),
            )
        } else if ctx_data
            .store
            .divergence_investigation_request_exists(&uuid)
            .await?
        {
            Ok(Some(DivergenceInvestigationReport {
                uuid,
                status: DivergenceInvestigationStatus::InProgress,
                bisection_runs: vec![],
                error: None,
            }))
        } else {
            Ok(None)
        }
    }

    /// Returns all networks known to Graphix. Subgraphs indexing other networks
    /// won't be available in this Graphix database.
    async fn networks(&self, ctx: &Context<'_>) -> Result<Vec<api_types::Network>> {
        let ctx_data = ctx_data(ctx);
        let networks = ctx_data.store.networks().await?;

        Ok(networks.into_iter().map(Into::into).collect())
    }
}

async fn live_pois(
    ctx: &Context<'_>,
    indexer_address: IndexerAddress,
) -> Result<Vec<api_types::ProofOfIndexing>> {
    let ctx_data = ctx_data(ctx);

    let pois = ctx_data
        .store
        .live_pois(Some(&indexer_address), None, None, None)
        .await?;

    Ok(pois.into_iter().map(Into::into).collect())
}
