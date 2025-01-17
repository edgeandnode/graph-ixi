use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use graphix_common_types::IndexerAddress;
use graphql_client::{GraphQLQuery, Response};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::*;

use super::{CachedEthereumCall, EntityChanges, IndexerClient};
use crate::{
    GraphNodeCollectedVersion, IndexerId, IndexingStatus, PoiRequest, ProofOfIndexing, WithIndexer,
};

const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Debug)]
pub struct RealIndexer {
    address: IndexerAddress,
    name: Option<String>,
    endpoint: String,
    client: reqwest::Client,
    // Metrics
    // -------
    public_poi_requests: prometheus::IntCounterVec,
}

impl RealIndexer {
    #[instrument(skip_all)]
    pub fn new(
        name: Option<String>,
        address: IndexerAddress,
        endpoint: String,
        public_poi_requests: prometheus::IntCounterVec,
    ) -> Self {
        Self {
            name,
            address,
            endpoint,
            client: reqwest::Client::new(),
            public_poi_requests,
        }
    }

    /// Internal utility method to make a GraphQL query to the indexer. `error`
    /// and `data` fields are treated as mutually exclusive (which is generally
    /// a good assumption, but some callers may want more control over error
    /// handling).
    async fn graphql_query<I: Serialize, O: DeserializeOwned>(
        &self,
        request: I,
    ) -> anyhow::Result<O> {
        let response_raw = self
            .client
            .post(self.endpoint.clone())
            .timeout(REQUEST_TIMEOUT)
            .json(&request)
            .send()
            .await?;

        let response: Response<O> = response_raw.json().await?;

        if let Some(errors) = response.errors {
            let errors = errors
                .iter()
                .map(|e| e.message.clone())
                .collect::<Vec<_>>()
                .join(",");
            warn!(%errors, "Indexer returned errors");
            return Err(anyhow::anyhow!("Indexer returned errors: {}", errors));
        }

        response.data.context("Indexer returned no data")
    }

    async fn proofs_of_indexing_batch(
        self: Arc<Self>,
        requests: &[PoiRequest],
    ) -> Result<Vec<ProofOfIndexing>, anyhow::Error> {
        use gql_types::proofs_of_indexing::{
            PublicProofOfIndexingRequest, ResponseData, Variables,
        };
        let request = gql_types::ProofsOfIndexing::build_query(Variables {
            requests: requests
                .iter()
                .map(|query| PublicProofOfIndexingRequest {
                    deployment: query.deployment.to_string(),
                    block_number: query.block_number.to_string(),
                })
                .collect(),
        });

        let response: ResponseData = self.graphql_query(request).await?;

        // Parse POI results
        response
            .public_proofs_of_indexing
            .into_iter()
            .map(|result| WithIndexer::new(self.clone(), result).try_into())
            .collect::<Result<Vec<_>, _>>()
    }
}

#[async_trait]
impl IndexerClient for RealIndexer {
    fn address(&self) -> IndexerAddress {
        self.address
    }

    fn name(&self) -> Option<Cow<str>> {
        self.name.as_ref().map(|s| Cow::Borrowed(s.as_str()))
    }

    async fn ping(self: Arc<Self>) -> anyhow::Result<()> {
        let request = gql_types::Typename::build_query(gql_types::typename::Variables);
        self.graphql_query::<_, serde_json::Value>(request).await?;
        Ok(())
    }

    async fn indexing_statuses(self: Arc<Self>) -> anyhow::Result<Vec<IndexingStatus>> {
        let request =
            gql_types::IndexingStatuses::build_query(gql_types::indexing_statuses::Variables);

        let response: gql_types::indexing_statuses::ResponseData =
            self.graphql_query(request).await?;

        let mut statuses = vec![];
        for indexing_status in response.indexing_statuses {
            let deployment = indexing_status.subgraph.clone();

            match WithIndexer::new(self.clone(), indexing_status).try_into() {
                Ok(status) => statuses.push(status),
                Err(e) => {
                    warn!(
                        address = %self.address_string(),
                        %e,
                        %deployment,
                        "Failed to parse indexing status, skipping deployment"
                    );
                }
            }
        }

        Ok(statuses)
    }

    async fn proofs_of_indexing(
        self: Arc<Self>,
        requests: Vec<PoiRequest>,
    ) -> Vec<ProofOfIndexing> {
        let mut pois = vec![];

        // Graph Node implements a limit of 10 POI requests per request, so split our requests up
        // accordingly.
        //
        // FIXME: This is temporarily set to 1 until we fix the error: 'Null value resolved for
        // non-null field `proofOfIndexing`' Which is probably a Graph Node bug. Setting it to 1
        // reduces the impact of this issue.
        for requests in requests.chunks(1) {
            trace!(
                indexer = %self.address_string(),
                batch_size = requests.len(),
                "Requesting public Pois batch"
            );

            let result = self.clone().proofs_of_indexing_batch(requests).await;

            match result {
                Ok(batch_pois) => {
                    self.public_poi_requests
                        .get_metric_with_label_values(&[&self.address_string(), "1"])
                        .unwrap()
                        .inc();

                    pois.extend(batch_pois);
                }
                Err(error) => {
                    self.public_poi_requests
                        .get_metric_with_label_values(&[&self.address_string(), "0"])
                        .unwrap()
                        .inc();

                    debug!(
                        id = %self.address_string(), %error,
                        "Failed to query POIs batch from indexer"
                    );

                    if error
                        .to_string()
                        .contains(r#"Cannot query field "publicProofsOfIndexing" on type "Query""#)
                    {
                        debug!(
                            id = %self.address_string(),
                            "Indexer doesn't seem to support 'publicProofsOfIndexing', skipping it"
                        );
                        break;
                    }
                }
            }
        }

        pois
    }

    async fn subgraph_api_versions(
        self: Arc<Self>,
        subgraph_id: &str,
    ) -> anyhow::Result<Vec<String>> {
        let request = gql_types::SubgraphApiVersions::build_query(
            gql_types::subgraph_api_versions::Variables {
                subgraph_id: subgraph_id.to_string(),
            },
        );

        let response: gql_types::subgraph_api_versions::ResponseData =
            self.graphql_query(request).await?;

        Ok(response
            .api_versions
            .into_iter()
            .map(|v| v.version)
            .collect())
    }

    async fn version(self: Arc<Self>) -> anyhow::Result<GraphNodeCollectedVersion> {
        let request = gql_types::IndexerVersion::build_query(gql_types::indexer_version::Variables);

        let response: gql_types::indexer_version::ResponseData =
            self.graphql_query(request).await?;

        Ok(GraphNodeCollectedVersion {
            version: Some(response.version.version),
            commit: Some(response.version.commit),
            error_response: None,
            collected_at: chrono::Utc::now().naive_utc(),
        })
    }

    async fn cached_eth_calls(
        self: Arc<Self>,
        network: &str,
        block_hash: &[u8],
    ) -> anyhow::Result<Vec<CachedEthereumCall>> {
        let request = gql_types::CachedEthereumCalls::build_query(
            gql_types::cached_ethereum_calls::Variables {
                network: network.to_string(),
                block_hash: hex::encode(block_hash),
            },
        );

        let response: gql_types::cached_ethereum_calls::ResponseData =
            self.graphql_query(request).await?;

        let eth_calls = response
            .cached_ethereum_calls
            .unwrap_or_default()
            .into_iter()
            .map(|eth_call| {
                Ok(CachedEthereumCall {
                    id_hash: gql_types::decode_bytes(&eth_call.id_hash)?,
                    return_value: gql_types::decode_bytes(&eth_call.return_value)?,
                    contract_address: gql_types::decode_bytes(&eth_call.contract_address)?,
                })
            })
            .collect::<anyhow::Result<Vec<CachedEthereumCall>>>()?;

        Ok(eth_calls)
    }

    async fn block_cache_contents(
        self: Arc<Self>,
        network: &str,
        block_hash: &[u8],
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let request = gql_types::BlockData::build_query(gql_types::block_data::Variables {
            network: network.to_string(),
            block_hash: hex::encode(block_hash),
        });

        let response: gql_types::block_data::ResponseData = self.graphql_query(request).await?;

        Ok(response.block_data)
    }

    async fn entity_changes(
        self: Arc<Self>,
        subgraph_id: &str,
        block_number: u64,
    ) -> anyhow::Result<EntityChanges> {
        let request = gql_types::EntityChangesInBlock::build_query(
            gql_types::entity_changes_in_block::Variables {
                subgraph_id: subgraph_id.to_string(),
                block_number: block_number as i64,
            },
        );

        let response: gql_types::entity_changes_in_block::ResponseData =
            self.graphql_query(request).await?;

        let mut updates = HashMap::new();
        for entity_type_updates in response.entity_changes_in_block.updates {
            updates
                .insert(entity_type_updates.type_, entity_type_updates.entities)
                .ok_or_else(|| anyhow!("duplicate entity types"))?;
        }

        let mut deletions = HashMap::new();
        for entity_type_deletions in response.entity_changes_in_block.deletions {
            deletions
                .insert(entity_type_deletions.type_, entity_type_deletions.entities)
                .ok_or_else(|| anyhow!("duplicate entity types"))?;
        }

        Ok(EntityChanges { updates, deletions })
    }
}

mod gql_types {
    use std::str::FromStr;

    use graphix_common_types::{BlockHash, IpfsCid, PoiBytes};

    use super::*;
    use crate::BlockPointer;

    pub type JSONObject = serde_json::Value;
    pub type BigInt = String;
    pub type Bytes = String;

    pub fn decode_bytes(s: &str) -> anyhow::Result<Vec<u8>> {
        if !s.starts_with("0x") {
            anyhow::bail!("hexstring must start with 0x");
        }
        Ok(hex::decode(&s[2..])?)
    }

    /// `__typename`
    #[derive(GraphQLQuery)]
    #[graphql(
        schema_path = "graphql/indexer/schema.gql",
        query_path = "graphql/indexer/queries/typename.gql",
        response_derives = "Debug",
        variables_derives = "Debug"
    )]
    pub struct Typename;

    /// Indexing Statuses

    #[derive(GraphQLQuery)]
    #[graphql(
        schema_path = "graphql/indexer/schema.gql",
        query_path = "graphql/indexer/queries/indexing-statuses.gql",
        response_derives = "Debug",
        variables_derives = "Debug"
    )]
    pub struct IndexingStatuses;

    impl TryInto<IndexingStatus> for WithIndexer<indexing_statuses::IndexingStatusesIndexingStatuses> {
        type Error = anyhow::Error;

        fn try_into(self) -> Result<IndexingStatus, Self::Error> {
            let chain = self
                .inner
                .chains
                .first()
                .ok_or_else(|| anyhow!("chain status missing"))?;

            let (latest_block, earliest_block_num) = match &chain.on {
            indexing_statuses::IndexingStatusesIndexingStatusesChainsOn::EthereumIndexingStatus(
                indexing_statuses::IndexingStatusesIndexingStatusesChainsOnEthereumIndexingStatus {
                    latest_block,
                    earliest_block,
                    ..
                },
            ) => match (latest_block, earliest_block) {
                (Some(block), Some(earliest_block)) => (BlockPointer {
                    number: block.number.parse()?,
                    hash: Some(str::parse::<BlockHash>(block.hash.as_str()).map_err(|e| anyhow!("invalid block hash: {}", e))?),
                }, earliest_block.number.parse()?),
                _ => {
                    return Err(anyhow!("deployment has not started indexing yet"));
                }
            },
        };

            let deployment = IpfsCid::from_str(&self.inner.subgraph)
                .map_err(|e| anyhow!("invalid subgraph CID: {}", e))?;

            Ok(IndexingStatus {
                indexer: self.indexer,
                deployment,
                network: chain.network.clone(),
                latest_block,
                earliest_block_num,
            })
        }
    }

    /// POIs

    #[derive(GraphQLQuery)]
    #[graphql(
        schema_path = "graphql/indexer/schema.gql",
        query_path = "graphql/indexer/queries/pois.gql",
        response_derives = "Debug",
        variables_derives = "Debug"
    )]
    pub struct ProofsOfIndexing;

    impl TryInto<ProofOfIndexing>
        for WithIndexer<proofs_of_indexing::ProofsOfIndexingPublicProofsOfIndexing>
    {
        type Error = anyhow::Error;

        fn try_into(self) -> Result<ProofOfIndexing, Self::Error> {
            let deployment = IpfsCid::from_str(&self.inner.deployment)
                .map_err(|e| anyhow!("invalid deployment CID: {}", e))?;

            Ok(ProofOfIndexing {
                indexer: self.indexer,
                deployment,
                block: BlockPointer {
                    number: self.inner.block.number.parse()?,
                    hash: self
                        .inner
                        .block
                        .hash
                        .map(|hash_string| str::parse::<BlockHash>(hash_string.as_str()))
                        .transpose()
                        .map_err(|e| anyhow!("invalid block hash: {}", e))?,
                },
                proof_of_indexing: str::parse::<PoiBytes>(self.inner.proof_of_indexing.as_str())
                    .map_err(|e| anyhow!("invalid PoI value: {}", e))?,
            })
        }
    }

    #[derive(GraphQLQuery)]
    #[graphql(
        schema_path = "graphql/indexer/schema.gql",
        query_path = "graphql/indexer/queries/indexer-version.gql",
        response_derives = "Debug",
        variables_derives = "Debug"
    )]
    pub struct IndexerVersion;

    #[derive(GraphQLQuery)]
    #[graphql(
        schema_path = "graphql/indexer/schema.gql",
        query_path = "graphql/indexer/queries/subgraph-api-versions.gql",
        response_derives = "Debug",
        variables_derives = "Debug"
    )]
    pub struct SubgraphApiVersions;

    #[derive(GraphQLQuery)]
    #[graphql(
        schema_path = "graphql/indexer/schema.gql",
        query_path = "graphql/indexer/queries/entity-changes-in-block.gql",
        response_derives = "Debug",
        variables_derives = "Debug"
    )]
    pub struct EntityChangesInBlock;

    #[derive(GraphQLQuery)]
    #[graphql(
        schema_path = "graphql/indexer/schema.gql",
        query_path = "graphql/indexer/queries/cached-eth-calls.gql",
        response_derives = "Debug",
        variables_derives = "Debug"
    )]
    pub struct CachedEthereumCalls;

    #[derive(GraphQLQuery)]
    #[graphql(
        schema_path = "graphql/indexer/schema.gql",
        query_path = "graphql/indexer/queries/block-data.gql",
        response_derives = "Debug",
        variables_derives = "Debug"
    )]
    pub struct BlockData;
}
