use super::schema::*;
use crate::types;
use chrono::NaiveDateTime;
use diesel::{
    backend, deserialize::FromSql, pg::Pg, sql_types::Jsonb, AsExpression, FromSqlRow, Insertable,
    Queryable,
};
use serde::{Deserialize, Serialize};
pub type IntId = i32;

// pub type PoIWithId = WithIntId<PoI>;

// #[derive(Debug)]
// pub enum Filter<S = String> {
//     None,
//     Id(IntId),
//     Value(S),
// }

// impl<T> FilterDsl<Filter> for T {
//     type Output = Filter<T>;

//     fn filter(self, predicate: Filter) -> Self::Output {}
// }

#[derive(Queryable, Debug)]
pub struct PoI {
    pub id: IntId,
    pub poi: Vec<u8>,
    pub created_at: NaiveDateTime,
    pub sg_deployment: SgDeployment,
    pub indexer: IndexerRow,
    pub block: Block,
}

impl PoI {
    pub fn poi_hex(&self) -> String {
        hex::encode(&self.poi)
    }
}

#[derive(Insertable, Debug)]
#[diesel(table_name = pois)]
pub struct NewPoI {
    pub poi: Vec<u8>,
    pub created_at: NaiveDateTime,
    pub sg_deployment_id: IntId,
    pub indexer_id: IntId,
    pub block_id: IntId,
}

#[derive(Queryable, Debug)]
pub struct Block {
    pub(super) id: IntId,
    _network_id: IntId,
    pub number: i64,
    pub hash: Vec<u8>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = blocks)]
pub struct NewBlock {
    pub network_id: IntId,
    pub number: i64,
    pub hash: Vec<u8>,
}

#[derive(Debug, Queryable)]
pub struct IndexerRow {
    pub id: IntId,
    pub name: Option<String>,
    pub address: Option<Vec<u8>>,
    pub created_at: NaiveDateTime,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = indexers)]
pub struct NewIndexer {
    pub address: Option<Vec<u8>>,
    pub created_at: NaiveDateTime,
}

#[derive(Debug, Queryable)]
pub struct SgDeployment {
    pub id: IntId,
    pub cid: String,
    pub network_id: IntId,
    pub created_at: NaiveDateTime,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = sg_deployments)]
pub struct NewSgDeployment {
    pub cid: String,
    pub created_at: NaiveDateTime,
}

#[derive(FromSqlRow, AsExpression, Serialize, Deserialize, Debug, Default)]
#[diesel(sql_type = Jsonb)]
pub struct DivergingBlock {
    pub block_number: i64,
    pub block_hash: Option<String>,
    pub proof_of_indexing1: String,
    pub proof_of_indexing2: String,
}

impl From<types::DivergingBlock> for DivergingBlock {
    fn from(block: types::DivergingBlock) -> Self {
        Self {
            block_number: block.block.number as i64,
            block_hash: block.block.hash.map(|hash| hash.to_string()),
            proof_of_indexing1: block.proof_of_indexing1.to_string(),
            proof_of_indexing2: block.proof_of_indexing2.to_string(),
        }
    }
}

impl FromSql<Jsonb, Pg> for DivergingBlock {
    fn from_sql(bytes: backend::RawValue<Pg>) -> diesel::deserialize::Result<Self> {
        let value = <serde_json::Value as FromSql<Jsonb, Pg>>::from_sql(bytes)?;
        Ok(serde_json::from_value(value)?)
    }
}

// impl ToSql<Jsonb, Pg> for DivergingBlock {
//     fn to_sql(&self, out: &mut Output<Pg>) -> diesel::serialize::Result {
//         let value = serde_json::to_value(self)?;
//         <serde_json::Value as ToSql<Jsonb, Pg>>::to_sql(&value, out)
//     }
// }

#[derive(Debug, Insertable, Queryable)]
#[diesel(table_name = poi_divergence_bisect_reports)]
pub struct PoiDivergenceBisectReport {
    pub id: IntId,
    pub poi1_id: IntId,
    pub poi2_id: IntId,
    pub divergence_block_id: IntId,
    pub created_at: NaiveDateTime,
}