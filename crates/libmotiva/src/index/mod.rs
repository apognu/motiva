use std::{
  collections::{HashMap, HashSet},
  sync::Arc,
};

use ahash::RandomState;
use elasticsearch::http::response::Response;
use jiff::civil::DateTime;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
  catalog::Collections,
  error::MotivaError,
  index::elastic::GetEntityResult,
  matching::MatchParams,
  model::{Entity, Schema, SearchEntity},
  schemas::SCHEMAS,
};

pub mod elastic;

#[allow(async_fn_in_trait)]
pub trait IndexProvider: Clone + Send + Sync {
  async fn health(&self) -> Result<Response, elasticsearch::Error>;
  async fn get_entity(&self, id: &str) -> Result<GetEntityResult, MotivaError>;
  async fn get_related_entities(&self, root: Option<&String>, values: &[String], negatives: &HashSet<String, RandomState>) -> anyhow::Result<Vec<EsEntity>>;

  fn search(&self, catalog: &Arc<RwLock<Collections>>, entity: &SearchEntity, params: &MatchParams) -> impl Future<Output = Result<Vec<Entity>, MotivaError>> + Send;
}

#[derive(Deserialize)]
struct EsResponse {
  error: Option<EsError>,
  hits: EsResults,
  took: u64,
}

#[derive(Deserialize)]
struct EsError {
  reason: String,
}

#[derive(Deserialize)]
struct EsResults {
  hits: Option<Vec<EsEntity>>,
  total: EsCounts,
}

#[derive(Deserialize)]
struct EsCounts {
  value: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EsEntity {
  #[serde(rename(deserialize = "_id"))]
  pub id: String,
  pub _source: EsEntitySource,
}

impl EsEntity {
  pub fn caption(&self) -> &str {
    if !self._source.caption.is_empty() {
      return &self._source.caption;
    }

    match SCHEMAS.get(self._source.schema.as_str()) {
      Some(schema) => {
        for prop in &schema.caption {
          if let Some(values) = self._source.properties.get(prop)
            && let Some(first) = values.first()
          {
            // TODO: heuristic to pick the "best" name for Things.
            return first;
          }
        }

        &self._source.caption
      }

      None => &self._source.caption,
    }
  }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EsEntitySource {
  pub caption: String,
  pub schema: Schema,
  pub datasets: Vec<String>,
  pub referents: Vec<String>,
  #[serde(default)]
  pub target: bool,
  pub first_seen: Option<DateTime>,
  pub last_seen: Option<DateTime>,
  pub last_change: Option<DateTime>,
  pub properties: HashMap<String, Vec<String>, RandomState>,
}
