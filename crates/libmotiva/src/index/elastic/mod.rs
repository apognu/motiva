pub(crate) mod builder;
pub(crate) mod queries;

use std::collections::HashMap;

use ahash::RandomState;
use elasticsearch::Elasticsearch;
use jiff::civil::DateTime;
use serde::{Deserialize, Serialize};

use crate::{
  model::{Entity, Properties, Schema},
  schemas::SCHEMAS,
};

#[derive(Clone)]
pub struct ElasticsearchProvider {
  pub es: Elasticsearch,
}

#[derive(Deserialize)]
struct EsHealth {
  status: String,
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
pub(crate) struct EsEntity {
  #[serde(rename(deserialize = "_id"))]
  pub id: String,
  pub _source: EsEntitySource,
}

impl From<EsEntity> for Entity {
  fn from(entity: EsEntity) -> Self {
    let caption = entity.caption().to_string();

    Self {
      id: entity.id,
      caption,
      schema: entity._source.schema,
      datasets: entity._source.datasets,
      referents: entity._source.referents,
      target: entity._source.target,
      first_seen: entity._source.first_seen,
      last_seen: entity._source.last_seen,
      last_change: entity._source.last_change,
      properties: Properties {
        strings: entity._source.properties,
        ..Default::default()
      },
      ..Default::default()
    }
  }
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
pub(crate) struct EsEntitySource {
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
