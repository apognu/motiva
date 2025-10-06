pub(crate) mod builder;
pub(crate) mod queries;

use std::{collections::HashMap, fmt::Debug};

use ahash::RandomState;
use elasticsearch::Elasticsearch;
use jiff::civil::DateTime;
use serde::{Deserialize, Serialize};

use crate::{
  model::{Entity, Properties, Schema},
  schemas::SCHEMAS,
};

/// Main index provider using Elasticsearch
#[derive(Clone)]
pub struct ElasticsearchProvider {
  pub es: Elasticsearch,
}

#[derive(Deserialize)]
struct EsHealth {
  status: String,
}

#[derive(Deserialize)]
struct EsErrorResponse {
  error: EsError,
}

#[derive(Deserialize)]
struct EsResponse {
  hits: EsResults,
  took: u64,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct EsError {
  #[serde(rename = "type")]
  type_: String,
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

#[cfg(test)]
mod tests {
  use std::collections::HashMap;

  use crate::{
    index::elastic::{EsEntity, EsEntitySource},
    model::{Entity, HasProperties, Schema},
  };

  fn build_entity() -> EsEntity {
    EsEntity {
      id: "id".to_string(),
      _source: EsEntitySource {
        schema: Schema::from("Person"),
        caption: "The Caption".to_string(),
        referents: vec!["ref1".to_string()],
        datasets: vec!["ds1".to_string()],
        target: false,
        last_change: None,
        first_seen: None,
        last_seen: None,
        properties: {
          let mut props = HashMap::default();

          props.insert("name".to_string(), vec!["The Name".to_string()]);
          props
        },
      },
    }
  }

  #[test]
  fn get_caption() {
    let mut entity = build_entity();

    assert_eq!(entity.caption(), "The Caption");

    entity._source.caption = String::new();

    assert_eq!(entity.caption(), "The Name");

    entity._source.properties.remove("name");
    entity._source.properties.insert("email".to_string(), vec!["bob@example.com".to_string()]);

    assert_eq!(entity.caption(), "bob@example.com");

    entity._source.properties.insert("lastName".to_string(), vec!["The Builder".to_string()]);

    assert_eq!(entity.caption(), "The Builder");
  }

  #[test]
  fn es_doc_to_entity() {
    let entity: Entity = build_entity().into();

    assert_eq!(entity.id, "id");
    assert!(entity.property("name").contains(&"The Name".to_string()));
  }
}
