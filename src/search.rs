use std::{collections::HashMap, sync::Arc};

use axum::http::StatusCode;
use elasticsearch::{Elasticsearch, SearchParts};
use jiff::civil::DateTime;
use rphonetic::Metaphone;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::RwLock;
use tracing::instrument;
use unicode_normalization::UnicodeNormalization;

use crate::{
  api::{dto::MatchParams, errors::AppError},
  catalog::Collections,
  matching::extractors,
  model::{Entity, Schema, SearchEntity},
  schemas::SCHEMAS,
};

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

    match SCHEMAS.get(&self._source.schema.0) {
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
  pub target: bool,
  pub first_seen: DateTime,
  pub last_seen: DateTime,
  pub last_change: DateTime,
  pub properties: HashMap<String, Vec<String>>,
}

#[instrument(level = "trace", skip_all)]
pub async fn search(catalog: Arc<RwLock<Collections>>, es: &Elasticsearch, entity: &SearchEntity, params: &MatchParams) -> Result<Vec<Entity>, AppError> {
  let query = build_query(catalog, entity, params).await?;

  let results = es
    .search(SearchParts::Index(&["yente-entities"]))
    .from(0)
    .size(params.limit.unwrap_or(5) as i64)
    .body(query)
    .send()
    .await?;

  let status = results.status_code();
  let body = results.json::<serde_json::Value>().await?;

  if status != StatusCode::OK {
    let err = body["error"]["reason"].as_str().unwrap().to_string();

    Err(AppError::OtherError(anyhow::anyhow!(err)))?;
  }

  tracing::trace!(
    latency = body["took"].as_u64(),
    hits = body["hits"]["total"]["value"].as_u64(),
    results = body["hits"]["hits"].as_array().iter().count(),
    "got response from index"
  );

  Ok(
    body["hits"]["hits"]
      .as_array()
      .ok_or(anyhow::anyhow!("invalid response"))?
      .iter()
      .map(|hit| serde_json::from_value::<EsEntity>(hit.clone()).unwrap().into())
      .collect::<Vec<_>>(),
  )
}

async fn build_query(catalog: Arc<RwLock<Collections>>, entity: &SearchEntity, params: &MatchParams) -> Result<serde_json::Value, AppError> {
  Ok(json!({
      "query": {
          "bool": {
              "filter": build_filters(catalog, entity, params).await?,
              "should": build_shoulds(entity)?,
          }
      }
  }))
}

async fn build_filters(catalog: Arc<RwLock<Collections>>, entity: &SearchEntity, params: &MatchParams) -> Result<Vec<serde_json::Value>, AppError> {
  let mut filters = Vec::<serde_json::Value>::new();

  build_schemas(entity, &mut filters)?;
  build_datasets(catalog, &mut filters, params).await;
  build_topics(params, &mut filters);

  Ok(filters)
}

fn build_schemas(entity: &SearchEntity, filters: &mut Vec<serde_json::Value>) -> Result<(), AppError> {
  let schema = SCHEMAS.get(&entity.schema.0).ok_or(AppError::BadRequest)?;
  let mut schemas = resolve_schemas(&entity.schema.0, true)?;
  schemas.extend(schema.descendants.clone());

  filters.push(json!({ "terms": { "schema": schemas } }));

  Ok(())
}

async fn build_datasets(catalog: Arc<RwLock<Collections>>, filters: &mut Vec<serde_json::Value>, params: &MatchParams) {
  let scope = {
    let guard = catalog.read().await;
    guard.get("default").and_then(|dataset| dataset.datasets.clone()).unwrap_or_default()
  };

  if let Some(datasets) = &params.include_dataset
    && !datasets.is_empty()
  {
    let datasets: Vec<_> = datasets.iter().filter(|dataset| scope.contains(*dataset)).collect();

    filters.push(json!({ "terms": { "datasets": datasets } }));
  } else {
    filters.push(json!({ "terms": { "datasets": scope } }));
  }
}

fn build_topics(params: &MatchParams, filters: &mut Vec<serde_json::Value>) {
  if let Some(topics) = &params.topics
    && !topics.is_empty()
  {
    filters.push(json!({ "terms": { "topics": topics.split(',').collect::<Vec<_>>() } }));
  }
}

fn build_shoulds(entity: &SearchEntity) -> anyhow::Result<Vec<serde_json::Value>> {
  let names = entity.properties["name"].iter().map(|s| s.nfc().collect::<String>()).collect::<Vec<_>>();
  let mut should = Vec::<serde_json::Value>::new();

  for name in names.iter() {
    should.push(json!({
        "match": {
            "names": {
                "query": name,
                "operator": "AND",
                "boost": 3.0,
                "fuzziness": "AUTO",
            }
        }
    }));
  }

  for name in extractors::name_keys(names.iter()) {
    add_term(&mut should, "name_keys", &name, 4.0);
  }
  for name in extractors::name_parts_flat(names.iter()) {
    add_term(&mut should, "name_parts", &name, 1.0);
  }
  for name in extractors::phonetic_name(&Metaphone::new(None), names.iter()) {
    add_term(&mut should, "name_phonetic", &name, 0.8);
  }

  let schema = SCHEMAS.get(&entity.schema.0).ok_or(anyhow::anyhow!("unknown schema"))?;

  for (property, values) in &entity.properties {
    if property == "name" || !schema.properties.get(property).map(|p| p.matchable).unwrap_or(false) {
      continue;
    }

    let lhs = match property.as_str() {
      "address" => "addresses",
      "birthDate" => "dates",
      "country" => "countries",
      "registrationNumber" => "identifiers",
      _ => "text",
    };

    for value in values {
      should.push(json!({
          "match": { lhs: value }
      }));
    }
  }

  Ok(should)
}

fn add_term(queries: &mut Vec<serde_json::Value>, key: &str, name: &str, boost: f64) {
  queries.push(json!({
      "term": {
          key: {
              "value": name,
              "boost": boost,
          }
      }
  }));
}

fn resolve_schemas(schema: &str, root: bool) -> Result<Vec<String>, AppError> {
  let mut out = Vec::new();

  if let Some(def) = SCHEMAS.get(schema) {
    if root && schema != "Thing" && !def.matchable {
      return Err(AppError::OtherError(anyhow::anyhow!("requested schema is not matchable")));
    }

    if root || def.matchable {
      out.push(schema.to_string());
    }

    for parent in &def.extends {
      out.extend(resolve_schemas(parent, false)?);
    }
  }

  Ok(out)
}

#[cfg(test)]
mod tests {
  use crate::search::resolve_schemas;

  #[test]
  fn resolve_schema_chain() {
    assert_eq!(resolve_schemas("Person", true).unwrap(), &["Person", "LegalEntity"]);
    assert_eq!(resolve_schemas("Company", true).unwrap(), &["Company", "Organization", "LegalEntity"]);
    assert_eq!(resolve_schemas("Airplane", true).unwrap(), &["Airplane"]);
    assert!(resolve_schemas("Vehicle", true).is_err());
    assert_eq!(resolve_schemas("Thing", true).unwrap(), &["Thing"]);
  }
}
