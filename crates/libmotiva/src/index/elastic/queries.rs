use std::{collections::HashSet, sync::Arc};

use ahash::RandomState;
use anyhow::Context;
use elasticsearch::{SearchParts, cluster::ClusterHealthParts, params::SearchType};
use itertools::Itertools;
use metrics::{counter, histogram};
use opentelemetry::global;
use reqwest::StatusCode;
use rphonetic::Metaphone;
use serde_json::json;
use tokio::sync::RwLock;
use tracing::instrument;
use unicode_normalization::UnicodeNormalization;

use crate::{
  catalog::Collections,
  error::MotivaError,
  index::{
    EntityHandle, IndexProvider,
    elastic::{EsEntity, EsErrorResponse, EsHealth, EsResponse},
  },
  matching::{MatchParams, extractors},
  model::{Entity, SearchEntity},
  prelude::ElasticsearchProvider,
  schemas::SCHEMAS,
};

impl IndexProvider for ElasticsearchProvider {
  #[instrument(skip_all)]
  async fn health(&self) -> Result<bool, MotivaError> {
    let Ok(health) = self
      .es
      .cluster()
      .health(ClusterHealthParts::Index(&["yente-entities"]))
      .send()
      .await
      .context("could not get cluster health")
    else {
      return Ok(false);
    };

    let Ok(health): Result<EsHealth, _> = health.json().await.context("could not deserialize cluster health") else {
      return Ok(false);
    };

    match health.status.as_str() {
      "green" | "yellow" => Ok(true),
      _ => Ok(false),
    }
  }

  #[instrument(skip_all)]
  async fn search(&self, catalog: &Arc<RwLock<Collections>>, entity: &SearchEntity, params: &MatchParams) -> Result<Vec<Entity>, MotivaError> {
    let query = build_query(catalog, entity, params).await?;

    tracing::trace!(%query, "running query");

    let response = self
      .es
      .search(SearchParts::Index(&["yente-entities"]))
      .from(0)
      .size(params.candidate_limit() as i64)
      .search_type(SearchType::DfsQueryThenFetch)
      .body(query)
      .send()
      .await?;

    if response.status_code() != StatusCode::OK {
      let body: EsErrorResponse = response.json().await?;

      return Err(MotivaError::OtherError(anyhow::anyhow!(body.error.reason)));
    }

    let body: EsResponse = response.json().await?;

    match body.hits.hits {
      Some(hits) => {
        tracing::debug!(latency = body.took, hits = body.hits.total.value, results = hits.len(), "got hits from index");

        counter!("motiva_indexer_matches_total").increment(hits.len() as u64);
        histogram!("motiva_indexer_latency_seconds").record(body.took as f64 / 1000.0);

        global::meter("motiva").u64_histogram("index_hits").build().record(hits.len() as u64, &[]);
        global::meter("motiva").u64_histogram("index_latency").build().record(body.took, &[]);

        Ok(hits.into_iter().map(Entity::from).collect())
      }

      None => Err(MotivaError::OtherError(anyhow::anyhow!("invalid response from elasticsearch"))),
    }
  }

  #[instrument(skip_all)]
  async fn get_entity(&self, id: &str) -> Result<EntityHandle, MotivaError> {
    let query = json!({
      "query": {
          "bool": {
              "should": [
                  { "ids": { "values": [id] } },
                  { "term": { "referents": { "value": id } } }
              ],
              "minimum_should_match": 1
          }
      },
      "sort": [
          { "_score": { "order": "desc" } },
          { "entity_id": { "order": "asc", "unmapped_type": "keyword" } }
      ]
    });

    let response = self.es.search(SearchParts::Index(&["yente-entities"])).from(0).size(1).body(query).send().await?;

    if response.status_code() != StatusCode::OK {
      let body: EsErrorResponse = response.json().await?;

      return Err(MotivaError::OtherError(anyhow::anyhow!(body.error.reason)));
    }

    let body: EsResponse = response.json().await?;

    match body.hits.hits {
      Some(hits) => {
        tracing::trace!(latency = body.took, hits = body.hits.total.value, results = hits.len(), "got response from index");

        if let Some(entity) = hits.into_iter().next() {
          if entity.id != id {
            return Ok(EntityHandle::Referent(entity.id));
          }

          return Ok(EntityHandle::Nominal(Box::new(entity.into())));
        }

        Err(MotivaError::ResourceNotFound)
      }

      None => Err(MotivaError::OtherError(anyhow::anyhow!("invalid response from elasticsearch"))),
    }
  }

  #[instrument(skip_all)]
  async fn get_related_entities(&self, root: Option<&String>, values: &[String], negatives: &HashSet<String, RandomState>) -> Result<Vec<Entity>, MotivaError> {
    let mut shoulds = vec![json!({ "ids": { "values": values } })];

    if let Some(root) = root {
      shoulds.push(json!(
          { "terms": { "entities": [root] } }
      ))
    }

    let query = json!({
      "query": {
          "bool": {
              "should": shoulds,
              "must_not": { "ids": { "values": negatives } },
              "minimum_should_match": 1
          },
      }
    });

    let response = self.es.search(SearchParts::Index(&["yente-entities"])).from(0).size(10).body(query).send().await?;

    if response.status_code() != StatusCode::OK {
      let body: EsErrorResponse = response.json().await?;

      return Err(MotivaError::OtherError(anyhow::anyhow!(body.error.reason)));
    }

    let body = response.json::<serde_json::Value>().await?;

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
        .map(|hit| serde_json::from_value::<EsEntity>(hit.clone()).map(Entity::from))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| anyhow::anyhow!(err))?,
    )
  }
}

async fn build_query(catalog: &Arc<RwLock<Collections>>, entity: &SearchEntity, params: &MatchParams) -> Result<serde_json::Value, MotivaError> {
  Ok(json!({
      "query": {
          "bool": {
              "filter": build_filters(catalog, entity, params).await?,
              "should": build_shoulds(entity)?,
              "minimum_should_match": 1,
          }
      }
  }))
}

async fn build_filters(catalog: &Arc<RwLock<Collections>>, entity: &SearchEntity, params: &MatchParams) -> Result<Vec<serde_json::Value>, MotivaError> {
  let mut filters = Vec::<serde_json::Value>::new();

  build_schemas(entity, &mut filters)?;
  build_datasets(catalog, &mut filters, params).await;
  build_topics(params, &mut filters);

  Ok(filters)
}

fn build_schemas(entity: &SearchEntity, filters: &mut Vec<serde_json::Value>) -> Result<(), MotivaError> {
  let schema = SCHEMAS.get(entity.schema.as_str()).ok_or(MotivaError::InvalidSchema(entity.schema.as_str().to_string()))?;
  let mut schemas = resolve_schemas(entity.schema.as_str(), true)?;
  schemas.extend(schema.descendants.clone());

  filters.push(json!({ "terms": { "schema": schemas } }));

  Ok(())
}

async fn build_datasets(catalog: &Arc<RwLock<Collections>>, filters: &mut Vec<serde_json::Value>, params: &MatchParams) {
  let scope = {
    let guard = catalog.read().await;

    guard.get(&params.scope).and_then(|dataset| dataset.children.clone()).unwrap_or_default()
  };

  if !params.include_dataset.is_empty() {
    let datasets: Vec<_> = params
      .include_dataset
      .iter()
      .filter(|dataset| scope.contains(*dataset) && !params.exclude_dataset.iter().contains(*dataset))
      .collect();

    filters.push(json!({ "terms": { "datasets": datasets } }));
  } else {
    filters.push(json!({ "terms": { "datasets": scope } }));
  }
}

fn build_topics(params: &MatchParams, filters: &mut Vec<serde_json::Value>) {
  if let Some(topics) = &params.topics
    && !topics.is_empty()
  {
    filters.push(json!({ "terms": { "topics": topics } }));
  }
}

fn build_shoulds(entity: &SearchEntity) -> anyhow::Result<Vec<serde_json::Value>> {
  let names = entity.properties["name"].iter().map(|s| s.nfc().collect::<String>()).collect::<Vec<_>>();
  let mut should = Vec::<serde_json::Value>::new();

  for name in &names {
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

  let schema = SCHEMAS.get(entity.schema.as_str()).ok_or(anyhow::anyhow!("unknown schema"))?;

  for (property, values) in &entity.properties {
    if property == "name" || !schema.properties.get(property).is_some_and(|p| p.matchable) {
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

fn resolve_schemas(schema: &str, root: bool) -> Result<Vec<String>, MotivaError> {
  let mut out = Vec::with_capacity(8);

  if let Some(def) = SCHEMAS.get(schema) {
    if root && schema != "Thing" && !def.matchable {
      return Err(MotivaError::OtherError(anyhow::anyhow!("requested schema is not matchable")));
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
  use std::sync::Arc;

  use serde_json::json;
  use serde_json_assert::assert_json_eq;
  use tokio::sync::RwLock;

  use crate::{
    catalog::{Collections, Dataset},
    index::elastic::queries::resolve_schemas,
    model::SearchEntity,
    prelude::MatchParams,
  };

  #[test]
  fn build_schemas() {
    let entity = SearchEntity::builder("Person").properties(&[]).build();
    let mut schemas = Vec::new();

    super::build_schemas(&entity, &mut schemas).unwrap();

    assert_eq!(schemas.len(), 1);
    assert_json_eq!(schemas[0], json!({ "terms": { "schema": ["Person", "LegalEntity"] } }));
  }

  #[test]
  fn build_should() {
    let entity = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();
    let shoulds = super::build_shoulds(&entity).unwrap();

    assert_json_eq!(
      shoulds,
      json!([
          {
              "match":  {
                  "names":  {
                      "boost": 3.0,
                      "fuzziness": "AUTO",
                      "operator": "AND",
                      "query": "Vladimir Putin",
                  },
              },
          },
          {
              "term":  {
                  "name_keys":  {
                      "boost": 4.0,
                      "value": "putinvladimir",
                  },
              },
          },
          {
              "term":  {
                  "name_parts":  {
                      "boost": 1.0,
                      "value": "vladimir",
                  },
              },
          },
          {
              "term": {
                  "name_parts":  {
                      "boost": 1.0,
                      "value": "putin",
                  },
              },
          },
          {
              "term": {
                  "name_phonetic":  {
                      "boost": 0.8,
                      "value": "FLTMR",
                  },
              },
          },
          {
              "term": {
                  "name_phonetic":  {
                      "boost": 0.8,
                      "value": "PTN",
                  },
              },
          },
      ])
    );
  }

  #[tokio::test]
  async fn build_datasets() {
    let catalog = Arc::new(RwLock::new({
      let mut catalog = Collections::default();

      catalog.insert(
        "myscope".to_string(),
        Dataset {
          name: "Real Dataset".to_string(),
          children: Some(vec!["realdataset".to_string()]),
        },
      );

      catalog.insert(
        "otherscope".to_string(),
        Dataset {
          name: "Other Dataset".to_string(),
          children: Some(vec!["otherdataset".to_string()]),
        },
      );

      catalog
    }));

    let params = MatchParams {
      scope: "myscope".to_string(),
      include_dataset: vec!["fakedataset".to_string(), "realdataset".to_string()],
      ..Default::default()
    };

    let mut datasets = Vec::new();

    super::build_datasets(&catalog, &mut datasets, &params).await;

    assert_eq!(datasets.len(), 1);
    assert_json_eq!(datasets[0], json!({ "terms": { "datasets": ["realdataset"] } }));
  }

  #[test]
  fn build_topics() {
    let mut filters = Vec::new();
    let params = MatchParams {
      topics: Some(vec!["topic1".to_string(), "topic2".to_string()]),
      ..Default::default()
    };

    super::build_topics(&params, &mut filters);

    assert_eq!(filters.len(), 1);
    assert_json_eq!(filters[0], json!({ "terms": { "topics": ["topic1", "topic2"] } }));
  }

  #[test]
  fn add_term() {
    let mut terms = Vec::new();

    super::add_term(&mut terms, "a", "b", 3.0);
    super::add_term(&mut terms, "c", "d", -10.0);

    assert_eq!(terms.len(), 2);
    assert_json_eq!(terms[0], json!({ "term": { "a": { "value": "b", "boost": 3.0 } } }));
    assert_json_eq!(terms[1], json!({ "term": { "c": { "value": "d", "boost": -10.0 } } }));
  }

  #[test]
  fn resolve_schema_chain() {
    assert_eq!(resolve_schemas("Person", true).unwrap(), &["Person", "LegalEntity"]);
    assert_eq!(resolve_schemas("Company", true).unwrap(), &["Company", "Organization", "LegalEntity"]);
    assert_eq!(resolve_schemas("Airplane", true).unwrap(), &["Airplane"]);
    assert!(resolve_schemas("Vehicle", true).is_err());
    assert_eq!(resolve_schemas("Thing", true).unwrap(), &["Thing"]);
  }
}
