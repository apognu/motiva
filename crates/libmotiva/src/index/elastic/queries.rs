use std::{
  collections::{HashMap, HashSet},
  sync::Arc,
};

use ahash::RandomState;
use anyhow::Context;
use elasticsearch::{SearchParts, cluster::ClusterHealthParts, indices::IndicesGetAliasParts, params::SearchType};
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
  Catalog,
  error::MotivaError,
  index::{
    EntityHandle, IndexProvider,
    elastic::{EsEntity, EsErrorResponse, EsHealth, EsResponse, version::IndexVersion},
  },
  matching::{MatchParams, extractors},
  model::{Entity, SearchEntity},
  prelude::ElasticsearchProvider,
  schemas::SCHEMAS,
};

impl IndexProvider for ElasticsearchProvider {
  fn index_version(&self) -> IndexVersion {
    self.index_version
  }

  /// Whether the Elasticsearch cluster is up and healthy.
  ///
  /// The cluster will only be considered healthy if the index is `green` or `yellow`.
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

  /// Search for candidate entities matching input parameters.
  #[instrument(skip_all)]
  async fn search(&self, catalog: &Arc<RwLock<Catalog>>, entity: &SearchEntity, params: &MatchParams) -> Result<Vec<Entity>, MotivaError> {
    let query = build_query(catalog, self.index_version, entity, params).await?;

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

  /// Get an entity from its ID.
  ///
  /// This will only return the requested entity, without recursing to nested
  /// entities. This has the effect that most links will only be represented
  /// with their IDs, and not their actual data.
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

  /// Get entities related to an entity.
  #[instrument(skip_all)]
  async fn get_related_entities(&self, root: Option<&String>, values: &[String], negatives: &HashSet<String, RandomState>) -> Result<Vec<Entity>, MotivaError> {
    const RELATED_ENTITIES_LIMIT: i64 = 100;

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

    let response = self.es.search(SearchParts::Index(&["yente-entities"])).from(0).size(RELATED_ENTITIES_LIMIT).body(query).send().await?;

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

  async fn list_indices(&self) -> Result<Vec<(String, String)>, MotivaError> {
    let indices: HashMap<String, serde_json::Value> = self.es.indices().get_alias(IndicesGetAliasParts::Name(&["yente-entities"])).send().await?.json().await?;

    Ok(parse_index_dataset_versions(indices))
  }
}

fn parse_index_dataset_versions(indices: HashMap<String, serde_json::Value>) -> Vec<(String, String)> {
  indices
    .keys()
    .cloned()
    .filter_map(|name| {
      if let Some(stripped) = name.strip_prefix("yente-entities-") {
        let mut stripped = stripped.split("-");

        match (stripped.next(), stripped.skip(1).join("-")) {
          (Some(name), version) if !version.is_empty() => Some((name.to_string(), version)),
          _ => None,
        }
      } else {
        None
      }
    })
    .collect::<Vec<_>>()
}

async fn build_query(catalog: &Arc<RwLock<Catalog>>, index_version: IndexVersion, entity: &SearchEntity, params: &MatchParams) -> Result<serde_json::Value, MotivaError> {
  Ok(json!({
      "query": {
          "bool": {
              "filter": build_filters(catalog, entity, params).await?,
              "should": build_shoulds(index_version, entity)?,
              "must_not": build_must_nots(params),
              "minimum_should_match": 1,
          }
      }
  }))
}

async fn build_filters(catalog: &Arc<RwLock<Catalog>>, entity: &SearchEntity, params: &MatchParams) -> Result<Vec<serde_json::Value>, MotivaError> {
  let mut filters = Vec::<serde_json::Value>::new();

  build_schemas(entity, &mut filters)?;
  build_datasets(catalog, &mut filters, params).await;
  build_topics(params, &mut filters);

  if let Some(since) = params.changed_since {
    filters.push(json!({"range": { "last_change": { "gt": since } } }));
  }

  Ok(filters)
}

fn build_must_nots(params: &MatchParams) -> Vec<serde_json::Value> {
  let mut filters = Vec::<serde_json::Value>::new();

  if !params.exclude_schema.is_empty() {
    filters.push(json!({ "terms": { "schema": params.exclude_schema } }));
  }

  if !params.exclude_entity_ids.is_empty() {
    filters.push(json!({ "terms": { "entity_id": params.exclude_entity_ids } }));
    filters.push(json!({ "terms": { "referents": params.exclude_entity_ids } }));
  }

  filters
}

fn build_schemas(entity: &SearchEntity, filters: &mut Vec<serde_json::Value>) -> Result<(), MotivaError> {
  let schema = SCHEMAS.get(entity.schema.as_str()).ok_or(MotivaError::InvalidSchema(entity.schema.as_str().to_string()))?;
  let mut schemas = resolve_schemas(entity.schema.as_str(), ResolveSchemaLevel::Root)?;
  schemas.extend(schema.descendants.clone());

  filters.push(json!({ "terms": { "schema": schemas } }));

  Ok(())
}

async fn build_datasets(catalog: &Arc<RwLock<Catalog>>, filters: &mut Vec<serde_json::Value>, params: &MatchParams) {
  let scope = {
    let guard = catalog.read().await;

    guard
      .loaded_datasets
      .get(&params.scope)
      .map(|dataset| match dataset._type.as_deref() {
        Some("collection") => dataset.children.clone(),
        _ => vec![dataset.name.clone()],
      })
      .unwrap_or_default()
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

fn build_shoulds(index_version: IndexVersion, entity: &SearchEntity) -> anyhow::Result<Vec<serde_json::Value>> {
  let mut should = Vec::<serde_json::Value>::new();

  if let Some(names) = entity.properties.get("name") {
    let names = names.iter().map(|s| s.nfc().collect::<String>()).collect::<Vec<_>>();

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

    if index_version == IndexVersion::V4 {
      for name in extractors::index_name_keys(names.iter()) {
        add_term(&mut should, "name_keys", &name, 4.0);
      }
    }

    if index_version == IndexVersion::V5 {
      // TODO: add name_symbols filters
    }

    for name in extractors::index_name_parts(names.iter()) {
      add_term(&mut should, "name_parts", &name, 1.0);
    }
    for name in extractors::phonetic_name(&Metaphone::new(None), names.iter()) {
      add_term(&mut should, "name_phonetic", &name, 0.8);
    }
  }

  let schema = SCHEMAS.get(entity.schema.as_str()).ok_or(anyhow::anyhow!("unknown schema"))?;
  let properties = schema.properties(&SCHEMAS);

  for (property, values) in &entity.properties {
    let Some(prop) = properties.get(property) else {
      continue;
    };

    if property == "name" || !prop.matchable {
      continue;
    }

    let lhs = match prop._type.as_str() {
      "address" => "addresses",
      "date" => "dates",
      "country" => "countries",
      "identifier" => "identifiers",
      "phone" => "phones",
      "email" => "emails",
      "language" => "languages",
      "gender" => "genders",
      "iban" => "ibans",
      "ip" => "ips",
      "url" => "urls",
      _ => continue,
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

#[derive(Eq, PartialEq)]
enum ResolveSchemaLevel {
  Root,
  Deep,
}

fn resolve_schemas(schema: &str, level: ResolveSchemaLevel) -> Result<Vec<String>, MotivaError> {
  let mut out = Vec::with_capacity(8);
  let root = level == ResolveSchemaLevel::Root;

  if let Some(def) = SCHEMAS.get(schema) {
    if root && schema != "Thing" && !def.matchable {
      return Ok(vec![]);
    }

    if root || def.matchable {
      out.push(schema.to_string());
    }

    for parent in &def.extends {
      out.extend(resolve_schemas(parent, ResolveSchemaLevel::Deep)?);
    }
  }

  Ok(out)
}

#[cfg(test)]
mod tests {
  use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
  };

  use serde_json::json;
  use serde_json_assert::{assert_json_contains, assert_json_eq, assert_json_include};
  use tokio::sync::RwLock;

  use crate::{
    Catalog,
    catalog::CatalogDataset,
    index::elastic::{
      queries::{ResolveSchemaLevel, resolve_schemas},
      version::IndexVersion,
    },
    model::SearchEntity,
    prelude::MatchParams,
  };

  fn fake_catalog() -> Arc<RwLock<Catalog>> {
    Arc::new(RwLock::new({
      let mut catalog = Catalog::default();

      catalog.loaded_datasets.insert(
        "myscope".to_string(),
        CatalogDataset {
          name: "Real Dataset".to_string(),
          children: vec!["realdataset".to_string()],
          _type: Some("collection".to_string()),
          ..Default::default()
        },
      );

      catalog.loaded_datasets.insert(
        "otherscope".to_string(),
        CatalogDataset {
          name: "Other Dataset".to_string(),
          children: vec!["otherdataset".to_string()],
          _type: Some("collection".to_string()),
          ..Default::default()
        },
      );

      catalog.loaded_datasets.insert(
        "baredataset".to_string(),
        CatalogDataset {
          name: "baredataset".to_string(),
          ..Default::default()
        },
      );

      catalog
    }))
  }

  #[test]
  fn build_schemas() {
    let entity = SearchEntity::builder("Person").properties(&[]).build();
    let mut schemas = Vec::new();

    super::build_schemas(&entity, &mut schemas).unwrap();

    assert_eq!(schemas.len(), 1);
    assert_json_eq!(schemas[0], json!({ "terms": { "schema": ["Person", "LegalEntity"] } }));
  }

  #[test]
  fn build_must_nots() {
    let params = MatchParams {
      exclude_schema: vec!["Person".into(), "Company".into()],
      exclude_entity_ids: vec!["A1".into(), "A2".into()],
      ..Default::default()
    };

    let must_nots = super::build_must_nots(&params);

    assert_json_eq!(
      must_nots,
      json!([
          { "terms": { "schema": ["Person", "Company"] } },
          { "terms": { "entity_id": ["A1", "A2"] } },
          { "terms": { "referents": ["A1", "A2"] } }
      ])
    );

    let must_nots = super::build_must_nots(&MatchParams::default());

    assert_json_eq!(must_nots, json!([]));
  }

  #[tokio::test]
  async fn build_query() {
    let entity = SearchEntity::builder("Person")
      .properties(&[
        ("name", &["Vladimir Putin"]),
        ("birthDate", &["01-01-1010"]),
        ("nationality", &["ru"]),
        ("registrationNumber", &["1234"]),
      ])
      .build();

    super::build_query(&fake_catalog(), IndexVersion::V4, &entity, &MatchParams::default()).await.unwrap();
  }

  #[test]
  fn build_should() {
    let entity = SearchEntity::builder("Person")
      .properties(&[
        ("name", &["Vladimir Putin"]),
        ("birthDate", &["01-01-1010"]),
        ("nationality", &["ru"]),
        ("registrationNumber", &["1234"]),
      ])
      .build();

    let shoulds = super::build_shoulds(IndexVersion::V4, &entity).unwrap();

    assert_json_contains!(
        container: shoulds,
        contained: json!([{ "match": { "names": { "boost": 3.0, "fuzziness": "AUTO", "operator": "AND", "query": "Vladimir Putin" } } }]),
    );

    assert_json_contains!(
        container: shoulds,
        contained: json!([{ "term": { "name_keys": { "boost": 4.0, "value": "putinvladimir", } } }]),
    );

    assert_json_contains!(
        container: shoulds,
        contained: json!([ { "term": { "name_parts": { "boost": 1.0, "value": "vladimir" } } }])
    );

    assert_json_contains!(
      container: shoulds,
      contained: json!([{ "term": { "name_parts": { "boost": 1.0, "value": "putin" } } }]),
    );

    assert_json_contains!(
      container: shoulds,
      contained: json!([{ "term": { "name_phonetic": { "boost": 0.8, "value": "FLTMR" } } }]),
    );

    assert_json_contains!(
        container: shoulds,
        contained: json!([{ "term": { "name_phonetic": { "boost": 0.8, "value": "PTN" } } }]),
    );

    assert_json_contains!(container: shoulds, contained: json!([{ "match": { "dates": "01-01-1010" } }]));
    assert_json_contains!(container: shoulds, contained: json!([{ "match": { "countries": "ru" } }]));
  }

  #[tokio::test]
  async fn build_datasets() {
    let catalog = fake_catalog();

    let params = MatchParams {
      scope: "myscope".to_string(),
      include_dataset: vec!["fakedataset".to_string(), "realdataset".to_string(), "otherdataset".to_string()],
      ..Default::default()
    };

    let mut datasets = Vec::new();

    super::build_datasets(&catalog, &mut datasets, &params).await;

    assert_eq!(datasets.len(), 1);
    assert_json_eq!(datasets[0], json!({ "terms": { "datasets": ["realdataset"] } }));
  }

  #[tokio::test]
  async fn build_datasets_bare() {
    let catalog = fake_catalog();

    let params = MatchParams {
      scope: "baredataset".to_string(),
      include_dataset: vec!["baredataset".to_string()],
      ..Default::default()
    };

    let mut datasets = Vec::new();

    super::build_datasets(&catalog, &mut datasets, &params).await;

    assert_eq!(datasets.len(), 1);
    assert_json_eq!(datasets[0], json!({ "terms": { "datasets": ["baredataset"] } }));
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

  #[tokio::test]
  async fn build_filters() {
    let catalog = fake_catalog();
    let entity = SearchEntity::builder("Person").properties(&[]).build();

    let params = MatchParams {
      changed_since: Some(jiff::Timestamp::UNIX_EPOCH),
      ..Default::default()
    };

    let filters = super::build_filters(&catalog, &entity, &params).await.unwrap();

    assert_json_include!(actual: filters, expected: json!([{}, {}, { "range": { "last_change": { "gt": "1970-01-01T00:00:00Z" } } }]));
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
    assert_eq!(resolve_schemas("Person", ResolveSchemaLevel::Root).unwrap(), &["Person", "LegalEntity"]);
    assert_eq!(resolve_schemas("Company", ResolveSchemaLevel::Root).unwrap(), &["Company", "Organization", "LegalEntity"]);
    assert_eq!(resolve_schemas("Airplane", ResolveSchemaLevel::Root).unwrap(), &["Airplane"]);
    assert!(resolve_schemas("Vehicle", ResolveSchemaLevel::Root).unwrap().is_empty());
    assert_eq!(resolve_schemas("Thing", ResolveSchemaLevel::Root).unwrap(), &["Thing"]);
  }

  #[test]
  fn test_parse_versions() {
    let input = [("dataset1", "20250901000000-abc"), ("dataset2", "20251127104000-xyz")]
      .into_iter()
      .map(|(n, v)| (format!("yente-entities-{n}-any-{v}"), json!({})))
      .collect::<HashMap<String, _>>();

    let versions = super::parse_index_dataset_versions(input);

    assert_eq!(versions.len(), 2);
    assert_eq!(
      HashSet::<(String, String)>::from_iter(versions.into_iter()),
      HashSet::from_iter(vec![("dataset1".to_string(), "20250901000000-abc".to_string()), ("dataset2".to_string(), "20251127104000-xyz".to_string())].into_iter())
    );
  }
}
