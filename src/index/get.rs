use std::collections::HashSet;

use elasticsearch::SearchParts;
use reqwest::StatusCode;
use serde_json::json;
use tracing::instrument;

use crate::{
  api::{AppState, errors::AppError},
  index::{EsEntity, EsResponse},
  model::Entity,
};

pub enum GetEntityResult {
  Nominal(Box<Entity>),
  Referent(String),
}

#[instrument(skip_all)]
pub async fn get_entity(AppState { es, .. }: &AppState, id: &str) -> Result<GetEntityResult, AppError> {
  let query = json!({
    "query": {
        "bool": {
            "should": [
                { "ids": { "values": [id] } },
                { "term": { "referents": { "value": id } } }
            ],
            "minimum_should_match": 1
        }
    }
  });

  let response = es.search(SearchParts::Index(&["yente-entities"])).from(0).size(1).body(query).send().await?;

  let status = response.status_code();
  let body: EsResponse = response.json().await?;

  if status != StatusCode::OK
    && let Some(error) = body.error
  {
    Err(AppError::OtherError(anyhow::anyhow!(error.reason)))?;
  }

  match body.hits.hits {
    Some(hits) => {
      tracing::trace!(latency = body.took, hits = body.hits.total.value, results = hits.len(), "got response from index");

      if let Some(entity) = hits.into_iter().next() {
        if entity.id != id {
          return Ok(GetEntityResult::Referent(entity.id));
        }

        return Ok(GetEntityResult::Nominal(Box::new(entity.into())));
      }

      Err(AppError::ResourceNotFound)
    }

    None => Err(AppError::OtherError(anyhow::anyhow!("invalid response from elasticsearch"))),
  }
}

#[instrument(skip_all)]
pub async fn get_related_entities(AppState { es, .. }: &AppState, root: Option<&String>, values: &[String], negatives: &HashSet<String>) -> anyhow::Result<Vec<EsEntity>> {
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

  let results = es.search(SearchParts::Index(&["yente-entities"])).from(0).size(10).body(query).send().await?;
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
      .map(|hit| serde_json::from_value::<EsEntity>(hit.clone()).unwrap())
      .collect::<Vec<_>>(),
  )
}
