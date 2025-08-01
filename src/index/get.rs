use elasticsearch::SearchParts;
use reqwest::StatusCode;
use serde_json::json;
use tracing::instrument;

use crate::{
  api::{AppState, errors::AppError},
  index::EsEntity,
  model::Entity,
};

pub enum GetEntityResult {
  Nominal(Box<Entity>),
  Referent(String),
}

#[instrument(skip_all)]
pub async fn get(AppState { es, .. }: &AppState, id: &str) -> anyhow::Result<GetEntityResult> {
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

  let results = es.search(SearchParts::Index(&["yente-entities"])).from(0).size(1).body(query).send().await?;
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

  let Some(hits) = body["hits"]["hits"].as_array() else {
    return Err(AppError::OtherError(anyhow::anyhow!("could not understand elasticsearch response")).into());
  };

  if let Some(hit) = hits.iter().next() {
    let Ok(entity) = serde_json::from_value::<EsEntity>(hit.clone()) else {
      return Err(AppError::OtherError(anyhow::anyhow!("could not decode entity")).into());
    };

    if entity.id != id {
      return Ok(GetEntityResult::Referent(entity.id));
    }

    return Ok(GetEntityResult::Nominal(Box::new(entity.into())));
  }

  Err(AppError::ResourceNotFound.into())
}
