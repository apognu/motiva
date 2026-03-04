use std::collections::HashMap;

use anyhow::Context;
use elasticsearch::indices::{IndicesCreateParts, IndicesDeleteParts, IndicesGetAliasParts, IndicesGetParts};
use rand::distr::{Alphanumeric, SampleString};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::ElasticsearchProvider;

type IndexResponse = HashMap<String, Index>;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Index {
  mappings: serde_json::Value,
  settings: serde_json::Value,
}

pub async fn create_scoped_index(provider: &ElasticsearchProvider, query: serde_json::Value) -> Result<(), anyhow::Error> {
  let new_index = format!("motiva-{}", Alphanumeric.sample_string(&mut rand::rng(), 8).to_lowercase());

  let mut actions: Vec<serde_json::Value> = vec![json!({ "add": { "alias": provider.scoped_alias_name(), "index": new_index } })];
  let mut old_index: Option<String> = None;

  let aliases = provider.es.indices().get_alias(IndicesGetAliasParts::Name(&[&provider.scoped_alias_name()])).send().await?;

  if aliases.status_code() == StatusCode::OK {
    let aliases: HashMap<String, serde_json::Value> = aliases.json().await?;
    let previous_index = aliases.keys().next().ok_or_else(|| anyhow::anyhow!("found no alias"))?;

    tracing::info!(index = previous_index, "found previous scoped index");

    actions.push(json!({ "remove": { "alias": provider.scoped_alias_name(), "index": previous_index, "must_exist": true } }));
    old_index = Some(previous_index.clone());
  }

  let indices: IndexResponse = provider.es.indices().get(IndicesGetParts::Index(&[&provider.main_index])).send().await?.json().await?;
  let mut index = indices.values().next().ok_or_else(|| anyhow::anyhow!("found no index"))?.to_owned();

  let nested = index.settings.get_mut("index").context("no settings")?.as_object_mut().context("no settings")?;
  nested.remove("uuid");
  nested.remove("provided_name");
  nested.remove("version");
  nested.remove("creation_date");

  let response = provider.es.indices().create(IndicesCreateParts::Index(&new_index)).body(index).send().await?;
  if response.status_code() != StatusCode::OK {
    anyhow::bail!("index creation returned {}: {:?}", response.status_code(), response.text().await);
  }

  tracing::info!(index = new_index, "created new index, starting reindexing data");

  let result: Result<(), anyhow::Error> = async {
    let reindex = provider
      .es
      .reindex()
      .body(json!({
        "dest": {
          "index": new_index
        },
        "source": {
          "index": provider.main_index,
          "query": query,
        }
      }))
      .wait_for_completion(true)
      .send()
      .await?;

    if reindex.status_code() != StatusCode::OK {
      anyhow::bail!("reindex returned {}: {:?}", reindex.status_code(), reindex.text().await);
    }

    tracing::info!(index = new_index, "reindexed data");

    let response = provider.es.indices().update_aliases().body(json!({ "actions": actions })).send().await?;
    if response.status_code() != StatusCode::OK {
      anyhow::bail!("update aliases returned {}: {:?}", response.status_code(), response.text().await);
    }

    Ok(())
  }
  .await;

  if let Err(e) = result {
    if let Err(err) = provider.es.indices().delete(IndicesDeleteParts::Index(&[&new_index])).send().await {
      tracing::warn!(index = new_index, error = %err, "failed to clean up orphaned index after error");
    }
    return Err(e);
  }

  tracing::info!(from = old_index, to = new_index, "atomically swapped index");

  if let Some(ref to_delete) = old_index {
    if provider.es.indices().delete(IndicesDeleteParts::Index(&[to_delete])).send().await?.status_code() != StatusCode::OK {
      tracing::warn!(index = to_delete, "could not delete previous scoped index, it may need manual cleanup");
    } else {
      tracing::info!(index = to_delete, "deleted old index");
    }
  }

  Ok(())
}
