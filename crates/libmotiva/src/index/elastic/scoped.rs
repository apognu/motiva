use std::collections::{BTreeMap, HashMap};

use anyhow::Context;
use opensearch::indices::{IndicesCreateParts, IndicesDeleteParts, IndicesGetAliasParts, IndicesGetMappingParts, IndicesGetParts};
use rand::distr::{Alphanumeric, SampleString};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::ElasticsearchProvider;

type IndexResponse = BTreeMap<String, Index>;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Index {
  mappings: serde_json::Value,
  settings: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct ReindexResult {
  timed_out: bool,
  total: u64,
  created: u64,
  failures: Vec<serde_json::Value>,
}

/// Inputs a scoped index was built from, stored in its mapping's `_meta.motiva`.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct ScopedMeta {
  sources: Vec<String>,
  query: serde_json::Value,
  excludes: Vec<String>,
  skip_sources: Vec<String>,
}

pub async fn create_scoped_index(provider: &ElasticsearchProvider, query: serde_json::Value, excludes: &[String], skip_sources: &[String], force: bool) -> Result<(), anyhow::Error> {
  let skip_patterns = skip_sources
    .iter()
    .map(|pattern| glob::Pattern::new(pattern).with_context(|| format!("invalid source skip pattern `{pattern}`")))
    .collect::<Result<Vec<_>, _>>()?;

  let new_index = format!("motiva-{}", Alphanumeric.sample_string(&mut rand::rng(), 8).to_lowercase());

  let mut actions: Vec<serde_json::Value> = vec![json!({ "add": { "alias": provider.scoped_alias_name(), "index": new_index } })];
  let mut old_index: Option<String> = None;
  let mut previous_meta: Option<ScopedMeta> = None;

  let aliases = provider.es.indices().get_alias(IndicesGetAliasParts::Name(&[&provider.scoped_alias_name()])).send().await?;

  if aliases.status_code() == StatusCode::OK {
    let aliases: HashMap<String, serde_json::Value> = aliases.json().await?;
    let previous_index = aliases.keys().next().ok_or_else(|| anyhow::anyhow!("found no alias"))?;

    tracing::info!(index = previous_index, "found previous scoped index");

    actions.push(json!({ "remove": { "alias": provider.scoped_alias_name(), "index": previous_index, "must_exist": true } }));
    old_index = Some(previous_index.clone());
    previous_meta = fetch_scoped_meta(provider, previous_index).await;
  }

  let indices: IndexResponse = provider.es.indices().get(IndicesGetParts::Index(&[&provider.main_index])).send().await?.json().await?;
  let (indices, skipped): (Vec<_>, Vec<_>) = indices.into_iter().partition(|(name, _)| !skip_patterns.iter().any(|pattern| pattern.matches(name)));

  for (name, _) in &skipped {
    tracing::info!(index = name, "skipping source index");
  }

  let meta = ScopedMeta {
    sources: indices.iter().map(|(name, _)| name.clone()).collect(),
    query,
    excludes: property_excludes(excludes),
    skip_sources: skip_sources.to_vec(),
  };

  if !force && previous_meta.as_ref() == Some(&meta) {
    tracing::info!(index = old_index, "scoped index is up to date, skipping");

    return Ok(());
  }

  let mut index = indices
    .into_iter()
    .next()
    .map(|(_, index)| index)
    .ok_or_else(|| anyhow::anyhow!("found no source index in {}", provider.main_index))?;

  let nested = index.settings.get_mut("index").context("no settings")?.as_object_mut().context("no settings")?;
  nested.remove("uuid");
  nested.remove("provided_name");
  nested.remove("version");
  nested.remove("creation_date");

  index.mappings.as_object_mut().context("no mappings")?.insert("_meta".into(), json!({ "motiva": meta }));

  let response = provider.es.indices().create(IndicesCreateParts::Index(&new_index)).body(index).send().await?;
  if response.status_code() != StatusCode::OK {
    anyhow::bail!("index creation returned {}: {:?}", response.status_code(), response.text().await);
  }

  tracing::info!(index = new_index, sources = ?meta.sources, "created new index, starting reindexing data");

  let result: Result<(), anyhow::Error> = async {
    let reindex = provider
      .es
      .reindex()
      .body(json!({
        "dest": {
          "index": new_index
        },
        "source": {
          "index": meta.sources,
          "query": meta.query,
          "_source": {
            "excludes": meta.excludes,
          },
        }
      }))
      .wait_for_completion(true)
      .refresh(true)
      .send()
      .await?;

    if reindex.status_code() != StatusCode::OK {
      anyhow::bail!("reindex returned {}: {:?}", reindex.status_code(), reindex.text().await);
    }

    let result: ReindexResult = reindex.json().await.context("could not parse reindex response")?;

    if result.timed_out || !result.failures.is_empty() {
      anyhow::bail!(
        "reindex did not complete: timed_out={}, total={}, created={}, failures={:?}",
        result.timed_out,
        result.total,
        result.created,
        result.failures
      );
    }

    if result.total == 0 {
      anyhow::bail!("reindex matched no documents in {} with the scoped query", provider.main_index);
    }
    if result.created != result.total {
      anyhow::bail!("reindex copied only {} of {} documents into {}", result.created, result.total, new_index);
    }

    tracing::info!(index = new_index, documents = result.created, "reindexed data");

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

/// Map excluded property names to their fields, other fields are used for matching.
fn property_excludes(excludes: &[String]) -> Vec<String> {
  excludes.iter().filter(|property| !property.is_empty()).map(|property| format!("properties.{property}")).collect()
}

async fn fetch_scoped_meta(provider: &ElasticsearchProvider, index: &str) -> Option<ScopedMeta> {
  let response = provider.es.indices().get_mapping(IndicesGetMappingParts::Index(&[index])).send().await.ok()?;
  let mappings: HashMap<String, serde_json::Value> = response.json().await.ok()?;

  parse_scoped_meta(mappings.get(index)?)
}

fn parse_scoped_meta(mapping: &serde_json::Value) -> Option<ScopedMeta> {
  let mut meta: ScopedMeta = serde_json::from_value(mapping.pointer("/mappings/_meta/motiva")?.clone()).ok()?;
  meta.sources.sort();

  Some(meta)
}

#[cfg(test)]
mod tests {
  use serde_json::json;
  use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{method, path, path_regex},
  };

  use super::ScopedMeta;
  use crate::{ElasticsearchProvider, index::elastic::config::EsOptions};

  const NEW_INDEX: &str = r"^/motiva-[a-z0-9]{8}$";

  fn source_index(name: &str) -> serde_json::Value {
    json!({
      "aliases": { "yente-entities": {} },
      "mappings": { "_meta": { "last_export": "2026-10-15T00:00:00" }, "properties": {} },
      "settings": { "index": { "uuid": "uuid", "provided_name": name, "creation_date": "0", "version": { "created": "1" }, "number_of_shards": "1" } }
    })
  }

  fn meta(sources: &[&str], excludes: &[&str]) -> serde_json::Value {
    json!({
      "sources": sources,
      "query": { "match_all": {} },
      "excludes": excludes,
      "skip_sources": ["yente-entities-internal_*"],
    })
  }

  /// Mock a cluster with three source indices, and a previous scoped index
  /// built with `previous_meta`, if any.
  async fn cluster(previous_meta: Option<serde_json::Value>, reindex_total: u64) -> (MockServer, ElasticsearchProvider) {
    let server = MockServer::start().await;

    match previous_meta {
      Some(meta) => {
        Mock::given(method("GET"))
          .and(path("/_alias/yente-motiva-scoped-entities"))
          .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "motiva-previous": { "aliases": { "yente-motiva-scoped-entities": {} } } })))
          .mount(&server)
          .await;

        Mock::given(method("GET"))
          .and(path("/motiva-previous/_mapping"))
          .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "motiva-previous": { "mappings": { "_meta": { "motiva": meta } } } })))
          .mount(&server)
          .await;
      }

      None => {
        Mock::given(method("GET"))
          .and(path("/_alias/yente-motiva-scoped-entities"))
          .respond_with(ResponseTemplate::new(404).set_body_json(json!({})))
          .mount(&server)
          .await;
      }
    }

    Mock::given(method("GET"))
      .and(path("/yente-entities"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({
        "yente-entities-default": source_index("yente-entities-default"),
        "yente-entities-internal_watchlist": source_index("yente-entities-internal_watchlist"),
        "yente-entities-sanctions": source_index("yente-entities-sanctions"),
      })))
      .mount(&server)
      .await;

    Mock::given(method("PUT"))
      .and(path_regex(NEW_INDEX))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "acknowledged": true })))
      .mount(&server)
      .await;

    Mock::given(method("POST"))
      .and(path("/_reindex"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "timed_out": false, "total": reindex_total, "created": reindex_total, "failures": [] })))
      .mount(&server)
      .await;

    Mock::given(method("POST"))
      .and(path("/_aliases"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "acknowledged": true })))
      .mount(&server)
      .await;

    Mock::given(method("DELETE"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "acknowledged": true })))
      .mount(&server)
      .await;

    let provider = ElasticsearchProvider::new(&server.uri(), EsOptions::default()).await.unwrap();

    (server, provider)
  }

  async fn requests(server: &MockServer, verb: &str, path: &str) -> Vec<Request> {
    let path = regex::Regex::new(path).unwrap();

    server
      .received_requests()
      .await
      .unwrap()
      .into_iter()
      .filter(|request| request.method.as_str() == verb && path.is_match(request.url.path()))
      .collect()
  }

  fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
  }

  #[tokio::test]
  async fn create_scoped_index_skips_sources_and_excludes_properties() {
    let (server, provider) = cluster(None, 10).await;

    super::create_scoped_index(
      &provider,
      json!({ "match_all": {} }),
      &strings(&["notes", "sourceUrl"]),
      &strings(&["yente-entities-internal_*"]),
      false,
    )
    .await
    .unwrap();

    let reindex: serde_json::Value = requests(&server, "POST", "^/_reindex$").await[0].body_json().unwrap();

    assert_eq!(reindex["source"]["index"], json!(["yente-entities-default", "yente-entities-sanctions"]));
    assert_eq!(reindex["source"]["_source"]["excludes"], json!(["properties.notes", "properties.sourceUrl"]));
    assert_eq!(reindex["source"]["query"], json!({ "match_all": {} }));

    let create: serde_json::Value = requests(&server, "PUT", NEW_INDEX).await[0].body_json().unwrap();

    assert_eq!(
      create["mappings"]["_meta"],
      json!({ "motiva": meta(&["yente-entities-default", "yente-entities-sanctions"], &["properties.notes", "properties.sourceUrl"]) })
    );
    assert_eq!(create["settings"]["index"], json!({ "number_of_shards": "1" }));

    let aliases: serde_json::Value = requests(&server, "POST", "^/_aliases$").await[0].body_json().unwrap();

    assert_eq!(aliases["actions"].as_array().unwrap().len(), 1);
    assert_eq!(aliases["actions"][0]["add"]["alias"], "yente-motiva-scoped-entities");
  }

  #[tokio::test]
  async fn create_scoped_index_is_skipped_when_up_to_date() {
    let previous = meta(&["yente-entities-sanctions", "yente-entities-default"], &["properties.notes"]);
    let (server, provider) = cluster(Some(previous), 10).await;

    super::create_scoped_index(&provider, json!({ "match_all": {} }), &strings(&["notes"]), &strings(&["yente-entities-internal_*"]), false)
      .await
      .unwrap();

    assert!(requests(&server, "PUT", NEW_INDEX).await.is_empty());
    assert!(requests(&server, "POST", "^/_reindex$").await.is_empty());
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());
    assert!(requests(&server, "DELETE", ".*").await.is_empty());
  }

  #[tokio::test]
  async fn create_scoped_index_is_rebuilt_when_inputs_changed() {
    let previous = meta(&["yente-entities-default", "yente-entities-sanctions"], &["properties.notes"]);

    for (excludes, skip_sources) in [
      // The skipped index is now included in the sources.
      (vec!["notes"], vec![]),
      // Excluded properties changed.
      (vec!["sourceUrl"], vec!["yente-entities-internal_*"]),
    ] {
      let (server, provider) = cluster(Some(previous.clone()), 10).await;

      super::create_scoped_index(&provider, json!({ "match_all": {} }), &strings(&excludes), &strings(&skip_sources), false)
        .await
        .unwrap();

      assert_eq!(requests(&server, "POST", "^/_reindex$").await.len(), 1);

      let aliases: serde_json::Value = requests(&server, "POST", "^/_aliases$").await[0].body_json().unwrap();

      assert_eq!(aliases["actions"][1]["remove"]["index"], "motiva-previous");

      let deleted = requests(&server, "DELETE", ".*").await;

      assert_eq!(deleted.len(), 1);
      assert_eq!(deleted[0].url.path(), "/motiva-previous");
    }
  }

  #[tokio::test]
  async fn create_scoped_index_is_rebuilt_when_query_changed() {
    let previous = meta(&["yente-entities-default", "yente-entities-sanctions"], &["properties.notes"]);
    let (server, provider) = cluster(Some(previous), 10).await;

    super::create_scoped_index(
      &provider,
      json!({ "term": { "topics": "sanction" } }),
      &strings(&["notes"]),
      &strings(&["yente-entities-internal_*"]),
      false,
    )
    .await
    .unwrap();

    assert_eq!(requests(&server, "POST", "^/_reindex$").await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_is_rebuilt_when_forced() {
    let previous = meta(&["yente-entities-default", "yente-entities-sanctions"], &["properties.notes"]);
    let (server, provider) = cluster(Some(previous), 10).await;

    super::create_scoped_index(&provider, json!({ "match_all": {} }), &strings(&["notes"]), &strings(&["yente-entities-internal_*"]), true)
      .await
      .unwrap();

    assert_eq!(requests(&server, "POST", "^/_reindex$").await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_is_rebuilt_without_previous_meta() {
    let (server, provider) = cluster(Some(json!({ "last_export": "2026-10-15T00:00:00" })), 10).await;

    super::create_scoped_index(&provider, json!({ "match_all": {} }), &strings(&["notes"]), &strings(&["yente-entities-internal_*"]), false)
      .await
      .unwrap();

    assert_eq!(requests(&server, "POST", "^/_reindex$").await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_fails_when_every_source_is_skipped() {
    let (server, provider) = cluster(None, 10).await;

    let result = super::create_scoped_index(&provider, json!({ "match_all": {} }), &[], &strings(&["*"]), false).await;

    assert!(result.unwrap_err().to_string().contains("found no source index"));
    assert!(requests(&server, "PUT", NEW_INDEX).await.is_empty());
  }

  #[tokio::test]
  async fn create_scoped_index_fails_with_invalid_skip_pattern() {
    let (server, provider) = cluster(None, 10).await;

    let result = super::create_scoped_index(&provider, json!({ "match_all": {} }), &[], &strings(&["["]), false).await;

    assert!(result.unwrap_err().to_string().contains("invalid source skip pattern"));
    assert!(requests(&server, "PUT", NEW_INDEX).await.is_empty());
  }

  #[tokio::test]
  async fn create_scoped_index_cleans_up_after_empty_reindex() {
    let (server, provider) = cluster(Some(json!({})), 0).await;

    let result = super::create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await;

    assert!(result.unwrap_err().to_string().contains("matched no documents"));
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());

    let deleted = requests(&server, "DELETE", ".*").await;

    assert_eq!(deleted.len(), 1);
    assert!(regex::Regex::new(NEW_INDEX).unwrap().is_match(deleted[0].url.path()));
  }

  #[test]
  fn parse_scoped_meta() {
    let mapping = json!({
      "mappings": {
        "_meta": {
          "motiva": {
            "sources": ["yente-entities-b", "yente-entities-a"],
            "query": { "term": { "topics": "sanction" } },
            "excludes": ["properties.sourceUrl"],
            "skip_sources": ["yente-entities-internal_*"],
          }
        }
      }
    });

    let expected = ScopedMeta {
      sources: vec!["yente-entities-a".into(), "yente-entities-b".into()],
      query: json!({ "term": { "topics": "sanction" } }),
      excludes: vec!["properties.sourceUrl".into()],
      skip_sources: vec!["yente-entities-internal_*".into()],
    };

    assert_eq!(super::parse_scoped_meta(&mapping), Some(expected));
  }

  #[test]
  fn property_excludes() {
    let excludes = ["notes", "", "sourceUrl"].map(String::from);

    assert_eq!(super::property_excludes(&excludes), vec!["properties.notes".to_string(), "properties.sourceUrl".to_string()]);
  }

  #[test]
  fn parse_scoped_meta_without_motiva_meta() {
    assert_eq!(super::parse_scoped_meta(&json!({ "mappings": {} })), None);
    assert_eq!(super::parse_scoped_meta(&json!({ "mappings": { "_meta": { "last_export": "2026-10-15T00:00:00" } } })), None);
    assert_eq!(super::parse_scoped_meta(&json!({ "mappings": { "_meta": { "motiva": { "sources": [] } } } })), None);
  }
}
