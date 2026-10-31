use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use anyhow::Context;
use opensearch::CountParts;
use opensearch::http::{Method, headers::HeaderMap};
use opensearch::indices::{IndicesCreateParts, IndicesDeleteParts, IndicesForcemergeParts, IndicesGetAliasParts, IndicesGetMappingParts, IndicesGetParts, IndicesRefreshParts};
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

/// How many polls in a row may fail transiently before giving up on the task.
const TASK_MAX_TRANSIENT_ERRORS: u32 = 5;

/// Timings used to follow a reindex task.
#[derive(Clone, Debug)]
struct TaskTimings {
  /// Delay between two polls of the reindex task.
  poll_interval: Duration,
  /// How long to wait for the reindex task before giving up on it.
  max_wait: Duration,
  /// How often to report progress while the reindex task runs.
  progress_interval: Duration,
  /// How many polls to wait for a cancelled task to stop.
  cancel_polls: u32,
}

impl Default for TaskTimings {
  fn default() -> Self {
    Self {
      poll_interval: Duration::from_secs(5),
      // A reindex from even the largest main index takes a few minutes, so this
      // leaves a wide margin while still failing a stuck task in reasonable time.
      max_wait: Duration::from_secs(30 * 60),
      progress_interval: Duration::from_secs(60),
      cancel_polls: 12,
    }
  }
}

#[derive(Debug, Deserialize)]
struct ReindexTask {
  task: String,
}

#[derive(Debug, Deserialize)]
struct TaskStatus {
  completed: bool,
  task: Option<TaskInfo>,
  response: Option<ReindexResult>,
  error: Option<serde_json::Value>,
}

impl TaskStatus {
  /// Extract the outcome of a completed task.
  fn into_result(self) -> Result<ReindexResult, anyhow::Error> {
    match self {
      TaskStatus { error: Some(error), .. } => anyhow::bail!("reindex task failed: {error}"),
      TaskStatus { response: Some(result), .. } => Ok(result),
      TaskStatus { .. } => anyhow::bail!("reindex task completed without a result"),
    }
  }
}

#[derive(Debug, Deserialize)]
struct TaskInfo {
  status: Option<TaskProgress>,
}

#[derive(Debug, Deserialize)]
struct TaskProgress {
  #[serde(default)]
  total: u64,
  #[serde(default)]
  created: u64,
}

#[derive(Debug, Deserialize)]
struct ReindexResult {
  timed_out: bool,
  total: u64,
  created: u64,
  failures: Vec<serde_json::Value>,
  #[serde(default)]
  canceled: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CountResult {
  count: u64,
}

#[derive(Debug, Deserialize)]
struct ForcemergeResult {
  #[serde(rename = "_shards")]
  shards: Shards,
}

#[derive(Debug, Deserialize)]
struct Shards {
  total: u64,
  failed: u64,
}

/// Why polling a task failed.
enum PollError {
  /// The poll may succeed if retried.
  Transient(anyhow::Error),
  /// The task cannot be followed anymore.
  Fatal(anyhow::Error),
}

/// Inputs a scoped index was built from, stored in its mapping's `_meta.motiva`.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct ScopedMeta {
  sources: Vec<String>,
  /// Stored as a serialized JSON string because Elasticsearch's mapping parser
  /// collapses arrays of single-key objects with identical keys (e.g., multiple
  /// `terms` clauses) under a single key in `_meta`, corrupting Query DSL.
  query: String,
  excludes: Vec<String>,
  skip_sources: Vec<String>,
}

pub async fn create_scoped_index(provider: &ElasticsearchProvider, query: serde_json::Value, excludes: &[String], skip_sources: &[String], force: bool) -> Result<(), anyhow::Error> {
  build_scoped_index(provider, query, excludes, skip_sources, force, &TaskTimings::default()).await
}

async fn build_scoped_index(provider: &ElasticsearchProvider, query: serde_json::Value, excludes: &[String], skip_sources: &[String], force: bool, timings: &TaskTimings) -> Result<(), anyhow::Error> {
  // Building a scoped index relies on the reindex API, which OpenSearch Serverless does not expose.
  if provider.is_serverless() {
    anyhow::bail!("scoped indexes are not supported on OpenSearch Serverless");
  }

  let skip_patterns = skip_sources
    .iter()
    .map(|pattern| glob::Pattern::new(pattern).with_context(|| format!("invalid source skip pattern `{pattern}`")))
    .collect::<Result<Vec<_>, _>>()?;

  let new_index = format!("motiva-{}", Alphanumeric.sample_string(&mut rand::rng(), 8).to_lowercase());

  let mut actions: Vec<serde_json::Value> = vec![json!({ "add": { "alias": provider.scoped_alias_name(), "index": new_index } })];
  let mut old_index: Option<String> = None;
  let mut previous_meta: Option<ScopedMeta> = None;

  let aliases = provider.es.indices().get_alias(IndicesGetAliasParts::Name(&[&provider.scoped_alias_name()])).send().await?;

  // Anything but a missing alias must fail, or the swap would add the new index
  // without removing the previous one, and the alias would then span both.
  match aliases.status_code() {
    StatusCode::OK | StatusCode::NOT_FOUND => {}
    status => anyhow::bail!("fetching scoped alias returned {status}: {:?}", aliases.text().await),
  }

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
    query: query.to_string(),
    excludes: property_excludes(excludes),
    skip_sources: skip_sources.to_vec(),
  };

  let up_to_date = previous_meta.as_ref() == Some(&meta);

  if up_to_date && !force {
    tracing::info!(index = old_index, "scoped index is up to date, skipping");

    return Ok(());
  }

  match previous_meta {
    Some(_) if up_to_date => tracing::info!(index = old_index, "scoped index is up to date, but recreation was forced"),
    Some(ref previous) => tracing::info!(index = old_index, changed = ?changed_inputs(previous, &meta), "scoped index inputs changed, recreating"),
    None if old_index.is_some() => tracing::info!(index = old_index, "previous scoped index has no recorded inputs, recreating"),
    None => {}
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

  // Set while a reindex task may be running on the cluster, so it can be
  // cancelled if anything fails. Otherwise, it would keep writing to (and
  // possibly recreate) the index once it has been deleted.
  let mut running_task: Option<String> = None;

  let result: Result<(), anyhow::Error> = async {
    let body = json!({
      "dest": {
        "index": new_index
      },
      "source": {
        "index": meta.sources,
        "query": query,
        "_source": {
          "excludes": meta.excludes,
        },
      }
    });

    let response = provider.es.reindex().body(body).wait_for_completion(false).refresh(true).send().await?;
    if response.status_code() != StatusCode::OK {
      anyhow::bail!("reindex returned {}: {:?}", response.status_code(), response.text().await);
    }

    let task: ReindexTask = response.json().await.context("could not parse reindex response")?;

    tracing::info!(index = new_index, task = task.task, "started reindex task");

    running_task = Some(task.task.clone());
    let status = wait_for_task(provider, &task.task, timings).await?;
    running_task = None;

    let result = status.into_result()?;

    check_reindex_result(provider, &new_index, &result)?;

    tracing::info!(index = new_index, documents = result.created, "reindexed data");

    force_merge(provider, &new_index).await;

    check_document_count(provider, &new_index, result.created).await?;

    let response = provider.es.indices().update_aliases().body(json!({ "actions": actions })).send().await?;
    if response.status_code() != StatusCode::OK {
      anyhow::bail!("update aliases returned {}: {:?}", response.status_code(), response.text().await);
    }

    Ok(())
  }
  .await;

  if let Err(e) = result {
    let stopped = match running_task {
      Some(ref task) => cancel_task(provider, task, timings).await,
      None => true,
    };

    match provider.es.indices().delete(IndicesDeleteParts::Index(&[&new_index])).send().await {
      Ok(response) if response.status_code().is_success() => {}
      Ok(response) => tracing::warn!(index = new_index, status = %response.status_code(), "failed to clean up orphaned index after error"),
      Err(err) => tracing::warn!(index = new_index, error = %err, "failed to clean up orphaned index after error"),
    }

    if !stopped {
      tracing::warn!(
        index = new_index,
        task = running_task,
        "reindex task may still be running and recreate the index, it may need manual cleanup"
      );
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

/// Check that a reindex copied every matching document.
fn check_reindex_result(provider: &ElasticsearchProvider, index: &str, result: &ReindexResult) -> Result<(), anyhow::Error> {
  if let Some(ref reason) = result.canceled {
    anyhow::bail!("reindex was cancelled ({reason}) after copying {} of {} documents", result.created, result.total);
  }

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
    anyhow::bail!("reindex copied only {} of {} documents into {}", result.created, result.total, index);
  }

  Ok(())
}

/// Check that the new index holds as many documents as the reindex reported,
/// so the alias only moves to an index whose data is actually there.
async fn check_document_count(provider: &ElasticsearchProvider, index: &str, expected: u64) -> Result<(), anyhow::Error> {
  let response = provider.es.count(CountParts::Index(&[index])).send().await?;
  if response.status_code() != StatusCode::OK {
    anyhow::bail!("counting documents in {index} returned {}: {:?}", response.status_code(), response.text().await);
  }

  let count: CountResult = response.json().await.context("could not parse document count")?;

  if count.count != expected {
    anyhow::bail!("{index} holds {} documents, but the reindex reported {expected}", count.count);
  }

  Ok(())
}

/// Wait for a reindex task to complete and return its final status.
///
/// Reindexing runs as a task instead of a blocking request because OpenSearch
/// attaches a response header for every scroll batch, which overflows the
/// HTTP client's header limit when copying millions of documents.
///
/// The task is polled instead of waited on server-side, because the status
/// returned when such a wait times out differs between Elasticsearch and
/// OpenSearch versions.
async fn wait_for_task(provider: &ElasticsearchProvider, task: &str, timings: &TaskTimings) -> Result<TaskStatus, anyhow::Error> {
  let started = Instant::now();
  let mut last_progress = started;
  let mut transient_errors = 0;

  loop {
    match fetch_task(provider, task).await {
      Ok(status) if status.completed => return Ok(status),

      Ok(status) => {
        transient_errors = 0;

        if last_progress.elapsed() >= timings.progress_interval {
          last_progress = Instant::now();

          match status.task.and_then(|info| info.status) {
            Some(progress) => tracing::info!(task, created = progress.created, total = progress.total, "reindex still running"),
            None => tracing::info!(task, "reindex still running"),
          }
        }
      }

      Err(PollError::Fatal(err)) => return Err(err),

      Err(PollError::Transient(err)) => {
        transient_errors += 1;

        if transient_errors >= TASK_MAX_TRANSIENT_ERRORS {
          return Err(err.context(format!("could not follow reindex task {task} after {transient_errors} attempts")));
        }

        tracing::warn!(task, error = %err, attempt = transient_errors, "could not poll reindex task, retrying");
      }
    }

    if started.elapsed() >= timings.max_wait {
      anyhow::bail!("reindex task {task} did not complete within {:?}", timings.max_wait);
    }

    tokio::time::sleep(timings.poll_interval).await;
  }
}

/// Fetch the current status of a task.
async fn fetch_task(provider: &ElasticsearchProvider, task: &str) -> Result<TaskStatus, PollError> {
  let response = provider
    .es
    .send(Method::Get, &format!("/_tasks/{task}"), HeaderMap::new(), None::<&()>, None::<()>, None)
    .await
    .map_err(|err| PollError::Transient(anyhow::Error::new(err).context("could not fetch reindex task")))?;

  match response.status_code() {
    StatusCode::OK => response.json().await.context("could not parse reindex task").map_err(PollError::Transient),

    status if status.is_server_error() || status == StatusCode::REQUEST_TIMEOUT || status == StatusCode::TOO_MANY_REQUESTS => {
      Err(PollError::Transient(anyhow::anyhow!("reindex task returned {status}: {:?}", response.text().await)))
    }

    status => Err(PollError::Fatal(anyhow::anyhow!("reindex task returned {status}: {:?}", response.text().await))),
  }
}

/// Cancel a task and wait for it to stop, returning whether it did.
async fn cancel_task(provider: &ElasticsearchProvider, task: &str, timings: &TaskTimings) -> bool {
  let response = provider
    .es
    .send(Method::Post, &format!("/_tasks/{task}/_cancel"), HeaderMap::new(), None::<&()>, None::<()>, None)
    .await;

  match response {
    Ok(response) if response.status_code().is_success() => tracing::info!(task, "cancelled reindex task"),
    Ok(response) => tracing::warn!(task, status = %response.status_code(), "could not cancel reindex task"),
    Err(err) => tracing::warn!(task, error = %err, "could not cancel reindex task"),
  }

  // Keep polling even if cancelling failed, the task may have completed anyway.
  for _ in 0..timings.cancel_polls {
    match fetch_task(provider, task).await {
      Ok(status) if status.completed => return true,
      Ok(_) | Err(PollError::Transient(_)) => {}
      Err(PollError::Fatal(_)) => return false,
    }

    tokio::time::sleep(timings.poll_interval).await;
  }

  false
}

/// Merge the freshly built index down to a single segment.
///
/// Fuzzy and term lookups run once per segment, so a single segment roughly
/// halves query latency on a static index. The index never has deleted documents,
/// so scoring statistics, and therefore scores, are unchanged.
/// A failed merge only costs performance, so it is not fatal.
async fn force_merge(provider: &ElasticsearchProvider, index: &str) {
  let response = provider.es.indices().forcemerge(IndicesForcemergeParts::Index(&[index])).max_num_segments(1).send().await;

  match response {
    Ok(response) if response.status_code() == StatusCode::OK => match response.json::<ForcemergeResult>().await {
      Ok(ForcemergeResult { shards: Shards { failed: 0, .. } }) => tracing::info!(index, "merged index into a single segment"),
      Ok(ForcemergeResult { shards }) => tracing::warn!(index, failed = shards.failed, total = shards.total, "could not merge index segments on some shards"),
      Err(err) => tracing::warn!(index, error = %err, "could not parse index merge response"),
    },

    Ok(response) => {
      tracing::warn!(index, status = %response.status_code(), "could not merge index segments");
      return;
    }

    Err(err) => {
      tracing::warn!(index, error = %err, "could not merge index segments");
      return;
    }
  }

  // Searches keep using the pre-merge segments until the next refresh.
  match provider.es.indices().refresh(IndicesRefreshParts::Index(&[index])).send().await {
    Ok(response) if response.status_code() == StatusCode::OK => {}
    Ok(response) => tracing::warn!(index, status = %response.status_code(), "could not refresh merged index"),
    Err(err) => tracing::warn!(index, error = %err, "could not refresh merged index"),
  }
}

/// List the inputs that differ between two scoped index builds.
fn changed_inputs(previous: &ScopedMeta, current: &ScopedMeta) -> Vec<&'static str> {
  [
    ("sources", previous.sources != current.sources),
    ("query", previous.query != current.query),
    ("excludes", previous.excludes != current.excludes),
    ("skip_sources", previous.skip_sources != current.skip_sources),
  ]
  .into_iter()
  .filter_map(|(name, changed)| changed.then_some(name))
  .collect()
}

/// Map excluded property names to their fields, other fields are used for matching.
fn property_excludes(excludes: &[String]) -> Vec<String> {
  excludes.iter().filter(|property| !property.is_empty()).map(|property| format!("properties.{property}")).collect()
}

async fn fetch_scoped_meta(provider: &ElasticsearchProvider, index: &str) -> Option<ScopedMeta> {
  let mappings: HashMap<String, serde_json::Value> = async { provider.es.indices().get_mapping(IndicesGetMappingParts::Index(&[index])).send().await?.json().await }
    .await
    .inspect_err(|err| tracing::warn!(index, error = %err, "could not fetch previous scoped index mapping"))
    .ok()?;

  parse_scoped_meta(mappings.get(index)?)
}

fn parse_scoped_meta(mapping: &serde_json::Value) -> Option<ScopedMeta> {
  let mut meta: ScopedMeta = serde_json::from_value(mapping.pointer("/mappings/_meta/motiva")?.clone()).ok()?;
  meta.sources.sort();

  Some(meta)
}

#[cfg(test)]
mod tests {
  use std::time::Duration;

  use serde_json::json;
  use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{method, path, path_regex},
  };

  use super::{ScopedMeta, TaskTimings};
  use crate::{ElasticsearchProvider, index::elastic::config::EsOptions};

  const NEW_INDEX: &str = r"^/motiva-[a-z0-9]{8}$";

  /// Timings that keep polling fast, with a deadline no test should reach.
  fn fast() -> TaskTimings {
    TaskTimings {
      poll_interval: Duration::from_millis(1),
      max_wait: Duration::from_secs(10),
      progress_interval: Duration::from_secs(60),
      cancel_polls: 12,
    }
  }

  async fn create_scoped_index(provider: &ElasticsearchProvider, query: serde_json::Value, excludes: &[String], skip_sources: &[String], force: bool) -> Result<(), anyhow::Error> {
    super::build_scoped_index(provider, query, excludes, skip_sources, force, &fast()).await
  }

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
      "query": r#"{"match_all":{}}"#,
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
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "task": "node:1" })))
      .mount(&server)
      .await;

    Mock::given(method("GET"))
      .and(path("/_tasks/node:1"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({
        "completed": true,
        "task": {},
        "response": { "timed_out": false, "total": reindex_total, "created": reindex_total, "failures": [] }
      })))
      .mount(&server)
      .await;

    Mock::given(method("POST"))
      .and(path("/_tasks/node:1/_cancel"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "nodes": {} })))
      .mount(&server)
      .await;

    Mock::given(method("POST"))
      .and(path_regex(r"^/motiva-[a-z0-9]{8}/_forcemerge$"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "_shards": { "total": 1, "successful": 1, "failed": 0 } })))
      .mount(&server)
      .await;

    Mock::given(method("GET"))
      .and(path_regex(r"^/motiva-[a-z0-9]{8}/_refresh$"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "_shards": { "total": 1, "successful": 1, "failed": 0 } })))
      .mount(&server)
      .await;

    Mock::given(path_regex(r"^/motiva-[a-z0-9]{8}/_count$"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "count": reindex_total })))
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

    create_scoped_index(
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
  async fn create_scoped_index_waits_for_reindex_task() {
    let (server, provider) = cluster(None, 10).await;

    // The first poll sees the task still running, then a transient error.
    Mock::given(method("GET"))
      .and(path("/_tasks/node:1"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "completed": false, "task": { "status": { "total": 10, "created": 5 } } })))
      .with_priority(1)
      .up_to_n_times(1)
      .mount(&server)
      .await;

    Mock::given(method("GET"))
      .and(path("/_tasks/node:1"))
      .respond_with(ResponseTemplate::new(500).set_body_json(json!({ "error": { "type": "timeout_exception" }, "status": 500 })))
      .with_priority(2)
      .up_to_n_times(1)
      .mount(&server)
      .await;

    create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await.unwrap();

    let reindex = &requests(&server, "POST", "^/_reindex$").await[0];

    assert!(reindex.url.query().unwrap().contains("wait_for_completion=false"));

    let polls = requests(&server, "GET", "^/_tasks/node:1$").await;

    assert_eq!(polls.len(), 3);
    assert!(polls.iter().all(|poll| poll.url.query().is_none()));
    assert_eq!(requests(&server, "POST", "^/_aliases$").await.len(), 1);
    assert!(requests(&server, "POST", "/_cancel$").await.is_empty());
  }

  #[tokio::test]
  async fn create_scoped_index_uses_default_timings() {
    let (server, provider) = cluster(None, 10).await;

    // The task completes on the first poll, so the production poll interval is never waited on.
    super::create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await.unwrap();

    assert_eq!(requests(&server, "GET", "^/_tasks/node:1$").await.len(), 1);
    assert_eq!(requests(&server, "POST", "^/_aliases$").await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_cleans_up_after_failed_reindex_task() {
    let (server, provider) = cluster(None, 10).await;

    Mock::given(method("GET"))
      .and(path("/_tasks/node:1"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "completed": true, "task": {}, "error": { "type": "index_not_found_exception" } })))
      .with_priority(1)
      .mount(&server)
      .await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await;

    assert!(result.unwrap_err().to_string().contains("index_not_found_exception"));
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());
    assert_eq!(requests(&server, "DELETE", ".*").await.len(), 1);

    // The task has completed, so there is nothing to cancel.
    assert!(requests(&server, "POST", "/_cancel$").await.is_empty());
  }

  #[tokio::test]
  async fn create_scoped_index_cancels_task_when_it_cannot_be_followed() {
    let (server, provider) = cluster(None, 10).await;

    // The first poll fails permanently, the task completes once cancelled.
    Mock::given(method("GET"))
      .and(path("/_tasks/node:1"))
      .respond_with(ResponseTemplate::new(400).set_body_json(json!({ "error": { "type": "illegal_argument_exception" } })))
      .with_priority(1)
      .up_to_n_times(1)
      .mount(&server)
      .await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await;

    assert!(result.unwrap_err().to_string().contains("400"));
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());

    let received = server.received_requests().await.unwrap();
    let cancel = received.iter().position(|request| request.url.path() == "/_tasks/node:1/_cancel").unwrap();
    let delete = received.iter().position(|request| request.method.as_str() == "DELETE").unwrap();

    assert!(cancel < delete);
  }

  #[tokio::test]
  async fn create_scoped_index_fails_when_task_is_lost() {
    let (server, provider) = cluster(None, 10).await;

    Mock::given(method("GET"))
      .and(path("/_tasks/node:1"))
      .respond_with(ResponseTemplate::new(404).set_body_json(json!({ "error": { "type": "resource_not_found_exception" } })))
      .with_priority(1)
      .mount(&server)
      .await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await;

    assert!(result.unwrap_err().to_string().contains("404"));
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());
    assert_eq!(requests(&server, "DELETE", ".*").await.len(), 1);

    // The first poll fails immediately, and the task cannot be found while cancelling.
    assert_eq!(requests(&server, "GET", "^/_tasks/node:1$").await.len(), 2);
  }

  #[tokio::test]
  async fn create_scoped_index_gives_up_after_repeated_transient_errors() {
    let (server, provider) = cluster(None, 10).await;

    Mock::given(method("GET"))
      .and(path("/_tasks/node:1"))
      .respond_with(ResponseTemplate::new(503).set_body_json(json!({})))
      .with_priority(1)
      .mount(&server)
      .await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await;

    assert!(result.unwrap_err().to_string().contains("after 5 attempts"));
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());
    assert_eq!(requests(&server, "POST", "/_cancel$").await.len(), 1);
    assert_eq!(requests(&server, "DELETE", ".*").await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_gives_up_on_task_after_deadline() {
    let (server, provider) = cluster(None, 10).await;

    Mock::given(method("GET"))
      .and(path("/_tasks/node:1"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "completed": false, "task": {} })))
      .with_priority(1)
      .mount(&server)
      .await;

    let timings = TaskTimings {
      max_wait: Duration::from_millis(100),
      ..fast()
    };

    let result = super::build_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false, &timings).await;

    assert!(result.unwrap_err().to_string().contains("did not complete within 100ms"));
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());
    assert_eq!(requests(&server, "POST", "/_cancel$").await.len(), 1);
    assert_eq!(requests(&server, "DELETE", ".*").await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_fails_when_task_was_cancelled() {
    let (server, provider) = cluster(None, 10).await;

    Mock::given(method("GET"))
      .and(path("/_tasks/node:1"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({
        "completed": true,
        "task": {},
        "response": { "timed_out": false, "total": 0, "created": 0, "failures": [], "canceled": "by user request" }
      })))
      .with_priority(1)
      .mount(&server)
      .await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await;

    assert!(result.unwrap_err().to_string().contains("cancelled (by user request)"));
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());
    assert_eq!(requests(&server, "DELETE", ".*").await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_fails_when_task_copied_partially() {
    let (server, provider) = cluster(None, 10).await;

    Mock::given(method("GET"))
      .and(path("/_tasks/node:1"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({
        "completed": true,
        "task": {},
        "response": { "timed_out": false, "total": 10, "created": 7, "failures": [] }
      })))
      .with_priority(1)
      .mount(&server)
      .await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await;

    assert!(result.unwrap_err().to_string().contains("copied only 7 of 10"));
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());
    assert_eq!(requests(&server, "DELETE", ".*").await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_fails_when_index_misses_documents() {
    let (server, provider) = cluster(None, 10).await;

    Mock::given(path_regex(r"^/motiva-[a-z0-9]{8}/_count$"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "count": 7 })))
      .with_priority(1)
      .mount(&server)
      .await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await;

    assert!(result.unwrap_err().to_string().contains("holds 7 documents, but the reindex reported 10"));
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());
    assert!(requests(&server, "POST", "^/_tasks/node:1/_cancel$").await.is_empty());
    assert_eq!(requests(&server, "DELETE", ".*").await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_fails_when_count_fails() {
    let (server, provider) = cluster(None, 10).await;

    Mock::given(path_regex(r"^/motiva-[a-z0-9]{8}/_count$"))
      .respond_with(ResponseTemplate::new(503).set_body_json(json!({})))
      .with_priority(1)
      .mount(&server)
      .await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await;

    assert!(result.unwrap_err().to_string().contains("counting documents"));
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());
    assert_eq!(requests(&server, "DELETE", ".*").await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_fails_when_alias_lookup_fails() {
    let (server, provider) = cluster(None, 10).await;

    Mock::given(method("GET"))
      .and(path("/_alias/yente-motiva-scoped-entities"))
      .respond_with(ResponseTemplate::new(500).set_body_json(json!({})))
      .with_priority(1)
      .mount(&server)
      .await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await;

    assert!(result.unwrap_err().to_string().contains("fetching scoped alias returned 500"));
    assert!(requests(&server, "PUT", NEW_INDEX).await.is_empty());
    assert!(requests(&server, "POST", "^/_reindex$").await.is_empty());
  }

  #[cfg(feature = "aws")]
  #[tokio::test]
  async fn create_scoped_index_is_rejected_on_serverless() {
    let (server, mut provider) = cluster(None, 10).await;

    provider.serverless = true;

    let before = server.received_requests().await.unwrap().len();
    let err = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await.unwrap_err();

    assert!(err.to_string().contains("not supported on OpenSearch Serverless"));
    assert_eq!(server.received_requests().await.unwrap().len(), before);
  }

  #[tokio::test]
  async fn create_scoped_index_merges_segments_before_swapping() {
    let (server, provider) = cluster(None, 10).await;

    create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await.unwrap();

    let received = server.received_requests().await.unwrap();
    let position = |suffix: &str| received.iter().position(|request| request.url.path().ends_with(suffix)).unwrap();

    let merge = &received[position("/_forcemerge")];
    let created = requests(&server, "PUT", NEW_INDEX).await[0].url.path().to_string();

    assert_eq!(merge.url.path(), format!("{created}/_forcemerge"));
    assert!(merge.url.query().unwrap().contains("max_num_segments=1"));
    assert!(position("/_reindex") < position("/_forcemerge"));
    assert!(position("/_forcemerge") < position("/_refresh"));
    assert!(position("/_refresh") < position("/_aliases"));
  }

  #[tokio::test]
  async fn create_scoped_index_succeeds_when_merge_fails() {
    let (server, provider) = cluster(None, 10).await;

    Mock::given(method("POST"))
      .and(path_regex(r"/_forcemerge$"))
      .respond_with(ResponseTemplate::new(500).set_body_json(json!({})))
      .with_priority(1)
      .mount(&server)
      .await;

    create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await.unwrap();

    assert_eq!(requests(&server, "POST", "/_forcemerge$").await.len(), 1);
    assert_eq!(requests(&server, "POST", "^/_aliases$").await.len(), 1);
    assert!(requests(&server, "DELETE", ".*").await.is_empty());
  }

  #[tokio::test]
  async fn create_scoped_index_succeeds_when_merge_fails_on_some_shards() {
    let (server, provider) = cluster(None, 10).await;

    Mock::given(method("POST"))
      .and(path_regex(r"/_forcemerge$"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "_shards": { "total": 2, "successful": 1, "failed": 1 } })))
      .with_priority(1)
      .mount(&server)
      .await;

    create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await.unwrap();

    assert_eq!(requests(&server, "GET", "/_refresh$").await.len(), 1);
    assert_eq!(requests(&server, "POST", "^/_aliases$").await.len(), 1);
    assert!(requests(&server, "DELETE", ".*").await.is_empty());
  }

  #[tokio::test]
  async fn create_scoped_index_is_skipped_when_up_to_date() {
    let previous = meta(&["yente-entities-sanctions", "yente-entities-default"], &["properties.notes"]);
    let (server, provider) = cluster(Some(previous), 10).await;

    create_scoped_index(&provider, json!({ "match_all": {} }), &strings(&["notes"]), &strings(&["yente-entities-internal_*"]), false)
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

      create_scoped_index(&provider, json!({ "match_all": {} }), &strings(&excludes), &strings(&skip_sources), false)
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

    create_scoped_index(
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

    create_scoped_index(&provider, json!({ "match_all": {} }), &strings(&["notes"]), &strings(&["yente-entities-internal_*"]), true)
      .await
      .unwrap();

    assert_eq!(requests(&server, "POST", "^/_reindex$").await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_is_rebuilt_without_previous_meta() {
    let (server, provider) = cluster(Some(json!({ "last_export": "2026-10-15T00:00:00" })), 10).await;

    create_scoped_index(&provider, json!({ "match_all": {} }), &strings(&["notes"]), &strings(&["yente-entities-internal_*"]), false)
      .await
      .unwrap();

    assert_eq!(requests(&server, "POST", "^/_reindex$").await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_fails_when_every_source_is_skipped() {
    let (server, provider) = cluster(None, 10).await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &strings(&["*"]), false).await;

    assert!(result.unwrap_err().to_string().contains("found no source index"));
    assert!(requests(&server, "PUT", NEW_INDEX).await.is_empty());
  }

  #[tokio::test]
  async fn create_scoped_index_fails_with_invalid_skip_pattern() {
    let (server, provider) = cluster(None, 10).await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &strings(&["["]), false).await;

    assert!(result.unwrap_err().to_string().contains("invalid source skip pattern"));
    assert!(requests(&server, "PUT", NEW_INDEX).await.is_empty());
  }

  #[tokio::test]
  async fn create_scoped_index_cleans_up_after_empty_reindex() {
    let (server, provider) = cluster(Some(json!({})), 0).await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await;

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
            "query": r#"{"term":{"topics":"sanction"}}"#,
            "excludes": ["properties.sourceUrl"],
            "skip_sources": ["yente-entities-internal_*"],
          }
        }
      }
    });

    let expected = ScopedMeta {
      sources: vec!["yente-entities-a".into(), "yente-entities-b".into()],
      query: r#"{"term":{"topics":"sanction"}}"#.into(),
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

    // Queries were stored as objects before, which Elasticsearch may rewrite.
    let object_query = json!({ "sources": [], "query": { "match_all": {} }, "excludes": [], "skip_sources": [] });

    assert_eq!(super::parse_scoped_meta(&json!({ "mappings": { "_meta": { "motiva": object_query } } })), None);
  }

  #[test]
  fn serialized_query_is_stable() {
    let lhs: serde_json::Value = serde_json::from_str(r#"{"bool":{"must":[{"terms":{"schema":["Person"]}}],"minimum_should_match":1}}"#).unwrap();
    let rhs: serde_json::Value = serde_json::from_str(
      r#"{ "bool": { "minimum_should_match": 1,
           "must": [ { "terms": { "schema": [ "Person" ] } } ] } }"#,
    )
    .unwrap();

    // Object keys are sorted and output is compact, so formatting does not
    // matter. Array order is meaningful and kept.
    assert_eq!(lhs.to_string(), r#"{"bool":{"minimum_should_match":1,"must":[{"terms":{"schema":["Person"]}}]}}"#);
    assert_eq!(lhs.to_string(), rhs.to_string());
  }

  #[test]
  fn changed_inputs() {
    let meta = || ScopedMeta {
      sources: vec!["yente-entities-default".into()],
      query: r#"{"match_all":{}}"#.into(),
      excludes: vec!["properties.notes".into()],
      skip_sources: vec![],
    };

    assert!(super::changed_inputs(&meta(), &meta()).is_empty());

    let current = ScopedMeta {
      sources: vec!["yente-entities-sanctions".into()],
      excludes: vec![],
      ..meta()
    };

    assert_eq!(super::changed_inputs(&meta(), &current), vec!["sources", "excludes"]);
  }
}
