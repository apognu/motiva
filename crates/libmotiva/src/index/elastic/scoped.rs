use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::time::{Duration, Instant};

use anyhow::Context;
use bytes::Bytes;
use futures::future::try_join;
use futures::{TryStreamExt, stream};
use opensearch::cluster::ClusterHealthParts;
use opensearch::http::{
  Method,
  headers::{CONTENT_TYPE, HeaderMap, HeaderValue},
  response::Response,
};
use opensearch::indices::{
  IndicesCreateParts, IndicesDeleteParts, IndicesForcemergeParts, IndicesGetAliasParts, IndicesGetMappingParts, IndicesGetParts, IndicesPutSettingsParts, IndicesRefreshParts,
};
use opensearch::params::WaitForEvents;
use opensearch::{ClearScrollParts, CountParts, ScrollParts, SearchParts};
use rand::distr::{Alphanumeric, SampleString};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{json, value::RawValue};
use tokio::sync::mpsc;

use crate::ElasticsearchProvider;

type IndexResponse = BTreeMap<String, Index>;

/// How long the cluster keeps a scroll open between two pages. This must outlast
/// the time a page spends being written, retries included.
const SCROLL_KEEP_ALIVE: &str = "5m";

const PAGE_FILTER: &[&str] = &["_scroll_id", "hits.total.value", "hits.hits._id", "hits.hits._source", "hits.hits.fields"];
const BULK_FILTER: &str = "errors,items.*.status,items.*.error";

/// How long a single page or batch may take.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Longest delay between two attempts at the same request.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// How often to report progress while copying.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(60);

/// How long to wait for replicas of the new index before swapping to it anyway.
const REPLICA_TIMEOUT: &str = "15m";

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Index {
  mappings: serde_json::Value,
  settings: serde_json::Value,
}

/// How documents are copied into the scoped index.
#[derive(Clone, Debug)]
struct CopyOptions {
  /// How many documents are read and written at a time.
  batch_size: usize,
  /// How many batches may be written at the same time.
  write_concurrency: usize,
  /// Delay before the first retry of a request, doubled on every attempt.
  retry_delay: Duration,
  /// How many times a request may be attempted before giving up.
  max_attempts: u32,
}

impl Default for CopyOptions {
  fn default() -> Self {
    Self {
      batch_size: 5000,
      write_concurrency: 4,
      retry_delay: Duration::from_secs(1),
      max_attempts: 8,
    }
  }
}

/// One page of source documents, borrowed from the raw response so documents do
/// not have to be parsed or duplicated on their way to the new index.
#[derive(Debug, Deserialize)]
struct Page<'a> {
  #[serde(rename = "_scroll_id")]
  scroll_id: Option<String>,
  #[serde(borrow, default)]
  hits: Hits<'a>,
}

#[derive(Debug, Default, Deserialize)]
struct Hits<'a> {
  total: Option<HitsTotal>,
  #[serde(borrow, default)]
  hits: Vec<Hit<'a>>,
}

#[derive(Debug, Deserialize)]
struct HitsTotal {
  value: u64,
}

#[derive(Debug, Deserialize)]
struct Hit<'a> {
  #[serde(rename = "_id", borrow)]
  id: &'a RawValue,
  #[serde(rename = "_source", borrow)]
  source: &'a RawValue,
  #[serde(borrow, default)]
  fields: Option<&'a RawValue>,
}

#[derive(Debug, Deserialize)]
struct BulkResponse {
  errors: bool,
  #[serde(default)]
  items: Vec<BTreeMap<String, BulkItem>>,
}

#[derive(Debug, Deserialize)]
struct BulkItem {
  status: u16,
  error: Option<serde_json::Value>,
}

/// A page of source documents, as the body of a bulk request.
struct Batch {
  body: Bytes,
  documents: u64,
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

/// Why a request failed.
struct RequestError {
  error: anyhow::Error,
  /// Whether the request may succeed if retried.
  transient: bool,
}

impl RequestError {
  fn transient(err: impl Into<anyhow::Error>) -> Self {
    Self { error: err.into(), transient: true }
  }
}

impl From<RequestError> for anyhow::Error {
  fn from(err: RequestError) -> Self {
    err.error
  }
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
  build_scoped_index(provider, query, excludes, skip_sources, force, &CopyOptions::default()).await
}

async fn build_scoped_index(provider: &ElasticsearchProvider, query: serde_json::Value, excludes: &[String], skip_sources: &[String], force: bool, options: &CopyOptions) -> Result<(), anyhow::Error> {
  // Building a scoped index relies on the refresh and force merge APIs, which OpenSearch Serverless does not expose.
  if provider.is_serverless() {
    anyhow::bail!("scoped indexes are not supported on OpenSearch Serverless");
  }

  let skip_patterns = skip_sources
    .iter()
    .map(|pattern| glob::Pattern::new(pattern).with_context(|| format!("invalid source skip pattern `{pattern}`")))
    .collect::<Result<Vec<_>, _>>()?;

  let old_index = find_previous_index(provider).await?;

  let previous_meta = match &old_index {
    Some(index) => fetch_scoped_meta(provider, index).await,
    None => None,
  };

  let indices: IndexResponse = checked(provider.es.indices().get(IndicesGetParts::Index(&[&provider.main_index])).send(), "fetching source indices")
    .await?
    .json()
    .await?;
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

  let new_index = format!("motiva-{}", Alphanumeric.sample_string(&mut rand::rng(), 8).to_lowercase());
  let (index_only, final_settings) = create_index(provider, &new_index, indices, &meta).await?;

  tracing::info!(index = new_index, sources = ?meta.sources, fields = ?index_only, "created new index, copying data");

  let result: Result<(), anyhow::Error> = async {
    let copied = copy_documents(provider, &meta, &query, &index_only, &new_index, options).await?;

    // A failed merge only costs performance, so it is not fatal.
    if let Err(error) = force_merge(provider, &new_index).await {
      tracing::warn!(index = new_index, %error, "could not merge index segments");
    }

    // Make everything written so far visible to searches, and use merged segments.
    checked(provider.es.indices().refresh(IndicesRefreshParts::Index(&[&new_index])).send(), &format!("refreshing {new_index}")).await?;

    check_document_count(provider, &new_index, copied).await?;

    checked(
      provider.es.indices().put_settings(IndicesPutSettingsParts::Index(&[&new_index])).body(&final_settings).send(),
      &format!("updating settings of {new_index}"),
    )
    .await?;

    wait_for_replicas(provider, &new_index).await;
    swap_alias(provider, &new_index, old_index.as_deref()).await
  }
  .await;

  if let Err(err) = result {
    delete_index(provider, &new_index).await;

    return Err(err);
  }

  tracing::info!(from = old_index, to = new_index, "atomically swapped index");

  if let Some(old_index) = old_index {
    delete_index(provider, &old_index).await;
  }

  Ok(())
}

/// Send a request, and fail on anything but a successful response.
async fn checked(request: impl Future<Output = Result<Response, opensearch::Error>>, action: &str) -> Result<Response, RequestError> {
  check_status(request.await.map_err(RequestError::transient)?, action).await
}

async fn check_status(response: Response, action: &str) -> Result<Response, RequestError> {
  let status = response.status_code();

  if status == StatusCode::OK {
    return Ok(response);
  }

  Err(RequestError {
    error: anyhow::anyhow!("{action} returned {status}: {}", response.text().await.unwrap_or_default()),
    transient: is_transient(status),
  })
}

/// Whether a request that got this status may succeed if retried.
fn is_transient(status: StatusCode) -> bool {
  matches!(
    status,
    StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS | StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
  )
}

/// Run a request until it succeeds, backing off after every transient failure.
async fn with_retries<T, F, R>(options: &CopyOptions, action: &str, mut request: F) -> Result<T, anyhow::Error>
where
  F: FnMut() -> R,
  R: Future<Output = Result<T, RequestError>>,
{
  let mut attempt = 1;
  let mut delay = options.retry_delay;

  loop {
    match request().await {
      Ok(value) => return Ok(value),
      Err(RequestError { error, transient: false }) => return Err(error),
      Err(RequestError { error, .. }) if attempt >= options.max_attempts => return Err(error.context(format!("could not {action} after {attempt} attempts"))),

      Err(RequestError { error, .. }) => {
        tracing::warn!(action, %error, attempt, "request failed, retrying");

        tokio::time::sleep(delay).await;

        attempt += 1;
        delay = (delay * 2).min(MAX_RETRY_DELAY);
      }
    }
  }
}

/// Find the index the scoped alias currently points to.
///
/// Anything but a missing alias must fail, or the swap would add the new index
/// without removing the previous one, and the alias would then span both.
async fn find_previous_index(provider: &ElasticsearchProvider) -> Result<Option<String>, anyhow::Error> {
  let response = provider.es.indices().get_alias(IndicesGetAliasParts::Name(&[&provider.scoped_alias_name()])).send().await?;

  if response.status_code() == StatusCode::NOT_FOUND {
    return Ok(None);
  }

  let aliases: HashMap<String, serde_json::Value> = check_status(response, "fetching scoped alias").await?.json().await?;
  let index = aliases.into_keys().next().context("found no alias")?;

  tracing::info!(index, "found previous scoped index");

  Ok(Some(index))
}

/// Create the new index from the first source, set up for a bulk load.
///
/// Returns the fields to copy from doc values, and the settings the index is to
/// be served with once it is built.
async fn create_index(provider: &ElasticsearchProvider, name: &str, sources: Vec<(String, Index)>, meta: &ScopedMeta) -> Result<(Vec<String>, serde_json::Value), anyhow::Error> {
  let mut sources = sources.into_iter();

  let (_, mut index) = sources.next().ok_or_else(|| anyhow::anyhow!("found no source index in {}", provider.main_index))?;

  // The new index gets the mapping of the first source, so it only knows of its fields.
  let index_only = index_only_fields(&index.mappings);

  for (source, other) in sources {
    let unmapped = index_only_fields(&other.mappings).into_iter().filter(|field| !index_only.contains(field)).collect::<Vec<_>>();

    if !unmapped.is_empty() {
      tracing::warn!(index = source, fields = ?unmapped, "source index has index-only fields the scoped index does not map, they will not be copied");
    }
  }

  strip_source_settings(&mut index.settings)?;

  let final_settings = apply_build_settings(&mut index.settings)?;

  index.mappings.as_object_mut().context("no mappings")?.insert("_meta".into(), json!({ "motiva": meta }));

  checked(provider.es.indices().create(IndicesCreateParts::Index(name)).body(index).send(), "index creation").await?;

  Ok((index_only, final_settings))
}

/// Remove the settings that describe the source index rather than how to build
/// one, which an index cannot be created with.
///
/// That includes what is left on an index created by cloning another one, which
/// yente does for datasets that did not change, and write blocks.
fn strip_source_settings(settings: &mut serde_json::Value) -> Result<(), anyhow::Error> {
  if let Some(allocation) = settings.pointer_mut("/index/routing/allocation").and_then(serde_json::Value::as_object_mut) {
    allocation.remove("initial_recovery");
  }

  let nested = settings.get_mut("index").and_then(serde_json::Value::as_object_mut).context("no settings")?;

  for setting in ["uuid", "provided_name", "version", "creation_date", "resize", "blocks"] {
    nested.remove(setting);
  }

  Ok(())
}

/// Set up the new index for a one-time bulk load, returning the settings it is to
/// be served with.
///
/// Nothing reads the index while it is being built, and a failed build is thrown
/// away, so it does not need refreshes, durable writes or replicas. Replicas are
/// instead copied from the merged primary, rather than built and merged twice.
fn apply_build_settings(settings: &mut serde_json::Value) -> Result<serde_json::Value, anyhow::Error> {
  let nested = settings.get_mut("index").and_then(serde_json::Value::as_object_mut).context("no settings")?;

  // Sources can expand replicas to all nodes, which would override their number.
  let auto_expand = nested.insert("auto_expand_replicas".into(), json!("false"));
  let replicas = nested.insert("number_of_replicas".into(), json!("0"));
  let refresh = nested.insert("refresh_interval".into(), json!("-1"));
  let translog = nested.entry("translog").or_insert_with(|| json!({})).as_object_mut().context("invalid translog settings")?;
  let durability = translog.insert("durability".into(), json!("async"));

  // A setting the source did not have goes back to its default, as if it was never set.
  Ok(json!({ "index": { "auto_expand_replicas": auto_expand, "number_of_replicas": replicas, "refresh_interval": refresh, "translog.durability": durability } }))
}

/// Copy the documents matching the scoped query into the new index, returning how
/// many were copied.
///
/// The `_reindex` API cannot be used here: it rebuilds documents from `_source`,
/// and yente excludes the fields matching relies on (such as `name_parts`) from
/// it. Those fields are only left in the inverted index and in doc values, which
/// neither reindex scripts nor ingest pipelines can read, so they are fetched from
/// doc values here and written back with each document.
///
/// Documents are read in index order through a scroll: any other order makes the
/// cluster jump around its stored fields and decompress a block for each document,
/// which is what `search_after` on an identifier would do. The scroll also holds on
/// to the source indices it started from if they are replaced while copying.
///
/// Pages are turned into bulk requests without parsing the documents, and written
/// while the next ones are read, as the cluster should never be left waiting for
/// them. Only one page can wait to be written, which keeps memory bounded to a few
/// batches however far ahead of the cluster reading gets.
async fn copy_documents(provider: &ElasticsearchProvider, meta: &ScopedMeta, query: &serde_json::Value, index_only: &[String], target: &str, options: &CopyOptions) -> Result<u64, anyhow::Error> {
  let started = Instant::now();

  let mut scroll = Scroll {
    provider,
    options,
    sources: meta.sources.iter().map(String::as_str).collect(),
    search: json!({
      "size": options.batch_size,
      "sort": ["_doc"],
      "query": query,
      "track_total_hits": true,
      // Listing the fields here too makes sure they cannot be in `_source` twice.
      "_source": { "excludes": meta.excludes.iter().chain(index_only).collect::<Vec<_>>() },
      "docvalue_fields": index_only,
    }),
    id: None,
    total: 0,
  };

  let (sender, receiver) = mpsc::channel(1);

  let result = try_join(read_batches(&mut scroll, target, sender), write_batches(provider, target, options, receiver)).await;

  scroll.clear().await;

  let (copied, ()) = result?;

  if scroll.total == 0 {
    anyhow::bail!("found no documents in {} matching the scoped query", provider.main_index);
  }

  if copied != scroll.total {
    anyhow::bail!("copied only {copied} of {} documents into {target}", scroll.total);
  }

  tracing::info!(index = target, documents = copied, elapsed = ?started.elapsed(), "copied data");

  Ok(copied)
}

/// Read source documents until there are none left, handing them to `batches`.
/// Returns how many were read.
async fn read_batches(scroll: &mut Scroll<'_>, target: &str, batches: mpsc::Sender<Bytes>) -> Result<u64, anyhow::Error> {
  let started = Instant::now();
  let mut last_progress = started;
  let mut read = 0;

  while let Some(batch) = scroll.next_batch().await? {
    read += batch.documents;

    // Writing stopping ends the copy with its own error, which is what gets reported.
    batches.send(batch.body).await.context("writing documents stopped")?;

    if last_progress.elapsed() >= PROGRESS_INTERVAL {
      last_progress = Instant::now();

      tracing::info!(index = target, read, total = scroll.total, elapsed = ?started.elapsed(), "still copying documents");
    }
  }

  Ok(read)
}

/// Write the batches coming out of `batches`, until it is closed.
async fn write_batches(provider: &ElasticsearchProvider, target: &str, options: &CopyOptions, batches: mpsc::Receiver<Bytes>) -> Result<(), anyhow::Error> {
  let batches = stream::unfold(batches, |mut batches| async move { Some((Ok::<_, anyhow::Error>(batches.recv().await?), batches)) });

  batches
    .try_for_each_concurrent(options.write_concurrency, |batch| async move {
      with_retries(options, "write documents", || write_batch(provider, target, &batch)).await
    })
    .await
}

/// A scroll over the source documents.
struct Scroll<'a> {
  provider: &'a ElasticsearchProvider,
  options: &'a CopyOptions,
  sources: Vec<&'a str>,
  search: serde_json::Value,
  /// Identifies the scroll to continue, once it has been opened.
  id: Option<String>,
  /// How many documents the search matched, known once the scroll has been opened.
  total: u64,
}

impl Scroll<'_> {
  /// Read the next page of documents, if there is one left.
  async fn next_batch(&mut self) -> Result<Option<Batch>, anyhow::Error> {
    // A scroll moves on as it is read, so a retried page could in theory be a lost
    // one. That shows as a document count that does not add up, which fails the build.
    let response = with_retries(self.options, "read source documents", || self.fetch_page()).await?;
    let page: Page = serde_json::from_slice(&response).context("could not parse source documents")?;

    if let Some(id) = page.scroll_id {
      self.id = Some(id);
    }

    if let Some(total) = page.hits.total {
      self.total = total.value;
    }

    // Only an empty page shows that there is nothing left.
    if page.hits.hits.is_empty() {
      return Ok(None);
    }

    let mut body = Vec::with_capacity(response.len());

    for hit in &page.hits.hits {
      write_document(&mut body, hit)?;
    }

    Ok(Some(Batch {
      body: body.into(),
      documents: page.hits.hits.len() as u64,
    }))
  }

  /// Open the scroll, or read its next page if it already is.
  async fn fetch_page(&self) -> Result<Bytes, RequestError> {
    let es = &self.provider.es;

    let response = match &self.id {
      // A partial page would otherwise silently leave documents behind.
      None => {
        let request = es
          .search(SearchParts::Index(&self.sources))
          .scroll(SCROLL_KEEP_ALIVE)
          .allow_partial_search_results(false)
          .filter_path(PAGE_FILTER)
          .request_timeout(REQUEST_TIMEOUT)
          .body(&self.search)
          .send();

        checked(request, "reading documents").await?
      }

      Some(id) => {
        let body = json!({ "scroll": SCROLL_KEEP_ALIVE, "scroll_id": id });
        let request = es.scroll(ScrollParts::None).filter_path(PAGE_FILTER).request_timeout(REQUEST_TIMEOUT).body(body).send();

        checked(request, "reading documents").await?
      }
    };

    response.bytes().await.map_err(RequestError::transient)
  }

  /// Release the scroll, which would otherwise only go away after its keep-alive.
  async fn clear(&self) {
    let Some(id) = &self.id else {
      return;
    };

    let request = self.provider.es.clear_scroll(ClearScrollParts::None).body(json!({ "scroll_id": [id] })).send();

    if let Err(RequestError { error, .. }) = checked(request, "clearing scroll").await {
      tracing::warn!(%error, "could not clear scroll");
    }
  }
}

/// Append the bulk action for a document, merging its doc values into its source.
fn write_document(body: &mut Vec<u8>, hit: &Hit<'_>) -> Result<(), anyhow::Error> {
  let source = compact(hit.source.get())?;
  let fields = hit.fields.map(|fields| compact(fields.get())).transpose()?;

  let source = object_members(&source).context("source document is not an object")?;
  let fields = fields.as_deref().and_then(object_members).unwrap_or_default();
  let separator = if source.is_empty() || fields.is_empty() { "" } else { "," };

  body.extend_from_slice(br#"{"index":{"_id":"#);
  body.extend_from_slice(hit.id.get().as_bytes());
  body.extend_from_slice(b"}}\n{");
  body.extend_from_slice(source.as_bytes());
  body.extend_from_slice(separator.as_bytes());
  body.extend_from_slice(fields.as_bytes());
  body.extend_from_slice(b"}\n");

  Ok(())
}

/// What is between the braces of a raw JSON object.
fn object_members(raw: &str) -> Option<&str> {
  Some(raw.strip_prefix('{')?.strip_suffix('}')?.trim())
}

/// Remove line breaks from JSON, which would otherwise end a bulk action.
///
/// Responses are compact unless the cluster is configured otherwise, and line
/// breaks in strings are escaped, so this very rarely has anything to do.
fn compact(raw: &str) -> Result<Cow<'_, str>, anyhow::Error> {
  if !raw.contains('\n') {
    return Ok(Cow::Borrowed(raw));
  }

  Ok(Cow::Owned(serde_json::from_str::<serde_json::Value>(raw).context("could not parse source document")?.to_string()))
}

/// Write a batch of documents.
///
/// Documents are written with their source identifier, so a batch can safely be
/// sent again as a whole. The bulk builder is not used because it copies its body.
async fn write_batch(provider: &ElasticsearchProvider, index: &str, body: &Bytes) -> Result<(), RequestError> {
  let headers = HeaderMap::from_iter([(CONTENT_TYPE, HeaderValue::from_static("application/x-ndjson"))]);
  let path = format!("/{index}/_bulk");
  let query = [("filter_path", BULK_FILTER)];

  let request = provider.es.send(Method::Post, &path, headers, Some(&query), Some(body.clone()), Some(REQUEST_TIMEOUT));
  let response = checked(request, "writing documents").await?;

  check_bulk_response(response.json().await.map_err(RequestError::transient)?)
}

/// Check that a batch was written in full, a document failing for any other
/// reason than the load on the cluster will fail the same way if sent again.
fn check_bulk_response(response: BulkResponse) -> Result<(), RequestError> {
  if !response.errors {
    return Ok(());
  }

  let failures = response
    .items
    .iter()
    .flat_map(BTreeMap::values)
    .filter(|item| item.error.is_some() || item.status >= 300)
    .collect::<Vec<_>>();

  let error = match failures.first() {
    Some(first) => anyhow::anyhow!("could not write some documents: {} {:?}", first.status, first.error),
    None => anyhow::anyhow!("writing documents reported errors, but no failed document"),
  };

  Err(RequestError {
    error,
    transient: !failures.is_empty() && failures.iter().all(|item| StatusCode::from_u16(item.status).is_ok_and(is_transient)),
  })
}

/// Check that the new index holds as many documents as were copied, so the alias
/// only moves to an index whose data is actually there.
///
/// This also catches an identifier found in two source indices, as writing the
/// second document replaces the first.
async fn check_document_count(provider: &ElasticsearchProvider, index: &str, expected: u64) -> Result<(), anyhow::Error> {
  let response = checked(provider.es.count(CountParts::Index(&[index])).send(), &format!("counting documents in {index}")).await?;
  let count: CountResult = response.json().await.context("could not parse document count")?;

  if count.count != expected {
    anyhow::bail!("{index} holds {} documents, but {expected} were copied", count.count);
  }

  Ok(())
}

/// Merge the freshly built index down to a single segment.
///
/// Fuzzy and term lookups run once per segment, so a single segment roughly
/// halves query latency on a static index. The only deleted documents the index
/// can hold come from batches written again, which merging removes.
async fn force_merge(provider: &ElasticsearchProvider, index: &str) -> Result<(), anyhow::Error> {
  let result: ForcemergeResult = checked(provider.es.indices().forcemerge(IndicesForcemergeParts::Index(&[index])).max_num_segments(1).send(), "merging segments")
    .await?
    .json()
    .await?;

  if result.shards.failed > 0 {
    anyhow::bail!("merging failed on {} of {} shards", result.shards.failed, result.shards.total);
  }

  tracing::info!(index, "merged index into a single segment");

  Ok(())
}

/// Wait for the replicas of the new index to be copied.
///
/// This does not wait for the index to turn green, which it never would on a
/// cluster with fewer nodes than copies. The primary can serve queries by itself,
/// so an index that is late to replicate is not worth failing the build over.
async fn wait_for_replicas(provider: &ElasticsearchProvider, index: &str) {
  let cluster = provider.es.cluster();
  let parts = [index];

  let request = cluster
    .health(ClusterHealthParts::Index(&parts))
    .wait_for_events(WaitForEvents::Languid)
    .wait_for_no_initializing_shards(true)
    .timeout(REPLICA_TIMEOUT)
    .send();

  match checked(request, "waiting for replicas").await {
    Ok(_) => tracing::info!(index, "replicas are ready"),
    Err(RequestError { error, .. }) => tracing::warn!(index, %error, "replicas are not ready, serving from the primary until they are"),
  }
}

/// Atomically move the scoped alias to the new index.
async fn swap_alias(provider: &ElasticsearchProvider, new_index: &str, old_index: Option<&str>) -> Result<(), anyhow::Error> {
  let alias = provider.scoped_alias_name();
  let mut actions = vec![json!({ "add": { "alias": alias, "index": new_index } })];

  if let Some(old_index) = old_index {
    actions.push(json!({ "remove": { "alias": alias, "index": old_index, "must_exist": true } }));
  }

  checked(provider.es.indices().update_aliases().body(json!({ "actions": actions })).send(), "update aliases").await?;

  Ok(())
}

/// Delete an index. Failing to is not worth failing over, as the build it belongs
/// to has already settled.
async fn delete_index(provider: &ElasticsearchProvider, index: &str) {
  match checked(provider.es.indices().delete(IndicesDeleteParts::Index(&[index])).send(), "deleting index").await {
    Ok(_) => tracing::info!(index, "deleted index"),
    Err(RequestError { error, .. }) => tracing::warn!(index, %error, "could not delete index, it may need manual cleanup"),
  }
}

/// List the fields that matching relies on but that are not in `_source`, and
/// that must therefore be copied from doc values.
///
/// Excluded fields that are the target of a `copy_to` are left out: those are
/// filled again when their source fields are written.
fn index_only_fields(mappings: &serde_json::Value) -> Vec<String> {
  let Some(excludes) = mappings.pointer("/_source/excludes").and_then(serde_json::Value::as_array) else {
    return Vec::new();
  };

  let properties = mappings.get("properties").unwrap_or(&serde_json::Value::Null);
  let mut targets = HashSet::new();

  collect_copy_targets(properties, &mut targets);

  excludes
    .iter()
    .filter_map(serde_json::Value::as_str)
    .filter(|name| !targets.contains(name))
    .filter(|name| {
      let readable = properties.get(name).is_some_and(|field| field["type"] == "keyword" && field["doc_values"] != false);

      if !readable {
        tracing::warn!(
          field = name,
          "field is excluded from _source and has no doc values to copy it from, it will be missing from the scoped index"
        );
      }

      readable
    })
    .map(String::from)
    .collect()
}

fn collect_copy_targets<'a>(properties: &'a serde_json::Value, targets: &mut HashSet<&'a str>) {
  for field in properties.as_object().into_iter().flat_map(serde_json::Map::values) {
    match field.get("copy_to") {
      Some(serde_json::Value::String(target)) => {
        targets.insert(target);
      }

      Some(serde_json::Value::Array(values)) => targets.extend(values.iter().filter_map(serde_json::Value::as_str)),
      _ => {}
    }

    if let Some(nested) = field.get("properties") {
      collect_copy_targets(nested, targets);
    }
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
    Mock, MockServer, Request, Respond, ResponseTemplate,
    matchers::{method, path, path_regex},
  };

  use super::{CopyOptions, Hit, ScopedMeta};
  use crate::{ElasticsearchProvider, index::elastic::config::EsOptions};

  const NEW_INDEX: &str = r"^/motiva-[a-z0-9]{8}$";
  const SEARCH: &str = "/_search(/scroll)?$";
  const BULK: &str = r"^/motiva-[a-z0-9]{8}/_bulk$";
  const SETTINGS: &str = r"^/motiva-[a-z0-9]{8}/_settings$";
  const HEALTH: &str = r"^/_cluster/health/motiva-[a-z0-9]{8}$";

  /// Options that keep batches small and retries fast.
  fn fast() -> CopyOptions {
    CopyOptions {
      batch_size: 4,
      write_concurrency: 1,
      retry_delay: Duration::from_millis(1),
      max_attempts: 3,
    }
  }

  async fn create_scoped_index(provider: &ElasticsearchProvider, query: serde_json::Value, excludes: &[String], skip_sources: &[String], force: bool) -> Result<(), anyhow::Error> {
    super::build_scoped_index(provider, query, excludes, skip_sources, force, &fast()).await
  }

  fn source_index(name: &str) -> serde_json::Value {
    json!({
      "aliases": { "yente-entities": {} },
      "mappings": {
        "_meta": { "last_export": "2026-10-15T00:00:00" },
        "_source": { "excludes": ["name_parts", "name_phonetic", "text"] },
        "properties": {
          "name_parts": { "type": "keyword", "copy_to": "text" },
          "name_phonetic": { "type": "keyword" },
          "text": { "type": "text" },
        }
      },
      "settings": { "index": { "uuid": "uuid", "provided_name": name, "creation_date": "0", "version": { "created": "1" }, "number_of_shards": "1", "number_of_replicas": "1", "auto_expand_replicas": "0-all", "refresh_interval": "10s" } }
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

  fn hit(id: &str) -> serde_json::Value {
    json!({ "_id": id, "_source": { "id": id }, "fields": { "name_parts": [id] } })
  }

  fn hits(count: u64) -> Vec<serde_json::Value> {
    (0..count).map(|i| format!("doc-{i:03}")).map(|id| hit(&id)).collect()
  }

  /// Source documents served the way a scroll pages through them, the scroll
  /// identifier holding the position and page size.
  struct Source {
    total: u64,
    hits: Vec<serde_json::Value>,
  }

  impl Respond for Source {
    fn respond(&self, request: &Request) -> ResponseTemplate {
      let body: serde_json::Value = request.body_json().unwrap();

      let (offset, size) = match body["scroll_id"].as_str() {
        Some(id) => id.split_once(':').map(|(offset, size)| (offset.parse().unwrap(), size.parse().unwrap())).unwrap(),
        None => (0, body["size"].as_u64().unwrap() as usize),
      };

      let hits = self.hits.iter().skip(offset).take(size).cloned().collect::<Vec<_>>();
      let next = offset + hits.len();

      ResponseTemplate::new(200).set_body_json(json!({ "_scroll_id": format!("{next}:{size}"), "hits": { "total": { "value": self.total }, "hits": hits } }))
    }
  }

  async fn serve_source(server: &MockServer, total: u64, hits: Vec<serde_json::Value>) {
    Mock::given(method("POST"))
      .and(path_regex(SEARCH))
      .respond_with(Source { total, hits })
      .with_priority(1)
      .mount(server)
      .await;
  }

  /// Mock a cluster with three source indices holding `total` matching documents,
  /// and a previous scoped index built with `previous_meta`, if any.
  async fn cluster(previous_meta: Option<serde_json::Value>, total: u64) -> (MockServer, ElasticsearchProvider) {
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
      .and(path_regex(SEARCH))
      .respond_with(Source { total, hits: hits(total) })
      .with_priority(2)
      .mount(&server)
      .await;

    Mock::given(method("POST"))
      .and(path_regex(BULK))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "errors": false })))
      .with_priority(2)
      .mount(&server)
      .await;

    Mock::given(method("POST"))
      .and(path_regex(r"^/motiva-[a-z0-9]{8}/_forcemerge$"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "_shards": { "total": 1, "successful": 1, "failed": 0 } })))
      .mount(&server)
      .await;

    Mock::given(path_regex(r"^/motiva-[a-z0-9]{8}/_refresh$"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "_shards": { "total": 1, "successful": 1, "failed": 0 } })))
      .mount(&server)
      .await;

    Mock::given(path_regex(r"^/motiva-[a-z0-9]{8}/_count$"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "count": total })))
      .mount(&server)
      .await;

    Mock::given(method("PUT"))
      .and(path_regex(SETTINGS))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "acknowledged": true })))
      .with_priority(2)
      .mount(&server)
      .await;

    Mock::given(method("GET"))
      .and(path_regex(HEALTH))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "status": "green" })))
      .with_priority(2)
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

  /// List the identifiers written by every bulk request received, in order.
  async fn written(server: &MockServer) -> Vec<String> {
    requests(server, "POST", BULK)
      .await
      .iter()
      .flat_map(|request| {
        String::from_utf8(request.body.clone())
          .unwrap()
          .lines()
          .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
          .filter_map(|line| line.pointer("/index/_id").and_then(|id| id.as_str().map(String::from)))
          .collect::<Vec<_>>()
      })
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

    let search = &requests(&server, "POST", SEARCH).await[0];
    let body: serde_json::Value = search.body_json().unwrap();

    assert!(search.url.path().contains("yente-entities-default"));
    assert!(search.url.path().contains("yente-entities-sanctions"));
    assert!(!search.url.path().contains("internal_watchlist"));
    assert_eq!(body["query"], json!({ "match_all": {} }));
    assert_eq!(body["_source"]["excludes"], json!(["properties.notes", "properties.sourceUrl", "name_parts", "name_phonetic"]));

    let create: serde_json::Value = requests(&server, "PUT", NEW_INDEX).await[0].body_json().unwrap();

    assert_eq!(
      create["mappings"]["_meta"],
      json!({ "motiva": meta(&["yente-entities-default", "yente-entities-sanctions"], &["properties.notes", "properties.sourceUrl"]) })
    );
    assert_eq!(
      create["settings"]["index"],
      json!({ "number_of_shards": "1", "number_of_replicas": "0", "auto_expand_replicas": "false", "refresh_interval": "-1", "translog": { "durability": "async" } })
    );

    let settings: serde_json::Value = requests(&server, "PUT", SETTINGS).await[0].body_json().unwrap();

    assert_eq!(
      settings,
      json!({ "index": { "auto_expand_replicas": "0-all", "number_of_replicas": "1", "refresh_interval": "10s", "translog.durability": null } })
    );

    let aliases: serde_json::Value = requests(&server, "POST", "^/_aliases$").await[0].body_json().unwrap();

    assert_eq!(aliases["actions"].as_array().unwrap().len(), 1);
    assert_eq!(aliases["actions"][0]["add"]["alias"], "yente-motiva-scoped-entities");
  }

  #[tokio::test]
  async fn create_scoped_index_copies_index_only_fields() {
    let (server, provider) = cluster(None, 1).await;

    create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await.unwrap();

    let search: serde_json::Value = requests(&server, "POST", SEARCH).await[0].body_json().unwrap();

    // Copy targets are rebuilt by the cluster, and have no doc values anyway.
    assert_eq!(search["docvalue_fields"], json!(["name_parts", "name_phonetic"]));

    let bulk = &requests(&server, "POST", BULK).await[0];
    let lines = String::from_utf8(bulk.body.clone()).unwrap();

    assert_eq!(lines, "{\"index\":{\"_id\":\"doc-000\"}}\n{\"id\":\"doc-000\",\"name_parts\":[\"doc-000\"]}\n");
    assert_eq!(bulk.headers.get("content-type").unwrap(), "application/x-ndjson");
  }

  #[tokio::test]
  async fn create_scoped_index_pages_through_sources() {
    let (server, provider) = cluster(None, 10).await;

    create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await.unwrap();

    let searches = requests(&server, "POST", SEARCH).await;
    let first: serde_json::Value = searches[0].body_json().unwrap();

    assert_eq!(first["sort"], json!(["_doc"]));
    assert_eq!(first["size"], 4);
    assert_eq!(first["track_total_hits"], true);

    let query = searches[0].url.query().unwrap();

    assert!(query.contains("allow_partial_search_results=false"));
    assert!(query.contains("scroll=5m"));

    let scrolls = searches[1..].iter().map(|search| search.body_json::<serde_json::Value>().unwrap()).collect::<Vec<_>>();

    assert!(searches[1..].iter().all(|search| search.url.path() == "/_search/scroll"));
    assert_eq!(scrolls.iter().map(|body| body["scroll_id"].clone()).collect::<Vec<_>>(), [json!("4:4"), json!("8:4"), json!("10:4")]);
    assert!(scrolls.iter().all(|body| body["scroll"] == "5m"));

    assert_eq!(requests(&server, "POST", BULK).await.len(), 3);
    assert_eq!(written(&server).await, (0..10).map(|i| format!("doc-{i:03}")).collect::<Vec<_>>());
  }

  #[tokio::test]
  async fn create_scoped_index_clears_its_scroll() {
    let (server, provider) = cluster(None, 10).await;

    create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await.unwrap();

    let clear: serde_json::Value = requests(&server, "DELETE", "^/_search/scroll$").await[0].body_json().unwrap();

    assert_eq!(clear, json!({ "scroll_id": ["10:4"] }));
  }

  #[tokio::test]
  async fn create_scoped_index_clears_its_scroll_when_writing_fails() {
    let (server, provider) = cluster(None, 10).await;

    Mock::given(method("POST"))
      .and(path_regex(BULK))
      .respond_with(ResponseTemplate::new(400).set_body_json(json!({})))
      .with_priority(1)
      .mount(&server)
      .await;

    create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await.unwrap_err();

    assert_eq!(requests(&server, "DELETE", "^/_search/scroll$").await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_reads_ahead_of_writes_without_piling_up() {
    let (server, provider) = cluster(None, 40).await;

    Mock::given(method("POST"))
      .and(path_regex(BULK))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "errors": false })).set_delay(Duration::from_secs(2)))
      .with_priority(1)
      .mount(&server)
      .await;

    let build = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false);

    tokio::time::timeout(Duration::from_millis(500), build).await.unwrap_err();

    // One page is being written, one is waiting for it, and one is waiting to be queued.
    assert_eq!(requests(&server, "POST", SEARCH).await.len(), 3);
    assert_eq!(requests(&server, "POST", BULK).await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_writes_batches_concurrently() {
    let (server, provider) = cluster(None, 40).await;

    Mock::given(method("POST"))
      .and(path_regex(BULK))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "errors": false })).set_delay(Duration::from_secs(2)))
      .with_priority(1)
      .mount(&server)
      .await;

    let options = CopyOptions { write_concurrency: 2, ..fast() };
    let build = super::build_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false, &options);

    tokio::time::timeout(Duration::from_millis(500), build).await.unwrap_err();

    // Two pages are being written, one is waiting for them, and one is waiting to be queued.
    assert_eq!(requests(&server, "POST", SEARCH).await.len(), 4);
    assert_eq!(requests(&server, "POST", BULK).await.len(), 2);
  }

  #[tokio::test]
  async fn create_scoped_index_stops_on_an_empty_page() {
    let (server, provider) = cluster(None, 8).await;

    create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await.unwrap();

    // Only an empty page shows that there is nothing left.
    assert_eq!(requests(&server, "POST", SEARCH).await.len(), 3);
    assert_eq!(requests(&server, "POST", BULK).await.len(), 2);
    assert_eq!(written(&server).await.len(), 8);
  }

  #[tokio::test]
  async fn create_scoped_index_uses_default_options() {
    let (server, provider) = cluster(None, 10).await;

    super::create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await.unwrap();

    assert_eq!(requests(&server, "POST", SEARCH).await.len(), 2);
    assert_eq!(requests(&server, "POST", BULK).await.len(), 1);
    assert_eq!(requests(&server, "POST", "^/_aliases$").await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_retries_failed_pages() {
    let (server, provider) = cluster(None, 10).await;

    Mock::given(method("POST"))
      .and(path_regex(SEARCH))
      .respond_with(ResponseTemplate::new(429).set_body_json(json!({})))
      .with_priority(1)
      .up_to_n_times(2)
      .mount(&server)
      .await;

    create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await.unwrap();

    assert_eq!(requests(&server, "POST", SEARCH).await.len(), 6);
    assert_eq!(written(&server).await.len(), 10);
  }

  #[tokio::test]
  async fn create_scoped_index_retries_failed_batches() {
    let (server, provider) = cluster(None, 3).await;

    Mock::given(method("POST"))
      .and(path_regex(BULK))
      .respond_with(ResponseTemplate::new(503).set_body_json(json!({})))
      .with_priority(1)
      .up_to_n_times(1)
      .mount(&server)
      .await;

    create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await.unwrap();

    let batches = requests(&server, "POST", BULK).await;

    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].body, batches[1].body);
  }

  #[tokio::test]
  async fn create_scoped_index_retries_batches_rejected_under_load() {
    let (server, provider) = cluster(None, 3).await;

    Mock::given(method("POST"))
      .and(path_regex(BULK))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({
        "errors": true,
        "items": [
          { "index": { "status": 201 } },
          { "index": { "status": 429, "error": { "type": "es_rejected_execution_exception" } } },
        ]
      })))
      .with_priority(1)
      .up_to_n_times(1)
      .mount(&server)
      .await;

    create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await.unwrap();

    assert_eq!(requests(&server, "POST", BULK).await.len(), 2);
    assert_eq!(requests(&server, "POST", "^/_aliases$").await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_fails_on_rejected_documents() {
    let (server, provider) = cluster(None, 3).await;

    // A single document that cannot be written fails the batch, whatever the others did.
    Mock::given(method("POST"))
      .and(path_regex(BULK))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({
        "errors": true,
        "items": [
          { "index": { "status": 429, "error": { "type": "es_rejected_execution_exception" } } },
          { "index": { "status": 400, "error": { "type": "mapper_parsing_exception" } } },
        ]
      })))
      .with_priority(1)
      .mount(&server)
      .await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await;

    assert!(result.unwrap_err().to_string().contains("could not write some documents"));
    assert_eq!(requests(&server, "POST", BULK).await.len(), 1);
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());
    assert_eq!(requests(&server, "DELETE", NEW_INDEX).await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_fails_when_errors_are_not_detailed() {
    let (server, provider) = cluster(None, 3).await;

    Mock::given(method("POST"))
      .and(path_regex(BULK))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "errors": true })))
      .with_priority(1)
      .mount(&server)
      .await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await;

    assert!(result.unwrap_err().to_string().contains("no failed document"));
    assert_eq!(requests(&server, "POST", BULK).await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_does_not_retry_invalid_searches() {
    let (server, provider) = cluster(None, 10).await;

    // Retrying cannot help a query the cluster refuses.
    Mock::given(method("POST"))
      .and(path_regex(SEARCH))
      .respond_with(ResponseTemplate::new(404).set_body_json(json!({ "error": { "type": "index_not_found_exception" } })))
      .with_priority(1)
      .mount(&server)
      .await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await;

    assert!(result.unwrap_err().to_string().contains("index_not_found_exception"));
    assert_eq!(requests(&server, "POST", SEARCH).await.len(), 1);
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());
    assert_eq!(requests(&server, "DELETE", NEW_INDEX).await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_gives_up_after_repeated_transient_errors() {
    let (server, provider) = cluster(None, 10).await;

    Mock::given(method("POST"))
      .and(path_regex(BULK))
      .respond_with(ResponseTemplate::new(503).set_body_json(json!({})))
      .with_priority(1)
      .mount(&server)
      .await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await;

    assert!(result.unwrap_err().to_string().contains("could not write documents after 3 attempts"));
    assert_eq!(requests(&server, "POST", BULK).await.len(), 3);
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());
    assert_eq!(requests(&server, "DELETE", NEW_INDEX).await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_fails_when_documents_are_missing_from_pages() {
    let (server, provider) = cluster(None, 10).await;

    serve_source(&server, 10, hits(7)).await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await;

    assert!(result.unwrap_err().to_string().contains("copied only 7 of 10"));
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());
    assert_eq!(requests(&server, "DELETE", NEW_INDEX).await.len(), 1);
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

    assert!(result.unwrap_err().to_string().contains("holds 7 documents, but 10 were copied"));
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());
    assert_eq!(requests(&server, "DELETE", NEW_INDEX).await.len(), 1);
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
    assert_eq!(requests(&server, "DELETE", NEW_INDEX).await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_fails_when_refresh_fails() {
    let (server, provider) = cluster(None, 10).await;

    Mock::given(path_regex(r"/_refresh$"))
      .respond_with(ResponseTemplate::new(500).set_body_json(json!({})))
      .with_priority(1)
      .mount(&server)
      .await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await;

    assert!(result.unwrap_err().to_string().contains("refreshing"));
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());
    assert_eq!(requests(&server, "DELETE", NEW_INDEX).await.len(), 1);
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
    assert!(requests(&server, "POST", SEARCH).await.is_empty());
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
    let last_write = received.iter().rposition(|request| request.url.path().ends_with("/_bulk")).unwrap();

    let merge = &received[position("/_forcemerge")];
    let created = requests(&server, "PUT", NEW_INDEX).await[0].url.path().to_string();

    assert_eq!(merge.url.path(), format!("{created}/_forcemerge"));
    assert!(merge.url.query().unwrap().contains("max_num_segments=1"));
    assert!(last_write < position("/_forcemerge"));
    assert!(position("/_forcemerge") < position("/_refresh"));
    assert!(position("/_refresh") < position("/_count"));
    assert!(position("/_count") < position("/_settings"));
    assert!(position("/_settings") < received.iter().position(|request| request.url.path().contains("/_cluster/health/motiva-")).unwrap());
    assert!(received.iter().position(|request| request.url.path().contains("/_cluster/health/motiva-")).unwrap() < position("/_aliases"));
  }

  #[tokio::test]
  async fn create_scoped_index_waits_for_replicas_without_requiring_green() {
    let (server, provider) = cluster(None, 10).await;

    create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await.unwrap();

    let health = &requests(&server, "GET", HEALTH).await[0];
    let query = health.url.query().unwrap();

    assert!(query.contains("wait_for_no_initializing_shards=true"));
    assert!(!query.contains("wait_for_status"));
  }

  #[tokio::test]
  async fn create_scoped_index_succeeds_when_replicas_are_late() {
    let (server, provider) = cluster(None, 10).await;

    Mock::given(method("GET"))
      .and(path_regex(HEALTH))
      .respond_with(ResponseTemplate::new(408).set_body_json(json!({ "timed_out": true })))
      .with_priority(1)
      .mount(&server)
      .await;

    create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await.unwrap();

    assert_eq!(requests(&server, "POST", "^/_aliases$").await.len(), 1);
  }

  #[tokio::test]
  async fn create_scoped_index_fails_when_settings_cannot_be_restored() {
    let (server, provider) = cluster(None, 10).await;

    Mock::given(method("PUT"))
      .and(path_regex(SETTINGS))
      .respond_with(ResponseTemplate::new(500).set_body_json(json!({})))
      .with_priority(1)
      .mount(&server)
      .await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await;

    assert!(result.unwrap_err().to_string().contains("updating settings"));
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());
    assert_eq!(requests(&server, "DELETE", NEW_INDEX).await.len(), 1);
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
    assert!(requests(&server, "DELETE", NEW_INDEX).await.is_empty());
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

    assert_eq!(requests(&server, "POST", "^/_aliases$").await.len(), 1);
    assert!(requests(&server, "DELETE", NEW_INDEX).await.is_empty());
  }

  #[tokio::test]
  async fn create_scoped_index_is_skipped_when_up_to_date() {
    let previous = meta(&["yente-entities-sanctions", "yente-entities-default"], &["properties.notes"]);
    let (server, provider) = cluster(Some(previous), 10).await;

    create_scoped_index(&provider, json!({ "match_all": {} }), &strings(&["notes"]), &strings(&["yente-entities-internal_*"]), false)
      .await
      .unwrap();

    assert!(requests(&server, "PUT", NEW_INDEX).await.is_empty());
    assert!(requests(&server, "POST", SEARCH).await.is_empty());
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());
    assert!(requests(&server, "DELETE", NEW_INDEX).await.is_empty());
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

      assert_eq!(written(&server).await.len(), 10);

      let aliases: serde_json::Value = requests(&server, "POST", "^/_aliases$").await[0].body_json().unwrap();

      assert_eq!(aliases["actions"][1]["remove"]["index"], "motiva-previous");

      let deleted = requests(&server, "DELETE", NEW_INDEX).await;

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

    assert_eq!(written(&server).await.len(), 10);
  }

  #[tokio::test]
  async fn create_scoped_index_is_rebuilt_when_forced() {
    let previous = meta(&["yente-entities-default", "yente-entities-sanctions"], &["properties.notes"]);
    let (server, provider) = cluster(Some(previous), 10).await;

    create_scoped_index(&provider, json!({ "match_all": {} }), &strings(&["notes"]), &strings(&["yente-entities-internal_*"]), true)
      .await
      .unwrap();

    assert_eq!(written(&server).await.len(), 10);
  }

  #[tokio::test]
  async fn create_scoped_index_is_rebuilt_without_previous_meta() {
    let (server, provider) = cluster(Some(json!({ "last_export": "2026-10-15T00:00:00" })), 10).await;

    create_scoped_index(&provider, json!({ "match_all": {} }), &strings(&["notes"]), &strings(&["yente-entities-internal_*"]), false)
      .await
      .unwrap();

    assert_eq!(written(&server).await.len(), 10);
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
  async fn create_scoped_index_cleans_up_when_nothing_matches() {
    let (server, provider) = cluster(Some(json!({})), 0).await;

    let result = create_scoped_index(&provider, json!({ "match_all": {} }), &[], &[], false).await;

    assert!(result.unwrap_err().to_string().contains("found no documents"));
    assert!(requests(&server, "POST", "^/_aliases$").await.is_empty());

    let deleted = requests(&server, "DELETE", NEW_INDEX).await;

    assert_eq!(deleted.len(), 1);
    assert!(regex::Regex::new(NEW_INDEX).unwrap().is_match(deleted[0].url.path()));
  }

  fn spliced(hit: &str) -> String {
    let hit: Hit = serde_json::from_str(hit).unwrap();
    let mut body = Vec::new();

    super::write_document(&mut body, &hit).unwrap();

    String::from_utf8(body).unwrap()
  }

  #[test]
  fn write_document_merges_doc_values_into_source() {
    let hit = r#"{"_id":"a","_source":{"id":"a","score":1.10},"fields":{"name_parts":["x","y"]}}"#;

    assert_eq!(spliced(hit), "{\"index\":{\"_id\":\"a\"}}\n{\"id\":\"a\",\"score\":1.10,\"name_parts\":[\"x\",\"y\"]}\n");
  }

  #[test]
  fn write_document_without_doc_values() {
    assert_eq!(spliced(r#"{"_id":"a","_source":{"id":"a"}}"#), "{\"index\":{\"_id\":\"a\"}}\n{\"id\":\"a\"}\n");
  }

  #[test]
  fn write_document_with_empty_source() {
    assert_eq!(spliced(r#"{"_id":"a","_source":{}}"#), "{\"index\":{\"_id\":\"a\"}}\n{}\n");
    assert_eq!(spliced(r#"{"_id":"a","_source":{},"fields":{"b":[1]}}"#), "{\"index\":{\"_id\":\"a\"}}\n{\"b\":[1]}\n");
  }

  #[test]
  fn write_document_keeps_escaped_values() {
    let hit = r#"{"_id":"a\"b","_source":{"note":"one\ntwo, \"three\", é"}}"#;

    assert_eq!(spliced(hit), "{\"index\":{\"_id\":\"a\\\"b\"}}\n{\"note\":\"one\\ntwo, \\\"three\\\", é\"}\n");
  }

  #[test]
  fn write_document_removes_line_breaks() {
    let hit = "{\"_id\":\"a\",\"_source\":{\n  \"id\": \"a\"\n},\"fields\":{\n  \"b\": [1]\n}}";

    assert_eq!(spliced(hit), "{\"index\":{\"_id\":\"a\"}}\n{\"id\":\"a\",\"b\":[1]}\n");
  }

  #[test]
  fn write_document_rejects_sources_that_are_not_objects() {
    let hit: Hit = serde_json::from_str(r#"{"_id":"a","_source":[1]}"#).unwrap();

    assert!(super::write_document(&mut Vec::new(), &hit).is_err());
  }

  #[test]
  fn index_only_fields() {
    let mappings = json!({
      "_source": { "excludes": ["name_parts", "name_phonetic", "disabled", "analyzed", "unmapped", "text", "nested_target"] },
      "properties": {
        "name_parts": { "type": "keyword", "copy_to": "text" },
        "name_phonetic": { "type": "keyword", "doc_values": true },
        "disabled": { "type": "keyword", "doc_values": false },
        "analyzed": { "type": "text" },
        "text": { "type": "text" },
        "nested_target": { "type": "keyword" },
        "properties": { "properties": { "name": { "type": "keyword", "copy_to": ["names", "nested_target"] } } },
      }
    });

    assert_eq!(super::index_only_fields(&mappings), vec!["name_parts".to_string(), "name_phonetic".to_string()]);
    assert!(super::index_only_fields(&json!({ "properties": {} })).is_empty());
  }

  #[test]
  fn strip_source_settings() {
    let mut settings = json!({
      "index": {
        "uuid": "uuid",
        "provided_name": "yente-entities-default",
        "creation_date": "0",
        "version": { "created": "1", "upgraded": "2" },
        "resize": { "source": { "name": "yente-entities-previous", "uuid": "uuid" } },
        "blocks": { "read_only": "false", "write": "true" },
        "routing": { "allocation": { "initial_recovery": { "_id": null }, "include": { "_tier_preference": "data_hot" } } },
        "number_of_shards": "1",
      }
    });

    super::strip_source_settings(&mut settings).unwrap();

    assert_eq!(
      settings,
      json!({
        "index": {
          "routing": { "allocation": { "include": { "_tier_preference": "data_hot" } } },
          "number_of_shards": "1",
        }
      })
    );

    assert!(super::strip_source_settings(&mut json!({})).is_err());
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
