use std::{
  sync::{Arc, Mutex},
  time::{Duration, Instant},
};

use anyhow::{Context, ensure};
use libmotiva::{ElasticsearchProvider, EsOptions, HttpCatalogFetcher, IndexProvider};
use reqwest::{Client, StatusCode};
use rustls::crypto::aws_lc_rs;
use serde_json::{Value, json};
use tokio::{runtime::Builder, sync::oneshot, task::JoinSet};
use tracing::{
  Event, Id, Subscriber,
  field::{Field, Visit},
  span::Attributes,
};
use tracing_subscriber::{
  Layer,
  filter::filter_fn,
  layer::{Context as LayerContext, SubscriberExt},
  registry::LookupSpan,
  util::SubscriberInitExt,
};

#[allow(unused)]
#[path = "../src/api/mod.rs"]
mod api;
#[allow(unused)]
#[path = "../src/trace.rs"]
mod trace;
#[allow(unused)]
#[path = "../src/util.rs"]
mod util;

fn git_version() -> String {
  String::new()
}

const CONCURRENCY_LEVELS: [usize; 8] = [1, 2, 4, 8, 16, 32, 48, 64];

// ============================================================================
// Tracing & Telemetry Collection
// ============================================================================

#[derive(Default)]
struct EsSamples {
  searches: Vec<Duration>,
  took: Vec<Duration>,
  candidates: Vec<u64>,
}

#[derive(Clone, Default)]
struct EsTracing {
  samples: Arc<Mutex<EsSamples>>,
}

impl EsTracing {
  fn clear(&self) {
    *self.samples.lock().unwrap() = EsSamples::default();
  }

  fn take(&self) -> EsSamples {
    std::mem::take(&mut *self.samples.lock().unwrap())
  }
}

struct SearchStarted(Instant);

#[derive(Default)]
struct EsEventVisitor {
  took_ms: Option<u64>,
  candidates: Option<u64>,
}

impl Visit for EsEventVisitor {
  fn record_u64(&mut self, field: &Field, value: u64) {
    match field.name() {
      "latency" => self.took_ms = Some(value),
      "results" => self.candidates = Some(value),
      _ => {}
    }
  }

  fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

impl<S> Layer<S> for EsTracing
where
  S: Subscriber + for<'span> LookupSpan<'span>,
{
  fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: LayerContext<'_, S>) {
    if attributes.metadata().name() == "search" {
      if let Some(span) = context.span(id) {
        span.extensions_mut().insert(SearchStarted(Instant::now()));
      }
    }
  }

  fn on_event(&self, event: &Event<'_>, _context: LayerContext<'_, S>) {
    let mut visitor = EsEventVisitor::default();
    event.record(&mut visitor);

    if let Some(took_ms) = visitor.took_ms {
      let mut samples = self.samples.lock().unwrap();
      samples.took.push(Duration::from_millis(took_ms));
      if let Some(candidates) = visitor.candidates {
        samples.candidates.push(candidates);
      }
    }
  }

  fn on_close(&self, id: Id, context: LayerContext<'_, S>) {
    let Some(span) = context.span(&id) else { return };
    let Some(started) = span.extensions().get::<SearchStarted>().map(|s| s.0) else { return };

    self.samples.lock().unwrap().searches.push(started.elapsed());
  }
}

// ============================================================================
// Metrics & Statistics
// ============================================================================

#[allow(unused)]
struct Measurements {
  throughput: f64,
  mean: Duration,
  p50: Duration,
  p95: Duration,
  p99: Duration,
  health_p95: Duration,
  search_mean: Duration,
  search_p95: Duration,
  es_took_mean: Duration,
  es_took_p95: Duration,
  candidates_mean: f64,
}

impl Measurements {
  fn calculate(mut latencies: Vec<Duration>, mut health_latencies: Vec<Duration>, mut es_samples: EsSamples, elapsed: Duration) -> anyhow::Result<Self> {
    latencies.sort_unstable();
    health_latencies.sort_unstable();
    es_samples.searches.sort_unstable();
    es_samples.took.sort_unstable();

    ensure!(
      es_samples.searches.len() == latencies.len(),
      "captured {} Elasticsearch searches for {} endpoint requests",
      es_samples.searches.len(),
      latencies.len()
    );
    ensure!(
      es_samples.took.len() == latencies.len(),
      "captured {} Elasticsearch responses for {} endpoint requests",
      es_samples.took.len(),
      latencies.len()
    );

    let candidates_mean = if es_samples.candidates.is_empty() {
      0.0
    } else {
      es_samples.candidates.iter().sum::<u64>() as f64 / es_samples.candidates.len() as f64
    };

    Ok(Self {
      throughput: latencies.len() as f64 / elapsed.as_secs_f64(),
      mean: mean(&latencies),
      p50: percentile(&latencies, 0.50),
      p95: percentile(&latencies, 0.95),
      p99: percentile(&latencies, 0.99),
      health_p95: percentile(&health_latencies, 0.95),
      search_mean: mean(&es_samples.searches),
      search_p95: percentile(&es_samples.searches, 0.95),
      es_took_mean: mean(&es_samples.took),
      es_took_p95: percentile(&es_samples.took, 0.95),
      candidates_mean,
    })
  }

  fn print_row(&self, label: &str, concurrency: usize) {
    println!(
      "| {label} | {concurrency} | {:.2} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} | {:.1} | {:.3} |",
      self.throughput,
      ms(self.mean),
      ms(self.p95),
      ms(self.search_mean),
      ms(self.search_p95),
      ms(self.es_took_mean),
      ms(self.es_took_p95),
      self.candidates_mean,
      ms(self.health_p95),
    );
  }
}

fn mean(samples: &[Duration]) -> Duration {
  if samples.is_empty() {
    return Duration::ZERO;
  }
  samples.iter().sum::<Duration>() / samples.len() as u32
}

fn percentile(samples: &[Duration], p: f64) -> Duration {
  if samples.is_empty() {
    return Duration::ZERO;
  }
  samples[((samples.len() - 1) as f64 * p).round() as usize]
}

fn ms(duration: Duration) -> f64 {
  duration.as_secs_f64() * 1_000.0
}

// ============================================================================
// Benchmark Execution
// ============================================================================

struct BenchmarkRunner {
  client: Client,
  match_url: Arc<str>,
  health_url: Arc<str>,
  api_key: Option<Arc<str>>,
  payload: Arc<Value>,
  es_tracing: EsTracing,
}

impl BenchmarkRunner {
  async fn measure(&self, concurrency: usize, samples: usize) -> anyhow::Result<Measurements> {
    let waves = samples.div_ceil(concurrency).max(1);
    let mut latencies = Vec::with_capacity(waves * concurrency);
    let mut health_latencies = Vec::with_capacity(waves);

    self.es_tracing.clear();
    let measured_at = Instant::now();

    for _ in 0..waves {
      let health_task = tokio::spawn({
        let client = self.client.clone();
        let health_url = Arc::clone(&self.health_url);
        async move { send_health(&client, &health_url).await }
      });

      tokio::task::yield_now().await;

      let mut match_tasks = JoinSet::new();
      for _ in 0..concurrency {
        let client = self.client.clone();
        let match_url = Arc::clone(&self.match_url);
        let api_key = self.api_key.clone();
        let payload = Arc::clone(&self.payload);
        let submitted_at = Instant::now();

        match_tasks.spawn(async move {
          send_match(&client, &match_url, api_key.as_deref(), &payload).await?;
          Ok::<_, anyhow::Error>(submitted_at.elapsed())
        });
      }

      health_latencies.push(health_task.await.context("/healthz task failed")??);
      while let Some(result) = match_tasks.join_next().await {
        latencies.push(result.context("/match task failed")??);
      }
    }

    Measurements::calculate(latencies, health_latencies, self.es_tracing.take(), measured_at.elapsed())
  }
}

async fn send_match(client: &Client, url: &str, api_key: Option<&str>, payload: &Value) -> anyhow::Result<()> {
  let mut request = client.post(url).json(payload);
  if let Some(api_key) = api_key {
    request = request.bearer_auth(api_key);
  }

  let response = request.send().await.context("/match request failed")?;
  ensure!(response.status() == StatusCode::OK, "/match returned HTTP {}", response.status());

  let body = response.json::<Value>().await.context("invalid /match response")?;
  ensure!(
    body.pointer("/responses/benchmark/status").and_then(Value::as_u64) == Some(200),
    "/match returned a failed query response: {body}"
  );

  Ok(())
}

async fn send_health(client: &Client, url: &str) -> anyhow::Result<Duration> {
  let started = Instant::now();
  let response = client.get(url).send().await.context("/healthz request failed")?;
  ensure!(response.status() == StatusCode::OK, "/healthz returned HTTP {}", response.status());
  response.bytes().await.context("could not read /healthz response")?;
  Ok(started.elapsed())
}

// ============================================================================
// Helpers & Startup
// ============================================================================

fn env_usize(name: &str, default: usize) -> usize {
  std::env::var(name).ok().and_then(|val| val.parse().ok()).filter(|&val| val > 0).unwrap_or(default)
}

fn request_payload() -> Value {
  let name = std::env::var("MATCH_BENCHMARK_NAME").unwrap_or_else(|_| "Mohammed".into());
  json!({
      "queries": {
          "benchmark": {
              "schema": "Person",
              "properties": {
                  "name": [name]
              }
          }
      }
  })
}

async fn wait_until_ready(client: &Client, url: &str) -> anyhow::Result<()> {
  tokio::time::timeout(Duration::from_secs(60), async {
    loop {
      if client.get(url).send().await.is_ok_and(|res| res.status() == StatusCode::OK) {
        return;
      }
      tokio::time::sleep(Duration::from_millis(250)).await;
    }
  })
  .await
  .context("Motiva did not become ready within 60 seconds")
}

async fn refresh_index_until_ready(provider: &ElasticsearchProvider) -> anyhow::Result<()> {
  tokio::time::timeout(Duration::from_secs(120), async {
    while !provider.ready() {
      provider.refresh().await;
      if !provider.ready() {
        tokio::time::sleep(Duration::from_millis(250)).await;
      }
    }
  })
  .await
  .context("Elasticsearch index did not become ready within 120 seconds")
}

// ============================================================================
// Main Execution
// ============================================================================

async fn run(tokio_threads: usize, rayon_threads: usize, es_tracing: EsTracing) -> anyhow::Result<()> {
  let _ = aws_lc_rs::default_provider().install_default();

  let mut config = api::config::Config::from_env().await?;
  config.enable_prometheus = false;
  config.enable_tracing = false;
  config.match_candidates = env_usize("BENCHMARK_CANDIDATES", 200);

  let label = std::env::var("MATCH_BENCHMARK_LABEL").unwrap_or_else(|_| "current".into());
  let scope = std::env::var("MATCH_BENCHMARK_SCOPE").unwrap_or_else(|_| "default".into());
  let samples = env_usize("BENCHMARK_REQUESTS", 128);

  // Verify Elasticsearch health
  let index_name = format!("{}-entities", config.index_name.as_deref().unwrap_or("yente"));
  let health_probe_url = format!("{}/_cluster/health/{index_name}", config.index_url.trim_end_matches('/'));
  Client::new()
    .get(health_probe_url)
    .send()
    .await
    .context("direct Elasticsearch health probe failed")?
    .error_for_status()
    .context("direct Elasticsearch health probe returned an error")?;

  let provider = ElasticsearchProvider::new(
    &config.index_url,
    EsOptions {
      auth: config.index_auth_method.clone(),
      tls: &config.index_tls_verification,
      index_name: config.index_name.clone(),
    },
  )
  .await?;
  refresh_index_until_ready(&provider).await?;

  let api_key = config.api_key.clone();
  let fetcher = HttpCatalogFetcher::from_manifest_url(config.manifest_url.clone())?;
  let app = api::routes(config, fetcher, provider).await?;

  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
  let address = listener.local_addr()?;
  let (shutdown_tx, shutdown_rx) = oneshot::channel();

  let server = tokio::spawn(async move {
    axum::serve(listener, app)
      .with_graceful_shutdown(async {
        let _ = shutdown_rx.await;
      })
      .await
  });

  let client = Client::builder().pool_max_idle_per_host(64).build()?;
  let base_url = format!("http://{address}");
  wait_until_ready(&client, &format!("{base_url}/readyz")).await?;

  let runner = BenchmarkRunner {
    client,
    match_url: format!("{base_url}/match/{scope}?limit=1&cutoff=0.0").into(),
    health_url: format!("{base_url}/healthz").into(),
    api_key: api_key.map(Arc::from),
    payload: Arc::new(request_payload()),
    es_tracing,
  };

  println!("/match/{scope}, {tokio_threads} Tokio workers, {rayon_threads} Rayon workers, at least {samples} requests/row\n");
  println!("| implementation | concurrency | requests/s | endpoint mean ms | endpoint p95 ms | search mean ms | search p95 ms | ES took mean ms | ES took p95 ms | candidates | /healthz p95 ms |");
  println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");

  for concurrency in CONCURRENCY_LEVELS {
    // Warm-up
    runner.measure(concurrency, 2 * concurrency).await?;

    // Measurement
    let result = runner.measure(concurrency, samples).await?;
    result.print_row(&label, concurrency);
  }

  let _ = shutdown_tx.send(());
  server.await.context("HTTP server task failed")??;
  Ok(())
}

fn main() -> anyhow::Result<()> {
  let es_tracing = EsTracing::default();
  tracing_subscriber::registry()
    .with(es_tracing.clone().with_filter(filter_fn(|metadata| {
      metadata.target() == "libmotiva::index::elastic::queries" && *metadata.level() <= tracing::Level::DEBUG
    })))
    .with(tracing_subscriber::fmt::layer().with_filter(filter_fn(|m| *m.level() <= tracing::Level::WARN)))
    .try_init()
    .context("could not initialize benchmark tracing")?;

  let (tthreads, rthreads) = util::runtime_thread_counts().expect("could not configure runtime thread pools");

  if rthreads > 0 {
    rayon::ThreadPoolBuilder::new().num_threads(rthreads).build_global().context("failed to initialize Rayon thread pool")?;
  }

  let runtime = if tthreads == 1 && rthreads == 0 {
    Builder::new_current_thread().enable_all().build()?
  } else {
    Builder::new_multi_thread().worker_threads(tthreads).enable_all().build()?
  };

  runtime.block_on(run(tthreads, rthreads, es_tracing))
}
