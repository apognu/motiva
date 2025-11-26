use std::net::SocketAddr;

use axum::{
  RequestPartsExt,
  body::{Body, HttpBody},
  extract::{ConnectInfo, State},
  http::{Request, StatusCode},
  middleware::Next,
  response::Response,
};
use jiff::Timestamp;
use libmotiva::prelude::*;
use opentelemetry::{TraceId, global, trace::TraceContextExt};
use tokio::time::Instant;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::api::{AppState, config::Config};

pub async fn api_logger<F, P>(State(state): State<AppState<F, P>>, request: Request<Body>, next: Next) -> Result<Response, StatusCode>
where
  F: CatalogFetcher,
  P: IndexProvider,
{
  let span = Span::current();
  let trace_id = span.context().span().span_context().trace_id();

  let time = Timestamp::now().strftime("%Y-%m-%dT%H:%M:%S%z").to_string();
  let method = request.method().clone();
  let uri = request.uri().clone();

  let (mut parts, body) = request.into_parts();
  let ip = if let Ok(ConnectInfo(addr)) = parts.extract::<ConnectInfo<SocketAddr>>().await {
    addr.ip().to_string()
  } else {
    "-".to_string()
  };

  let then = Instant::now();
  let response = next.run(Request::from_parts(parts, body)).await;

  global::meter("motiva").f64_histogram("request_latency").build().record(then.elapsed().as_secs_f64() * 1000.0, &[]);

  let span = add_trace_id(&state.config, trace_id);
  let _guard = span.enter();

  tracing::info!(
    time = time,
    remote = ip,
    method = %method,
    path = uri.path(),
    status = response.status().as_u16(),
    latency = then.elapsed().as_millis(),
    size = response.size_hint().exact().unwrap_or(0),
    "{} {}",
    method,
    uri,
  );

  Ok(response)
}

fn add_trace_id(config: &Config, trace_id: TraceId) -> Span {
  #[cfg(feature = "gcp")]
  use crate::api::config::TracingExporter;

  match config.tracing_exporter {
    #[cfg(feature = "gcp")]
    TracingExporter::Gcp => tracing::info_span!("request", trace_id = ?trace_id, "logging.googleapis.com/trace" = format!("projects/{}/traces/{trace_id}", config.gcp_project_id)),
    _ => tracing::info_span!("request", trace_id = ?trace_id),
  }
}
