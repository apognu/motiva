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

use crate::api::{
  AppState,
  config::{Config, TracingExporter},
  middlewares::RequestId,
};

pub async fn api_logger<P>(State(state): State<AppState<P>>, request: Request<Body>, next: Next) -> Result<Response, StatusCode>
where
  P: IndexProvider,
{
  let span = Span::current();
  let trace_id = trace_id_for_logs(&state.config, span.context().span().span_context().trace_id());

  let time = Timestamp::now().strftime("%Y-%m-%dT%H:%M:%S%z").to_string();
  let method = request.method().clone();
  let uri = request.uri().clone();

  let (mut parts, body) = request.into_parts();
  let request_id = parts.extensions.get::<RequestId>().map(|id| id.0).unwrap_or_default();
  let ip = if let Ok(ConnectInfo(addr)) = parts.extract::<ConnectInfo<SocketAddr>>().await {
    addr.ip().to_string()
  } else {
    "-".to_string()
  };

  let then = Instant::now();
  let response = next.run(Request::from_parts(parts, body)).await;

  global::meter("motiva").f64_histogram("request_latency").build().record(then.elapsed().as_secs_f64() * 1000.0, &[]);

  tracing::info!(
    request_id = %request_id,
    trace = %trace_id,
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

fn trace_id_for_logs(config: &Config, trace_id: TraceId) -> String {
  match config.tracing_exporter {
    TracingExporter::Otlp => trace_id.to_string(),
    #[cfg(feature = "gcp")]
    TracingExporter::Gcp => format!("projects/{}/traces/{trace_id}", config.gcp_project_id),
  }
}
