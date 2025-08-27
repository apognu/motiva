use std::net::SocketAddr;

use axum::{
  RequestPartsExt,
  body::{Body, HttpBody},
  extract::ConnectInfo,
  http::{Request, StatusCode},
  middleware::Next,
  response::Response,
};
use jiff::Timestamp;
use opentelemetry::{global, trace::TraceContextExt};
use tokio::time::Instant;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::api::middlewares::RequestId;

pub async fn api_logger(request: Request<Body>, next: Next) -> Result<Response, StatusCode> {
  let span = Span::current();
  let trace_id = span.context().span().span_context().trace_id();

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
    path = uri.to_string(),
    status = response.status().as_u16(),
    latency = then.elapsed().as_millis(),
    size = response.size_hint().exact().unwrap_or(0),
    "{} {}",
    method,
    uri,
  );

  Ok(response)
}
