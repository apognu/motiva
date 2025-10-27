use axum::{
  body::Body,
  http::{Request, StatusCode},
  middleware::Next,
  response::Response,
};
use metrics::counter;
use opentelemetry::global;
use opentelemetry_http::HeaderExtractor;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

pub(crate) mod auth;
pub(crate) mod logging;
pub(crate) mod types;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct RequestId(pub Uuid);

pub(crate) async fn request_id(request: Request<Body>, next: Next) -> Result<Response, StatusCode> {
  let (mut parts, body) = request.into_parts();
  let request_id = RequestId(Uuid::new_v4());

  parts.extensions.insert(request_id.clone());

  let mut response = next.run(Request::from_parts(parts, body)).await;

  response.extensions_mut().insert::<RequestId>(request_id);

  Ok(response)
}

pub(super) async fn metrics(request: Request<Body>, next: Next) -> Result<Response, StatusCode> {
  let response = next.run(request).await;

  counter!("http_requests_total", "status" => response.status().as_u16().to_string()).increment(1);

  Ok(response)
}

pub(crate) fn create_request_span(req: &axum::extract::Request) -> Span {
  let parent = global::get_text_map_propagator(|propagator| propagator.extract(&HeaderExtractor(req.headers())));
  let span = tracing::info_span!("request", request_id = req.extensions().get::<RequestId>().unwrap().0.to_string());

  let _ = span.set_parent(parent);
  span
}
