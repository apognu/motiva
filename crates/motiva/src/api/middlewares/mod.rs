use axum::{
  body::Body,
  http::{Request, StatusCode},
  middleware::Next,
  response::Response,
};
use metrics::counter;
use uuid::Uuid;

pub(super) mod auth;
pub(super) mod json_rejection;
pub(super) mod logging;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(super) struct RequestId(Uuid);

pub(super) async fn request_id(request: Request<Body>, next: Next) -> Result<Response, StatusCode> {
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
