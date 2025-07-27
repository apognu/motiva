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
use tokio::time::Instant;

pub async fn api_logger(request: Request<Body>, next: Next) -> Result<Response, StatusCode> {
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

  tracing::info!(
    time = time,
    remote = ip,
    method = method.to_string(),
    path = uri.to_string(),
    status = response.status().as_u16(),
    latency = format!("{:.2}", then.elapsed().as_secs_f32()),
    size = response.size_hint().exact().unwrap_or(0),
    "{} {}",
    method,
    uri,
  );

  Ok(response)
}
