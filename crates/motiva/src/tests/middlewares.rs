use std::sync::{Arc, Mutex};

use axum_test::TestServer;
use libmotiva::{MockedElasticsearch, prelude::*};
use reqwest::header::AUTHORIZATION;

use crate::{
  api::{self, AppState, config::Config},
  tests::log_writer::VecLogWriter,
  trace::{build_prometheus, init_tracing},
};

#[tokio::test]
async fn api_invalid_credentials() {
  let index = MockedElasticsearch::builder().healthy(false).build();

  let state = AppState {
    config: Config {
      api_key: Some("myapikey".into()),
      ..Default::default()
    },
    prometheus: None,
    motiva: Motiva::new(index, None).await.unwrap(),
  };

  let app = api::router(state);
  let server = TestServer::new(app).unwrap();
  let response = server.post("/match/default").await;

  assert_eq!(response.status_code(), 401);

  response.assert_text_contains("invalid credentials");

  let response = server.post("/match/default").add_header(AUTHORIZATION, "Bearer invalidkey").await;

  assert_eq!(response.status_code(), 401);

  response.assert_text_contains("invalid credentials");
}

#[tokio::test]
async fn api_valid_credentials() {
  let index = MockedElasticsearch::builder().healthy(false).build();

  let state = AppState {
    config: Config {
      api_key: Some("myapikey".into()),
      ..Default::default()
    },
    prometheus: None,
    motiva: Motiva::new(index, None).await.unwrap(),
  };

  let app = api::router(state);
  let server = TestServer::new(app).unwrap();
  let response = server.post("/match/default").add_header(AUTHORIZATION, "Bearer myapikey").await;

  assert_eq!(response.status_code(), 415);
}

#[tokio::test]
async fn logging() {
  let index = MockedElasticsearch::builder().healthy(true).build();
  let buf = Arc::new(Mutex::new(Vec::default()));

  let state = AppState {
    config: Config {
      enable_tracing: true,
      ..Default::default()
    },
    prometheus: None,
    motiva: Motiva::new(index, None).await.unwrap(),
  };

  let (_guard, _) = init_tracing(&state.config, VecLogWriter::new(Arc::clone(&buf))).await;

  let app = api::router(state);
  let server = TestServer::new(app).unwrap();
  let _ = server.post("/match/default").add_header("traceparent", "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01").await;

  let lines = buf.lock().unwrap();

  assert_eq!(lines.len(), 1);
  assert!(lines[0].contains("POST http://localhost/match/default"));
  assert!(lines[0].contains("request_id="));
  assert!(lines[0].contains("trace=0af7651916cd43dd8448eb211c80319c"));
  assert!(lines[0].contains(r#"remote="-" method=POST path="/match/default" status=415"#));
}

#[tokio::test]
async fn metrics() {
  let index = MockedElasticsearch::builder().healthy(true).build();

  let state = AppState {
    config: Config {
      enable_prometheus: true,
      ..Default::default()
    },
    prometheus: Some(build_prometheus().unwrap()),
    motiva: Motiva::new(index, None).await.unwrap(),
  };

  let app = api::router(state);
  let server = TestServer::new(app).unwrap();
  let _ = server.post("/match/default").await;
  let resp = server.get("/metrics").await;

  assert!(resp.text().contains(r#"http_requests_total{service="motiva",status="415"}"#))
}
