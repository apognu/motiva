use std::sync::Arc;

use axum::{
  Router,
  routing::{get, post},
};
use axum_test::TestServer;
use libmotiva::{MockedElasticsearch, prelude::*};
use serde_json::json;

use crate::api::{AppState, config::Config, handlers};

use libmotiva::TestFetcher;

#[tokio::test]
async fn api_not_found() {
  let app = Router::new().fallback(handlers::not_found);
  let server = TestServer::new(app).unwrap();
  let response = server.get("/nope").await;

  assert_eq!(response.status_code(), 404);
}

#[tokio::test]
async fn api_not_version() {
  let index = MockedElasticsearch::builder().healthy(false).build();

  let state = AppState {
    config: Arc::new(Config::default()),
    prometheus: None,
    motiva: Motiva::test(index).fetcher(TestFetcher::default()).build().await.unwrap(),
  };

  let app = Router::new().route("/-/version", get(handlers::version)).with_state(state);
  let server = TestServer::new(app).unwrap();
  let response = server.get("/-/version").await;

  assert_eq!(response.status_code(), 200);

  response.assert_json_contains(&json!({
      "motiva": env!("VERSION"),
      "index": "v4",
  }))
}

#[tokio::test]
async fn api_health_unhealthy() {
  let index = MockedElasticsearch::builder().healthy(false).build();

  let state = AppState {
    config: Arc::new(Config::default()),
    prometheus: None,
    motiva: Motiva::test(index).fetcher(TestFetcher::default()).build().await.unwrap(),
  };

  let app = Router::new().route("/readyz", post(handlers::readyz)).with_state(state);
  let server = TestServer::new(app).unwrap();
  let response = server.post("/readyz").await;

  assert_eq!(response.status_code(), 503);
}

#[tokio::test]
async fn api_health_healthy() {
  let index = MockedElasticsearch::builder().healthy(true).build();

  let state = AppState {
    config: Arc::new(Config::default()),
    prometheus: None,
    motiva: Motiva::test(index).build().await.unwrap(),
  };

  let app = Router::new().route("/readyz", post(handlers::readyz)).with_state(state);
  let server = TestServer::new(app).unwrap();
  let response = server.post("/readyz").await;

  assert_eq!(response.status_code(), 200);
}

#[tokio::test]
async fn api_algorithms() {
  let index = MockedElasticsearch::builder().healthy(true).build();

  let state = AppState {
    config: Arc::new(Config::default()),
    prometheus: None,
    motiva: Motiva::test(index).build().await.unwrap(),
  };

  let app = Router::new().route("/algorithms", post(handlers::algorithms)).with_state(state);
  let server = TestServer::new(app).unwrap();
  let response = server.post("/algorithms").await;

  assert_eq!(response.status_code(), 200);

  response.assert_json_contains(&json!({
      "algorithms": [
          { "name": "name-based" },
          { "name": "name-qualified" },
          { "name": "logic-v1" },
      ],
      "best": "logic-v1",
      "default": "logic-v1"
  }))
}

#[tokio::test]
async fn api_match() {
  let index = MockedElasticsearch::builder()
    .entities(vec![
      Entity::builder("Person").id("Q7747").properties(&[("name", &["Vladimir Putin"]), ("weakAlias", &["That Guy"])]).build(),
      Entity::builder("Person").id("A1234").properties(&[("name", &["Bob the Builder"])]).build(),
    ])
    .build();

  let state = AppState {
    config: Arc::new(Config::default()),
    prometheus: None,
    motiva: Motiva::test(index).build().await.unwrap(),
  };

  let app = Router::new().route("/match/{scope}", post(handlers::match_entities)).with_state(state);
  let server = TestServer::new(app).unwrap();

  let response = server
    .post("/match/default?cutoff=0.0")
    .json(&json!({
        "queries": {
            "test": {
                "schema": "Person",
                "properties": {
                    "name": ["Vladimir Putin", "that guy"],
                }
            }
        }
    }))
    .await;

  response.assert_json_contains(&json!({
      "responses": {
        "test": {
            "status": 200,
            "total": { "relation": "eq", "value": 1 },
            "results": [
                {
                    "id": "Q7747",
                    "schema": "Person",
                    "match": true,
                    "properties": {
                        "name": ["Vladimir Putin"]
                    },
                    "features": {
                        "name_literal_match": 1.0,
                        "person_name_jaro_winkler": 1.0,
                        "person_name_phonetic_match": 1.0,
                        "weak_alias_match": 1.0
                    }
                }
            ]
        }
      }
  }));
}

#[tokio::test]
async fn api_invalid_query() {
  let index = MockedElasticsearch::builder().healthy(false).build();

  let state = AppState {
    config: Arc::new(Config::default()),
    prometheus: None,
    motiva: Motiva::test(index).build().await.unwrap(),
  };

  let app = Router::new().route("/match/{scope}", post(handlers::match_entities)).with_state(state);
  let server = TestServer::new(app).unwrap();
  let response = server.post("/match/default?changed_since=invalid").await;

  assert_eq!(response.status_code(), 400);

  response.assert_text_contains("failed to parse year in date");
}

#[tokio::test]
async fn api_unparsable_payload() {
  let index = MockedElasticsearch::builder().healthy(false).build();

  let state = AppState {
    config: Arc::new(Config::default()),
    prometheus: None,
    motiva: Motiva::test(index).build().await.unwrap(),
  };

  let app = Router::new().route("/match/{scope}", post(handlers::match_entities)).with_state(state);
  let server = TestServer::new(app).unwrap();

  let payloads = &[r#"{ "queries": { "test": { "schema": } } }"#, r#"{ "queries": { "test": { "schema": 12 } } }"#];

  for payload in payloads {
    let response = server.post("/match/default?changed_since=invalid").json(payload).await;

    assert_eq!(response.status_code(), 400);
  }

  let response = server.post("/match/default?changed_since=invalid").text("{}").await;

  assert_eq!(response.status_code(), 400);
}

#[tokio::test]
async fn api_invalid_payload() {
  let index = MockedElasticsearch::builder().healthy(false).build();

  let state = AppState {
    config: Arc::new(Config::default()),
    prometheus: None,
    motiva: Motiva::test(index).build().await.unwrap(),
  };

  let app = Router::new().route("/match/{scope}", post(handlers::match_entities)).with_state(state);
  let server = TestServer::new(app).unwrap();

  let response = server
    .post("/match/default")
    .json(&json!({
        "queries": {}
    }))
    .await;

  assert_eq!(response.status_code(), 422);

  response.assert_json_contains(&json!({
      "details": [
          "at least one query must be provided"
      ]
  }));
}
