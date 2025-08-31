use std::sync::Arc;

use axum::{Router, routing::post};
use axum_test::TestServer;
use libmotiva::prelude::*;
use serde_json::json;

use crate::api::{AppState, config::Config, handlers};

#[tokio::test]
async fn api_simple() {
  let index = MockedElasticsearch::with_entities(vec![
    Entity::builder("Person").id("Q7747").properties(&[("name", &["Vladimir Putin"])]).build(),
    Entity::builder("Person").id("A1234").properties(&[("name", &["Bob the Builder"])]).build(),
  ]);

  let state = AppState {
    config: Config::from_env().await.unwrap(),
    catalog: Arc::default(),
    index,
  };

  let app = Router::new().route("/match/{scope}", post(handlers::match_entities)).with_state(state);
  let server = TestServer::new(app).unwrap();

  let response = server
    .post("/match/default")
    .json(&json!({
        "queries": {
            "test": {
                "schema": "Person",
                "properties": {
                    "name": ["Vladimir Putin"]
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
