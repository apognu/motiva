use std::{sync::Arc, time::Duration};

use axum::{
  Router,
  extract::{FromRef, Request},
  middleware,
  routing::post,
};
use elasticsearch::{Elasticsearch, http::transport::Transport};
use tokio::sync::RwLock;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

use crate::catalog::{Collections, fetch_catalog};

pub mod dto;
pub mod errors;

mod handlers;
mod middlewares;

#[derive(Clone)]
struct AppState {
  pub es: Elasticsearch,
  pub catalog: Arc<RwLock<Collections>>,
}

impl FromRef<AppState> for () {
  fn from_ref(_: &AppState) -> Self {}
}

pub async fn routes(catalog: Collections) -> Router {
  let catalog = Arc::new(RwLock::new(catalog));

  let state = AppState {
    es: Elasticsearch::new(Transport::single_node("http://127.0.0.1:9200").unwrap()),
    catalog: Arc::clone(&catalog),
  };

  tokio::spawn(async move {
    loop {
      match fetch_catalog().await {
        Ok(new_catalog) => {
          let mut guard = catalog.write().await;
          *guard = new_catalog;
        }

        Err(err) => tracing::error!(error = err.to_string(), "could not refresh catalog"),
      }

      tokio::time::sleep(Duration::from_secs(10)).await;
    }
  });

  Router::new()
    .route("/match/{dataset}", post(handlers::match_entity))
    .fallback(handlers::not_found)
    .layer(middleware::from_fn(middlewares::logging::api_logger))
    .layer(middleware::from_fn(middlewares::request_id))
    .layer(TraceLayer::new_for_http().make_span_with(|_req: &Request| {
      let request_id = Uuid::new_v4().to_string();

      tracing::info_span!("request", request_id = request_id)
    }))
    .with_state(state)
}
