use std::{sync::Arc, time::Duration};

use axum::{
  Router,
  extract::{FromRef, Request},
  middleware,
  routing::{get, post},
};
use elasticsearch::{Elasticsearch, auth::Credentials, http::transport::Transport};
use tokio::sync::RwLock;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

use crate::{
  api::config::{Config, EsAuthMethod},
  catalog::{Collections, fetch_catalog},
};

pub mod config;
pub mod dto;
pub mod errors;

mod handlers;
mod middlewares;

#[derive(Clone)]
pub struct AppState {
  pub config: Config,
  pub es: Elasticsearch,
  pub catalog: Arc<RwLock<Collections>>,
}

impl FromRef<AppState> for () {
  fn from_ref(_: &AppState) -> Self {}
}

pub async fn routes(config: &Config, catalog: Collections) -> anyhow::Result<Router> {
  let catalog = Arc::new(RwLock::new(catalog));

  let es = {
    let transport = Transport::single_node(&config.index_url)?;
    let client_id = config.index_client_id.clone().unwrap_or_default();
    let client_secret = config.index_client_secret.clone().unwrap_or_default();

    match config.index_auth_method {
      EsAuthMethod::Basic => transport.set_auth(Credentials::Basic(client_id, client_secret)),
      EsAuthMethod::Bearer => transport.set_auth(Credentials::Bearer(client_secret)),
      EsAuthMethod::ApiKey => transport.set_auth(Credentials::ApiKey(client_id, client_secret)),
      EsAuthMethod::EncodedApiKey => transport.set_auth(Credentials::EncodedApiKey(client_secret)),
      _ => {}
    }

    Elasticsearch::new(transport)
  };

  let state = AppState {
    config: config.clone(),
    es,
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

      tokio::time::sleep(Duration::from_secs(60 * 60)).await;
    }
  });

  Ok(
    Router::new()
      .route("/match/{dataset}", post(handlers::match_entities))
      .route("/entities/{id}", get(handlers::get_entity))
      .fallback(handlers::not_found)
      .layer(middleware::from_fn(middlewares::logging::api_logger))
      .layer(middleware::from_fn(middlewares::request_id))
      .layer(TraceLayer::new_for_http().make_span_with(|_req: &Request| {
        let request_id = Uuid::new_v4().to_string();

        tracing::info_span!("request", request_id = request_id)
      }))
      .with_state(state),
  )
}
