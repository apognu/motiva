use std::{sync::Arc, time::Duration};

use axum::{
  Router,
  extract::Request,
  middleware,
  routing::{get, post},
};
use elasticsearch::{Elasticsearch, auth::Credentials, http::transport::Transport};
use opentelemetry::global;
use opentelemetry_http::HeaderExtractor;
use tokio::sync::RwLock;
use tower_http::trace::TraceLayer;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::{
  api::config::{Config, EsAuthMethod},
  catalog::{Collections, fetch_catalog},
  index::{IndexProvider, search::ElasticsearchProvider},
};

pub mod config;
pub mod dto;
pub mod errors;

pub mod handlers;
mod middlewares;

#[derive(Clone)]
pub struct AppState<P: IndexProvider> {
  pub config: Config,
  pub index: P,
  pub catalog: Arc<RwLock<Collections>>,
}

pub fn routes(config: &Config, catalog: Collections) -> anyhow::Result<Router> {
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
    index: ElasticsearchProvider { es },
    catalog: Arc::clone(&catalog),
  };

  tokio::spawn({
    let catalog_url = config.catalog_url.clone();

    async move {
      loop {
        tokio::time::sleep(Duration::from_secs(60 * 60)).await;

        match fetch_catalog(&catalog_url).await {
          Ok(new_catalog) => {
            let mut guard = catalog.write().await;
            *guard = new_catalog;
          }

          Err(err) => tracing::error!(error = err.to_string(), "could not refresh catalog"),
        }
      }
    }
  });

  Ok(
    Router::new()
      .route("/healthz", get(handlers::healthz))
      .route("/readyz", get(handlers::readyz))
      .route("/catalog", get(handlers::catalog))
      .route("/match/{scope}", post(handlers::match_entities))
      .route("/entities/{id}", get(handlers::get_entity))
      .fallback(handlers::not_found)
      .layer(middleware::from_fn_with_state(state.clone(), middlewares::logging::api_logger))
      .layer(middleware::from_fn(middlewares::request_id))
      .layer(TraceLayer::new_for_http().make_span_with(|req: &Request| {
        let parent = global::get_text_map_propagator(|propagator| propagator.extract(&HeaderExtractor(req.headers())));
        let request_id = Uuid::new_v4();

        let span = tracing::info_span!("request", %request_id);
        span.set_parent(parent);

        span
      }))
      .with_state(state),
  )
}
