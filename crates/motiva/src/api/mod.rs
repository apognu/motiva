use std::time::Duration;

use axum::{
  Router, middleware,
  routing::{get, post},
};
use libmotiva::prelude::*;
use metrics_exporter_prometheus::PrometheusHandle;
use tower_http::trace::TraceLayer;

use crate::{
  api::{config::Config, middlewares::create_request_span},
  trace::build_prometheus,
};

pub mod config;
pub mod dto;
pub mod errors;

pub mod handlers;
pub mod middlewares;

#[derive(Clone)]
pub struct AppState<P: IndexProvider> {
  pub config: Config,
  pub prometheus: Option<PrometheusHandle>,
  pub motiva: Motiva<P>,
}

pub async fn routes(config: &Config) -> anyhow::Result<Router> {
  let motiva = Motiva::new(ElasticsearchProvider::new(&config.index_url, config.index_auth_method.clone())?, config.manifest_url.clone()).await?;

  tokio::spawn({
    let motiva = motiva.clone();

    async move {
      loop {
        tokio::time::sleep(Duration::from_secs(60 * 60)).await;

        motiva.refresh_catalog().await;
      }
    }
  });

  let prometheus = match config.enable_prometheus {
    true => Some(build_prometheus()?),
    false => None,
  };

  let state = AppState {
    config: config.clone(),
    prometheus,
    motiva,
  };

  Ok(router(state))
}

pub(crate) fn router<P: IndexProvider>(state: AppState<P>) -> Router {
  Router::new()
    .route("/catalog", get(handlers::get_catalog))
    .route("/match/{scope}", post(handlers::match_entities))
    .route("/entities/{id}", get(handlers::get_entity))
    .fallback(handlers::not_found)
    .layer(middleware::from_fn_with_state(state.clone(), middlewares::logging::api_logger))
    .layer(TraceLayer::new_for_http().make_span_with(create_request_span))
    .layer(middleware::from_fn(middlewares::metrics))
    // The routes below will not go through the observability middlewares above
    .route("/healthz", get(handlers::healthz))
    .route("/readyz", get(handlers::readyz))
    .route("/metrics", get(handlers::prometheus))
    .layer(middleware::from_fn(middlewares::request_id))
    .with_state(state)
}
