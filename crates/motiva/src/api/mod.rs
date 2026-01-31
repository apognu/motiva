use std::{sync::Arc, time::Duration};

use axum::{
  Router, middleware,
  routing::{get, post},
};
use libmotiva::prelude::*;
use metrics_exporter_prometheus::PrometheusHandle;
use reqwest::StatusCode;
use tower_http::{timeout::TimeoutLayer, trace::TraceLayer};

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
pub struct AppState<F: CatalogFetcher, P: IndexProvider> {
  pub config: Arc<Config>,
  pub prometheus: Option<PrometheusHandle>,
  pub motiva: Motiva<P, F>,
}

pub async fn routes<F: CatalogFetcher, P: IndexProvider>(config: Config, fetcher: F, provider: P) -> anyhow::Result<Router> {
  let motiva = {
    let config = MotivaConfig {
      outdated_grace: config.outdated_grace,
    };

    Motiva::custom(provider.clone()).fetcher(fetcher).config(config).build().await?
  };

  tokio::spawn({
    let motiva = motiva.clone();
    let interval = config.catalog_refresh_interval.try_into().unwrap();

    async move {
      loop {
        tokio::time::sleep(interval).await;

        motiva.refresh_catalog().await;
      }
    }
  });

  let prometheus = match config.enable_prometheus {
    true => Some(build_prometheus()?),
    false => None,
  };

  let state = AppState {
    config: Arc::new(config),
    prometheus,
    motiva,
  };

  Ok(router(state))
}

pub(crate) fn router<F: CatalogFetcher, P: IndexProvider>(state: AppState<F, P>) -> Router {
  Router::new()
    .route("/catalog", get(handlers::get_catalog))
    .route("/match/{scope}", post(handlers::match_entities))
    .route("/entities/{id}", get(handlers::get_entity))
    .fallback(handlers::not_found)
    .layer(TimeoutLayer::with_status_code(
      StatusCode::REQUEST_TIMEOUT,
      state.config.request_timeout.try_into().unwrap_or(Duration::from_secs(10)),
    ))
    .layer(middleware::from_fn_with_state(state.clone(), middlewares::logging::api_logger))
    .layer(TraceLayer::new_for_http().make_span_with(create_request_span))
    .layer(middleware::from_fn(middlewares::metrics))
    // The routes below will not go through the observability middlewares above
    .route("/algorithms", get(handlers::algorithms))
    .route("/healthz", get(handlers::healthz))
    .route("/readyz", get(handlers::readyz))
    .route("/metrics", get(handlers::prometheus))
    .route("/-/version", get(handlers::version))
    .layer(middleware::from_fn(middlewares::request_id))
    .with_state(state)
}
