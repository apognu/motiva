use std::time::Duration;

use axum::{
  Router,
  extract::Request,
  middleware,
  routing::{get, post},
};
use libmotiva::prelude::*;
use opentelemetry::global;
use opentelemetry_http::HeaderExtractor;
use tower_http::trace::TraceLayer;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::api::config::Config;

pub mod config;
pub mod dto;
pub mod errors;

pub mod handlers;
mod middlewares;

#[derive(Clone)]
pub struct AppState<P: IndexProvider> {
  pub config: Config,
  pub motiva: Motiva<P>,
}

pub async fn routes(config: &Config) -> anyhow::Result<Router> {
  let motiva = Motiva::new(ElasticsearchProvider::new(&config.index_url, config.index_auth_method.clone())?, config.yente_url.clone())
    .await
    .unwrap();

  tokio::spawn({
    let motiva = motiva.clone();

    async move {
      loop {
        tokio::time::sleep(Duration::from_secs(60 * 60)).await;

        motiva.refresh_catalog().await;
      }
    }
  });

  let state = AppState { config: config.clone(), motiva };

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
