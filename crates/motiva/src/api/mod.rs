use std::time::Duration;

use axum::{
  Router,
  extract::Request,
  middleware,
  routing::{get, post},
};
use libmotiva::prelude::*;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
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
  pub prometheus: Option<PrometheusHandle>,
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

  let prometheus = match config.enable_prometheus {
    true => {
      let builder = PrometheusBuilder::new()
        .add_global_label("service", "motiva")
        .set_buckets_for_metric(Matcher::Full("motiva_scoring_scores".into()), &[0.2, 0.5, 0.7, 0.9])?
        .set_buckets_for_metric(Matcher::Full("motiva_scoring_latency_seconds".into()), &[0.000001, 0.000005, 0.000015, 0.0000050, 0.000100])?
        .set_buckets_for_metric(Matcher::Full("motiva_indexer_latency_seconds".into()), &[0.03, 0.06, 0.1, 0.2, 0.3])?;

      Some(builder.install_recorder().expect("failed to install recorder"))
    }

    false => None,
  };

  let state = AppState {
    config: config.clone(),
    prometheus,
    motiva,
  };

  Ok(
    Router::new()
      .route("/catalog", get(handlers::catalog))
      .route("/match/{scope}", post(handlers::match_entities))
      .route("/entities/{id}", get(handlers::get_entity))
      .fallback(handlers::not_found)
      .layer(middleware::from_fn(middlewares::metrics))
      .layer(TraceLayer::new_for_http().make_span_with(|req: &Request| {
        let parent = global::get_text_map_propagator(|propagator| propagator.extract(&HeaderExtractor(req.headers())));
        let request_id = Uuid::new_v4();

        let span = tracing::info_span!("request", %request_id);
        span.set_parent(parent);

        span
      }))
      // The routes below will not go through the observability middlewares above
      .route("/healthz", get(handlers::healthz))
      .route("/readyz", get(handlers::readyz))
      .route("/metrics", get(handlers::prometheus))
      .layer(middleware::from_fn_with_state(state.clone(), middlewares::logging::api_logger))
      .layer(middleware::from_fn(middlewares::request_id))
      .with_state(state),
  )
}
