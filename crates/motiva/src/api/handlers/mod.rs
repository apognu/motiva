mod catalog;
mod get_entity;
mod match_entities;

use axum::Json;
use axum::extract::State;
use axum::response::IntoResponse;
use libmotiva::prelude::*;
use reqwest::StatusCode;

use crate::api::AppState;
use crate::api::dto::{AlgorithmDescription, Algorithms};
use crate::api::errors::AppError;

pub use self::catalog::get_catalog;
pub use self::get_entity::get_entity;
pub use self::match_entities::match_entities;

pub async fn not_found() -> impl IntoResponse {
  AppError::ResourceNotFound
}

pub async fn healthz() -> StatusCode {
  StatusCode::OK
}

pub async fn readyz<F: CatalogFetcher, P: IndexProvider>(State(state): State<AppState<F, P>>) -> Result<impl IntoResponse, AppError> {
  match state.motiva.health().await? {
    true => Ok(StatusCode::OK),
    false => Ok(StatusCode::SERVICE_UNAVAILABLE),
  }
}

pub async fn prometheus<F: CatalogFetcher, P: IndexProvider>(State(state): State<AppState<F, P>>) -> (StatusCode, String) {
  let Some(prometheus) = state.prometheus else {
    return (StatusCode::NOT_FOUND, String::default());
  };

  (StatusCode::OK, prometheus.render())
}

pub async fn algorithms() -> Json<Algorithms> {
  const ALGORITHMS: [Algorithm; 3] = [Algorithm::NameBased, Algorithm::NameQualified, Algorithm::LogicV1];

  Json(Algorithms {
    algorithms: ALGORITHMS.into_iter().map(|alg| AlgorithmDescription { name: alg.name() }).collect(),
    best: Algorithm::best().name(),
    default: Algorithm::default().name(),
  })
}
