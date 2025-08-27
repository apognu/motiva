mod get_entity;
mod match_entities;
mod proxy;

use anyhow::Context;
use axum::extract::State;
use axum::response::IntoResponse;
use elasticsearch::cluster::ClusterHealthParts;
use reqwest::StatusCode;
use serde::Deserialize;

use crate::api::AppState;
use crate::api::errors::AppError;

pub(super) use self::get_entity::get_entity;
pub(super) use self::match_entities::match_entities;
pub(super) use self::proxy::catalog;

pub async fn not_found() -> impl IntoResponse {
  AppError::ResourceNotFound
}

#[derive(Deserialize)]
struct EsHealth {
  status: String,
}

pub async fn healthz() -> StatusCode {
  StatusCode::OK
}

pub async fn readyz(State(state): State<AppState>) -> Result<impl IntoResponse, AppError> {
  let Ok(health) = state
    .es
    .cluster()
    .health(ClusterHealthParts::Index(&["yente-entities"]))
    .send()
    .await
    .context("could not get cluster health")
  else {
    return Ok(StatusCode::SERVICE_UNAVAILABLE);
  };

  let Ok(health): Result<EsHealth, _> = health.json().await.context("could not deserialize cluster health") else {
    return Ok(StatusCode::SERVICE_UNAVAILABLE);
  };

  match health.status.as_str() {
    "green" | "yellow" => Ok(StatusCode::OK),
    _ => Ok(StatusCode::SERVICE_UNAVAILABLE),
  }
}
