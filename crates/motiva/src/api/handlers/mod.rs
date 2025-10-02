mod get_entity;
mod match_entities;
mod proxy;

use axum::extract::State;
use axum::response::IntoResponse;
use libmotiva::prelude::*;
use reqwest::StatusCode;

use crate::api::AppState;
use crate::api::errors::AppError;

pub(super) use self::get_entity::get_entity;
pub use self::match_entities::match_entities;
pub(super) use self::proxy::catalog;

pub async fn not_found() -> impl IntoResponse {
  AppError::ResourceNotFound
}

pub async fn healthz() -> StatusCode {
  StatusCode::OK
}

pub async fn readyz<P: IndexProvider>(State(state): State<AppState<P>>) -> Result<impl IntoResponse, AppError> {
  match state.motiva.health().await? {
    true => Ok(StatusCode::OK),
    false => Ok(StatusCode::SERVICE_UNAVAILABLE),
  }
}

pub async fn prometheus<P: IndexProvider>(State(state): State<AppState<P>>) -> (StatusCode, String) {
  let Some(prometheus) = state.prometheus else {
    return (StatusCode::NOT_FOUND, String::default());
  };

  (StatusCode::OK, prometheus.render())
}
