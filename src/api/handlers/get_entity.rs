use axum::{
  Json,
  extract::{Path, State},
  response::{IntoResponse, Redirect},
};
use reqwest::StatusCode;
use tracing::instrument;

use crate::{
  api::{AppState, errors::AppError},
  index::{self, get::GetEntityResult},
};

#[instrument(skip_all)]
pub async fn get_entity(State(state): State<AppState>, Path(id): Path<String>) -> Result<impl IntoResponse, AppError> {
  match index::get::get(&state, &id).await? {
    GetEntityResult::Nominal(entity) => Ok((StatusCode::OK, Json(entity)).into_response()),
    GetEntityResult::Referent(id) => Ok(Redirect::permanent(&format!("/entities/{id}")).into_response()),
  }
}
