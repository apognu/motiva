use axum::{
  Json,
  extract::{Path, State},
  response::{IntoResponse, Redirect},
};
use axum_extra::extract::Query;
use libmotiva::prelude::*;
use reqwest::StatusCode;
use tracing::instrument;

use crate::api::{AppState, dto::GetEntityParams, errors::AppError};

#[instrument(skip_all)]
pub async fn get_entity<P: IndexProvider>(State(state): State<AppState<P>>, Path(id): Path<String>, Query(params): Query<GetEntityParams>) -> Result<impl IntoResponse, AppError> {
  match state.motiva.get_entity(&id, params.nested).await.map_err(Into::<AppError>::into)? {
    EntityHandle::Referent(id) => Ok(Redirect::permanent(&format!("/entities/{id}")).into_response()),
    EntityHandle::Nominal(entity) => Ok((StatusCode::OK, Json(entity)).into_response()),
  }
}
