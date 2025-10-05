use axum::{
  Json,
  extract::{Path, State},
  response::{IntoResponse, Redirect},
};
use axum_extra::extract::Query;
use libmotiva::prelude::*;
use reqwest::StatusCode;
use tracing::instrument;

use crate::api::{AppState, dto::GetEntityParams, errors::AppError, middlewares::auth::Auth};

#[instrument(skip_all)]
pub async fn get_entity<P: IndexProvider>(State(state): State<AppState<P>>, _: Auth<P>, Path(id): Path<String>, Query(params): Query<GetEntityParams>) -> Result<impl IntoResponse, AppError> {
  let behavior = if params.nested { GetEntityBehavior::FetchNestedEntity } else { GetEntityBehavior::RootOnly };

  match state.motiva.get_entity(&id, behavior).await.map_err(Into::<AppError>::into)? {
    EntityHandle::Referent(id) => Ok(Redirect::permanent(&format!("/entities/{id}")).into_response()),
    EntityHandle::Nominal(entity) => Ok((StatusCode::OK, Json(entity)).into_response()),
  }
}
