use std::collections::HashMap;

use axum::{Json, extract::State};
use libmotiva::prelude::*;
use serde::Deserialize;
use serde_inline_default::serde_inline_default;
use tracing::instrument;

use crate::api::{
  AppState,
  errors::AppError,
  middlewares::{auth::Auth, types::Query},
};

#[serde_inline_default]
#[derive(Clone, Debug, Default, Deserialize)]
pub struct GetCatalogParams {
  #[serde_inline_default(false)]
  force_refresh: bool,
}

#[instrument(skip_all)]
pub async fn get_catalog<F: CatalogFetcher, P: IndexProvider>(State(state): State<AppState<F, P>>, _: Auth<F, P>, Query(query): Query<GetCatalogParams>) -> Result<Json<Catalog>, AppError> {
  Ok(Json(state.motiva.get_catalog(query.force_refresh).await?))
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct GetFieldValuesBody {
  fields: Vec<String>,
  query: Option<serde_json::Value>,
}

#[instrument(skip_all)]
pub async fn get_field_values<F: CatalogFetcher, P: IndexProvider>(
  State(state): State<AppState<F, P>>,
  _: Auth<F, P>,
  Json(query): Json<GetFieldValuesBody>,
) -> Result<Json<HashMap<String, Vec<String>>>, AppError> {
  let fields = query.fields.iter().map(String::as_ref).collect::<Vec<_>>();

  Ok(Json(state.motiva.get_field_values(&fields, query.query).await?))
}
