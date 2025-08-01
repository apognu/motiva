mod get_entity;
mod match_entities;

use axum::response::IntoResponse;

use crate::api::errors::AppError;

pub(super) use self::get_entity::get_entity;
pub(super) use self::match_entities::match_entities;

pub async fn not_found() -> impl IntoResponse {
  AppError::ResourceNotFound
}
