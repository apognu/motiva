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
  model::{Entity, HasProperties},
};

#[instrument(skip_all)]
pub async fn get_entity(State(state): State<AppState>, Path(id): Path<String>) -> Result<impl IntoResponse, AppError> {
  match index::get::get_entity(&state, &id).await? {
    GetEntityResult::Nominal(mut entity) => {
      if let Some(properties) = entity.schema.properties() {
        for (name, property) in properties {
          if property._type != "entity" {
            continue;
          }

          let values = entity.property(&name).to_vec();

          if values.is_empty() {
            continue;
          }

          // TODO: study how getting related entities work beyond this
          entity.properties.entities.insert(
            name,
            serde_json::to_value(index::get::get_related_entities(&state, &values).await?.into_iter().map(Entity::from).collect::<Vec<_>>()).map_err(|err| AppError::OtherError(err.into()))?,
          );
        }
      }

      Ok((StatusCode::OK, Json(entity)).into_response())
    }

    GetEntityResult::Referent(id) => Ok(Redirect::permanent(&format!("/entities/{id}")).into_response()),
  }
}
