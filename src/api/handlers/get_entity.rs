use std::{
  collections::{HashMap, HashSet},
  sync::{Arc, Mutex},
};

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
  schemas::SCHEMAS,
};

#[instrument(skip_all)]
pub async fn get_entity(State(state): State<AppState>, Path(id): Path<String>) -> Result<impl IntoResponse, AppError> {
  match index::get::get_entity(&state, &id).await? {
    GetEntityResult::Nominal(mut entity) => {
      let mut root = Some(&id);
      let mut seen = HashSet::from([id.clone()]);

      let mut ids: Vec<String> = Vec::new();
      let mut root_arena: HashMap<String, String> = Default::default();
      let mut arena: HashMap<String, (Arc<Mutex<Entity>>, String)> = Default::default();

      if let Some(properties) = entity.schema.properties() {
        for (name, property) in properties {
          if property._type != "entity" {
            continue;
          }

          for assoc in entity.property(&name) {
            root_arena.insert(assoc.to_string(), name.clone());
          }

          ids.extend(entity.property(&name).iter().cloned());
        }

        while !ids.is_empty() {
          let associations = index::get::get_related_entities(&state, root, &ids, &seen).await?.into_iter().map(Entity::from).collect::<Vec<_>>();

          root = None;
          ids.clear();

          for association in associations {
            let Some(schema) = SCHEMAS.get(association.schema.as_str()) else {
              continue;
            };

            let ptr = Arc::new(Mutex::new(association.clone()));

            match root_arena.get_mut(&association.id) {
              Some(attr) => entity.properties.entities.entry(attr.clone()).or_default().push(Arc::clone(&ptr)),

              _ => {
                if let Some((parent, attr)) = arena.get_mut(&association.id)
                  && let Ok(mut e) = parent.lock()
                {
                  e.properties.entities.entry(attr.clone()).or_default().push(Arc::clone(&ptr));
                }
              }
            }

            for (name, values) in &association.properties.strings {
              let Some(property) = schema.properties.get(name) else {
                continue;
              };
              if property._type != "entity" {
                continue;
              }

              ids.extend(values.iter().cloned());

              for value in values {
                arena.insert(value.clone(), (Arc::clone(&ptr), name.clone()));
              }

              if let Some(reverse) = property.reverse.as_ref()
                && values.contains(&entity.id)
              {
                entity.properties.entities.entry(reverse.name.clone()).or_default().push(Arc::clone(&ptr));
              }
            }

            seen.insert(association.id.clone());
          }
        }
      }

      Ok((StatusCode::OK, Json(entity)).into_response())
    }

    GetEntityResult::Referent(id) => Ok(Redirect::permanent(&format!("/entities/{id}")).into_response()),
  }
}
