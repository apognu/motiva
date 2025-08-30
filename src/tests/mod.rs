#[cfg(test)]
pub mod api;
#[cfg(test)]
pub mod python;

use std::collections::{HashMap, HashSet};

use ahash::RandomState;
use elasticsearch::http::response::Response;

use crate::{
  api::{AppState, dto::MatchParams, errors::AppError},
  index::{EsEntity, IndexProvider, search::GetEntityResult},
  model::{Entity, Properties, Schema, SearchEntity},
};

#[derive(Clone)]
pub struct MockedElasticsearch {
  entities: Vec<Entity>,
}

impl MockedElasticsearch {
  pub fn with_entities(entities: Vec<Entity>) -> MockedElasticsearch {
    MockedElasticsearch { entities }
  }
}

impl IndexProvider for MockedElasticsearch {
  async fn health(&self) -> Result<Response, elasticsearch::Error> {
    unimplemented!();
  }

  async fn search(&self, _: &AppState<Self>, _: &SearchEntity, _: &MatchParams) -> Result<Vec<Entity>, AppError> {
    Ok(self.entities.clone())
  }

  async fn get_entity(&self, _: &str) -> Result<GetEntityResult, AppError> {
    unimplemented!();
  }

  async fn get_related_entities(&self, _: Option<&String>, _: &[String], _: &HashSet<String, RandomState>) -> anyhow::Result<Vec<EsEntity>> {
    unimplemented!();
  }
}

#[bon::builder]
pub fn e(#[builder(start_fn)] schema: &str, id: Option<&str>, properties: &[(&str, &[&str])]) -> Entity {
  let mut props: HashMap<_, _, RandomState> = HashMap::default();

  for (prop, values) in properties {
    props.insert(prop.to_string(), values.iter().map(|s| s.to_string()).collect::<Vec<_>>());
  }

  Entity {
    schema: Schema::from(schema),
    id: id.map(ToOwned::to_owned).unwrap_or_default(),
    caption: String::new(),
    properties: Properties { strings: props, ..Default::default() },
    ..Default::default()
  }
}

#[bon::builder]
pub fn se(#[builder(start_fn)] schema: &str, properties: &[(&str, &[&str])]) -> SearchEntity {
  let mut props: HashMap<_, _, RandomState> = HashMap::default();

  for (prop, values) in properties {
    props.insert(prop.to_string(), values.iter().map(|s| s.to_string()).collect::<Vec<_>>());
  }

  let mut entity = SearchEntity {
    schema: Schema::from(schema),
    properties: props,
    name_parts: Default::default(),
  };

  entity.precompute();
  entity
}
