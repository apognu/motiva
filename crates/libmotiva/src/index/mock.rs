use std::{collections::HashSet, sync::Arc};

use ahash::RandomState;
use tokio::sync::RwLock;

use crate::{
  catalog::Collections,
  error::MotivaError,
  index::IndexProvider,
  model::{Entity, SearchEntity},
  prelude::{GetEntityResult, MatchParams},
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
  async fn health(&self) -> Result<bool, MotivaError> {
    unimplemented!();
  }

  async fn search(&self, _: &Arc<RwLock<Collections>>, _: &SearchEntity, _: &MatchParams) -> Result<Vec<Entity>, MotivaError> {
    Ok(self.entities.clone())
  }

  async fn get_entity(&self, _: &str) -> Result<GetEntityResult, MotivaError> {
    unimplemented!();
  }

  async fn get_related_entities(&self, _: Option<&String>, _: &[String], _: &HashSet<String, RandomState>) -> anyhow::Result<Vec<Entity>> {
    unimplemented!();
  }
}
