use std::{collections::HashSet, sync::Arc};

use ahash::RandomState;
use tokio::sync::RwLock;

use crate::{
  catalog::Collections,
  error::MotivaError,
  index::{EntityHandle, IndexProvider},
  matching::MatchParams,
  model::{Entity, SearchEntity},
};

#[derive(Clone, Default)]
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

  async fn get_entity(&self, _: &str) -> Result<EntityHandle, MotivaError> {
    unimplemented!();
  }

  async fn get_related_entities(&self, _: Option<&String>, _: &[String], _: &HashSet<String, RandomState>) -> anyhow::Result<Vec<Entity>> {
    unimplemented!();
  }
}
