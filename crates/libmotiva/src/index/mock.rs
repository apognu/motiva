use std::{collections::HashSet, sync::Arc};

use ahash::RandomState;
use bon::Builder;
use tokio::sync::RwLock;

use crate::{
  Catalog,
  error::MotivaError,
  index::{EntityHandle, IndexProvider, elastic::version::IndexVersion},
  matching::MatchParams,
  model::{Entity, SearchEntity},
};

#[doc(hidden)]
#[allow(clippy::type_complexity)]
#[derive(Clone, Builder, Default)]
pub struct MockedElasticsearch {
  healthy: Option<bool>,
  #[builder(default)]
  entities: Vec<Entity>,
  #[builder(default)]
  indices: Vec<(String, String)>,
  #[builder(default)]
  related_entitites: Vec<((Option<String>, Vec<String>, HashSet<String>), Vec<Entity>)>,
}

impl IndexProvider for MockedElasticsearch {
  fn index_version(&self) -> IndexVersion {
    IndexVersion::V4
  }

  async fn health(&self) -> Result<bool, MotivaError> {
    match self.healthy {
      None => Err(MotivaError::OtherError(anyhow::anyhow!("an error"))),
      Some(health) => Ok(health),
    }
  }

  async fn search(&self, _: &Arc<RwLock<Catalog>>, _: &SearchEntity, _: &MatchParams) -> Result<Vec<Entity>, MotivaError> {
    Ok(self.entities.clone())
  }

  async fn get_entity(&self, _: &str) -> Result<EntityHandle, MotivaError> {
    unimplemented!();
  }

  async fn get_related_entities(&self, root: Option<&String>, ids: &[String], negatives: &HashSet<String, RandomState>) -> Result<Vec<Entity>, MotivaError> {
    let negatives = HashSet::from_iter(negatives.iter().map(|id| id.to_owned()));

    for (args, entities) in &self.related_entitites {
      if args == &(root.map(|id| id.to_owned()), ids.to_vec(), negatives.to_owned()) {
        return Ok(entities.clone());
      }
    }

    Ok(vec![])
  }

  async fn list_indices(&self) -> Result<Vec<(String, String)>, MotivaError> {
    Ok(self.indices.clone())
  }
}
