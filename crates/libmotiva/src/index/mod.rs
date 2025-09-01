pub mod elastic;
pub mod mock;

use std::{collections::HashSet, sync::Arc};

use ahash::RandomState;
use tokio::sync::RwLock;

use crate::{
  catalog::Collections,
  error::MotivaError,
  matching::MatchParams,
  model::{Entity, SearchEntity},
};

#[allow(async_fn_in_trait)]
pub trait IndexProvider: Clone + Send + Sync {
  async fn health(&self) -> Result<bool, MotivaError>;
  async fn get_entity(&self, id: &str) -> Result<EntityHandle, MotivaError>;
  async fn get_related_entities(&self, root: Option<&String>, values: &[String], negatives: &HashSet<String, RandomState>) -> anyhow::Result<Vec<Entity>>;

  fn search(&self, catalog: &Arc<RwLock<Collections>>, entity: &SearchEntity, params: &MatchParams) -> impl Future<Output = Result<Vec<Entity>, MotivaError>> + Send;
}

pub enum EntityHandle {
  Nominal(Box<Entity>),
  Referent(String),
}
