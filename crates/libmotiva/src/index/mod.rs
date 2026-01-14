pub mod elastic;
pub mod mock;

use std::{collections::HashSet, sync::Arc};

use ahash::RandomState;
use tokio::sync::RwLock;

use crate::{
  Catalog,
  error::MotivaError,
  index::elastic::version::IndexVersion,
  matching::MatchParams,
  model::{Entity, SearchEntity},
};

#[allow(async_fn_in_trait)]
pub trait IndexProvider: Clone + Send + Sync + 'static {
  fn index_version(&self) -> IndexVersion;
  fn health(&self) -> impl Future<Output = Result<bool, MotivaError>> + Send;
  fn get_entity(&self, id: &str) -> impl Future<Output = Result<EntityHandle, MotivaError>> + Send;
  fn get_related_entities(&self, root: Option<&String>, values: &[String], negatives: &HashSet<String, RandomState>) -> impl Future<Output = Result<Vec<Entity>, MotivaError>> + Send;
  fn search(&self, catalog: &Arc<RwLock<Catalog>>, entity: &SearchEntity, params: &MatchParams) -> impl Future<Output = Result<Vec<Entity>, MotivaError>> + Send;
  fn list_indices(&self) -> impl Future<Output = Result<Vec<(String, String)>, MotivaError>> + Send;
}

/// Reference to an entity
///
/// If an entity changes IDs over time, it will not be found at previous IDs.
/// Instead, a `Referent` will be returned with the entity's canonical ID.
#[derive(Clone, Debug)]
pub enum EntityHandle {
  /// The data of the actual entity that was requested
  Nominal(Box<Entity>),
  /// The canonical ID of the requested entity that should be requested
  Referent(String),
}
