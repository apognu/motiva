pub mod elastic;
pub mod mock;

use std::{
  collections::{HashMap, HashSet},
  sync::Arc,
};

use ahash::RandomState;
use tokio::sync::RwLock;

use crate::{
  Catalog,
  error::MotivaError,
  index::elastic::config::IndexVersion,
  matching::MatchParams,
  model::{Entity, SearchEntity},
};

#[allow(async_fn_in_trait)]
pub trait IndexProvider: Clone + Send + Sync + 'static {
  fn after_init(&self) {}

  fn ready(&self) -> bool {
    true
  }

  fn refresh(&self) -> impl Future<Output = ()> + Send {
    async {}
  }

  fn index_version(&self) -> IndexVersion;
  fn health(&self) -> impl Future<Output = Result<bool, MotivaError>> + Send;
  fn get_entity(&self, id: &str) -> impl Future<Output = Result<EntityHandle, MotivaError>> + Send;
  fn get_related_entities(&self, root: Option<&String>, values: &[String], negatives: &HashSet<String, RandomState>, limit: usize) -> impl Future<Output = Result<Vec<Entity>, MotivaError>> + Send;

  /// Search for candidates matching an entity.
  ///
  /// Decoding the candidates can be CPU-intensive, so implementations may
  /// return them undecoded, and leave it to [`Candidates::decode`], which the
  /// caller can run away from the async runtime.
  fn search(&self, catalog: &Arc<RwLock<Catalog>>, entity: &SearchEntity, params: &MatchParams) -> impl Future<Output = Result<Candidates, MotivaError>> + Send;

  fn list_indices(&self) -> impl Future<Output = Result<Vec<(String, String)>, MotivaError>> + Send;

  fn list_field_values(&self, fields: &[&str], query: Option<serde_json::Value>) -> impl Future<Output = Result<HashMap<String, Vec<String>>, MotivaError>> + Send;
}

/// Search candidates, as returned by an index, that might not be decoded yet.
///
/// Obtained from [`IndexProvider::search`].
#[must_use]
pub struct Candidates(pub(crate) Candidate);

pub(crate) enum Candidate {
  Decoded(Vec<Entity>),
  Json(bytes::Bytes),
}

impl From<Vec<Entity>> for Candidates {
  fn from(entities: Vec<Entity>) -> Self {
    Candidates(Candidate::Decoded(entities))
  }
}

impl Candidates {
  /// Decode the candidates into entities.
  ///
  /// This is CPU-bound and should not be called from an async context.
  pub fn decode(self) -> Result<Vec<Entity>, MotivaError> {
    match self.0 {
      Candidate::Decoded(entities) => Ok(entities),
      Candidate::Json(body) => elastic::queries::decode_search_response(&body),
    }
  }
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
