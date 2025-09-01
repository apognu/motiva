#![allow(unexpected_cfgs)]

use std::{collections::HashSet, sync::Arc};

use ahash::RandomState;
use tokio::sync::RwLock;

use crate::{
  catalog::{Collections, fetch_catalog},
  error::MotivaError,
  index::{GetEntityResult, IndexProvider},
  matching::MatchParams,
  model::{Entity, SearchEntity},
  prelude::MatchingAlgorithm,
};

mod catalog;
mod error;
mod index;
mod matching;
mod model;
mod schemas;
mod scoring;

#[cfg(any(test, feature = "benchmarks"))]
mod tests;

pub fn init() {
  let _ = *crate::schemas::SCHEMAS;
  let _ = *crate::matching::replacers::company_types::ORG_TYPES;
  let _ = *crate::matching::replacers::addresses::ADDRESS_FORMS;
  let _ = *crate::matching::replacers::ordinals::ORDINALS;
}

pub mod prelude {
  pub use crate::Motiva;

  pub use crate::schemas::SCHEMAS;

  pub use crate::catalog::Collections;
  pub use crate::error::MotivaError;
  pub use crate::index::{
    GetEntityResult, IndexProvider,
    elastic::{ElasticsearchProvider, builder::EsAuthMethod},
    mock::MockedElasticsearch,
  };
  pub use crate::matching::{Algorithm, MatchParams, MatchingAlgorithm, logic_v1::LogicV1, name_based::NameBased, name_qualified::NameQualified};
  pub use crate::model::{Entity, HasProperties, SearchEntity};
}

#[derive(Clone, Debug)]
pub struct Motiva<P: IndexProvider> {
  yente: Option<String>,
  catalog: Arc<RwLock<Collections>>,
  index: P,
}

impl<P: IndexProvider> Motiva<P> {
  pub async fn new(provider: P, yente: Option<String>) -> Result<Self, MotivaError> {
    init();

    let catalog = fetch_catalog(&yente.as_ref().map(|y| format!("{y}/catalog"))).await?;

    Ok(Self {
      index: provider,
      yente,
      catalog: Arc::new(RwLock::new(catalog)),
    })
  }

  pub async fn health(&self) -> Result<bool, MotivaError> {
    self.index.health().await
  }

  pub async fn search(&self, entity: &SearchEntity, params: &MatchParams) -> Result<Vec<Entity>, MotivaError> {
    self.index.search(&self.catalog, entity, params).await
  }

  pub async fn get_entity(&self, id: &str) -> Result<GetEntityResult, MotivaError> {
    self.index.get_entity(id).await
  }

  pub async fn get_related_entities(&self, root: Option<&String>, values: &[String], negatives: &HashSet<String, RandomState>) -> anyhow::Result<Vec<Entity>> {
    self.index.get_related_entities(root, values, negatives).await
  }

  pub async fn refresh_catalog(&self) {
    match fetch_catalog(&self.yente.as_ref().map(|y| format!("{y}/catalog"))).await {
      Ok(catalog) => {
        *self.catalog.write().await = catalog;
      }

      Err(err) => tracing::error!(error = err.to_string(), "could not refresh catalog"),
    }
  }

  pub fn score<A: MatchingAlgorithm>(&self, entity: &SearchEntity, hits: Vec<Entity>, cutoff: f64) -> anyhow::Result<Vec<(Entity, f64)>> {
    scoring::score::<A>(entity, hits, cutoff)
  }
}
