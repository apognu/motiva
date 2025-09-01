#![allow(unexpected_cfgs)]

use std::{collections::HashSet, sync::Arc};

use ahash::RandomState;
use tokio::sync::RwLock;

use crate::{
  catalog::{Collections, fetch_catalog},
  error::MotivaError,
  index::{EntityHandle, IndexProvider},
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
    EntityHandle, IndexProvider,
    elastic::{ElasticsearchProvider, builder::EsAuthMethod},
    mock::MockedElasticsearch,
  };
  pub use crate::matching::{Algorithm, MatchParams, MatchingAlgorithm, logic_v1::LogicV1, name_based::NameBased, name_qualified::NameQualified};
  pub use crate::model::{Entity, HasProperties, SearchEntity};
}

/// The main entrypoint for using the Motiva library.
///
/// `motiva` provides functionality to search for entities within sanctioned lists
/// and score them against a given input entity. It requires an [`IndexProvider`]
/// to connect to a search backend like Elasticsearch.
///
/// # Examples
///
/// ```rust
/// # use libmotiva::prelude::*;
/// # use std::collections::HashMap;
///
/// # tokio_test::block_on(async {
///   # let es = MockedElasticsearch::default();
///   let motiva = Motiva::new(es, None).await.unwrap();
///
///   let search = SearchEntity::builder("Person").properties(&[("name", &["John Doe"])]).build();
///   let results = motiva.search(&search, &MatchParams::default()).await.unwrap();
///   let scores = motiva.score::<NameBased>(&search, results, 0.5).unwrap();
///
///   for (entity, score) in scores {
///       if let Some(name) = entity.property("name").iter().next() {
///           println!("Scored entity: {} with score: {}", name, score);
///       }
///   }
/// # });
/// ```
#[derive(Clone, Debug)]
pub struct Motiva<P: IndexProvider> {
  yente: Option<String>,
  catalog: Arc<RwLock<Collections>>,
  index: P,
}

impl<P: IndexProvider> Motiva<P> {
  /// Create a new Motiva instance.
  ///
  /// It takes an instance of [`IndexProvider`] and an optional URL to the
  /// underlying Yente instance (so it can be used to retrieve the actual
  /// synchronized catalog). If missing, motiva will use the published Open
  /// Sanctions catalog.
  ///
  /// This function has side-effects, and will perform to actions when called: -
  ///
  ///  - Initialize the data structures used for matching. Those can be heavy and
  ///    will only be initialized once in the lifetime of the program.
  ///  - Perform an initial fetch of the catalog. After this, it is the caller's
  ///    responsibility to refresh it as needed.
  ///
  /// This struct can be safely cloned and sent across thread boundaries.
  pub async fn new(provider: P, yente: Option<String>) -> Result<Self, MotivaError> {
    init();

    let catalog = fetch_catalog(&yente.as_ref().map(|y| format!("{y}/catalog"))).await?;

    Ok(Self {
      index: provider,
      yente,
      catalog: Arc::new(RwLock::new(catalog)),
    })
  }

  /// Retrieve the backing store availability.
  ///
  /// The actual implementation is dependent on the concrete type of
  /// [`IndexProvider`]. For Elasticsearch, for example, it checks that the
  /// index is available and ready to be queried.
  pub async fn health(&self) -> Result<bool, MotivaError> {
    self.index.health().await
  }

  pub async fn search(&self, entity: &SearchEntity, params: &MatchParams) -> Result<Vec<Entity>, MotivaError> {
    self.index.search(&self.catalog, entity, params).await
  }

  pub async fn get_entity(&self, id: &str) -> Result<EntityHandle, MotivaError> {
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

#[cfg(test)]
mod testing {
  use crate::{
    matching::replacers::{addresses::ADDRESS_FORMS, company_types::ORG_TYPES, ordinals::ORDINALS},
    schemas::SCHEMAS,
  };

  #[test]
  fn initialize_data_structures() {
    super::init();

    assert!(SCHEMAS.len() > 50);
    assert!(ORG_TYPES.1.len() > 1000);
    assert!(ORDINALS.1.len() > 4000);
    assert!(ADDRESS_FORMS.1.len() > 300);

    assert_eq!(ORG_TYPES.0.patterns_len(), ORG_TYPES.1.len());
    assert_eq!(ORDINALS.0.patterns_len(), ORDINALS.1.len());
    assert_eq!(ADDRESS_FORMS.0.patterns_len(), ADDRESS_FORMS.1.len());
  }
}
