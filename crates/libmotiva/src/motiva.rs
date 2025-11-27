use std::{
  collections::HashSet,
  sync::{Arc, Mutex},
};

use ahash::{HashMap, RandomState};
use anyhow::Context;
use bon::bon;
use jiff::Span;
use tokio::sync::RwLock;

use crate::{
  HttpCatalogFetcher, TestFetcher,
  catalog::{Catalog, get_merged_catalog},
  error::MotivaError,
  fetcher::CatalogFetcher,
  index::{EntityHandle, IndexProvider},
  matching::MatchParams,
  model::{Entity, HasProperties, SearchEntity},
  prelude::MatchingAlgorithm,
  schemas::SCHEMAS,
  scoring,
};

/// Whether to fetch related entities.
pub enum GetEntityBehavior {
  /// Only fetch the requested entity
  RootOnly,
  /// Recursive into related entities and join them to the requested one.
  FetchNestedEntity,
}

#[derive(Clone, Debug, Default)]
pub struct MotivaConfig {
  pub outdated_grace: Span,
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
/// # use libmotiva::{prelude::*, MockedElasticsearch};
/// # use std::collections::HashMap;
///
/// # tokio_test::block_on(async {
///   # let es = MockedElasticsearch::default();
///   let motiva = Motiva::new(es).build().await.unwrap();
///
///   let search = SearchEntity::builder("Person").properties(&[("name", &["John Doe"])]).build();
///   let results = motiva.search(&search, &MatchParams::default()).await.unwrap();
///   let scores = motiva.score::<NameBased>(&search, results, 0.5).unwrap();
///
///   for (entity, score) in scores {
///       if let Some(name) = entity.props(&["name"]).iter().next() {
///           println!("Scored entity: {} with score: {}", name, score);
///       }
///   }
/// # });
/// ```
#[derive(Clone, Debug)]
pub struct Motiva<P: IndexProvider, F: CatalogFetcher = HttpCatalogFetcher> {
  index: P,
  fetcher: F,
  config: MotivaConfig,
  catalog: Arc<RwLock<Catalog>>,
}

#[bon]
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
  #[allow(clippy::new_ret_no_self)]
  #[builder(start_fn = new, finish_fn = build)]
  pub async fn _new(#[builder(start_fn)] provider: P, #[builder(default)] config: MotivaConfig) -> Result<Motiva<P, HttpCatalogFetcher>, MotivaError> {
    crate::init();

    let fetcher = HttpCatalogFetcher::default();
    let catalog = get_merged_catalog(&fetcher, &provider, config.outdated_grace).await.context("could not initialize manifest")?;

    Ok(Motiva {
      config,
      index: provider,
      fetcher,
      catalog: Arc::new(RwLock::new(catalog)),
    })
  }

  #[builder(finish_fn = build)]
  pub async fn custom<F: CatalogFetcher>(#[builder(start_fn)] provider: P, fetcher: F, #[builder(default)] config: MotivaConfig) -> Result<Motiva<P, F>, MotivaError> {
    crate::init();

    let catalog = get_merged_catalog(&fetcher, &provider, config.outdated_grace).await.context("could not initialize manifest")?;

    Ok(Motiva {
      config,
      index: provider,
      fetcher,
      catalog: Arc::new(RwLock::new(catalog)),
    })
  }
}

#[bon]
impl<P: IndexProvider> Motiva<P, TestFetcher> {
  #[builder(finish_fn = build)]
  pub async fn test(
    #[builder(start_fn)] provider: P,
    #[builder(default = TestFetcher::default())] fetcher: TestFetcher,
    #[builder(default)] config: MotivaConfig,
  ) -> Result<Motiva<P, TestFetcher>, MotivaError> {
    crate::init();

    let catalog = get_merged_catalog(&fetcher, &provider, config.outdated_grace).await.context("could not initialize manifest")?;

    Ok(Motiva::<P, _> {
      config,
      index: provider,
      fetcher,
      catalog: Arc::new(RwLock::new(catalog)),
    })
  }
}

impl<P: IndexProvider, F: CatalogFetcher> Motiva<P, F> {
  /// Retrieve the backing store availability.
  ///
  /// The actual implementation is dependent on the concrete type of
  /// [`IndexProvider`]. For Elasticsearch, for example, it checks that the
  /// index is available and ready to be queried.
  pub async fn health(&self) -> Result<bool, MotivaError> {
    self.index.health().await
  }

  /// Perform an entity search and return the candidates.
  pub async fn search(&self, entity: &SearchEntity, params: &MatchParams) -> Result<Vec<Entity>, MotivaError> {
    self.index.search(&self.catalog, entity, params).await
  }

  /// Get an entity from its ID.
  ///
  /// The `behavior` parameter defines whether to recurse into related entities
  /// to fetch their details. It returns an [`EntityHandle`], that can either be
  /// the entity, or the ID of another entity.
  pub async fn get_entity(&self, id: &str, behavior: GetEntityBehavior) -> Result<EntityHandle, MotivaError> {
    match self.index.get_entity(id).await? {
      EntityHandle::Referent(id) => Ok(EntityHandle::Referent(id)),

      EntityHandle::Nominal(mut entity) => {
        let id = id.to_string();

        if let GetEntityBehavior::RootOnly = behavior {
          return Ok(EntityHandle::Nominal(entity));
        }

        let mut root = Some(&id);
        let mut seen = HashSet::<_, RandomState>::from_iter([id.clone()]);

        let mut ids: Vec<String> = Vec::new();
        let mut root_arena: HashMap<String, String> = Default::default();
        let mut arena: HashMap<String, (Arc<Mutex<Entity>>, String)> = Default::default();

        if let Some(properties) = entity.schema.properties() {
          for (name, property) in properties {
            if property._type != "entity" {
              continue;
            }

            for assoc in entity.props(&[&name]).iter() {
              root_arena.insert(assoc.to_string(), name.clone());
            }

            ids.extend(entity.props(&[&name]).iter().cloned());
          }

          while !ids.is_empty() {
            let associations = self.index.get_related_entities(root, &ids, &seen).await?;

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

              seen.insert(association.id);
            }
          }
        }

        Ok(EntityHandle::Nominal(entity))
      }
    }
  }

  /// Perform the scoring of all candidates against the search parameters.
  pub fn score<A: MatchingAlgorithm>(&self, entity: &SearchEntity, hits: Vec<Entity>, cutoff: f64) -> anyhow::Result<Vec<(Entity, f64)>> {
    scoring::score::<A>(entity, hits, cutoff)
  }

  /// Refresh the local catalog from upstream.
  ///
  /// This will fetch the latest catalogs and bare datasets, as configured
  /// by the manifest, and merge it with the currently synced indices.
  pub async fn refresh_catalog(&self) {
    match get_merged_catalog(&self.fetcher, &self.index, self.config.outdated_grace).await {
      Ok(catalog) => {
        *self.catalog.write().await = catalog;
      }

      Err(err) => tracing::warn!(error = err.to_string(), "could not refresh catalog"),
    }
  }

  /// Return the merged catalog.
  ///
  /// By default, returns the cached merged dataset from the latest pull.
  /// If `force_refresh` is set to `true`, will perform a synchronous
  /// synchronization and merge from upstream.
  pub async fn get_catalog(&self, force_refresh: bool) -> anyhow::Result<Catalog> {
    if force_refresh {
      self.refresh_catalog().await;
    }

    Ok(self.catalog.read().await.clone())
  }
}
#[cfg(test)]
mod tests {
  use std::collections::HashMap;

  use crate::{
    Catalog, CatalogDataset, MockedElasticsearch, Motiva, TestFetcher,
    catalog::{Manifest, ManifestCatalog},
  };

  #[tokio::test]
  async fn catalog_refresh() {
    let mut catalogs = HashMap::default();
    catalogs.insert(
      "dummyurl".to_string(),
      Catalog {
        datasets: vec![CatalogDataset {
          name: "dataset1".to_string(),
          ..Default::default()
        }],
        ..Default::default()
      },
    );

    let fetcher = TestFetcher {
      manifest: Manifest {
        catalogs: vec![ManifestCatalog {
          url: "dummyurl".to_string(),
          ..Default::default()
        }],
        ..Default::default()
      },
      catalogs,
    };

    let index = MockedElasticsearch::builder().healthy(true).build();
    let motiva = Motiva::custom(index).fetcher(fetcher).build().await.unwrap();
    let initial_catalog = { motiva.catalog.read().await.clone() };

    assert_eq!(initial_catalog.datasets.len(), 1);
    assert!(initial_catalog.datasets.iter().find(|ds| ds.name == "dataset1").is_some());

    motiva.refresh_catalog().await;
  }
}
