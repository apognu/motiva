#![allow(unexpected_cfgs)]
#![doc = include_str!("../README.md")]

mod catalog;
mod error;
mod fetcher;
mod index;
mod matching;
mod model;
mod motiva;
mod nested;
mod schemas;
mod scoring;
mod symbols;

#[cfg(any(test, feature = "benchmarks"))]
mod tests;

pub(crate) fn init() {
  let _ = *crate::schemas::SCHEMAS;
  let _ = *crate::matching::replacers::company_types::ORG_TYPES;
  let _ = *crate::matching::replacers::addresses::ADDRESS_FORMS;
  let _ = *crate::matching::replacers::ordinals::ORDINALS;

  let _ = *crate::symbols::tagger::ORG_TAGGER;
  let _ = *crate::symbols::tagger::PERSON_TAGGER;
}

/// Module including most features needed to use the library.
pub mod prelude {
  pub use crate::catalog::{Catalog, CatalogDataset};
  pub use crate::fetcher::{CatalogFetcher, HttpCatalogFetcher};
  pub use crate::motiva::{GetEntityBehavior, Motiva, MotivaConfig};

  pub use crate::error::MotivaError;
  pub use crate::index::{
    EntityHandle, IndexProvider,
    elastic::{ElasticsearchProvider, builder::EsAuthMethod},
  };
  pub use crate::matching::{Algorithm, Feature, MatchParams, MatchingAlgorithm, logic_v1::LogicV1, name_based::NameBased, name_qualified::NameQualified};
  pub use crate::model::{Entity, HasProperties, SearchEntity};
}

#[doc(inline)]
pub use self::prelude::*;

#[doc(hidden)]
pub use crate::fetcher::TestFetcher;
#[doc(hidden)]
pub use crate::index::mock::MockedElasticsearch;

#[cfg(test)]
mod testing {
  use bumpalo::Bump;
  use libmotiva_macros::scoring_feature;

  use crate::{
    Entity, Feature, SearchEntity,
    matching::replacers::{addresses::ADDRESS_FORMS, company_types::ORG_TYPES, ordinals::ORDINALS},
    schemas::SCHEMAS,
  };

  #[test]
  fn initialize_data_structures() {
    super::init();

    assert!(SCHEMAS.len() > 50);
    assert!(ORG_TYPES.1.len() > 250);
    assert!(ORDINALS.1.len() > 4000);
    assert!(ADDRESS_FORMS.1.len() > 300);

    assert_eq!(ORG_TYPES.0.patterns_len(), ORG_TYPES.1.len());
    assert_eq!(ORDINALS.0.patterns_len(), ORDINALS.1.len());
    assert_eq!(ADDRESS_FORMS.0.patterns_len(), ADDRESS_FORMS.1.len());
  }

  #[scoring_feature(TestFeature, name = "test_feature")]
  fn score_feature(&self, _: &Bump, _: &SearchEntity, rhs: &Entity) -> f64 {
    42.0
  }

  #[test]
  fn feature_macro() {
    let lhs = SearchEntity::builder("Person").properties(&[]).build();
    let rhs = Entity::builder("Person").properties(&[]).build();

    assert_eq!(TestFeature.name(), "test_feature");
    assert_eq!(TestFeature.score_feature(&Bump::default(), &lhs, &rhs), 42.0);
  }
}
