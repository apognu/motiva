#![allow(unexpected_cfgs)]

mod error;
mod index;
mod matching;
mod model;
mod schemas;

pub mod catalog;
pub mod scoring;

#[cfg(any(test, feature = "benchmarks"))]
mod tests;

pub fn init() {
  let _ = *crate::schemas::SCHEMAS;
  let _ = *crate::matching::replacers::company_types::ORG_TYPES;
  let _ = *crate::matching::replacers::addresses::ADDRESS_FORMS;
  let _ = *crate::matching::replacers::ordinals::ORDINALS;
}

pub mod prelude {
  pub use crate::schemas::SCHEMAS;

  pub use crate::catalog::Collections;
  pub use crate::error::MotivaError;
  pub use crate::index::{
    IndexProvider,
    elastic::{ElasticsearchProvider, GetEntityResult},
  };
  pub use crate::matching::{Algorithm, MatchParams, MatchingAlgorithm, logic_v1::LogicV1, name_based::NameBased, name_qualified::NameQualified};
  pub use crate::model::{Entity, HasProperties, SearchEntity};

  pub use crate::index::mock::MockedElasticsearch;
}
