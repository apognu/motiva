mod matchers;

use tracing::instrument;

use crate::model::{Entity, SearchEntity};

pub mod extractors;
pub mod logic_v1;
pub mod name_based;
pub mod name_qualified;

pub trait MatchingAlgorithm {
  fn name() -> &'static str;
  fn score(lhs: &SearchEntity, rhs: &Entity) -> (f64, Vec<(&'static str, f64)>);
}

pub trait Feature<'e>: Send + Sync {
  fn name(&self) -> &'static str;
  fn score_feature(&self, lhs: &'e SearchEntity, rhs: &'e Entity) -> f64;
}

#[instrument(skip_all, fields(entity_id = rhs.id))]
pub fn run_features<'e>(lhs: &'e SearchEntity, rhs: &'e Entity, init: f64, features: &[(&dyn Feature<'e>, f64)], results: &mut Vec<(&'static str, f64)>) -> f64 {
  features.iter().fold(init, move |score, (func, weight)| {
    let feature_score = func.score_feature(lhs, rhs);

    results.push((func.name(), feature_score));

    tracing::debug!(feature = func.name(), score = feature_score, "computed feature score");

    score + (feature_score * weight)
  })
}
