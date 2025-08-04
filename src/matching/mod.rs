mod matchers;

use bumpalo::Bump;

use crate::model::{Entity, SearchEntity};

pub mod comparers;
pub mod extractors;
pub mod logic_v1;
pub mod name_based;
pub mod name_qualified;
pub mod replacers;
pub mod validators;

pub trait MatchingAlgorithm {
  fn name() -> &'static str;
  fn score(bump: &Bump, lhs: &SearchEntity, rhs: &Entity) -> (f64, Vec<(&'static str, f64)>);
}

pub trait Feature<'e>: Send + Sync {
  fn name(&self) -> &'static str;
  fn score_feature(&self, bump: &Bump, lhs: &'e SearchEntity, rhs: &'e Entity) -> f64;
}

pub fn run_features<'e>(bump: &Bump, lhs: &'e SearchEntity, rhs: &'e Entity, init: f64, features: &[(&dyn Feature<'e>, f64)], results: &mut Vec<(&'static str, f64)>) -> f64 {
  features.iter().fold(init, move |score, (func, weight)| {
    let feature_score = func.score_feature(bump, lhs, rhs);

    results.push((func.name(), feature_score));

    tracing::debug!(feature = func.name(), score = feature_score, "computed feature score");

    score + (feature_score * weight)
  })
}
