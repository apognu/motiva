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
  fn score(bump: &Bump, lhs: &SearchEntity, rhs: &Entity, cutoff: f64) -> (f64, Vec<(&'static str, f64)>);
}

pub trait Feature<'e>: Send + Sync {
  fn name(&self) -> &'static str;
  fn score_feature(&self, bump: &Bump, lhs: &'e SearchEntity, rhs: &'e Entity) -> f64;
}

pub fn run_features<'e>(bump: &Bump, lhs: &'e SearchEntity, rhs: &'e Entity, cutoff: f64, init: f64, features: &[(&dyn Feature<'e>, f64)], results: &mut Vec<(&'static str, f64)>) -> f64 {
  features.iter().fold(init, move |score, (func, weight)| {
    // We assume all modifiers (with negative weights) tail the models, so if we
    // are already below the cutoff, there is no way the score could go up
    // again, so we skip the rest.
    if score < cutoff && weight < &0.0 {
      return score;
    }

    let feature_score = func.score_feature(bump, lhs, rhs);

    results.push((func.name(), feature_score));

    tracing::debug!(feature = func.name(), score = feature_score, "computed feature score");

    score + (feature_score * weight)
  })
}
