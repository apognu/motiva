mod matchers;

use bumpalo::Bump;
use serde::Deserialize;
use serde_inline_default::serde_inline_default;

use crate::model::{Entity, SearchEntity};

pub(crate) mod comparers;
pub(crate) mod extractors;
pub(crate) mod logic_v1;
pub(crate) mod name_based;
pub(crate) mod name_qualified;
pub(crate) mod replacers;
pub(crate) mod validators;

#[derive(Clone, Copy, Debug, Default, Deserialize)]
pub enum Algorithm {
  #[serde(rename = "name-based")]
  NameBased,
  #[serde(rename = "name-qualified")]
  NameQualified,
  #[default]
  #[serde(rename = "logic-v1")]
  LogicV1,
}

pub trait MatchingAlgorithm {
  fn name() -> &'static str;
  fn score(bump: &Bump, lhs: &SearchEntity, rhs: &Entity, cutoff: f64) -> (f64, Vec<(&'static str, f64)>);
}

trait Feature<'e>: Send + Sync {
  fn name(&self) -> &'static str;
  fn score_feature(&self, bump: &Bump, lhs: &'e SearchEntity, rhs: &'e Entity) -> f64;
}

fn run_features<'e>(bump: &Bump, lhs: &'e SearchEntity, rhs: &'e Entity, cutoff: f64, init: f64, features: &[(&dyn Feature<'e>, f64)], results: &mut Vec<(&'static str, f64)>) -> f64 {
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

#[serde_inline_default]
#[derive(Clone, Debug, Default, Deserialize)]
pub struct MatchParams {
  #[serde(skip_deserializing)]
  pub scope: String,
  #[serde_inline_default(5)]
  pub limit: usize,
  #[serde_inline_default(0.7)]
  pub threshold: f64,
  #[serde_inline_default(0.5)]
  pub cutoff: f64,
  #[serde_inline_default(Algorithm::LogicV1)]
  pub algorithm: Algorithm,
  pub topics: Option<Vec<String>>,
  #[serde(default)]
  pub include_dataset: Vec<String>,
  #[serde(default)]
  pub exclude_dataset: Vec<String>,
}
