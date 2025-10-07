mod matchers;

#[cfg(test)]
mod tests;

use bumpalo::Bump;
use jiff::Timestamp;
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

/// Matching algorithms supported by motiva
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

/// Algorithm used to score a SearchEntity against an Entity
pub trait MatchingAlgorithm {
  /// Readable name of the matching algorithm.
  fn name() -> &'static str;
  /// Score an entity against search parameters.
  ///
  /// The configured `cutoff` needs to be passed in order to skip features that
  /// cannot influence the score.
  ///
  /// It returns a tuple of the resulting score and a vector of features and
  /// their resulting score.
  fn score(bump: &Bump, lhs: &SearchEntity, rhs: &Entity, cutoff: f64) -> (f64, Vec<(&'static str, f64)>);
}

/// A scoring facet composed into a [`MatchingAlgorithm`]
pub trait Feature<'e>: Send + Sync {
  /// Readable name for the feature
  fn name(&self) -> &'static str;
  /// Score an entity against search parameters.
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

/// Settings for a search
#[serde_inline_default]
#[derive(Clone, Debug, Default, Deserialize)]
pub struct MatchParams {
  /// Root dataset for all search operations
  #[serde(skip_deserializing)]
  pub scope: String,
  /// Maximum number of results to return
  #[serde_inline_default(5)]
  pub limit: usize,
  /// Factor to `limit` to retrieve initial results from the index.
  ///
  /// `limit`*`candidate_factor` entities will be fetched, and `limit` will be returned at most.
  #[serde(skip)]
  pub candidate_factor: usize,
  /// Minimum score to be considered a match.
  ///
  /// An entity can still be returned if it is not a match, if it meet the `cutoff`.
  #[serde_inline_default(0.7)]
  pub threshold: f64,
  /// Minimum score to be returned.
  #[serde_inline_default(0.5)]
  pub cutoff: f64,
  /// Algorithm to use for scoring.
  #[serde_inline_default(Algorithm::LogicV1)]
  pub algorithm: Algorithm,
  /// Filter topics an entity must be part of to be considered.
  pub topics: Option<Vec<String>>,
  #[serde(default)]
  /// Datasets to search from.
  pub include_dataset: Vec<String>,
  #[serde(default)]
  /// Datasets to exclude from the search.
  pub exclude_dataset: Vec<String>,
  /// Only consider entities that were modified after the provided timestamp.
  pub changed_since: Option<Timestamp>,
  /// List of schema to exclude from the search.
  #[serde(default)]
  pub exclude_schema: Vec<String>,
}

impl MatchParams {
  /// Get the number of candidates to fetch from the index.
  ///
  /// It is computed by multiplying `limit` and `candidate_factor` and clamped
  /// between sensible values. The more input entities there are, the more
  /// accurate the results will be.
  pub fn candidate_limit(&self) -> usize {
    (self.limit * self.candidate_factor).clamp(20, 9999)
  }
}
