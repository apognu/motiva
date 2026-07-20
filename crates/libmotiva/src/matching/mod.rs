mod explanation;
mod matchers;

#[cfg(test)]
mod tests;

pub use explanation::{CodedPair, Detail, Explanation};

use std::{collections::HashMap, time::Instant};

use bumpalo::Bump;
use jiff::Timestamp;
use serde::Deserialize;
use serde_inline_default::serde_inline_default;
use tracing::info_span;

use crate::{
  model::{Entity, SearchEntity},
  scoring::ScoringOptions,
};

pub(crate) mod comparers;
pub(crate) mod extractors;
pub(crate) mod latinize;
pub(crate) mod logic_v1;
pub(crate) mod marble_v0;
pub(crate) mod name_based;
pub(crate) mod name_qualified;
pub(crate) mod replacers;
pub(crate) mod validators;

/// Matching algorithms supported by motiva
#[derive(Clone, Copy, Eq, PartialEq, Debug, Default, Deserialize)]
pub enum Algorithm {
  #[serde(rename = "name-based")]
  NameBased,
  #[serde(rename = "name-qualified")]
  NameQualified,
  #[default]
  #[serde(rename = "logic-v1")]
  LogicV1,
  #[serde(rename = "marble-v0")]
  MarbleV0,
  #[serde(rename = "best")]
  Best,
}

impl Algorithm {
  pub const fn best() -> Algorithm {
    Algorithm::LogicV1
  }

  pub const fn name(&self) -> &'static str {
    match self {
      Algorithm::NameBased => "name-based",
      Algorithm::NameQualified => "name-qualified",
      Algorithm::LogicV1 => "logic-v1",
      Algorithm::MarbleV0 => "marble-v0",
      Algorithm::Best => "best",
    }
  }
}

pub struct ScoreResult(pub f64, pub Option<Detail>);

impl From<ScoreResult> for f64 {
  fn from(result: ScoreResult) -> Self {
    result.0
  }
}

impl From<f64> for ScoreResult {
  fn from(score: f64) -> Self {
    Self(score, None)
  }
}

impl From<(f64, Option<Detail>)> for ScoreResult {
  fn from(result: (f64, Option<Detail>)) -> Self {
    Self(result.0, result.1)
  }
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
  /// It returns a tuple of the resulting score and a vector of per-feature
  /// [`Explanation`]s (name, raw score, weighted score and an optional detail).
  fn score(bump: &Bump, lhs: &SearchEntity, rhs: &Entity, options: &ScoringOptions) -> (f64, Vec<Explanation>);
}

/// A scoring facet composed into a [`MatchingAlgorithm`]
pub trait Feature: Send + Sync {
  /// Readable name for the feature
  fn name(&self) -> &'static str;
  /// Score an entity against search parameters.
  ///
  /// When `explain` is set, the feature also returns a structured [`Detail`]
  /// describing how it scored, computed in the same pass as the score. When it
  /// is not set, the feature returns `None` and does no explanation work at all.
  fn score(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity, explain: bool) -> ScoreResult;

  /// Convenience for callers (mostly tests) that only need the raw score.
  fn score_scalar(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity) -> f64 {
    self.score(bump, lhs, rhs, false).into()
  }
}

pub struct FeaturesConfig<'f, F>
where
  F: IntoIterator<Item = &'f (&'f dyn Feature, f64)>,
{
  features: F,
  weights: &'f HashMap<String, f64>,
  behavior: FeaturesBehavior,
  skip: FeaturesSkip,
  explain: bool,
}

impl<'f, F> FeaturesConfig<'f, F>
where
  F: IntoIterator<Item = &'f (&'f dyn Feature, f64)>,
{
  pub fn summed_features(features: F, options: &'f ScoringOptions) -> Self {
    Self {
      features,
      weights: &options.weights,
      behavior: FeaturesBehavior::Sum,
      skip: FeaturesSkip::Never,
      explain: options.explain,
    }
  }

  pub fn highest_features(features: F, options: &'f ScoringOptions) -> Self {
    Self {
      features,
      weights: &options.weights,
      behavior: FeaturesBehavior::Highest,
      skip: FeaturesSkip::Never,
      explain: options.explain,
    }
  }

  pub fn disqualifiers(features: F, options: &'f ScoringOptions) -> Self {
    Self {
      features,
      weights: &options.weights,
      behavior: FeaturesBehavior::Sum,
      skip: FeaturesSkip::ScoreBelow(options.cutoff),
      explain: options.explain,
    }
  }
}

#[derive(Clone, Copy)]
pub enum FeaturesBehavior {
  Highest,
  Sum,
}

#[derive(Clone, Copy, Default)]
pub enum FeaturesSkip {
  #[default]
  Never,
  ScoreBelow(f64),
}

fn run_features<'f, F>(bump: &Bump, lhs: &SearchEntity, rhs: &Entity, init: f64, config: FeaturesConfig<'f, F>, results: &mut Vec<Explanation>) -> f64
where
  F: IntoIterator<Item = &'f (&'f dyn Feature, f64)>,
{
  config.features.into_iter().fold(init, move |score, (func, weight)| {
    let weight = config.weights.get(func.name()).unwrap_or(weight);

    if weight == &0.0 {
      return score;
    }

    let span = info_span!("scoring_feature", feature = func.name());
    let _span = span.enter();

    if let FeaturesSkip::ScoreBelow(cutoff) = config.skip {
      // We assume all modifiers (with negative weights) tail the models, so if we
      // are already below the cutoff, there is no way the score could go up
      // again, so we skip the rest.
      if score < cutoff && weight < &0.0 {
        return score;
      }
    }

    let then = Instant::now();
    // The detail is only built when explanations are requested; otherwise the
    // feature returns `None` and does no explanation work at all.
    let ScoreResult(feature_score, detail) = func.score(bump, lhs, rhs, config.explain);

    let weighted = feature_score * weight;

    results.push(Explanation {
      name: func.name(),
      score: feature_score,
      weighted,
      detail: detail.unwrap_or_default(),
    });

    tracing::debug!(score = feature_score, latency = ?then.elapsed(), "computed feature score");

    match config.behavior {
      FeaturesBehavior::Sum => score + weighted,
      FeaturesBehavior::Highest if weighted > score => weighted,
      _ => score,
    }
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
  /// Datasets to search from.
  #[serde(default)]
  pub include_dataset: Vec<String>,
  /// Datasets to exclude from the search.
  #[serde(default)]
  pub exclude_dataset: Vec<String>,
  /// List of entity IDs that should not be returned with the matches
  #[serde(default)]
  pub exclude_entity_ids: Vec<String>,
  /// Only consider entities that were modified after the provided timestamp.
  pub changed_since: Option<Timestamp>,
  /// List of schema to exclude from the search.
  #[serde(default)]
  pub exclude_schema: Vec<String>,

  // Motiva-specific params
  #[serde(default)]
  pub partition: bool,
  #[serde(default)]
  pub index_type: IndexType,
  #[serde(default)]
  pub match_candidates: usize,
  /// How many names to sample from the list of names and aliases
  #[serde_inline_default(10)]
  pub name_sample_size: usize,
  /// Return a per-feature `explanations` object detailing how each feature
  /// scored. Disabled by default; enabling it costs extra computation.
  #[serde(default)]
  pub explain: bool,
}

/// Variant of the index to use.
#[derive(Clone, Copy, Eq, PartialEq, Debug, Default, Deserialize)]
pub enum IndexType {
  #[default]
  #[serde(rename = "main")]
  Main,
  #[serde(rename = "scoped")]
  Scoped,
}

impl MatchParams {
  /// Get the number of candidates to fetch from the index.
  ///
  /// It is computed by multiplying `limit` and `candidate_factor` and clamped
  /// between sensible values. The more input entities there are, the more
  /// accurate the results will be.
  pub fn candidate_limit(&self, query: usize) -> usize {
    (self.limit * self.candidate_factor).max(query).clamp(20, 9999)
  }
}

#[cfg(test)]
mod testing {
  use crate::Algorithm;
  use crate::matching::{IndexType, MatchParams};

  #[test]
  fn default_algorithm() {
    assert_eq!(Algorithm::default(), Algorithm::LogicV1);
  }

  #[test]
  fn algorithm_to_name() {
    use super::Algorithm::*;

    for (alg, name) in [
      (NameBased, "name-based"),
      (NameQualified, "name-qualified"),
      (LogicV1, "logic-v1"),
      (MarbleV0, "marble-v0"),
      (Best, "best"),
    ] {
      assert_eq!(alg.name(), name);
    }
  }

  #[test]
  fn index_type_deserialize() {
    assert_eq!(serde_json::from_str::<IndexType>(r#""main""#).unwrap(), IndexType::Main);
    assert_eq!(serde_json::from_str::<IndexType>(r#""scoped""#).unwrap(), IndexType::Scoped);
    assert!(serde_json::from_str::<IndexType>(r#""unknown""#).is_err());
  }

  #[test]
  fn match_params_index_type_defaults_to_main() {
    let params: MatchParams = serde_json::from_str("{}").unwrap();
    assert_eq!(params.index_type, IndexType::Main);
  }

  #[test]
  fn match_params_index_type_parses_scoped() {
    let params: MatchParams = serde_json::from_str(r#"{"index_type":"scoped"}"#).unwrap();
    assert_eq!(params.index_type, IndexType::Scoped);
  }

  #[test]
  fn candidate_limit() {
    fn p(limit: usize, factor: usize) -> MatchParams {
      super::MatchParams {
        limit,
        candidate_factor: factor,
        ..Default::default()
      }
    }

    assert_eq!(p(10, 10).candidate_limit(50), 100);
    assert_eq!(p(10, 10).candidate_limit(101), 101);
    assert_eq!(p(1, 1).candidate_limit(1), 20);
    assert_eq!(p(10, 1000).candidate_limit(1), 9999);
  }
}
