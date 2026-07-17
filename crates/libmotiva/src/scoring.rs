use std::collections::HashMap;

use bumpalo::Bump;

use metrics::histogram;
use opentelemetry::global;

use tokio::time::Instant;
use tracing::{Span, instrument};

use crate::{
  matching::MatchingAlgorithm,
  model::{Entity, SearchEntity},
};

#[derive(Debug, Default)]
pub struct ScoringOptions {
  pub cutoff: f64,
  pub weights: HashMap<String, f64>,
  pub explain: bool,
}

impl ScoringOptions {
  pub fn new(cutoff: f64) -> Self {
    ScoringOptions { cutoff, ..Default::default() }
  }
}

#[instrument(name = "compute_scores", skip_all, fields(algorithm = A::name()))]
pub fn score<A: MatchingAlgorithm>(entity: &SearchEntity, hits: Vec<Entity>, options: &ScoringOptions) -> anyhow::Result<Vec<(Entity, f64)>> {
  let span = Span::current();

  let mut bump = Bump::with_capacity(1024);
  let mut results = Vec::with_capacity(hits.len());
  let then = Instant::now();

  let scores = hits.into_iter().map(|mut hit| {
    let then = Instant::now();
    let _enter = span.enter();

    if !hit.schema.can_match(entity.schema.as_str()) {
      tracing::debug!(score = 0.0, "incomparable schemas, skipping");

      return (hit, 0.0);
    }

    let (score, explanations) = A::score(&bump, entity, &hit, options);

    hit.features = explanations.iter().filter(|e| e.score != 0.0).map(|e| (e.name, e.score)).collect();

    if options.explain {
      hit.explanations = explanations;
    }

    tracing::debug!(score = score, latency = ?then.elapsed(), "computed score");

    bump.reset();

    histogram!("motiva_scoring_scores").record(score);

    (hit, score)
  });

  histogram!("motiva_scoring_latency_seconds").record(then.elapsed().as_secs_f64());

  global::meter("motiva").f64_histogram("scoring_latency").build().record(then.elapsed().as_secs_f64() * 1000.0, &[]);

  results.extend(scores);

  Ok(results)
}

#[cfg(test)]
mod tests {
  use float_cmp::approx_eq;

  use crate::{Entity, LogicV1, SearchEntity, scoring::ScoringOptions};

  #[test]
  fn incomparable_schemas() {
    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();
    let rhs = Entity::builder("Company").properties(&[("name", &["Vladimir Putin"])]).build();
    let result = super::score::<LogicV1>(&lhs, vec![rhs], &Default::default()).unwrap();

    assert_eq!(result.len(), 1);
    assert!(approx_eq!(f64, result[0].1, 0.0));
  }

  #[test]
  fn explanations_are_opt_in() {
    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();

    let result = super::score::<LogicV1>(&lhs, vec![rhs.clone()], &Default::default()).unwrap();
    assert!(result[0].0.explanations.is_empty());
    assert!(!result[0].0.features.is_empty());

    let options = ScoringOptions { explain: true, ..Default::default() };
    let result = super::score::<LogicV1>(&lhs, vec![rhs], &options).unwrap();
    let explanations = &result[0].0.explanations;

    assert!(!explanations.is_empty());

    let literal = explanations.iter().find(|e| e.name == "name_literal_match").unwrap();
    assert_eq!(literal.score, 1.0);
    assert!(approx_eq!(f64, literal.weighted, 1.0));
    assert_eq!(literal.detail.to_string(), "vladimir putin == vladimir putin");

    let dob = explanations.iter().find(|e| e.name == "dob_year_disjoint").unwrap();
    assert_eq!(dob.score, 0.0);
    assert_eq!(dob.detail.to_string(), "no data to match against");

    let phonetic = explanations.iter().find(|e| e.name == "person_name_phonetic_match").unwrap();
    let detail = phonetic.detail.to_string();
    assert!(detail.contains(" [") && detail.contains("] ~= "), "unexpected phonetic detail: {detail}");
    assert_eq!(detail.matches("~=").count(), 2, "expected both name parts reported: {detail}");
  }
}
