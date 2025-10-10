use bumpalo::Bump;

use metrics::histogram;
use opentelemetry::global;

use tokio::time::Instant;
use tracing::{Span, instrument};

use crate::{
  matching::MatchingAlgorithm,
  model::{Entity, SearchEntity},
};

#[instrument(name = "compute_scores", skip_all, fields(algorithm = A::name()))]
pub fn score<A: MatchingAlgorithm>(entity: &SearchEntity, hits: Vec<Entity>, cutoff: f64) -> anyhow::Result<Vec<(Entity, f64)>> {
  let span = Span::current();

  let mut bump = Bump::with_capacity(1024);
  let mut results = Vec::with_capacity(hits.len());
  let then = Instant::now();

  let scores = hits.into_iter().map(|mut hit| {
    let then = Instant::now();
    let _enter = span.enter();

    if !hit.schema.is_a(entity.schema.as_str()) {
      tracing::debug!(score = 0.0, "incomparable schemas, skipping");

      return (hit, 0.0);
    }

    let (score, features) = A::score(&bump, entity, &hit, cutoff);

    hit.features = features.into_iter().filter(|(_, score)| score > &0.0).collect::<Vec<(_, _)>>();

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

  use crate::{Entity, LogicV1, SearchEntity};

  #[test]
  fn incomparable_schemas() {
    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();
    let rhs = Entity::builder("Company").properties(&[("name", &["Vladimir Putin"])]).build();
    let result = super::score::<LogicV1>(&lhs, vec![rhs], 0.0).unwrap();

    assert_eq!(result.len(), 1);
    assert!(approx_eq!(f64, result[0].1, 0.0));
  }
}
