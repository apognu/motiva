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
    let _enter = span.enter();

    if !hit.schema.is_a(entity.schema.as_str()) {
      return (hit, 0.0);
    }

    let (score, features) = A::score(&bump, entity, &hit, cutoff);

    hit.features = features.into_iter().filter(|(_, score)| score > &0.0).collect::<Vec<(_, _)>>();

    tracing::debug!(score = score, "computed score");

    bump.reset();

    histogram!("motiva_scoring_scores").record(score);

    (hit, score)
  });

  histogram!("motiva_scoring_latency_seconds").record(then.elapsed().as_secs_f64());

  global::meter("motiva").f64_histogram("scoring_latency").build().record(then.elapsed().as_secs_f64() * 1000.0, &[]);

  results.extend(scores);

  Ok(results)
}
