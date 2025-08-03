use tracing::{Span, instrument};

use crate::{
  matching::MatchingAlgorithm,
  model::{Entity, SearchEntity},
};

#[instrument(name = "compute_scores", skip_all, fields(algorithm = A::name()))]
pub fn score<A: MatchingAlgorithm>(entity: &SearchEntity, hits: Vec<Entity>) -> anyhow::Result<Vec<(Entity, f64)>> {
  let span = Span::current();

  let mut results = Vec::with_capacity(hits.len());

  let out = hits.into_iter().map(|mut hit| {
    let _enter = span.enter();

    let (score, features) = A::score(entity, &hit);

    hit.features = features.into_iter().filter(|(_, score)| score > &0.0).collect::<Vec<(_, _)>>();

    tracing::debug!(score = score, "computed score");

    (hit, score)
  });

  results.extend(out);

  Ok(results)
}
