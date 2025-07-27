use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tracing::{Span, instrument};

use crate::{
  matching::MatchingAlgorithm,
  model::{Entity, SearchEntity},
};

#[instrument(name = "compute_scores", skip_all, fields(algorithm = A::name()))]
pub fn score<A: MatchingAlgorithm>(entity: &SearchEntity, hits: Vec<Entity>) -> anyhow::Result<Vec<(Entity, f64)>> {
  let span = Span::current();

  Ok(
    hits
      .into_par_iter()
      .map(|mut hit| {
        let _enter = span.enter();

        let (score, features) = A::score(entity, &hit);

        hit.features = features.into_iter().filter(|(_, score)| score > &0.0).collect::<Vec<(_, _)>>();

        tracing::debug!(score = score, "computed score");

        (hit, score)
      })
      .collect::<Vec<_>>(),
  )
}
