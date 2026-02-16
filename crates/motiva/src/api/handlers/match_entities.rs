use std::collections::HashMap;
use std::sync::Arc;

use ahash::RandomState;
use axum::extract::Path;
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use itertools::Itertools;
use libmotiva::prelude::*;
use metrics::histogram;
use tracing::{Instrument, instrument};

use crate::api::errors::AppError;
use crate::api::middlewares::auth::Auth;
use crate::api::middlewares::types::Query;
use crate::api::{
  AppState,
  dto::{MatchHit, MatchResponse, MatchResults, MatchTotal, Payload},
  middlewares::types::TypedJson,
};

#[instrument(skip_all)]
pub async fn match_entities<F: CatalogFetcher, P: IndexProvider + 'static>(
  State(state): State<AppState<F, P>>,
  _: Auth<F, P>,
  Path((scope,)): Path<(String,)>,
  Query(mut query): Query<MatchParams>,
  TypedJson(mut body): TypedJson<Payload>,
) -> Result<(StatusCode, impl IntoResponse), AppError> {
  query.scope = scope;
  query.candidate_factor = state.config.match_candidates;

  if let Some(datasets) = body.params.include_datasets {
    query.include_dataset = datasets;
  }
  if let Some(datasets) = body.params.exclude_datasets {
    query.exclude_dataset = datasets;
  }
  if let Some(entity_ids) = body.params.exclude_entity_ids {
    query.exclude_entity_ids = entity_ids;
  }

  body.queries.iter_mut().for_each(|(_, entity)| {
    entity.precompute();
  });

  let state = Arc::new(state);
  let query = Arc::new(query);

  let tasks = body.queries.into_iter().map(|(id, entity)| {
    tokio::spawn({
      let state = Arc::clone(&state);
      let query = Arc::clone(&query);

      async move {
        let hits = match state.motiva.search(&entity, &query).await {
          Ok(hits) => hits,

          Err(err) => {
            tracing::error!(error = ?err, "index query returned an error");

            return (id, MatchResults { status: 500, ..Default::default() });
          }
        };

        let scores = match query.algorithm {
          Algorithm::NameBased => state.motiva.score::<NameBased>(&entity, hits, query.cutoff),
          Algorithm::NameQualified => state.motiva.score::<NameQualified>(&entity, hits, query.cutoff),
          Algorithm::LogicV1 | Algorithm::Best => state.motiva.score::<LogicV1>(&entity, hits, query.cutoff),
        };

        match scores {
          Ok(scores) => {
            let pre_cutoff_count = scores.len();
            let post_threshold_count = scores.iter().filter(|(_, score)| score >= &query.threshold).count();

            let hits = scores
              .into_iter()
              .filter(|(_, score)| score > &query.cutoff)
              // Yente's implementation sorts by descending score, but let's order by (-score, id) so we get stable ordering
              .sorted_by(|(lhs, lscore), (rhs, rscore)| lscore.total_cmp(rscore).reverse().then_with(|| lhs.id.cmp(&rhs.id)))
              .take(query.limit)
              .map(|(entity, score)| MatchHit {
                entity,
                score,
                match_: score > query.threshold,
              })
              .collect::<Vec<_>>();

            histogram!("motiva_matches_above_cutoff_total").record(hits.len() as f64);
            histogram!("motiva_matches_below_cutoff_total").record((pre_cutoff_count - hits.len()) as f64);

            (
              id,
              MatchResults {
                status: 200,
                total: Some(MatchTotal {
                  relation: "eq",
                  value: post_threshold_count,
                }),
                results: hits,
              },
            )
          }

          Err(_) => (id, MatchResults { status: 500, ..Default::default() }),
        }
      }
      .in_current_span()
    })
  });

  let mut responses = HashMap::with_capacity_and_hasher(tasks.len(), RandomState::default());

  for task in tasks {
    match task.await {
      Err(_) => return Err(AppError::ServerError),
      Ok((id, results)) => {
        responses.insert(id, results);
      }
    }
  }

  let response = MatchResponse { responses, limit: query.limit };

  Ok((StatusCode::OK, Json(response)))
}
