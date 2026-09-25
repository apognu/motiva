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
  if !state.motiva.ready() {
    return Err(AppError::ServiceUnavailable);
  }

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

  let options = Arc::new(ScoringOptions {
    cutoff: query.cutoff,
    weights: state.config.weights.clone().into_iter().chain(body.weights.clone()).collect(),
    explain: query.explain,
  });

  let tasks = body.queries.into_iter().map(|(id, entity)| {
    let mut query = query.clone();
    let options = options.clone();

    if let Some(ref params) = entity.params {
      if let Some(ref datasets) = params.include_datasets {
        query.include_dataset = datasets.clone();
      }
      if let Some(ref datasets) = params.exclude_datasets {
        query.exclude_dataset = datasets.clone();
      }
    }

    tokio::spawn({
      let state = Arc::clone(&state);

      async move {
        if entity.properties.is_empty() {
          return Ok((
            id,
            MatchResults {
              status: StatusCode::OK.as_u16(),
              total: Some(MatchTotal { relation: "eq", value: 0 }),
              results: vec![],
            },
          ));
        }

        let candidates = match state.motiva.search(&entity, &query).await {
          Ok(candidates) => candidates,

          Err(err) => {
            tracing::error!(error = ?err, "index query returned an error");

            return Err(err);
          }
        };

        let scores = match query.algorithm {
          Algorithm::NameBased => state.motiva.score::<NameBased>(entity, candidates, options).await,
          Algorithm::NameQualified => state.motiva.score::<NameQualified>(entity, candidates, options).await,
          Algorithm::MarbleV0 => state.motiva.score::<MarbleV0>(entity, candidates, options).await,
          Algorithm::LogicV1 | Algorithm::Best => state.motiva.score::<LogicV1>(entity, candidates, options).await,
        };

        match scores {
          Ok(scores) => {
            let pre_cutoff_count = scores.len();
            let post_threshold_count = scores.iter().filter(|(_, score)| score >= &query.threshold).count();

            let hits = scores
              .into_iter()
              .filter(|(_, score)| score >= &query.cutoff)
              // Yente's implementation sorts by descending score, but let's order by (-score, id) so we get stable ordering
              .sorted_by(|(lhs, lscore), (rhs, rscore)| lscore.total_cmp(rscore).reverse().then_with(|| lhs.id.cmp(&rhs.id)))
              .take(query.limit)
              .map(|(entity, score)| MatchHit {
                entity,
                score,
                match_: score >= query.threshold,
              })
              .collect::<Vec<_>>();

            histogram!("motiva_matches_above_cutoff_total").record(hits.len() as f64);
            histogram!("motiva_matches_below_cutoff_total").record((pre_cutoff_count - hits.len()) as f64);

            Ok((
              id,
              MatchResults {
                status: StatusCode::OK.as_u16(),
                total: Some(MatchTotal {
                  relation: "eq",
                  value: post_threshold_count,
                }),
                results: hits,
              },
            ))
          }

          Err(err) => Err(err),
        }
      }
      .in_current_span()
    })
  });

  let mut responses = HashMap::with_capacity_and_hasher(tasks.len(), RandomState::default());

  for task in tasks {
    match task.await.map_err(|err| AppError::OtherError(anyhow::anyhow!(err)))? {
      Err(err) => return Err(err.into()),

      Ok((id, results)) => {
        responses.insert(id, results);
      }
    }
  }

  let response = MatchResponse { responses, limit: query.limit };

  Ok((StatusCode::OK, Json(response)))
}
