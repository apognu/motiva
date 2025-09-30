use std::collections::HashMap;

use ahash::RandomState;
use axum::extract::Path;
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use axum_extra::extract::Query;
use axum_extra::extract::QueryRejection;
use axum_extra::extract::WithRejection;
use itertools::Itertools;
use libmotiva::prelude::*;
use tracing::{Instrument, instrument};

use crate::api::errors::AppError;
use crate::api::{
  AppState,
  dto::{MatchHit, MatchResponse, MatchResults, MatchTotal, Payload},
  middlewares::json_rejection::TypedJson,
};

#[instrument(skip_all)]
pub async fn match_entities<P: IndexProvider + 'static>(
  State(state): State<AppState<P>>,
  Path((scope,)): Path<(String,)>,
  WithRejection(Query(mut query), _): WithRejection<Query<MatchParams>, QueryRejection>,
  TypedJson(mut body): TypedJson<Payload>,
) -> Result<(StatusCode, impl IntoResponse), AppError> {
  let limit = query.limit;

  query.scope = scope;
  query.limit = (query.limit * state.config.match_candidates).clamp(20, 9999);

  body.queries.iter_mut().for_each(|(_, entity)| {
    entity.precompute();
  });

  let tasks = body.queries.into_iter().map(|(id, entity)| {
    tokio::spawn({
      let state = state.clone();
      let query = query.clone();

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
          Algorithm::LogicV1 => state.motiva.score::<LogicV1>(&entity, hits, query.cutoff),
        };

        match scores {
          Ok(scores) => {
            let hits = scores
              .into_iter()
              .filter(|(_, score)| score > &query.cutoff)
              .sorted_by(|(_, lhs), (_, rhs)| lhs.total_cmp(rhs).reverse())
              .take(limit)
              .map(|(entity, score)| MatchHit {
                entity,
                score,
                match_: score > query.threshold,
              })
              .collect::<Vec<_>>();

            (
              id,
              MatchResults {
                status: 200,
                total: Some(MatchTotal { relation: "eq", value: hits.len() }),
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

  let mut hits = Vec::with_capacity(tasks.len());

  for task in tasks {
    match task.await {
      Err(_) => return Err(AppError::ServerError),
      Ok(results) => hits.push(results),
    }
  }

  let response = MatchResponse {
    responses: hits.into_iter().collect::<HashMap<_, _, RandomState>>(),
    limit: query.limit,
  };

  Ok((StatusCode::OK, Json(response)))
}
