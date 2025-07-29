use std::{collections::HashMap, sync::Arc};

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use axum_extra::extract::Query;
use axum_extra::extract::QueryRejection;
use axum_extra::extract::WithRejection;
use itertools::Itertools;
use tracing::{Instrument, instrument};

use crate::{
  api::{
    AppState,
    dto::{Algorithm, MatchHit, MatchParams, MatchResponse, MatchResults, MatchTotal, Payload},
    errors::AppError,
    middlewares::json_rejection::TypedJson,
  },
  matching::{logic_v1::LogicV1, name_based::NameBased, name_qualified::NameQualified},
  scoring, search,
};

pub(super) async fn not_found() -> impl IntoResponse {
  AppError::ResourceNotFound
}

#[instrument(skip_all)]
pub(super) async fn match_entity(
  State(AppState { es, catalog }): State<AppState>,
  WithRejection(Query(mut query), _): WithRejection<Query<MatchParams>, QueryRejection>,
  TypedJson(mut body): TypedJson<Payload>,
) -> Result<(StatusCode, impl IntoResponse), AppError> {
  let limit = query.limit.unwrap_or(5);

  query.limit = Some((limit * 10).clamp(20, 9999));

  body.queries.iter_mut().for_each(|(_, entity)| {
    entity.precompute();
  });

  let tasks = body.queries.into_iter().map(|(id, entity)| {
    tokio::spawn(
      {
        let catalog = Arc::clone(&catalog);
        let query = query.clone();
        let es = es.clone();

        async move {
          let hits = match search::search(catalog, &es, &entity, &query).await {
            Ok(hits) => hits,

            Err(err) => {
              tracing::error!(error = err.to_string(), "index query returned an error");

              return (id, MatchResults { status: 500, ..Default::default() });
            }
          };

          let scores = match query.algorithm.unwrap_or(Algorithm::NameBased) {
            Algorithm::NameBased => scoring::score::<NameBased>(&entity, hits),
            Algorithm::NameQualified => scoring::score::<NameQualified>(&entity, hits),
          };

          match scores {
            Ok(scores) => {
              let hits = scores
                .into_iter()
                .filter(|(_, score)| score > &query.cutoff.unwrap_or(0.5))
                .sorted_by(|(_, lhs), (_, rhs)| lhs.total_cmp(rhs).reverse())
                .take(limit)
                .map(|(entity, score)| MatchHit {
                  entity,
                  score,
                  match_: score > query.threshold.unwrap_or(0.7),
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
      }
      .in_current_span(),
    )
  });

  let mut hits = Vec::new();

  for task in tasks {
    match task.await {
      Err(_) => return Err(AppError::ServerError),
      Ok(results) => hits.push(results),
    }
  }

  let response = MatchResponse {
    responses: hits.into_iter().collect::<HashMap<_, _>>(),
    limit,
  };

  Ok((StatusCode::OK, Json(response)))
}
