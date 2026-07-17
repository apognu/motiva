use axum::{
  Json,
  extract::{Path, State},
  response::{IntoResponse, Redirect},
};
use axum_extra::extract::Query;
use libmotiva::prelude::*;
use reqwest::StatusCode;
use tracing::instrument;

use crate::api::{AppState, dto::GetEntityParams, errors::AppError, middlewares::auth::Auth};

#[instrument(skip_all)]
pub async fn get_entity<F: CatalogFetcher, P: IndexProvider>(
  State(state): State<AppState<F, P>>,
  _: Auth<F, P>,
  Path(id): Path<String>,
  Query(params): Query<GetEntityParams>,
) -> Result<impl IntoResponse, AppError> {
  if !state.motiva.ready() {
    return Err(AppError::ServiceUnavailable);
  }

  let behavior = if params.nested { GetEntityBehavior::FetchNestedEntity } else { GetEntityBehavior::RootOnly };
  let limit = GetEntityLimits::new(state.config.enrichment_max_recursion, state.config.enrichment_query_limit);

  match state.motiva.get_entity(&id, behavior, limit).await.map_err(Into::<AppError>::into)? {
    EntityHandle::Referent(id) => Ok(Redirect::permanent(&format!("/entities/{id}")).into_response()),
    EntityHandle::Nominal(entity) => Ok((StatusCode::OK, Json(entity)).into_response()),
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use axum::{
    extract::{Path, State},
    response::IntoResponse,
  };
  use axum_extra::extract::Query;
  use libmotiva::{Entity, EntityHandle, MockedElasticsearch, Motiva, TestFetcher};
  use reqwest::StatusCode;

  use crate::api::{AppState, config::Config, dto::GetEntityParams, middlewares::auth::Auth};

  async fn state_with(entity: EntityHandle) -> AppState<TestFetcher, MockedElasticsearch> {
    let index = MockedElasticsearch::builder().entity(entity).build();

    AppState {
      config: Arc::new(Config::default()),
      prometheus: None,
      motiva: Motiva::test(index).fetcher(TestFetcher::default()).build().await.unwrap(),
    }
  }

  #[tokio::test]
  async fn get_entity_referent_redirects() {
    let state = state_with(EntityHandle::Referent("canonical".to_string())).await;

    let response = super::get_entity(State(state), Auth::noop(), Path("some-id".to_string()), Query(GetEntityParams { nested: false }))
      .await
      .unwrap()
      .into_response();

    assert_eq!(response.status(), StatusCode::PERMANENT_REDIRECT);
    assert_eq!(response.headers().get("location").unwrap().to_str().unwrap(), "/entities/canonical");
  }

  #[tokio::test]
  async fn get_entity_not_ready_returns_503() {
    let index = MockedElasticsearch::builder().ready(false).build();
    let state = AppState {
      config: Arc::new(Config::default()),
      prometheus: None,
      motiva: Motiva::test(index).fetcher(TestFetcher::default()).build().await.unwrap(),
    };

    let response = super::get_entity(State(state), Auth::noop(), Path("some-id".to_string()), Query(GetEntityParams { nested: false }))
      .await
      .into_response();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
  }

  #[tokio::test]
  async fn get_entity_nominal_returns_json() {
    let entity = Entity::builder("Person").id("person-1").properties(&[("name", &["John Doe"])]).build();
    let state = state_with(EntityHandle::Nominal(Box::new(entity))).await;

    let response = super::get_entity(State(state), Auth::noop(), Path("person-1".to_string()), Query(GetEntityParams { nested: false }))
      .await
      .unwrap()
      .into_response();

    assert_eq!(response.status(), StatusCode::OK);
  }
}
