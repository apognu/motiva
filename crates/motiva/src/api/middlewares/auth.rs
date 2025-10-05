use std::marker::PhantomData;

use anyhow::Context;
use axum::{
  RequestPartsExt,
  extract::{FromRef, FromRequestParts, State},
  http::request::Parts,
};
use axum_extra::{
  TypedHeader,
  headers::{Authorization, authorization::Bearer},
};
use libmotiva::prelude::IndexProvider;

use crate::api::{AppState, errors::AppError};

#[non_exhaustive]
pub(crate) struct Auth<P> {
  _marker: PhantomData<P>,
}

impl<S, P> FromRequestParts<S> for Auth<P>
where
  for<'s> P: IndexProvider + 's,
  S: Send + Sync,
  AppState<P>: FromRef<S>,
{
  type Rejection = AppError;

  async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
    let State(app_state) = parts.extract_with_state::<State<AppState<P>>, S>(state).await.unwrap();

    let Some(api_key) = app_state.config.api_key else {
      return Ok(Auth { _marker: PhantomData });
    };

    let header = parts
      .extract::<TypedHeader<Authorization<Bearer>>>()
      .await
      .context("no authorization header found")
      .context(AppError::InvalidCredentials)?;

    if header.token() != api_key {
      return Err(AppError::InvalidCredentials);
    }

    Ok(Auth::<P> { _marker: PhantomData })
  }
}
