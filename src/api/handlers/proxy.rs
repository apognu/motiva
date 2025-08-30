use anyhow::Context;
use axum::{extract::State, response::IntoResponse};
use reqwest::{StatusCode, header};

use crate::{
  api::{AppState, errors::AppError},
  index::IndexProvider,
};

pub async fn catalog<P: IndexProvider>(State(state): State<AppState<P>>) -> Result<impl IntoResponse, AppError> {
  match state.config.yente_url {
    None => Err(AppError::ResourceNotFound),

    Some(url) => {
      let body = reqwest::get(&format!("{}/catalog", url))
        .await
        .context("could not read body")?
        .bytes()
        .await
        .context("could not read body")?;

      Ok((StatusCode::OK, [(header::CONTENT_TYPE, "application/json")], body))
    }
  }
}
