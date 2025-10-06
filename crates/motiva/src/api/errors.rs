use std::error::Error;

use axum::{
  Json,
  http::StatusCode,
  response::{IntoResponse, Response},
};
use libmotiva::prelude::*;
use serde_json::json;
use tracing::*;

pub(super) struct ApiError(pub StatusCode, pub String, pub Option<Vec<String>>);

#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum AppError {
  #[error("bad request")]
  BadRequest,
  #[error("invalid credentials")]
  InvalidCredentials,
  #[error("missing resource")]
  ResourceNotFound,
  #[error("server error, please check your logs for more information")]
  ServerError,
  #[error(transparent)]
  OtherError(#[from] anyhow::Error),

  #[error("invalid configuration: {0}")]
  ConfigError(String),
  #[error("error from indexer: {0}")]
  IndexError(String),

  #[error("invalid query parameter")]
  InvalidQuery(#[from] axum::extract::rejection::QueryRejection),
}

impl From<MotivaError> for AppError {
  fn from(value: MotivaError) -> Self {
    match value {
      MotivaError::ConfigError(err) => AppError::ConfigError(err),
      MotivaError::IndexError(err) => AppError::IndexError(err.to_string()),
      MotivaError::InvalidSchema(_) => AppError::BadRequest,
      MotivaError::ResourceNotFound => AppError::ResourceNotFound,
      MotivaError::OtherError(err) => AppError::OtherError(err),
    }
  }
}

impl IntoResponse for AppError {
  fn into_response(self) -> Response {
    error!(error = self.source(), "{}", self.to_string());

    ApiError::from(&self).into_response()
  }
}

impl From<&AppError> for ApiError {
  fn from(value: &AppError) -> Self {
    match value {
      AppError::BadRequest => ApiError(StatusCode::BAD_REQUEST, value.to_string(), None),
      AppError::InvalidCredentials => ApiError(StatusCode::UNAUTHORIZED, value.to_string(), None),
      AppError::ResourceNotFound => ApiError(StatusCode::NOT_FOUND, value.to_string(), None),
      AppError::IndexError(_) => ApiError(StatusCode::INTERNAL_SERVER_ERROR, value.to_string(), None),
      AppError::InvalidQuery(err) => ApiError(StatusCode::BAD_REQUEST, value.to_string(), Some(vec![err.to_string()])),
      AppError::OtherError(inner) if inner.is::<AppError>() => match inner.downcast_ref::<AppError>() {
        Some(inner) => inner.into(),
        _ => ApiError(StatusCode::INTERNAL_SERVER_ERROR, value.to_string(), None),
      },
      _ => ApiError(StatusCode::INTERNAL_SERVER_ERROR, value.to_string(), None),
    }
  }
}

impl IntoResponse for ApiError {
  fn into_response(self) -> Response {
    let payload = match self.2 {
      Some(details) => json!({
          "message": self.1.to_string(),
          "details": details,
      }),
      None => json!({
          "message": self.1.to_string(),
      }),
    };

    (self.0, Json(payload)).into_response()
  }
}

#[cfg(test)]
mod tests {
  use axum::{
    body::to_bytes,
    response::{IntoResponse, Response},
  };
  use reqwest::StatusCode;
  use serde_json::json;
  use serde_json_assert::assert_json_include;

  use crate::api::errors::AppError;

  #[tokio::test]
  async fn error_to_response() {
    let expecteds = vec![
      (AppError::BadRequest, StatusCode::BAD_REQUEST, "bad request"),
      (AppError::ResourceNotFound, StatusCode::NOT_FOUND, "missing resource"),
      (AppError::InvalidCredentials, StatusCode::UNAUTHORIZED, "invalid credentials"),
      (AppError::IndexError("index error".into()), StatusCode::INTERNAL_SERVER_ERROR, "error from indexer: index error"),
      (AppError::ConfigError("config error".into()), StatusCode::INTERNAL_SERVER_ERROR, "invalid configuration: config error"),
      (AppError::ServerError, StatusCode::INTERNAL_SERVER_ERROR, "server error, please check your logs for more information"),
      (AppError::OtherError(anyhow::anyhow!("any error")), StatusCode::INTERNAL_SERVER_ERROR, "any error"),
    ];

    for expected in expecteds {
      let resp: Response = expected.0.into_response();

      assert_eq!(resp.status(), expected.1);

      let body: serde_json::Value = serde_json::from_slice(&to_bytes(resp.into_body(), 128).await.unwrap()).unwrap();

      assert_json_include!(
          actual: body,
          expected: json!({
              "message": expected.2
          })
      );
    }
  }
}
