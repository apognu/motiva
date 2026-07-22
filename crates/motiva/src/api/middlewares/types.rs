use axum::{
  Json, RequestExt,
  body::Body,
  extract::{FromRequest, rejection::JsonRejection},
  http::{Request, StatusCode},
  response::{IntoResponse, Response},
};
use axum_macros::FromRequestParts;
use serde::{Deserialize, de::DeserializeOwned};
use validator::{Validate, ValidationErrors, ValidationErrorsKind};

use crate::api::errors::ApiError;

pub struct TypedJson<T>(pub T);

#[derive(Debug)]
pub enum TypedJsonRejection {
  JsonRejection(JsonRejection),
  ValidationFailed(ValidationErrors),
}

impl IntoResponse for TypedJsonRejection {
  fn into_response(self) -> Response {
    let (status, message, details) = match self {
      TypedJsonRejection::JsonRejection(err) => match err {
        JsonRejection::JsonSyntaxError(_) => (StatusCode::BAD_REQUEST, "invalid payload format".to_string(), None),
        JsonRejection::JsonDataError(err) => (StatusCode::BAD_REQUEST, "payload does not match expected format".to_string(), Some(vec![err.to_string()])),
        JsonRejection::MissingJsonContentType(_) => (StatusCode::UNSUPPORTED_MEDIA_TYPE, "invalid media type, expected application/json".to_string(), None),
        err => (StatusCode::BAD_REQUEST, "invalid payload".to_string(), Some(vec![err.to_string()])),
      },

      TypedJsonRejection::ValidationFailed(errs) => {
        let mut messages = Vec::new();

        flatten_validation_errors(&errs, "", &mut messages);

        (StatusCode::UNPROCESSABLE_ENTITY, "payload failed validation".to_string(), Some(messages))
      }
    };

    tracing::warn!(
      error = message,
      details = details.as_ref().map(|errs| errs.join(" / ")).unwrap_or_default(),
      "encountered an issue parsing input json"
    );

    ApiError(status, message, details).into_response()
  }
}

fn flatten_validation_errors(errs: &ValidationErrors, prefix: &str, out: &mut Vec<String>) {
  for (field, kind) in errs.errors() {
    let path = if prefix.is_empty() { field.to_string() } else { format!("{prefix}.{field}") };

    match kind {
      ValidationErrorsKind::Field(items) => {
        for item in items {
          let msg = item.message.as_deref().unwrap_or(&item.code);
          out.push(format!("{path}: {msg}"));
        }
      }
      ValidationErrorsKind::Struct(nested) => flatten_validation_errors(nested, &path, out),
      ValidationErrorsKind::List(items) => {
        for (index, nested) in items {
          flatten_validation_errors(nested, &format!("{path}[{index}]"), out);
        }
      }
    }
  }
}

impl<T, S> FromRequest<S> for TypedJson<T>
where
  T: DeserializeOwned + Validate + 'static,
  S: Send + Sync,
{
  type Rejection = TypedJsonRejection;

  async fn from_request(request: Request<Body>, _state: &S) -> Result<Self, Self::Rejection> {
    match request.extract::<Json<T>, _>().await {
      Ok(Json(dto)) => match dto.validate() {
        Ok(()) => Ok(TypedJson(dto)),
        Err(errs) => Err(TypedJsonRejection::ValidationFailed(errs)),
      },

      Err(err) => Err(TypedJsonRejection::JsonRejection(err)),
    }
  }
}

#[derive(Deserialize, FromRequestParts)]
#[from_request(via(axum_extra::extract::Query), rejection(QueryRejection))]
pub struct Query<T>(pub T);

pub struct QueryRejection(axum_extra::extract::QueryRejection);

impl From<axum_extra::extract::QueryRejection> for QueryRejection {
  fn from(value: axum_extra::extract::QueryRejection) -> Self {
    QueryRejection(value)
  }
}

impl IntoResponse for QueryRejection {
  fn into_response(self) -> Response {
    ApiError(StatusCode::BAD_REQUEST, "invalid query parameter".to_string(), Some(vec![self.0.to_string()])).into_response()
  }
}

#[cfg(test)]
mod tests {
  use validator::Validate;

  use super::flatten_validation_errors;

  #[derive(Validate)]
  struct Inner {
    #[validate(length(min = 3, message = "too short"))]
    name: String,
  }

  #[derive(Validate)]
  struct Outer {
    #[validate(range(min = 1, message = "must be positive"))]
    count: i32,
    #[validate(nested)]
    inner: Inner,
    #[validate(nested)]
    items: Vec<Inner>,
  }

  #[test]
  fn flattens_field_struct_and_list_errors() {
    let outer = Outer {
      count: 0,
      inner: Inner { name: "ab".to_string() },
      items: vec![Inner { name: "ok!".to_string() }, Inner { name: "x".to_string() }],
    };

    let errs = outer.validate().unwrap_err();
    let mut messages = Vec::new();

    flatten_validation_errors(&errs, "", &mut messages);

    messages.sort();

    assert_eq!(messages, &["count: must be positive", "inner.name: too short", "items[1].name: too short"]);
  }

  #[test]
  fn falls_back_to_code_when_message_is_missing() {
    #[derive(Validate)]
    struct NoMessage {
      #[validate(length(min = 3))]
      name: String,
    }

    let errs = NoMessage { name: "x".to_string() }.validate().unwrap_err();
    let mut messages = Vec::new();

    flatten_validation_errors(&errs, "", &mut messages);

    assert_eq!(messages, &["name: length"]);
  }
}
