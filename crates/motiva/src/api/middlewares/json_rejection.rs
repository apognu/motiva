use std::borrow::Cow;

use axum::{
  Json, RequestExt,
  body::Body,
  extract::{FromRequest, rejection::JsonRejection},
  http::{Request, StatusCode},
  response::IntoResponse,
};
use serde::de::DeserializeOwned;
use validator::{Validate, ValidationErrors};

use crate::api::errors::ApiError;

pub struct TypedJson<T>(pub T);

pub enum TypedJsonRejection {
  JsonRejection(JsonRejection),
  ValidationFailed(ValidationErrors),
}

impl IntoResponse for TypedJsonRejection {
  fn into_response(self) -> axum::response::Response {
    match self {
      TypedJsonRejection::JsonRejection(err) => match err {
        JsonRejection::JsonSyntaxError(_) => ApiError(StatusCode::BAD_REQUEST, "invalid payload format".to_string(), None).into_response(),
        JsonRejection::JsonDataError(err) => ApiError(StatusCode::BAD_REQUEST, "payload does not match expected format".to_string(), Some(vec![err.to_string()])).into_response(),
        JsonRejection::MissingJsonContentType(_) => ApiError(StatusCode::UNSUPPORTED_MEDIA_TYPE, "invalid media type, expected application/json".to_string(), None).into_response(),
        err => ApiError(StatusCode::BAD_REQUEST, "invalid payload".to_string(), Some(vec![err.to_string()])).into_response(),
      },

      TypedJsonRejection::ValidationFailed(errs) => {
        let messages: Vec<String> = errs.field_errors().into_iter().flat_map(|(_, f)| f.clone()).filter_map(|f| f.message.map(Cow::into_owned)).collect();

        ApiError(StatusCode::UNPROCESSABLE_ENTITY, "payload failed validation".to_string(), Some(messages)).into_response()
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
