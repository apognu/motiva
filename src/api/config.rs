use std::{
  env::{self, VarError},
  fmt::Display,
  str::FromStr,
};

use crate::api::errors::AppError;

#[derive(Clone)]
pub struct Config {
  pub env: Env,
  pub listen_addr: String,

  // Elasticsearch
  pub index_url: String,
  pub index_auth_method: EsAuthMethod,
  pub index_client_id: Option<String>,
  pub index_client_secret: Option<String>,

  // Match settings
  pub match_candidates: usize,

  // Debugging
  pub enable_tracing: bool,
}

impl Config {
  pub fn from_env() -> Result<Config, AppError> {
    let config = Config {
      env: Env::from(env::var("ENV").unwrap_or("dev".into())),
      listen_addr: env::var("LISTEN_ADDR").unwrap_or("0.0.0.0:8000".into()),
      match_candidates: parse_env("MATCH_CANDIDATES", 10)?,
      index_url: env::var("INDEX_URL").unwrap_or("http://localhost:9200".into()),
      index_auth_method: env::var("INDEX_AUTH_METHOD").unwrap_or("none".into()).parse()?,
      index_client_id: env::var("INDEX_CLIENT_ID").map(Some).unwrap_or_default(),
      index_client_secret: env::var("INDEX_CLIENT_SECRET").map(Some).unwrap_or_default(),
      enable_tracing: env::var("ENABLE_TRACING").unwrap_or_default() == "1",
    };

    if let EsAuthMethod::Basic | EsAuthMethod::ApiKey = config.index_auth_method {
      if config.index_client_id.is_none() || config.index_client_secret.is_none() {
        return Err(AppError::ConfigError(
          "ES_CLIENT_ID and ES_CLIENT_SECRET are required when using Basic or ApiKey authentication methods".into(),
        ));
      }
    }

    Ok(config)
  }
}

#[derive(Clone)]
pub enum Env {
  Dev,
  Production,
}

impl From<String> for Env {
  fn from(value: String) -> Self {
    match value.as_ref() {
      "dev" => Env::Dev,
      "production" => Env::Production,
      _ => Env::Dev,
    }
  }
}

#[derive(Clone)]
pub enum EsAuthMethod {
  None,
  Basic,
  Bearer,
  ApiKey,
  EncodedApiKey,
}

impl FromStr for EsAuthMethod {
  type Err = AppError;

  fn from_str(value: &str) -> Result<Self, Self::Err> {
    match value {
      "none" => Ok(EsAuthMethod::None),
      "basic" => Ok(EsAuthMethod::Basic),
      "bearer" => Ok(EsAuthMethod::Bearer),
      "api_key" => Ok(EsAuthMethod::ApiKey),
      "encoded_api_key" => Ok(EsAuthMethod::EncodedApiKey),
      _ => Err(AppError::ConfigError("invalid elasticsearch authentication method".into())),
    }
  }
}

fn parse_env<T>(name: &str, default: T) -> anyhow::Result<T>
where
  T: FromStr,
  T::Err: Display,
{
  match env::var(name) {
    Ok(value) if value.is_empty() => Ok(default),
    Ok(value) => Ok(value.parse::<T>().map_err(|err| AppError::ConfigError(format!("could not read {name}: {err}")))?),
    Err(err) => match err {
      VarError::NotPresent => Ok(default),
      _ => Err(AppError::ConfigError(format!("could not read {name}: {err}")).into()),
    },
  }
}
