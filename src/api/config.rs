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
  pub yente_url: Option<String>,
  pub catalog_url: Option<String>,
  pub match_candidates: usize,

  // Debugging
  pub enable_tracing: bool,
  pub tracing_exporter: TracingExporter,
  #[cfg(feature = "gcp")]
  pub gcp_project_id: String,
}

impl Config {
  pub async fn from_env() -> Result<Config, AppError> {
    let config = Config {
      env: Env::from(env::var("ENV").unwrap_or("dev".into())),
      listen_addr: env::var("LISTEN_ADDR").unwrap_or("0.0.0.0:8000".into()),
      match_candidates: parse_env("MATCH_CANDIDATES", 10)?,
      yente_url: env::var("YENTE_URL").ok(),
      catalog_url: env::var("CATALOG_URL").ok(),
      index_url: env::var("INDEX_URL").unwrap_or("http://localhost:9200".into()),
      index_auth_method: env::var("INDEX_AUTH_METHOD").unwrap_or("none".into()).parse()?,
      index_client_id: env::var("INDEX_CLIENT_ID").map(Some).unwrap_or_default(),
      index_client_secret: env::var("INDEX_CLIENT_SECRET").map(Some).unwrap_or_default(),
      enable_tracing: env::var("ENABLE_TRACING").unwrap_or_default() == "1",
      tracing_exporter: TracingExporter::try_from(parse_env("TRACING_EXPORTER", "otlp".to_string())?)?,
      #[cfg(feature = "gcp")]
      gcp_project_id: detect_gcp_project_id().await,
    };

    if let EsAuthMethod::Basic | EsAuthMethod::ApiKey = config.index_auth_method
      && (config.index_client_id.is_none() || config.index_client_secret.is_none())
    {
      return Err(AppError::ConfigError(
        "ES_CLIENT_ID and ES_CLIENT_SECRET are required when using Basic or ApiKey authentication methods".into(),
      ));
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

#[derive(Clone)]
pub enum TracingExporter {
  Otlp,
  #[cfg(feature = "gcp")]
  Gcp,
}

impl TryFrom<String> for TracingExporter {
  type Error = AppError;

  fn try_from(value: String) -> Result<Self, Self::Error> {
    match value.as_ref() {
      "otlp" => Ok(TracingExporter::Otlp),
      #[cfg(feature = "gcp")]
      "gcp" => Ok(TracingExporter::Gcp),
      other => Err(AppError::ConfigError(format!("unsupported tracing exporter kind: {other}"))),
    }
  }
}

pub fn parse_env<T>(name: &str, default: T) -> anyhow::Result<T>
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

#[cfg(feature = "gcp")]
async fn detect_gcp_project_id() -> String {
  match env::var("GOOGLE_CLOUD_PROJECT") {
    Ok(project) => project,
    Err(_) => match gcp_auth::provider().await {
      Ok(provider) => match provider.project_id().await {
        Ok(project) => project.to_string(),
        _ => String::new(),
      },
      _ => String::new(),
    },
  }
}
