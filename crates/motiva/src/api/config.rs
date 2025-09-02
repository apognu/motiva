use std::{
  env::{self, VarError},
  fmt::Display,
  str::FromStr,
};

use libmotiva::prelude::EsAuthMethod;

use crate::api::errors::AppError;

#[derive(Clone, Debug)]
pub struct Config {
  pub env: Env,
  pub listen_addr: String,

  // Elasticsearch
  pub index_url: String,
  pub index_auth_method: EsAuthMethod,

  // Match settings
  pub yente_url: Option<String>,
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
      index_url: env::var("INDEX_URL").unwrap_or("http://localhost:9200".into()),
      index_auth_method: env::var("INDEX_AUTH_METHOD").unwrap_or("none".into()).parse::<WrappedEsAuthMethod>()?.0,
      enable_tracing: env::var("ENABLE_TRACING").unwrap_or_default() == "1",
      tracing_exporter: env::var("TRACING_EXPORTER").unwrap_or("otlp".into()).parse()?,
      #[cfg(feature = "gcp")]
      gcp_project_id: detect_gcp_project_id().await,
    };

    Ok(config)
  }
}

#[derive(Clone, Debug, Eq, PartialEq)]
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

struct WrappedEsAuthMethod(EsAuthMethod);

impl FromStr for WrappedEsAuthMethod {
  type Err = AppError;

  fn from_str(value: &str) -> Result<Self, Self::Err> {
    let client_id = env::var("INDEX_CLIENT_ID").ok();
    let client_secret = env::var("INDEX_CLIENT_SECRET").ok();

    Ok(WrappedEsAuthMethod(match value {
      "none" => EsAuthMethod::None,
      "basic" if client_id.is_some() && client_secret.is_some() => EsAuthMethod::Basic(client_id.unwrap(), client_secret.unwrap()),
      "bearer" if client_secret.is_some() => EsAuthMethod::Bearer(client_secret.unwrap()),
      "api_key" if client_id.is_some() && client_secret.is_some() => EsAuthMethod::ApiKey(client_id.unwrap(), client_secret.unwrap()),
      "encoded_api_key" if client_secret.is_some() => EsAuthMethod::EncodedApiKey(client_secret.unwrap()),

      "basic" | "bearer" | "api_key" | "encoded_api_key" => Err(AppError::ConfigError("chosen index authentication method is missing a credential setting".into()))?,

      _ => Err(AppError::ConfigError("invalid elasticsearch authentication method".into()))?,
    }))
  }
}

#[derive(Clone, Debug)]
pub enum TracingExporter {
  Otlp,
  #[cfg(feature = "gcp")]
  Gcp,
}

impl FromStr for TracingExporter {
  type Err = AppError;

  fn from_str(value: &str) -> Result<Self, Self::Err> {
    match value {
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

#[cfg(test)]
mod tests {
  use std::{
    env,
    net::{IpAddr, Ipv4Addr},
  };

  use crate::api::config::WrappedEsAuthMethod;

  use super::{Config, Env, EsAuthMethod, TracingExporter};

  #[serial_test::serial]
  #[tokio::test]
  async fn parse_config_from_env() {
    unsafe {
      env::set_var("ENV", "production");
      env::set_var("LISTEN_ADDR", "0.0.0.0:8080");
      env::set_var("MATCH_CANDIDATES", "3");
      env::set_var("YENTE_URL", "http://yente");
      env::set_var("CATALOG_URL", "http://catalog");
      env::set_var("INDEX_URL", "http://index");
      env::set_var("INDEX_AUTH_METHOD", "encoded_api_key");
      env::set_var("INDEX_CLIENT_SECRET", "secret");
      env::set_var("ENABLE_TRACING", "1");
    }

    let config = Config::from_env().await.unwrap();

    assert_eq!(config.env, Env::Production);
    assert_eq!(config.listen_addr, "0.0.0.0:8080");
    assert_eq!(config.match_candidates, 3);
    assert_eq!(config.yente_url, Some("http://yente".to_string()));
    assert_eq!(config.index_url, "http://index");
    assert_eq!(config.index_auth_method, EsAuthMethod::EncodedApiKey("secret".to_string()));
    assert_eq!(config.enable_tracing, true);
  }

  #[tokio::test]
  #[serial_test::serial]
  async fn invalid_es_auth_method_combination() {
    unsafe {
      env::set_var("INDEX_AUTH_METHOD", "basic");
      env::set_var("INDEX_CLIENT_SECRET", "secret");
    }

    assert!(matches!(Config::from_env().await, Err(_)));

    unsafe {
      env::set_var("INDEX_AUTH_METHOD", "api_key");
      env::set_var("INDEX_CLIENT_SECRET", "secret");
    }

    assert!(matches!(Config::from_env().await, Err(_)));

    unsafe {
      env::set_var("INDEX_AUTH_METHOD", "basic");
      env::set_var("INDEX_CLIENT_ID", "secret");
      env::set_var("INDEX_CLIENT_SECRET", "secret");
    }

    let config = Config::from_env().await.unwrap();

    assert_eq!(config.index_auth_method, EsAuthMethod::Basic("secret".to_string(), "secret".to_string()));

    unsafe {
      env::set_var("INDEX_AUTH_METHOD", "api_key");
      env::set_var("INDEX_CLIENT_ID", "secret");
      env::set_var("INDEX_CLIENT_SECRET", "secret");
    }

    let config = Config::from_env().await.unwrap();

    assert_eq!(config.index_auth_method, EsAuthMethod::ApiKey("secret".to_string(), "secret".to_string()));

    unsafe {
      env::remove_var("INDEX_AUTH_METHOD");
      env::remove_var("INDEX_CLIENT_ID");
      env::remove_var("INDEX_CLIENT_SECRET");
    }
  }

  #[test]
  #[serial_test::serial]
  fn parse_env() {
    unsafe {
      env::set_var("INT", "42");
      env::set_var("BOOL", "true");
      env::set_var("IP", "1.2.3.4");
    }

    assert_eq!(super::parse_env::<u32>("INT", 0).unwrap(), 42);
    assert_eq!(super::parse_env::<bool>("BOOL", true).unwrap(), true);
    assert_eq!(super::parse_env::<IpAddr>("IP", IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4))).unwrap(), IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)));

    assert!(matches!(super::parse_env::<u32>("BOOL", 0), Err(_)));
  }

  #[test]
  fn es_auth_method_from_str() {
    assert!(matches!("otlp".parse(), Ok(TracingExporter::Otlp)));
    assert!(matches!("other".parse::<TracingExporter>(), Err(_)));
  }

  #[test]
  #[serial_test::serial]
  fn tracing_exporter_from_str() {
    unsafe {
      env::set_var("INDEX_CLIENT_ID", "secret");
      env::set_var("INDEX_CLIENT_SECRET", "secret");
    }

    assert!(matches!("none".parse::<WrappedEsAuthMethod>(), Ok(WrappedEsAuthMethod(EsAuthMethod::None))));
    assert!(matches!("basic".parse::<WrappedEsAuthMethod>(), Ok(WrappedEsAuthMethod(EsAuthMethod::Basic(_, _)))));
    assert!(matches!("bearer".parse::<WrappedEsAuthMethod>(), Ok(WrappedEsAuthMethod(EsAuthMethod::Bearer(_)))));
    assert!(matches!("api_key".parse::<WrappedEsAuthMethod>(), Ok(WrappedEsAuthMethod(EsAuthMethod::ApiKey(_, _)))));
    assert!(matches!("encoded_api_key".parse::<WrappedEsAuthMethod>(), Ok(WrappedEsAuthMethod(EsAuthMethod::EncodedApiKey(_)))));

    assert!(matches!("other".parse::<WrappedEsAuthMethod>(), Err(_)));

    unsafe {
      env::remove_var("INDEX_CLIENT_ID");
      env::remove_var("INDEX_CLIENT_SECRET");
    }
  }
}
