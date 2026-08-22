use std::{
  collections::HashMap,
  env::{self, VarError},
  fmt::Display,
  fs,
  str::FromStr,
};

use anyhow::Context;
use base64::Engine;
use jiff::Span;
use libmotiva::{EsTlsVerification, GetEntityLimits, prelude::EsAuthMethod};
use tokio::net::TcpListener;

#[cfg(feature = "aws")]
use libmotiva::prelude::AwsService;

use crate::api::errors::AppError;

#[derive(Default, Debug)]
pub struct Config {
  pub env: Env,
  pub listen_addr: String,
  pub listener: Option<TcpListener>,
  pub api_key: Option<String>,

  // Elasticsearch
  pub index_url: String,
  pub index_auth_method: EsAuthMethod,
  pub index_tls_verification: EsTlsVerification,
  pub index_name: Option<String>,

  // Timeouts
  pub request_timeout: Span,

  // Match settings
  pub manifest_url: Option<String>,
  pub catalog_refresh_interval: Span,
  pub outdated_grace: Span,
  pub match_candidates: usize,
  pub weights: HashMap<String, f64>,

  // Enrichment settings
  pub enrichment_max_recursion: usize,
  pub enrichment_query_limit: usize,

  // Observability
  pub enable_prometheus: bool,
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
      listener: None,
      api_key: env::var("API_KEY").ok(),
      match_candidates: parse_env("MATCH_CANDIDATES", 10)?,
      weights: parse_weights_from_env()?,
      manifest_url: env::var("MANIFEST_URL").ok(),
      request_timeout: parse_env("REQUEST_TIMEOUT", Span::from_str("10s").unwrap())?,
      catalog_refresh_interval: parse_env("CATALOG_REFRESH_INTERVAL", Span::from_str("1h").unwrap())?,
      outdated_grace: parse_env("OUTDATED_GRACE", Span::default())?,
      index_url: env::var("INDEX_URL").unwrap_or("http://localhost:9200".into()),
      index_auth_method: env::var("INDEX_AUTH_METHOD").unwrap_or("none".into()).parse::<WrappedEsAuthMethod>()?.0,
      index_tls_verification: parse_index_tls_verification()?,
      index_name: env::var("INDEX_NAME").ok(),
      enrichment_max_recursion: parse_env("ENRICHMENT_MAX_RECURSION", GetEntityLimits::default().max_recursion)?,
      enrichment_query_limit: parse_env("ENRICHMENT_QUERY_LIMIT", GetEntityLimits::default().query_limit)?,
      enable_prometheus: env::var("ENABLE_PROMETHEUS").unwrap_or_default() == "1",
      enable_tracing: env::var("ENABLE_TRACING").unwrap_or_default() == "1",
      tracing_exporter: env::var("TRACING_EXPORTER").unwrap_or("otlp".into()).parse()?,
      #[cfg(feature = "gcp")]
      gcp_project_id: detect_gcp_project_id().await,
    };

    Ok(config)
  }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum Env {
  #[default]
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

      "encoded_api_key" if client_secret.is_some() => {
        let (client_id, client_secret) = encoded_api_key(&client_secret.unwrap())?;

        EsAuthMethod::ApiKey(client_id, client_secret)
      }

      #[cfg(feature = "aws")]
      "aws-iam-service" => EsAuthMethod::AwsIam(AwsService::Service),
      #[cfg(feature = "aws")]
      "aws-iam-serverless" => EsAuthMethod::AwsIam(AwsService::Serverless),

      "basic" | "bearer" | "api_key" | "encoded_api_key" => Err(AppError::ConfigError("chosen index authentication method is missing a credential setting".into()))?,

      _ => Err(AppError::ConfigError("invalid elasticsearch authentication method".into()))?,
    }))
  }
}

fn encoded_api_key(value: &str) -> anyhow::Result<(String, String)> {
  use base64::engine::general_purpose::STANDARD;

  let value = STANDARD.decode(value)?;
  let value = String::from_utf8(value)?;

  let (username, password) = value.split_once(':').ok_or(anyhow::anyhow!("invalid shape for encoded api key"))?;

  Ok((username.to_string(), password.to_string()))
}

#[derive(Clone, Debug, Default)]
pub enum TracingExporter {
  #[default]
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

fn parse_weights_from_env() -> anyhow::Result<HashMap<String, f64>> {
  let mut weights = HashMap::new();

  for (k, v) in env::vars() {
    if let Some(feature) = k.strip_prefix("WEIGHT_") {
      let feature = feature.to_lowercase();
      let weight = v.parse::<f64>().context(format!("weight value for {k} is outside [-1.0,1.0] ({v})"))?.clamp(-1.0, 1.0);

      if weight.is_nan() {
        return Err(anyhow::anyhow!(format!("weight value for {feature} (through {k}) is NaN")));
      }

      weights.insert(feature, weight);
    }
  }

  Ok(weights)
}

fn parse_index_tls_verification() -> Result<EsTlsVerification, anyhow::Error> {
  if env::var("INDEX_TLS_SKIP_VERIFY").unwrap_or_default() == "1" {
    return Ok(EsTlsVerification::SkipVerify);
  }

  if let Ok(path) = env::var("INDEX_TLS_CA_CERT")
    && !path.is_empty()
  {
    let pem = fs::read(path).context("could not read certificate chain")?;

    return Ok(EsTlsVerification::CaCertChain(pem));
  }

  Ok(EsTlsVerification::Default)
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
  use std::net::{IpAddr, Ipv4Addr};

  use crate::api::config::WrappedEsAuthMethod;

  use super::{Config, Env, EsAuthMethod, TracingExporter};

  #[serial_test::serial]
  #[tokio::test]
  async fn parse_config_from_env() {
    let vars = [
      ("ENV", Some("production")),
      ("LISTEN_ADDR", Some("0.0.0.0:8080")),
      ("MATCH_CANDIDATES", Some("3")),
      ("YENTE_URL", Some("http://yente")),
      ("INDEX_URL", Some("http://index")),
      ("INDEX_AUTH_METHOD", Some("encoded_api_key")),
      ("INDEX_CLIENT_SECRET", Some("dXNlcm5hbWU6cGFzc3dvcmQ=")),
      ("ENABLE_TRACING", Some("1")),
    ];

    temp_env::async_with_vars(vars, async {
      let config = Config::from_env().await.unwrap();

      assert_eq!(config.env, Env::Production);
      assert_eq!(config.listen_addr, "0.0.0.0:8080");
      assert_eq!(config.match_candidates, 3);
      assert_eq!(config.index_url, "http://index");
      assert_eq!(config.index_auth_method, EsAuthMethod::ApiKey("username".to_string(), "password".to_string()));
      assert!(config.enable_tracing);
    })
    .await;
  }

  #[tokio::test]
  #[serial_test::serial]
  async fn invalid_es_auth_method_combination() {
    for method in ["basic", "api_key"] {
      temp_env::async_with_vars([("INDEX_AUTH_METHOD", Some(method)), ("INDEX_CLIENT_ID", None), ("INDEX_CLIENT_SECRET", Some("secret"))], async {
        assert!(Config::from_env().await.is_err());
      })
      .await;
    }

    temp_env::async_with_vars(
      [("INDEX_AUTH_METHOD", Some("basic")), ("INDEX_CLIENT_ID", Some("secret")), ("INDEX_CLIENT_SECRET", Some("secret"))],
      async {
        let config = Config::from_env().await.unwrap();

        assert_eq!(config.index_auth_method, EsAuthMethod::Basic("secret".to_string(), "secret".to_string()));
      },
    )
    .await;

    temp_env::async_with_vars(
      [("INDEX_AUTH_METHOD", Some("api_key")), ("INDEX_CLIENT_ID", Some("secret")), ("INDEX_CLIENT_SECRET", Some("secret"))],
      async {
        let config = Config::from_env().await.unwrap();

        assert_eq!(config.index_auth_method, EsAuthMethod::ApiKey("secret".to_string(), "secret".to_string()));
      },
    )
    .await;
  }

  #[cfg(feature = "aws")]
  #[tokio::test]
  #[serial_test::serial]
  async fn aws_iam() {
    use libmotiva::AwsService;

    for (value, expected) in [("aws-iam-serverless", AwsService::Serverless), ("aws-iam-service", AwsService::Service)] {
      temp_env::async_with_vars([("INDEX_AUTH_METHOD", Some(value))], async {
        let config = Config::from_env().await.unwrap();

        assert_eq!(config.index_auth_method, EsAuthMethod::AwsIam(expected));
      })
      .await;
    }
  }

  #[test]
  #[serial_test::serial]
  fn parse_env() {
    temp_env::with_vars([("INT", Some("42")), ("BOOL", Some("true")), ("IP", Some("1.2.3.4"))], || {
      assert_eq!(super::parse_env::<u32>("INT", 0).unwrap(), 42);
      assert!(super::parse_env::<bool>("BOOL", true).unwrap());
      assert_eq!(super::parse_env::<IpAddr>("IP", IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4))).unwrap(), IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)));

      assert!(super::parse_env::<u32>("BOOL", 0).is_err());
    });
  }

  #[test]
  fn es_auth_method_from_str() {
    assert!(matches!("otlp".parse(), Ok(TracingExporter::Otlp)));
    assert!("other".parse::<TracingExporter>().is_err());
  }

  #[test]
  #[serial_test::serial]
  fn tracing_exporter_from_str() {
    temp_env::with_vars([("INDEX_CLIENT_ID", Some("secret")), ("INDEX_CLIENT_SECRET", Some("secret"))], || {
      assert!(matches!("none".parse::<WrappedEsAuthMethod>(), Ok(WrappedEsAuthMethod(EsAuthMethod::None))));
      assert!(matches!("basic".parse::<WrappedEsAuthMethod>(), Ok(WrappedEsAuthMethod(EsAuthMethod::Basic(_, _)))));
      assert!(matches!("bearer".parse::<WrappedEsAuthMethod>(), Ok(WrappedEsAuthMethod(EsAuthMethod::Bearer(_)))));
      assert!(matches!("api_key".parse::<WrappedEsAuthMethod>(), Ok(WrappedEsAuthMethod(EsAuthMethod::ApiKey(_, _)))));

      assert!("other".parse::<WrappedEsAuthMethod>().is_err());
    });

    temp_env::with_vars([("INDEX_CLIENT_ID", Some("secret")), ("INDEX_CLIENT_SECRET", Some("dXNlcm5hbWU6cGFzc3dvcmQ="))], || {
      assert!(matches!("encoded_api_key".parse::<WrappedEsAuthMethod>(), Ok(WrappedEsAuthMethod(EsAuthMethod::ApiKey(_, _)))));
    });
  }

  #[test]
  fn env_from_string() {
    assert_eq!(Env::from("dev".to_string()), Env::Dev);
    assert_eq!(Env::from("production".to_string()), Env::Production);
    assert_eq!(Env::from("garbage".to_string()), Env::Dev);
    assert_eq!(Env::default(), Env::Dev);
  }

  #[test]
  #[serial_test::serial]
  fn parse_index_tls() {
    use libmotiva::EsTlsVerification;

    temp_env::with_vars([("INDEX_TLS_SKIP_VERIFY", Some("1")), ("INDEX_TLS_CA_CERT", None)], || {
      assert_eq!(super::parse_index_tls_verification().unwrap(), EsTlsVerification::SkipVerify);
    });

    temp_env::with_vars([("INDEX_TLS_SKIP_VERIFY", None), ("INDEX_TLS_CA_CERT", Some("Cargo.toml"))], || {
      assert!(matches!(super::parse_index_tls_verification().unwrap(), EsTlsVerification::CaCertChain(_)));
    });

    temp_env::with_vars([("INDEX_TLS_SKIP_VERIFY", None), ("INDEX_TLS_CA_CERT", Some(""))], || {
      assert_eq!(super::parse_index_tls_verification().unwrap(), EsTlsVerification::Default);
    });

    temp_env::with_vars_unset(["INDEX_TLS_SKIP_VERIFY", "INDEX_TLS_CA_CERT"], || {
      assert_eq!(super::parse_index_tls_verification().unwrap(), EsTlsVerification::Default);
    });

    temp_env::with_vars([("INDEX_TLS_SKIP_VERIFY", None), ("INDEX_TLS_CA_CERT", Some("/nonexistent/path/to/cert.pem"))], || {
      assert!(super::parse_index_tls_verification().is_err());
    });
  }

  #[test]
  #[serial_test::serial]
  fn parse_env_empty_returns_default() {
    temp_env::with_var("MOTIVA_TEST_EMPTY", Some(""), || {
      assert_eq!(super::parse_env::<u32>("MOTIVA_TEST_EMPTY", 7).unwrap(), 7);
    });
  }

  #[test]
  #[serial_test::serial]
  fn es_auth_method_missing_credentials() {
    temp_env::with_vars_unset(["INDEX_CLIENT_ID", "INDEX_CLIENT_SECRET"], || {
      assert!("bearer".parse::<WrappedEsAuthMethod>().is_err());
      assert!("encoded_api_key".parse::<WrappedEsAuthMethod>().is_err());
    });
  }

  #[test]
  #[serial_test::serial]
  fn parse_weights() {
    let weights = [
      ("WEIGHT_POSITIVE", Some("0.1")),
      ("WEIGHT_NEGATIVE", Some("-0.7")),
      ("WEIGHT_LOWER_CLAMPED", Some("-2.0")),
      ("WEIGHT_HIGHER_CLAMPED", Some("2.0")),
    ];

    temp_env::with_vars(weights, || {
      let weights = super::parse_weights_from_env().unwrap();

      assert!(matches!(weights.get("positive"), Some(0.1)));
      assert!(matches!(weights.get("negative"), Some(-0.7)));
      assert!(matches!(weights.get("lower_clamped"), Some(-1.0)));
      assert!(matches!(weights.get("higher_clamped"), Some(1.0)));
    });

    temp_env::with_var("WEIGHT_NAN", Some("nan"), || {
      assert!(super::parse_weights_from_env().is_err());
    });
  }
}
