use std::env;

pub struct Config {
  pub env: Env,
  pub listen_addr: String,
  pub es_url: String,
  pub enable_tracing: bool,
}

impl Config {
  pub fn from_env() -> Config {
    Config {
      env: Env::from(env::var("ENV").unwrap_or("dev".into())),
      listen_addr: env::var("LISTEN_ADDR").unwrap_or("0.0.0.0:8000".into()),
      es_url: env::var("ES_URL").unwrap_or("http://localhost:9200".into()),
      enable_tracing: env::var("ENABLE_TRACING").unwrap_or_default() == "1",
    }
  }
}

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
