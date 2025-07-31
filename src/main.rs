#![allow(dead_code)]

mod api;
mod catalog;
mod matching;
mod model;
mod schemas;
mod scoring;
mod search;

#[cfg(test)]
mod tests;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt;

use crate::{
  api::config::{Config, Env},
  catalog::fetch_catalog,
  schemas::SCHEMAS,
};

#[tokio::main]
async fn main() {
  let config = Config::from_env();

  let _logger = init_logger(&config);
  let _ = *SCHEMAS;
  let catalog = fetch_catalog().await.expect("could not fetch initial catalog");

  let app = api::routes(&config, catalog).await;

  tracing::info!("listening on {}", config.listen_addr);

  let listener = tokio::net::TcpListener::bind(&config.listen_addr).await.expect("could not create listener");

  axum::serve(listener, app).await.expect("could not start app");
}

fn init_logger(config: &Config) -> WorkerGuard {
  use tracing_subscriber::{EnvFilter, prelude::*};

  let (appender, guard) = tracing_appender::non_blocking(std::io::stdout());

  let formatter = match config.env {
    Env::Dev => fmt::layer().compact().with_writer(appender).boxed(),
    Env::Production => fmt::layer().json().with_writer(appender).boxed(),
  };

  tracing_subscriber::registry()
    .with(EnvFilter::builder().try_from_env().or_else(|_| EnvFilter::try_new("info")).unwrap())
    .with(formatter)
    .init();

  guard
}
