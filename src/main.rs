#![allow(dead_code)]

mod api;
mod catalog;
mod index;
mod matching;
mod model;
mod schemas;
mod scoring;

#[cfg(test)]
mod tests;

use opentelemetry::{KeyValue, trace::TracerProvider as _};
use opentelemetry_sdk::{Resource, trace::SdkTracerProvider};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt;

use crate::{
  api::config::{Config, Env},
  catalog::fetch_catalog,
  schemas::SCHEMAS,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  let config = Config::from_env()?;

  let (_logger, _provider) = init_logger(&config);
  let _ = *SCHEMAS;
  let catalog = fetch_catalog().await.expect("could not fetch initial catalog");

  let app = api::routes(&config, catalog).await?;

  tracing::info!("listening on {}", config.listen_addr);

  let listener = tokio::net::TcpListener::bind(&config.listen_addr).await.expect("could not create listener");

  axum::serve(listener, app).await.expect("could not start app");

  Ok(())
}

fn init_logger(config: &Config) -> (WorkerGuard, Option<SdkTracerProvider>) {
  use tracing_subscriber::{EnvFilter, prelude::*};

  let (appender, guard) = tracing_appender::non_blocking(std::io::stdout());

  let formatter = match config.env {
    Env::Dev => fmt::layer().compact().with_writer(appender).boxed(),
    Env::Production => fmt::layer().json().with_writer(appender).boxed(),
  };

  let (tracing_layer, tracing_provider) = match config.enable_tracing {
    true => {
      let otlp = opentelemetry_otlp::SpanExporter::builder().with_tonic().build().unwrap();

      let provider = SdkTracerProvider::builder()
        .with_batch_exporter(otlp)
        .with_resource(Resource::builder_empty().with_attributes([KeyValue::new("service.name", "motiva")]).build())
        .build();

      let tracer = provider.tracer("motiva");
      let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

      (Some(telemetry), Some(provider))
    }

    false => (None, None),
  };

  tracing_subscriber::registry()
    .with(EnvFilter::builder().try_from_env().or_else(|_| EnvFilter::try_new("info")).unwrap())
    .with(tracing_layer)
    .with(formatter)
    .init();

  (guard, tracing_provider)
}
