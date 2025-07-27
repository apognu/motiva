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

use opentelemetry::{KeyValue, trace::TracerProvider as _};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, trace::SdkTracerProvider};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt;

use crate::{catalog::fetch_catalog, schemas::SCHEMAS};

#[tokio::main]
async fn main() {
  let (_logger, _provider) = init_logger();
  let _ = *SCHEMAS;
  let catalog = fetch_catalog().await.expect("could not fetch initial catalog");

  let app = api::routes(catalog).await;

  tracing::info!("listening on 0.0.0.0:8080");

  let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await.expect("could not create listener");

  axum::serve(listener, app).await.expect("could not start app");
}

fn init_logger() -> (WorkerGuard, SdkTracerProvider) {
  use tracing_subscriber::{EnvFilter, prelude::*};

  let (appender, guard) = tracing_appender::non_blocking(std::io::stdout());

  // TODO: for production, implement configuration
  let formatter = match true {
    true => fmt::layer().compact().with_writer(appender).boxed(),
    false => fmt::layer().json().with_writer(appender).boxed(),
  };

  let otlp = opentelemetry_otlp::SpanExporter::builder().with_tonic().with_endpoint("http://localhost:4317").build().unwrap();

  let provider = SdkTracerProvider::builder()
    .with_batch_exporter(otlp)
    .with_resource(Resource::builder_empty().with_attributes([KeyValue::new("service.name", "motiva")]).build())
    .build();

  let tracer = provider.tracer("motiva");
  let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

  tracing_subscriber::registry()
    .with(telemetry)
    .with(EnvFilter::builder().try_from_env().or_else(|_| EnvFilter::try_new("info")).unwrap())
    .with(formatter)
    .init();

  (guard, provider)
}
