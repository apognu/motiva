#![allow(dead_code)]

mod api;
mod catalog;
mod index;
mod matching;
mod model;
mod schemas;
pub mod scoring;

#[cfg(any(test, feature = "benchmarks"))]
pub mod tests;
#[cfg(feature = "benchmarks")]
pub use crate::matching::{logic_v1::LogicV1, name_based::NameBased, name_qualified::NameQualified};

use opentelemetry::{KeyValue, global, trace::TracerProvider as _};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
  Resource,
  metrics::MeterProviderBuilder,
  propagation::TraceContextPropagator,
  trace::{BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider},
};
use tokio::signal;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_opentelemetry::MetricsLayer;
use tracing_subscriber::fmt;

use crate::{
  api::config::{self, Config, Env},
  catalog::fetch_catalog,
  matching::replacers::{addresses::ADDRESS_FORMS, company_types::ORG_TYPES, ordinals::ORDINALS},
  schemas::SCHEMAS,
};

pub async fn entrypoint() -> anyhow::Result<()> {
  let config = Config::from_env()?;

  let (_logger, tracer) = init_logger(&config);
  let _ = *SCHEMAS;
  let _ = *ORG_TYPES;
  let _ = *ADDRESS_FORMS;
  let _ = *ORDINALS;

  let catalog = fetch_catalog(&config.catalog_url).await.expect("could not fetch initial catalog");

  let app = api::routes(&config, catalog)?;

  tracing::info!("listening on {}", config.listen_addr);

  let listener = tokio::net::TcpListener::bind(&config.listen_addr).await.expect("could not create listener");

  axum::serve(listener, app).with_graceful_shutdown(shutdown()).await.expect("could not start app");

  if let Some(provider) = tracer {
    provider.shutdown().unwrap();
  }

  Ok(())
}

fn init_logger(config: &Config) -> (WorkerGuard, Option<SdkTracerProvider>) {
  use tracing_subscriber::{EnvFilter, prelude::*};

  let (appender, logging_guard) = tracing_appender::non_blocking(std::io::stdout());

  let formatter = match config.env {
    Env::Dev => fmt::layer().compact().with_writer(appender).boxed(),
    Env::Production => fmt::layer().json().with_writer(appender).boxed(),
  };

  let (tracing_layer, tracing_provider, metrics_layer, metrics_provider) = match config.enable_tracing {
    true => {
      let resource = Resource::builder_empty().with_attributes([KeyValue::new("service.name", "motiva")]).build();

      let tracing_otlp = opentelemetry_otlp::SpanExporter::builder().with_tonic().build().unwrap();
      let metrics_otlp = opentelemetry_otlp::MetricExporter::builder().with_tonic().with_endpoint("http://localhost:4317").build().unwrap();

      let processor = BatchSpanProcessor::builder(tracing_otlp)
        .with_batch_config(BatchConfigBuilder::default().with_max_queue_size(8192).build())
        .build();

      let tracing_provider = SdkTracerProvider::builder()
        .with_span_processor(processor)
        .with_sampler(Sampler::TraceIdRatioBased(config::parse_env("OTEL_TRACES_SAMPLER_ARGS", 0.1).unwrap_or(0.1)))
        .with_resource(resource.clone())
        .build();

      let tracer = tracing_provider.tracer("motiva");
      let tracing_layer = tracing_opentelemetry::layer().with_tracer(tracer);

      let metrics_provider = MeterProviderBuilder::default().with_periodic_exporter(metrics_otlp).with_resource(resource).build();
      let metrics_layer = MetricsLayer::new(metrics_provider.clone());

      (Some(tracing_layer), Some(tracing_provider), Some(metrics_layer), Some(metrics_provider))
    }

    false => (None, None, None, None),
  };

  if let Some(provider) = metrics_provider {
    global::set_meter_provider(provider);
  }
  global::set_text_map_propagator(TraceContextPropagator::new());

  tracing_subscriber::registry()
    .with(EnvFilter::builder().try_from_env().or_else(|_| EnvFilter::try_new("info")).unwrap())
    .with(metrics_layer)
    .with(tracing_layer)
    .with(formatter)
    .init();

  (logging_guard, tracing_provider)
}

async fn shutdown() {
  let ctrl_c = async {
    signal::ctrl_c().await.expect("failed to install ^C handler");
  };

  let terminate = async {
    signal::unix::signal(signal::unix::SignalKind::terminate())
      .expect("failed to install terminate signal handler")
      .recv()
      .await;
  };

  tokio::select! {
      () = ctrl_c => tracing::info!("received ^C, initiating shutdown"),
      () = terminate => tracing::info!("received terminate signal, initiating shutdown"),
  }
}
