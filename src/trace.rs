use opentelemetry::{KeyValue, global, trace::TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
  Resource,
  metrics::MeterProviderBuilder,
  propagation::TraceContextPropagator,
  trace::{BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider},
};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_opentelemetry::MetricsLayer;
use tracing_subscriber::fmt;

#[cfg(feature = "gcp")]
use opentelemetry_gcloud_trace::GcpCloudTraceExporterBuilder;

use crate::api::config::{self, Config, Env, TracingExporter};

pub async fn init_logger(config: &Config) -> (WorkerGuard, Option<SdkTracerProvider>) {
  use tracing_subscriber::{EnvFilter, prelude::*};

  let (appender, logging_guard) = tracing_appender::non_blocking(std::io::stdout());

  let formatter = match config.env {
    Env::Dev => fmt::layer().compact().with_writer(appender).boxed(),
    Env::Production => fmt::layer().json().with_writer(appender).flatten_event(true).with_current_span(false).with_span_list(false).boxed(),
  };

  let (tracing_layer, tracing_provider, metrics_layer, metrics_provider, error) = match config.enable_tracing {
    true => {
      let resource = Resource::builder_empty().with_attributes([KeyValue::new("service.name", "motiva")]).build();

      match config.tracing_exporter {
        TracingExporter::Otlp => {
          let tracing_otlp = opentelemetry_otlp::SpanExporter::builder().with_tonic().build().unwrap();
          let processor = BatchSpanProcessor::builder(tracing_otlp)
            .with_batch_config(BatchConfigBuilder::default().with_max_queue_size(8192).build())
            .build();

          let tracing_provider = SdkTracerProvider::builder()
            .with_span_processor(processor)
            .with_sampler(Sampler::TraceIdRatioBased(config::parse_env("OTEL_TRACES_SAMPLER_ARGS", 0.1).unwrap_or(0.1)))
            .with_resource(resource.clone())
            .build();

          let tracer = tracing_provider.tracer("motiva");
          let _ = opentelemetry_otlp::SpanExporter::builder().with_tonic().build().unwrap();
          let tracing_layer = tracing_opentelemetry::layer().with_tracer(tracer);

          let metrics_otlp = opentelemetry_otlp::MetricExporter::builder().with_tonic().with_endpoint("http://localhost:4317").build().unwrap();
          let metrics_provider = MeterProviderBuilder::default().with_periodic_exporter(metrics_otlp).with_resource(resource).build();
          let metrics_layer = MetricsLayer::new(metrics_provider.clone());

          (Some(tracing_layer), Some(tracing_provider), Some(metrics_layer), Some(metrics_provider), Option::<anyhow::Error>::None)
        }

        #[cfg(feature = "gcp")]
        TracingExporter::Gcp => {
          let gcp_trace_exporter = GcpCloudTraceExporterBuilder::new(config.gcp_project_id.clone()).with_resource(resource.clone());

          let tracing_provider = gcp_trace_exporter
            .create_provider_from_builder(
              SdkTracerProvider::builder()
                .with_sampler(Sampler::TraceIdRatioBased(config::parse_env("OTEL_TRACES_SAMPLER_ARGS", 0.1).unwrap_or(0.1)))
                .with_resource(resource.clone()),
            )
            .await;

          match tracing_provider {
            Ok(tracing_provider) => {
              let tracer: opentelemetry_sdk::trace::Tracer = gcp_trace_exporter.install(&tracing_provider).await.unwrap();

              let tracing_layer = tracing_opentelemetry::layer().with_tracer(tracer);

              (Some(tracing_layer), Some(tracing_provider), None, None, None)
            }

            Err(err) => (None, None, None, None, Some(err.into())),
          }
        }
      }
    }

    false => (None, None, None, None, None),
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

  if let Some(error) = error {
    tracing::warn!(%error, "could not initialize tracing provider");
  }

  (logging_guard, tracing_provider)
}
