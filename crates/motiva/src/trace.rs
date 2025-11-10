use std::io::Write;

use metrics_exporter_prometheus::{BuildError, Matcher, PrometheusBuilder, PrometheusHandle};
use opentelemetry::{KeyValue, global, trace::TracerProvider};
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

pub fn build_prometheus() -> Result<PrometheusHandle, BuildError> {
  let builder = PrometheusBuilder::new()
    .add_global_label("service", "motiva")
    .set_buckets_for_metric(Matcher::Full("motiva_scoring_scores".into()), &[0.2, 0.5, 0.7, 0.9])?
    .set_buckets_for_metric(Matcher::Full("motiva_scoring_latency_seconds".into()), &[0.000001, 0.000005, 0.000015, 0.0000050, 0.000100])?
    .set_buckets_for_metric(Matcher::Full("motiva_indexer_latency_seconds".into()), &[0.03, 0.06, 0.1, 0.2, 0.3])?;

  builder.install_recorder()
}

pub async fn init_tracing(config: &Config, writer: impl Write + Send + 'static) -> (WorkerGuard, Option<SdkTracerProvider>) {
  use tracing_subscriber::{EnvFilter, prelude::*};

  let (appender, logging_guard) = tracing_appender::non_blocking(writer);

  let formatter = match config.env {
    #[cfg(not(test))]
    Env::Dev => fmt::layer().compact().with_writer(appender).with_ansi(true).boxed(),
    Env::Production => json_subscriber::layer()
      .with_writer(appender)
      .flatten_event(true)
      .flatten_span_list_on_top_level(true)
      .with_current_span(false)
      .with_span_list(false)
      .boxed(),

    #[cfg(test)]
    Env::Dev => fmt::layer().compact().with_writer(appender).with_ansi(false).boxed(),
  };

  let (tracing_layer, tracing_provider, metrics_layer, metrics_provider, error) = match config.enable_tracing {
    true => {
      let resource = Resource::builder_empty().with_attributes([KeyValue::new("service.name", "motiva")]).build();

      let tracing_provider_builder = SdkTracerProvider::builder()
        .with_sampler(Sampler::TraceIdRatioBased(config::parse_env("OTEL_TRACES_SAMPLER_ARGS", 0.1).unwrap_or(0.1)))
        .with_resource(resource.clone());

      match config.tracing_exporter {
        TracingExporter::Otlp => {
          let tracing_otlp = opentelemetry_otlp::SpanExporter::builder().with_tonic().build().unwrap();
          let processor = BatchSpanProcessor::builder(tracing_otlp)
            .with_batch_config(BatchConfigBuilder::default().with_max_queue_size(8192).build())
            .build();

          let tracing_provider = tracing_provider_builder.with_span_processor(processor).build();
          let tracer = tracing_provider.tracer("motiva");
          let tracing_layer = tracing_opentelemetry::layer().with_tracer(tracer);

          let metrics_otlp = opentelemetry_otlp::MetricExporter::builder().with_tonic().build().unwrap();
          let metrics_provider = MeterProviderBuilder::default().with_periodic_exporter(metrics_otlp).with_resource(resource).build();
          let metrics_layer = MetricsLayer::new(metrics_provider.clone());

          (Some(tracing_layer), Some(tracing_provider), Some(metrics_layer), Some(metrics_provider), Option::<anyhow::Error>::None)
        }

        #[cfg(feature = "gcp")]
        TracingExporter::Gcp => {
          let gcp_trace_exporter = GcpCloudTraceExporterBuilder::new(config.gcp_project_id.clone()).with_resource(resource.clone());
          let tracing_provider = gcp_trace_exporter.create_provider_from_builder(tracing_provider_builder).await;

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
