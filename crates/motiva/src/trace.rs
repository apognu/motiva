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
use tracing_subscriber::{EnvFilter, Layer, Registry, fmt, layer::SubscriberExt, util::SubscriberInitExt};

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

pub struct TraceGuards {
  _logging: WorkerGuard,
  trace: Option<SdkTracerProvider>,
}

impl Drop for TraceGuards {
  fn drop(&mut self) {
    if let Some(provider) = &self.trace {
      provider.shutdown().unwrap();
    }
  }
}

pub async fn init_tracing(config: &Config, writer: impl Write + Send + 'static) -> TraceGuards {
  let (appender, logging_guard) = tracing_appender::non_blocking(writer);

  let logging_formatter = match config.env {
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

  let guard = TraceGuards { _logging: logging_guard, trace: None };
  let (guard, tracing_layers) = tracing_layers(guard, config).await;
  let mut errors: Vec<anyhow::Error> = vec![];

  global::set_text_map_propagator(TraceContextPropagator::new());

  let layers = EnvFilter::builder().try_from_env().or_else(|_| EnvFilter::try_new("info")).unwrap().and_then(logging_formatter);

  let layers = match tracing_layers {
    Ok(tracing_layers) => tracing_layers.into_iter().fold(layers.boxed(), |registry, layer| registry.and_then(layer).boxed()),

    Err(err) => {
      errors.push(err);
      layers.boxed()
    }
  };

  tracing_subscriber::registry().with(layers).init();

  for err in errors {
    tracing::warn!(%err, "could not initialize tracing provider");
  }

  guard
}

type TracingLayers = Vec<Box<dyn Layer<Registry> + Send + Sync>>;

async fn tracing_layers(mut guards: TraceGuards, config: &Config) -> (TraceGuards, Result<TracingLayers, anyhow::Error>) {
  if !config.enable_tracing {
    return (guards, Ok(vec![]));
  }

  let resource = Resource::builder_empty().with_attributes([KeyValue::new("service.name", "motiva")]).build();

  let tracing_provider_builder = SdkTracerProvider::builder()
    .with_sampler(Sampler::TraceIdRatioBased(config::parse_env("OTEL_TRACES_SAMPLER_ARGS", 0.1).unwrap_or(0.1)))
    .with_resource(resource.clone());

  let layers: Result<TracingLayers, anyhow::Error> = match config.tracing_exporter {
    TracingExporter::Otlp => {
      let tracing_otlp = opentelemetry_otlp::SpanExporter::builder().with_tonic().build().unwrap();
      let processor = BatchSpanProcessor::builder(tracing_otlp)
        .with_batch_config(BatchConfigBuilder::default().with_max_queue_size(8192).build())
        .build();

      let provider = tracing_provider_builder.with_span_processor(processor).build();
      let tracer = provider.tracer("motiva");

      let metrics_otlp = opentelemetry_otlp::MetricExporter::builder().with_tonic().build().unwrap();
      let metrics_provider = MeterProviderBuilder::default().with_periodic_exporter(metrics_otlp).with_resource(resource).build();

      global::set_meter_provider(metrics_provider.clone());

      guards.trace = Some(provider);

      Ok(vec![tracing_opentelemetry::layer().with_tracer(tracer).boxed(), MetricsLayer::new(metrics_provider).boxed()])
    }

    #[cfg(feature = "gcp")]
    TracingExporter::Gcp => {
      let gcp_trace_exporter = GcpCloudTraceExporterBuilder::new(config.gcp_project_id.clone()).with_resource(resource.clone());
      let provider_result = gcp_trace_exporter.create_provider_from_builder(tracing_provider_builder).await;

      match provider_result {
        Ok(provider) => {
          let tracer: opentelemetry_sdk::trace::Tracer = gcp_trace_exporter.install(&provider).await.unwrap();

          guards.trace = Some(provider);

          Ok(vec![tracing_opentelemetry::layer().with_tracer(tracer).boxed()])
        }

        Err(err) => Err(err.into()),
      }
    }
  };

  (guards, layers)
}
