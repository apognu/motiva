#![allow(dead_code)]

mod api;
mod catalog;
mod index;
mod matching;
mod model;
mod schemas;
pub mod scoring;
mod trace;

#[cfg(any(test, feature = "benchmarks"))]
pub mod tests;
#[cfg(feature = "benchmarks")]
pub use crate::matching::{logic_v1::LogicV1, name_based::NameBased, name_qualified::NameQualified};

use crate::{
  api::config::Config,
  catalog::fetch_catalog,
  matching::replacers::{addresses::ADDRESS_FORMS, company_types::ORG_TYPES, ordinals::ORDINALS},
  schemas::SCHEMAS,
};

pub async fn entrypoint<S>(shutdown: fn() -> S) -> anyhow::Result<()>
where
  S: Future<Output = ()> + Send + 'static,
{
  let config = Config::from_env().await?;

  let (_logger, tracer) = trace::init_logger(&config).await;
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
