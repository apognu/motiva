mod api;
mod trace;

#[cfg(test)]
mod tests;

use libmotiva::{ElasticsearchProvider, HttpCatalogFetcher, IndexProvider};
use rustls::crypto::aws_lc_rs;
use tokio::signal;

use crate::api::config::Config;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  aws_lc_rs::default_provider().install_default().expect("could not install default cryptography provider");

  let config = Config::from_env().await?;
  let provider = ElasticsearchProvider::new(&config.index_url, config.index_auth_method.clone(), &config.index_tls_verification, None).await?;

  run(config, provider).await
}

async fn run<P: IndexProvider>(config: Config, provider: P) -> anyhow::Result<()> {
  let _guards = trace::init_tracing(&config, std::io::stdout()).await;
  let app = api::routes(&config, HttpCatalogFetcher::from_manifest_url(config.manifest_url.clone())?, provider).await?;
  let listener = tokio::net::TcpListener::bind(&config.listen_addr).await.expect("could not create listener");

  tracing::info!(motiva = env!("VERSION"), "listening on {}", listener.local_addr()?.to_string());

  axum::serve(listener, app).with_graceful_shutdown(shutdown()).await.expect("could not start app");

  Ok(())
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
