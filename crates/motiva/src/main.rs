mod api;
mod trace;

#[cfg(test)]
mod tests;

use libmotiva::{ElasticsearchProvider, IndexProvider};
use tokio::signal;

use crate::api::config::Config;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  let config = Config::from_env().await?;
  let provider = ElasticsearchProvider::new(&config.index_url, config.index_auth_method.clone())?;

  run(config, provider).await
}

async fn run<P: IndexProvider>(config: Config, provider: P) -> anyhow::Result<()> {
  let (_logger, tracer) = trace::init_tracing(&config, std::io::stdout()).await;
  let app = api::routes(&config, provider).await?;
  let listener = tokio::net::TcpListener::bind(&config.listen_addr).await.expect("could not create listener");

  tracing::info!("listening on {}", listener.local_addr()?.to_string());

  axum::serve(listener, app).with_graceful_shutdown(shutdown()).await.expect("could not start app");

  if let Some(provider) = tracer {
    provider.shutdown().unwrap();
  }

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
