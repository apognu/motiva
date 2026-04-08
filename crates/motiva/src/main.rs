mod api;
mod oneoff;
mod trace;

#[cfg(test)]
mod tests;

use libmotiva::{ElasticsearchProvider, EsOptions, HttpCatalogFetcher, IndexProvider};
use rustls::crypto::aws_lc_rs;
use shadow_rs::shadow;
use tokio::signal;

use crate::api::config::Config;

shadow!(build);

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  if let Some("version") = std::env::args().nth(1).as_deref() {
    return oneoff::version();
  }

  aws_lc_rs::default_provider().install_default().expect("could not install default cryptography provider");

  let config = Config::from_env().await?;

  let options = EsOptions {
    auth: config.index_auth_method.clone(),
    tls: &config.index_tls_verification,
    index_name: config.index_name.clone(),
    ..Default::default()
  };

  let provider = ElasticsearchProvider::new(&config.index_url, options).await?;

  if let Some(cmd) = std::env::args().nth(1) {
    let _guards = trace::init_tracing(&config, std::io::stdout()).await;

    match cmd.as_str() {
      "create-scoped-index" => oneoff::create_scoped_index(&provider).await?,
      _ => anyhow::bail!("unsupported command `{cmd}`"),
    }

    return Ok(());
  }

  run(config, provider).await
}

async fn run<P: IndexProvider>(mut config: Config, provider: P) -> anyhow::Result<()> {
  let _guards = trace::init_tracing(&config, std::io::stdout()).await;

  let listener = match config.listener {
    Some(_) => config.listener.take().unwrap(),
    None => tokio::net::TcpListener::bind(&config.listen_addr).await.expect("could not create listener"),
  };

  let manifest_url = config.manifest_url.clone();
  let app = api::routes(config, HttpCatalogFetcher::from_manifest_url(manifest_url)?, provider).await?;

  tracing::info!(motiva = git_version(), "listening on {}", listener.local_addr()?.to_string());

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

fn git_version() -> String {
  match crate::build::TAG {
    "" => format!("{}-{}-g{}", crate::build::LAST_TAG, crate::build::COMMITS_SINCE_TAG, crate::build::SHORT_COMMIT),
    tag => tag.to_string(),
  }
}
