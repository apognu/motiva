use std::{collections::HashMap, fs::File};

use anyhow::Context;

use crate::{
  Catalog,
  catalog::{Manifest, OPENSANCTIONS_CATALOG_URL},
};

pub trait CatalogFetcher: Clone + Default + Send + Sync + 'static {
  fn fetch_manifest(&self) -> impl Future<Output = anyhow::Result<Manifest>> + Send;
  fn fetch_catalog(&self, url: &str) -> impl Future<Output = anyhow::Result<Catalog>> + Send;
}

#[derive(Clone, Default)]
pub enum ManifestProtocol {
  #[default]
  Http,
  LocalFile,
}

#[derive(Clone, Default)]
pub struct HttpCatalogFetcher {
  pub protocol: ManifestProtocol,
  pub manifest_url: Option<String>,
}

impl HttpCatalogFetcher {
  pub fn from_manifest_url(url: Option<String>) -> Self {
    let protocol = match &url {
      Some(url) if url.starts_with("https://") || url.starts_with("http://") => ManifestProtocol::Http,
      Some(_) => ManifestProtocol::LocalFile,
      None => ManifestProtocol::Http,
    };

    Self { protocol, manifest_url: url }
  }
}

impl CatalogFetcher for HttpCatalogFetcher {
  async fn fetch_manifest(&self) -> anyhow::Result<Manifest> {
    match &self.manifest_url {
      Some(url) => match self.protocol {
        ManifestProtocol::Http => self.fetch_http(url).await,
        ManifestProtocol::LocalFile => self.fetch_local_file(url).await,
      },

      None => Ok(Manifest::default()),
    }
  }

  async fn fetch_catalog(&self, url: &str) -> anyhow::Result<Catalog> {
    Ok(reqwest::get(url).await?.json::<Catalog>().await?)
  }
}

impl HttpCatalogFetcher {
  async fn fetch_http(&self, url: &str) -> anyhow::Result<Manifest> {
    let response = reqwest::get(url).await.context("could not reach manifest location")?;

    if url.ends_with(".json") {
      response.json().await.context("invalid manifest file")
    } else if url.ends_with(".yml") || url.ends_with(".yaml") {
      tracing::warn!("using a YAML manifest is deprecated, support will be removed in a future version, use JSON instead");

      serde_yaml::from_str(&response.text().await?).context("could not parse local manifest file as YAML")
    } else {
      Err(anyhow::anyhow!("unknown extension for manifest file"))
    }
  }

  async fn fetch_local_file(&self, url: &str) -> anyhow::Result<Manifest> {
    let file = File::open(url)?;

    if url.ends_with(".json") {
      serde_json::from_reader(file).context("could not parse local manifest file as JSON")
    } else if url.ends_with(".yml") || url.ends_with(".yaml") {
      tracing::warn!("using a YAML manifest is deprecated, support will be removed in a future version, use JSON instead");

      serde_yaml::from_reader(file).context("could not parse local manifest file as YAML")
    } else {
      Err(anyhow::anyhow!("unknown extension for manifest file"))
    }
  }
}

#[derive(Clone)]
pub struct TestFetcher {
  pub manifest: Manifest,
  pub catalogs: HashMap<String, Catalog>,
}

impl Default for TestFetcher {
  fn default() -> Self {
    let catalogs = {
      let mut m = HashMap::default();

      m.insert(OPENSANCTIONS_CATALOG_URL.to_string(), Catalog::default());
      m
    };

    Self {
      manifest: Manifest::default(),
      catalogs,
    }
  }
}

impl CatalogFetcher for TestFetcher {
  async fn fetch_manifest(&self) -> anyhow::Result<Manifest> {
    Ok(self.manifest.clone())
  }

  async fn fetch_catalog(&self, url: &str) -> anyhow::Result<Catalog> {
    self.catalogs.get(url).ok_or_else(|| anyhow::anyhow!("unknown catalog url")).cloned()
  }
}
