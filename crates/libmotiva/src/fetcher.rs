use std::collections::HashMap;

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
pub struct HttpCatalogFetcher {
  pub manifest_url: Option<String>,
}

impl HttpCatalogFetcher {
  pub fn from_manifest_url(url: Option<String>) -> Self {
    Self { manifest_url: url }
  }
}

impl CatalogFetcher for HttpCatalogFetcher {
  async fn fetch_manifest(&self) -> anyhow::Result<Manifest> {
    match &self.manifest_url {
      Some(url) => reqwest::get(url).await.context("could not reach manifest location")?.json().await.context("invalid manifest file"),
      None => Ok(Manifest::default()),
    }
  }

  async fn fetch_catalog(&self, url: &str) -> anyhow::Result<Catalog> {
    Ok(reqwest::get(url).await?.json::<Catalog>().await?)
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
