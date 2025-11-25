#[cfg(test)]
use std::collections::HashMap;

use anyhow::Context;

use crate::{Catalog, catalog::Manifest};

pub trait Fetcher: Default {
  async fn fetch_manifest(&self, url: &str) -> anyhow::Result<Manifest>;
  async fn fetch_catalog(&self, url: &str) -> anyhow::Result<Catalog>;
}

pub(crate) struct RealFetcher;

impl Default for RealFetcher {
  fn default() -> Self {
    RealFetcher
  }
}

impl Fetcher for RealFetcher {
  async fn fetch_manifest(&self, url: &str) -> anyhow::Result<Manifest> {
    reqwest::get(url).await.context("could not reach manifest location")?.json().await.context("invalid manifest file")
  }

  async fn fetch_catalog(&self, url: &str) -> anyhow::Result<Catalog> {
    Ok(reqwest::get(url).await?.json::<Catalog>().await?)
  }
}

#[cfg(test)]
pub(crate) struct TestFetcher {
  pub(crate) manifest: Manifest,
  pub(crate) catalogs: HashMap<String, Catalog>,
}

#[cfg(test)]
impl Default for TestFetcher {
  fn default() -> Self {
    unimplemented!("TestFetcher does not have a default implementation, it needs to be mocked");
  }
}

#[cfg(test)]
impl Fetcher for TestFetcher {
  async fn fetch_manifest(&self, _url: &str) -> anyhow::Result<Manifest> {
    Ok(self.manifest.clone())
  }

  async fn fetch_catalog(&self, url: &str) -> anyhow::Result<Catalog> {
    self.catalogs.get(url).ok_or_else(|| anyhow::anyhow!("unknown catalog url")).cloned()
  }
}
