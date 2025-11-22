use std::collections::HashMap;

use anyhow::Context;
use jiff::{ToSpan, civil::DateTime};
use serde::{Deserialize, Serialize};

use crate::IndexProvider;

const OPENSANCTIONS_CATALOG_URL: &str = "https://data.opensanctions.org/datasets/latest/index.json";

pub type LoadedDatasets = HashMap<String, CatalogDataset>;

#[derive(Debug, Deserialize, Serialize)]
pub struct Manifest {
  #[serde(default)]
  pub catalogs: Vec<ManifestCatalog>,
  #[serde(default)]
  pub datasets: Vec<ManifestDataset>,
}

impl Default for Manifest {
  fn default() -> Self {
    Self {
      catalogs: vec![ManifestCatalog {
        url: OPENSANCTIONS_CATALOG_URL.to_string(),
        resource_name: "entities.ftm.json".to_string(),
        scope: Some("default".to_string()),
        ..Default::default()
      }],
      datasets: Default::default(),
    }
  }
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ManifestCatalog {
  pub url: String,
  pub scope: Option<String>,
  #[serde(default)]
  pub scopes: Vec<String>,
  pub resource_name: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ManifestDataset {
  pub name: String,
  pub title: String,
  pub version: String,
  pub entities_url: Option<String>,
  pub datasets: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Catalog {
  pub datasets: Vec<CatalogDataset>,
  #[serde(default)]
  pub index_stale: bool,
  #[serde(default)]
  pub outdated: Vec<String>,

  #[serde(skip)]
  pub(crate) loaded_datasets: LoadedDatasets,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct CatalogDataset {
  pub name: String,
  pub title: String,
  pub summary: String,
  #[serde(default)]
  pub tags: Vec<String>,
  #[serde(default)]
  pub description: String,
  #[serde(default)]
  pub children: Vec<String>,
  #[serde(default)]
  pub load: bool,
  pub version: String,
  pub index_version: Option<String>,
  #[serde(default)]
  pub index_current: bool,
  pub last_export: DateTime,
  pub updated_at: DateTime,
}

pub async fn get_manifest(url: &str) -> anyhow::Result<Manifest> {
  reqwest::get(url).await.context("could not reach manifest location")?.json().await.context("invalid manifest file")
}

pub async fn get_local_catalog<P: IndexProvider>(index: &P, manifest_url: Option<&String>) -> anyhow::Result<Catalog> {
  let manifest = match manifest_url {
    Some(url) => get_manifest(url).await?,
    None => Manifest::default(),
  };

  let indices = index.list_indices().await?;
  let mut catalog = Catalog::default();

  for mut spec in manifest.catalogs {
    let mut upstream: Catalog = reqwest::get(&spec.url).await?.json().await?;

    if let Some(scope) = spec.scope {
      spec.scopes.push(scope);
    }
    for ds in &mut upstream.datasets {
      if spec.scopes.contains(&ds.name) {
        ds.load = true;
      }

      if let Some((_, version)) = indices.iter().find(|(name, _)| name == &ds.name) {
        ds.index_version = Some(version.clone());

        match version.as_str() == ds.version {
          true => ds.index_current = true,
          false => {
            let Some(indexed_ts_str) = version.split("-").next() else {
              continue;
            };

            let Ok(indexed_timestamp) = DateTime::strptime("%Y%m%d%H%M%S", indexed_ts_str) else { continue };

            // TODO: parameterize this offset
            if ds.last_export > indexed_timestamp + 7.days() {
              catalog.outdated.push(ds.name.clone());
            }
          }
        }
      }
    }

    catalog.datasets.extend(upstream.datasets.into_iter());
  }

  for ds in manifest.datasets {
    let mut dataset = CatalogDataset {
      name: ds.name.clone(),
      title: ds.title,
      load: true,
      version: ds.version.clone(),
      index_version: None,
      index_current: false,
      ..Default::default()
    };

    if let Some((_, version)) = indices.iter().find(|(name, _)| name == &ds.name) {
      dataset.index_version = Some(version.clone());
      dataset.index_current = Some(&dataset.version) == dataset.index_version.as_ref();
    }

    catalog.datasets.push(dataset);
  }

  catalog.index_stale = !catalog.outdated.is_empty();
  catalog.loaded_datasets = catalog.datasets.iter().map(|dataset| (dataset.name.clone(), dataset.clone())).collect::<HashMap<_, _>>();

  tracing::info!(datasets = catalog.datasets.len(), "fetched catalog");

  Ok(catalog)
}
