use std::collections::HashMap;

use jiff::{
  Span, Timestamp,
  civil::{Date, DateTime},
};
use serde::{Deserialize, Serialize};

use crate::{IndexProvider, fetcher::CatalogFetcher};

pub(crate) const OPENSANCTIONS_CATALOG_URL: &str = "https://data.opensanctions.org/datasets/latest/index.json";

pub type LoadedDatasets = HashMap<String, CatalogDataset>;

#[derive(Clone, Debug, Deserialize, Serialize)]
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

#[cfg(test)]
impl Manifest {
  fn test() -> Self {
    Self {
      catalogs: vec![ManifestCatalog {
        url: OPENSANCTIONS_CATALOG_URL.to_string(),
        resource_name: "entities.ftm.json".to_string(),
        scope: Some("default".to_string()),
        ..Default::default()
      }],
      datasets: vec![ManifestDataset {
        name: "bare_dataset_1".into(),
        title: "Bare dataset #1".into(),
        ..Default::default()
      }],
    }
  }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ManifestCatalog {
  pub url: String,
  pub scope: Option<String>,
  #[serde(default)]
  pub scopes: Vec<String>,
  pub resource_name: String,
  #[serde(default)]
  pub datasets: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ManifestDataset {
  pub name: String,
  pub title: String,
  pub version: Option<String>,
  pub entities_url: Option<String>,
  pub datasets: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Catalog {
  pub datasets: Vec<CatalogDataset>,
  #[serde(default)]
  pub index_stale: bool,
  #[serde(default)]
  pub current: Vec<String>,
  #[serde(default)]
  pub outdated: Vec<String>,

  #[serde(skip)]
  pub loaded_datasets: LoadedDatasets,
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
  pub category: Option<String>,
  #[serde(default)]
  pub url: String,
  pub delta_url: Option<String>,
  pub entity_count: u64,
  #[serde(default)]
  pub thing_count: u64,
  #[serde(default)]
  pub children: Vec<String>,
  #[serde(default)]
  pub load: bool,
  pub version: String,
  pub index_version: Option<String>,
  #[serde(default)]
  pub index_current: bool,
  pub publisher: Option<CatalogDatasetPublisher>,
  pub coverage: Option<CatalogDatasetCoverage>,
  pub last_change: DateTime,
  pub last_export: DateTime,
  pub updated_at: DateTime,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct CatalogDatasetPublisher {
  pub name: String,
  pub acronym: Option<String>,
  pub url: String,
  pub country: Option<String>,
  pub description: Option<String>,
  pub official: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct CatalogDatasetCoverage {
  pub start: Date,
  pub end: Option<Date>,
  pub countries: Vec<String>,
  pub schedule: Option<String>,
  pub frequency: String,
}

pub async fn get_merged_catalog<P: IndexProvider, F: CatalogFetcher>(fetcher: &F, index: &P, outdated_grace: Span) -> anyhow::Result<Catalog> {
  let manifest = fetcher.fetch_manifest().await?;

  let indices = index.list_indices().await?;
  let mut catalog = Catalog::default();

  for mut spec in manifest.catalogs {
    let mut upstream: Catalog = fetcher.fetch_catalog(&spec.url).await?;

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

            if ds.last_export > indexed_timestamp + outdated_grace {
              catalog.outdated.push(ds.name.clone());
            } else {
              catalog.current.push(ds.name.clone());
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
      version: ds.version.unwrap_or_else(|| format!("{}-mot", Timestamp::now().strftime("%Y%m%d%H%M%S"))),
      index_version: None,
      index_current: false,
      children: ds.datasets.unwrap_or_default(),
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

#[cfg(test)]
mod tests {
  use std::collections::HashMap;

  use jiff::{Span, civil::DateTime};

  use crate::{
    Catalog, MockedElasticsearch,
    catalog::{CatalogDataset, Manifest, OPENSANCTIONS_CATALOG_URL},
    fetcher::TestFetcher,
  };

  #[tokio::test]
  async fn merge_catalog() {
    let catalog = Catalog {
      datasets: vec![
        CatalogDataset {
          name: "default".to_string(),
          children: vec!["dataset1".to_string(), "dataset2".to_string(), "dataset3".to_string()],
          ..Default::default()
        },
        CatalogDataset {
          name: "dataset1".to_string(),
          version: "20251125100000-pop".to_string(),
          last_export: DateTime::constant(2025, 11, 25, 10, 0, 0, 0),
          ..Default::default()
        },
        CatalogDataset {
          name: "dataset2".to_string(),
          version: "20251125100000-pop".to_string(),
          last_export: DateTime::constant(2025, 11, 25, 10, 0, 0, 0),
          ..Default::default()
        },
        CatalogDataset {
          name: "dataset3".to_string(),
          version: "3".to_string(),
          last_export: DateTime::constant(2025, 11, 25, 10, 0, 0, 0),
          ..Default::default()
        },
      ],
      ..Default::default()
    };

    let mut catalogs = HashMap::default();
    catalogs.insert(OPENSANCTIONS_CATALOG_URL.to_string(), catalog);

    let fetcher = TestFetcher { manifest: Manifest::test(), catalogs };

    let indices = vec![("dataset1".to_string(), "20251125100000-pop".to_string()), ("dataset2".to_string(), "2025110100000-pop".to_string())];
    let catalog = super::get_merged_catalog(&fetcher, &MockedElasticsearch::builder().indices(indices).build(), Span::default())
      .await
      .unwrap();

    assert_eq!(catalog.datasets.len(), 5);
    assert_eq!(catalog.outdated.len(), 1);

    let datasets_by_name = catalog.datasets.into_iter().map(|ds| (ds.name.clone(), ds)).collect::<HashMap<_, _>>();

    assert!(datasets_by_name["default"].load);

    assert!(!datasets_by_name["dataset1"].load);
    assert_eq!(Some(&datasets_by_name["dataset1"].version), datasets_by_name["dataset1"].index_version.as_ref());
    assert!(datasets_by_name["dataset1"].index_current);

    assert!(!datasets_by_name["dataset2"].load);
    assert_ne!(Some(&datasets_by_name["dataset2"].version), datasets_by_name["dataset2"].index_version.as_ref());
    assert!(!datasets_by_name["dataset2"].index_current);

    assert!(!datasets_by_name["dataset3"].load);
    assert!(datasets_by_name["dataset3"].index_version.is_none());
    assert!(!datasets_by_name["dataset3"].index_current);
  }
}
