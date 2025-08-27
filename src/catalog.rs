use std::collections::HashMap;

use serde::Deserialize;

pub type Collections = HashMap<String, Dataset>;

const OPENSANCTIONS_CATALOG_URL: &str = "https://data.opensanctions.org/datasets/latest/index.json";

#[derive(Debug, Deserialize)]
pub struct Catalog {
  pub datasets: Vec<Dataset>,
}

#[derive(Debug, Deserialize)]
pub struct Dataset {
  pub name: String,
  pub children: Option<Vec<String>>,
}

pub async fn fetch_catalog(url: &Option<String>) -> anyhow::Result<Collections> {
  let catalog = reqwest::get(url.as_ref().map_or(OPENSANCTIONS_CATALOG_URL, |url| url))
    .await?
    .json::<Catalog>()
    .await?
    .datasets
    .into_iter()
    .map(|dataset| (dataset.name.clone(), dataset))
    .collect::<HashMap<_, _>>();

  tracing::info!(datasets = catalog.len(), "fetched catalog");

  Ok(catalog)
}
