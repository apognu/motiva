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
  #[serde(rename = "type")]
  pub type_: String,
  pub datasets: Option<Vec<String>>,
}

pub async fn fetch_catalog() -> anyhow::Result<Collections> {
  let catalog = reqwest::get(OPENSANCTIONS_CATALOG_URL)
    .await?
    .json::<Catalog>()
    .await?
    .datasets
    .into_iter()
    .filter(|dataset| dataset.type_ == "collection")
    .map(|dataset| (dataset.name.clone(), dataset))
    .collect::<HashMap<_, _>>();

  Ok(catalog)
}
