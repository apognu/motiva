use std::fmt::Display;

use ahash::HashMap;
use elasticsearch::indices::IndicesGetMappingParts;
use reqwest::StatusCode;
use serde::Deserialize;

use crate::{ElasticsearchProvider, EsAuthMethod, EsTlsVerification, MotivaError};

#[derive(Default)]
pub struct EsOptions<'o> {
  pub auth: EsAuthMethod,
  pub tls: &'o EsTlsVerification,
  pub index_version: Option<IndexVersion>,
  pub index_name: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexVersion {
  V4,
  V5,
}

impl Display for IndexVersion {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "{}",
      match self {
        IndexVersion::V4 => "v4",
        IndexVersion::V5 => "v5",
      }
    )
  }
}

#[derive(Deserialize)]
pub(crate) struct MappingIndex {
  pub(crate) mappings: MappingIndexMappings,
}

#[derive(Deserialize)]
pub(crate) struct MappingIndexMappings {
  #[serde(rename = "_source")]
  pub(crate) source: MappingIndexSource,
}

#[derive(Deserialize)]
pub(crate) struct MappingIndexSource {
  pub(crate) excludes: Vec<String>,
}

impl ElasticsearchProvider {
  pub(crate) async fn detect_index_version(&self) -> Result<IndexVersion, MotivaError> {
    let mappings = self.es.indices().get_mapping(IndicesGetMappingParts::Index(&[&self.main_index])).send().await?;

    if mappings.status_code() != StatusCode::OK {
      Err(MotivaError::MissingIndex(self.main_index.to_string()))?
    }

    let mappings: HashMap<String, MappingIndex> = mappings.json().await?;

    for (_, index) in mappings {
      if index.mappings.source.excludes.contains(&"name_symbols".to_string()) {
        tracing::info!(version = ?IndexVersion::V4, "detected indexed version");
        return Ok(IndexVersion::V5);
      }
      if index.mappings.source.excludes.contains(&"name_keys".to_string()) {
        tracing::info!(version = ?IndexVersion::V4, "detected indexed version");
        return Ok(IndexVersion::V4);
      }
    }

    tracing::warn!(version = ?IndexVersion::V4, "could not definitely determine index version, falling back");

    Ok(IndexVersion::V4)
  }
}
