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

#[cfg(test)]
mod tests {
  use elasticsearch::{
    Elasticsearch,
    http::{
      Url,
      transport::{SingleNodeConnectionPool, TransportBuilder},
    },
  };
  use serde_json::json;
  use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

  use super::IndexVersion;
  use crate::{ElasticsearchProvider, MotivaError};

  #[test]
  fn index_version_display() {
    assert_eq!(IndexVersion::V4.to_string(), "v4");
    assert_eq!(IndexVersion::V5.to_string(), "v5");
  }

  fn provider(server: &MockServer) -> ElasticsearchProvider {
    let url = Url::parse(&server.uri()).unwrap();
    let transport = TransportBuilder::new(SingleNodeConnectionPool::new(url)).build().unwrap();

    ElasticsearchProvider {
      es: Elasticsearch::new(transport),
      index_version: IndexVersion::V4,
      index_prefix: "yente".to_string(),
      main_index: "yente-entities".to_string(),
      scoped_index: None,
    }
  }

  async fn detect_with_excludes(excludes: &str) -> Result<IndexVersion, MotivaError> {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({
          "yente-entities": {
              "mappings": {
                  "_source": {
                      "excludes":[excludes]
                  }
              }
          }
      })))
      .mount(&server)
      .await;

    provider(&server).detect_index_version().await
  }

  #[tokio::test]
  async fn detect_index_version_v5() {
    assert_eq!(detect_with_excludes("name_symbols").await.unwrap(), IndexVersion::V5);
  }

  #[tokio::test]
  async fn detect_index_version_v4() {
    assert_eq!(detect_with_excludes("name_keys").await.unwrap(), IndexVersion::V4);
  }

  #[tokio::test]
  async fn detect_index_version_fallback() {
    assert_eq!(detect_with_excludes("something_else").await.unwrap(), IndexVersion::V4);
  }

  #[tokio::test]
  async fn detect_index_version_missing_index() {
    let server = MockServer::start().await;

    Mock::given(method("GET")).respond_with(ResponseTemplate::new(404)).mount(&server).await;

    let error = provider(&server).detect_index_version().await.unwrap_err();

    assert!(matches!(error, MotivaError::MissingIndex(_)));
  }
}
