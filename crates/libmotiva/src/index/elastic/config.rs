use std::{fmt::Display, sync::PoisonError};

use ahash::HashMap;
use elasticsearch::indices::IndicesGetMappingParts;
use reqwest::StatusCode;
use serde::Deserialize;

use crate::{ElasticsearchProvider, EsAuthMethod, EsTlsVerification, MotivaError, index::IndexProvider};

#[derive(Default)]
pub struct EsOptions<'o> {
  pub auth: EsAuthMethod,
  pub tls: &'o EsTlsVerification,
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
  pub(crate) async fn refresh_index_state(&self) {
    let (ready, version, scoped_index) = match self.detect_index_version().await {
      Ok(version) => {
        let healthy = self.health().await.unwrap_or(false);
        let scoped_index = if healthy { self.detect_scoped_index().await } else { None };

        (healthy, version, scoped_index)
      }

      Err(err) => {
        tracing::warn!(error = err.to_string(), index = self.main_index, "index is not ready");

        (false, self.index_version(), None)
      }
    };

    {
      let mut state = self.state.write().unwrap_or_else(PoisonError::into_inner);

      let recovered = ready && !state.ready;

      state.ready = ready;
      state.index_version = version;
      state.scoped_index = scoped_index;

      if recovered {
        tracing::info!(version = %version, index = self.main_index, "index is now ready");
      }
    }
  }

  pub(crate) async fn detect_index_version(&self) -> Result<IndexVersion, MotivaError> {
    let mappings = self.es.indices().get_mapping(IndicesGetMappingParts::Index(&[&self.main_index])).send().await?;

    if mappings.status_code() != StatusCode::OK {
      Err(MotivaError::MissingIndex(self.main_index.to_string()))?
    }

    let mappings: HashMap<String, MappingIndex> = mappings.json().await?;

    for (_, index) in mappings {
      if index.mappings.source.excludes.contains(&"name_symbols".to_string()) {
        tracing::info!(version = ?IndexVersion::V5, "detected indexed version");
        return Ok(IndexVersion::V5);
      }
      if index.mappings.source.excludes.contains(&"name_keys".to_string()) {
        tracing::info!(version = ?IndexVersion::V4, "detected indexed version");
        return Ok(IndexVersion::V4);
      }
    }

    Err(MotivaError::OtherError(anyhow::anyhow!("index {} has an unrecognized mapping", self.main_index)))
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
  use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
  };

  use super::IndexVersion;
  use crate::{ElasticsearchProvider, MotivaError, index::IndexProvider};

  #[test]
  fn index_version_display() {
    assert_eq!(IndexVersion::V4.to_string(), "v4");
    assert_eq!(IndexVersion::V5.to_string(), "v5");
  }

  fn provider(server: &MockServer) -> ElasticsearchProvider {
    use std::sync::{Arc, RwLock};

    use crate::index::elastic::IndexState;

    let url = Url::parse(&server.uri()).unwrap();
    let transport = TransportBuilder::new(SingleNodeConnectionPool::new(url)).build().unwrap();

    ElasticsearchProvider {
      es: Elasticsearch::new(transport),
      index_prefix: "yente".to_string(),
      main_index: "yente-entities".to_string(),
      state: Arc::new(RwLock::new(IndexState {
        ready: false,
        index_version: IndexVersion::V4,
        scoped_index: None,
      })),
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
  async fn detect_index_version_unrecognized() {
    assert!(detect_with_excludes("something_else").await.is_err());
  }

  #[tokio::test]
  async fn detect_index_version_missing_index() {
    let server = MockServer::start().await;

    Mock::given(method("GET")).respond_with(ResponseTemplate::new(404)).mount(&server).await;

    let error = provider(&server).detect_index_version().await.unwrap_err();

    assert!(matches!(error, MotivaError::MissingIndex(_)));
  }

  #[tokio::test]
  async fn refresh_index_state_ready() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
      .and(path("/yente-entities/_mapping"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({
          "yente-entities": { "mappings": { "_source": { "excludes": ["name_keys"] } } }
      })))
      .mount(&server)
      .await;

    Mock::given(method("GET"))
      .and(path("/_cluster/health/yente-entities"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "status": "yellow" })))
      .mount(&server)
      .await;

    let provider = provider(&server);
    provider.refresh_index_state().await;

    assert!(provider.ready());
    assert_eq!(provider.index_version(), IndexVersion::V4);
    assert_eq!(provider.state.read().unwrap().scoped_index, None);
  }

  #[tokio::test]
  async fn refresh_index_state_ready_with_scoped_index() {
    let server = MockServer::start().await;

    // Recognized mapping (V5), healthy cluster, and a scoped index
    Mock::given(method("GET"))
      .and(path("/yente-entities/_mapping"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({
          "yente-entities": { "mappings": { "_source": { "excludes": ["name_symbols"] } } }
      })))
      .mount(&server)
      .await;

    Mock::given(method("GET"))
      .and(path("/_cluster/health/yente-entities"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "status": "green" })))
      .mount(&server)
      .await;

    Mock::given(method("GET"))
      .and(path("/yente-motiva-scoped-entities/_alias"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
      .mount(&server)
      .await;

    let provider = provider(&server);
    provider.refresh_index_state().await;

    assert!(provider.ready());
    assert_eq!(provider.index_version(), IndexVersion::V5);
    assert_eq!(provider.state.read().unwrap().scoped_index, Some("yente-motiva-scoped-entities".to_string()));
  }

  #[tokio::test]
  async fn refresh_index_state_not_ready() {
    let server = MockServer::start().await;

    // Missing index: the mapping probe returns 404, so we never reach the health
    // check and the provider stays not-ready.
    Mock::given(method("GET"))
      .and(path("/yente-entities/_mapping"))
      .respond_with(ResponseTemplate::new(404))
      .mount(&server)
      .await;

    let provider = provider(&server);
    provider.refresh_index_state().await;

    assert!(!provider.ready());
    assert_eq!(provider.state.read().unwrap().scoped_index, None);
  }
}
