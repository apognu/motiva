use elasticsearch::{Elasticsearch, auth::Credentials, http::transport::Transport};

use crate::{error::MotivaError, index::elastic::version::IndexVersion, prelude::ElasticsearchProvider};

impl ElasticsearchProvider {
  pub async fn new(url: &str, auth: EsAuthMethod, version: Option<IndexVersion>) -> Result<ElasticsearchProvider, MotivaError> {
    let es = {
      let transport = Transport::single_node(url)?;

      match auth {
        EsAuthMethod::Basic(username, password) => transport.set_auth(Credentials::Basic(username, password)),
        EsAuthMethod::Bearer(token) => transport.set_auth(Credentials::Bearer(token)),
        EsAuthMethod::ApiKey(client_id, client_secret) => transport.set_auth(Credentials::ApiKey(client_id, client_secret)),
        EsAuthMethod::EncodedApiKey(api_key) => transport.set_auth(Credentials::EncodedApiKey(api_key)),
        _ => {}
      }

      Elasticsearch::new(transport)
    };

    let mut provider = ElasticsearchProvider { es, index_version: IndexVersion::V4 };

    if version.is_none() {
      provider.index_version = provider.detect_index_version().await?;
    }

    Ok(provider)
  }
}

/// Authentication method to Elasticsearch
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum EsAuthMethod {
  /// Unauthenticated
  #[default]
  None,
  /// Basic authentication (username and password)
  Basic(String, String),
  /// Bearer token
  Bearer(String),
  /// API key (client ID and API key)
  ApiKey(String, String),
  /// API key
  EncodedApiKey(String),
}

#[cfg(test)]
mod tests {
  use crate::{
    index::elastic::version::IndexVersion,
    prelude::{ElasticsearchProvider, EsAuthMethod},
  };

  #[tokio::test]
  async fn es_builder() {
    let (u, p) = ("secret".to_string(), "secret".to_string());

    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::None, Some(IndexVersion::V4)).await.unwrap();
    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::Basic(u.clone(), p.clone()), Some(IndexVersion::V4))
      .await
      .unwrap();
    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::Bearer(p.clone()), Some(IndexVersion::V4)).await.unwrap();
    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::ApiKey(u.clone(), p.clone()), Some(IndexVersion::V4))
      .await
      .unwrap();
    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::EncodedApiKey(p.clone()), Some(IndexVersion::V4))
      .await
      .unwrap();
  }
}
