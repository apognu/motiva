use crate::{error::MotivaError, index::elastic::version::IndexVersion, prelude::ElasticsearchProvider};
use anyhow::Context;
use elasticsearch::cert::{Certificate, CertificateValidation};
use elasticsearch::http::Url;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::{Elasticsearch, auth::Credentials, http::transport::Transport};
use std::fs;

impl ElasticsearchProvider {
  pub async fn new(url: &str, auth: EsAuthMethod, version: Option<IndexVersion>, tls_option: EsTLSOption) -> Result<ElasticsearchProvider, MotivaError> {
    let es = {
      let parsed_url = Url::parse(url).unwrap();
      let transport_builder = TransportBuilder::new(
        SingleNodeConnectionPool::new(parsed_url)
      );
      let transport = match tls_option {
        EsTLSOption::None => {
          Transport::single_node(url)?
        }
        EsTLSOption::SkipVerify => {
          transport_builder
              .cert_validation(CertificateValidation::None)
              .build()
              .context("could not build single node connection pool with SkipVerify ssl option")?
        }
        EsTLSOption::CAFilePath(ca_path) => {
          let pem = fs::read(ca_path.clone()).context(format!("could not read root CA from {}", ca_path))?;
          let cert = Certificate::from_pem(&pem).context("invalid CA certificate")?;
          let cert_validation = CertificateValidation::Full(cert);
          transport_builder
              .cert_validation(cert_validation)
              .build()
              .context("could not build single node connection pool with CAFilePath ssl option")?
        }
      };

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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum EsTLSOption {
  // Not using TLS
  #[default]
  None,
  // Skip server certificate verification
  SkipVerify,
  // Inject server CA.crt from file path
  CAFilePath(String),
}

#[cfg(test)]
mod tests {
  use crate::{
    index::elastic::version::IndexVersion,
    prelude::{ElasticsearchProvider, EsAuthMethod},
  };
  use crate::index::elastic::builder::{EsTLSOption};

  #[tokio::test]
  async fn es_builder() {
    let (u, p, ca_path) = ("secret".to_string(), "secret".to_string(), "/etc/ssl/ca.crt".to_string());

    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::None, Some(IndexVersion::V4), EsTLSOption::None).await.unwrap();
    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::Basic(u.clone(), p.clone()), Some(IndexVersion::V4), EsTLSOption::None)
      .await
      .unwrap();
    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::Bearer(p.clone()), Some(IndexVersion::V4), EsTLSOption::None).await.unwrap();
    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::ApiKey(u.clone(), p.clone()), Some(IndexVersion::V4), EsTLSOption::None)
      .await
      .unwrap();
    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::EncodedApiKey(p.clone()), Some(IndexVersion::V4), EsTLSOption::None)
      .await
      .unwrap();
    ElasticsearchProvider::new("https://url:9200", EsAuthMethod::Basic(u.clone(), p.clone()), None, EsTLSOption::SkipVerify)
        .await
        .unwrap();
    ElasticsearchProvider::new("https://url:9200", EsAuthMethod::Basic(u.clone(), p.clone()), None, EsTLSOption::CAFilePath(ca_path.clone()))
        .await
        .unwrap();
  }
}
