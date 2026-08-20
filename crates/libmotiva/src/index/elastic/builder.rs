use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::index::elastic::config::EsOptions;
use crate::index::elastic::{DEFAULT_INDEX_PREFIX, IndexState, SCOPED_INDEX_SUFFIX};
use crate::{error::MotivaError, index::elastic::config::IndexVersion, prelude::ElasticsearchProvider};
use anyhow::Context;
use opensearch::cert::{Certificate, CertificateValidation};
use opensearch::http::Url;
use opensearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use opensearch::indices::IndicesGetAliasParts;
use opensearch::{OpenSearch, auth::Credentials};
use reqwest::StatusCode;

impl ElasticsearchProvider {
  pub async fn new<'o>(url: &str, options: EsOptions<'o>) -> Result<ElasticsearchProvider, MotivaError> {
    let es = {
      let parsed_url = Url::parse(url).context("invalid index URL")?;
      let transport_builder = TransportBuilder::new(SingleNodeConnectionPool::new(parsed_url));

      let transport = match options.tls {
        EsTlsVerification::Default => transport_builder,
        EsTlsVerification::SkipVerify => transport_builder.cert_validation(CertificateValidation::None),
        EsTlsVerification::CaCertChain(pem) => transport_builder.cert_validation(CertificateValidation::Full(Certificate::from_pem(pem)?)),
      };

      let transport = match options.auth {
        EsAuthMethod::Basic(username, password) => transport.auth(Credentials::Basic(username, password)),
        EsAuthMethod::Bearer(token) => transport.auth(Credentials::Bearer(token)),
        EsAuthMethod::ApiKey(client_id, client_secret) => transport.auth(Credentials::ApiKey(client_id, client_secret)),

        #[cfg(feature = "aws")]
        EsAuthMethod::AwsIam(service) => {
          use aws_config::{BehaviorVersion, meta::region::RegionProviderChain};

          let region = RegionProviderChain::default_provider().or_else("us-east-1");
          let iam = aws_config::defaults(BehaviorVersion::latest()).region(region).load().await.clone();
          let transport = transport.auth(iam.try_into()?);

          match service {
            AwsService::Service => transport.service_name("es"),
            AwsService::Serverless => transport.service_name("aoss"),
          }
        }

        _ => transport,
      };

      let transport = transport.build().context("could not build index client")?;

      OpenSearch::new(transport)
    };

    let index_prefix = options.index_name.unwrap_or_else(|| DEFAULT_INDEX_PREFIX.to_string());

    let provider = ElasticsearchProvider {
      es,
      index_prefix: index_prefix.clone(),
      main_index: format!("{}-entities", index_prefix),
      state: Arc::new(RwLock::new(IndexState {
        ready: false,
        index_version: IndexVersion::V4,
        scoped_index: None,
      })),
    };

    let _ = tokio::time::timeout(Duration::from_secs(5), provider.refresh_index_state()).await;

    Ok(provider)
  }

  /// Detect the scoped-entities alias, returning its name if it exists.
  pub(crate) async fn detect_scoped_index(&self) -> Option<String> {
    let alias = self
      .es
      .indices()
      .get_alias(IndicesGetAliasParts::Index(&[&self.scoped_alias_name()]))
      .send()
      .await
      .map(|resp| resp.status_code())
      .unwrap_or(StatusCode::NOT_FOUND);

    (alias == StatusCode::OK).then(|| self.scoped_alias_name())
  }

  pub fn scoped_alias_name(&self) -> String {
    format!("{}-{SCOPED_INDEX_SUFFIX}", self.index_prefix)
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

  #[cfg(feature = "aws")]
  /// AWS IAM
  AwsIam(AwsService),
}

#[cfg(feature = "aws")]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum AwsService {
  /// Amazon OpenSearch
  #[default]
  Service,
  /// Amazon OpenSearch Serverless
  Serverless,
}

/// TLS certificate method to use when using an HTTPS URL
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum EsTlsVerification {
  /// Use default TLS certificate validation
  #[default]
  Default,
  /// Skip server certificate verification
  SkipVerify,
  /// Validate certificate against a provided PEM CA certificate chain
  CaCertChain(Vec<u8>),
}

impl Default for &EsTlsVerification {
  fn default() -> Self {
    &EsTlsVerification::Default
  }
}

#[cfg(test)]
mod tests {
  use std::sync::{Arc, RwLock};

  use crate::index::elastic::builder::EsTlsVerification;
  use crate::index::elastic::config::EsOptions;
  use crate::{
    index::elastic::{IndexState, config::IndexVersion},
    prelude::{ElasticsearchProvider, EsAuthMethod},
  };
  use opensearch::OpenSearch;

  #[tokio::test]
  async fn es_builder() {
    let (u, p) = ("secret".to_string(), "secret".to_string());
    let cert = "-----BEGIN CERTIFICATE-----\nMFAwRgIBADADBgEAMAAwHhcNNTAwMTAxMDAwMDAwWhcNNDkxMjMxMjM1OTU5WjAAMBgwCwYJKoZIhvcNAQEBAwkAMAYCAQACAQAwAwYBAAMBAA==\n-----END CERTIFICATE-----";

    ElasticsearchProvider::new("http://url:9200", EsOptions { ..Default::default() }).await.unwrap();

    ElasticsearchProvider::new(
      "http://url:9200",
      EsOptions {
        auth: EsAuthMethod::Basic(u.clone(), p.clone()),
        ..Default::default()
      },
    )
    .await
    .unwrap();

    ElasticsearchProvider::new(
      "http://url:9200",
      EsOptions {
        auth: EsAuthMethod::Bearer(p.clone()),
        ..Default::default()
      },
    )
    .await
    .unwrap();

    ElasticsearchProvider::new(
      "http://url:9200",
      EsOptions {
        auth: EsAuthMethod::ApiKey(u.clone(), p.clone()),
        ..Default::default()
      },
    )
    .await
    .unwrap();

    ElasticsearchProvider::new(
      "https://url:9200",
      EsOptions {
        auth: EsAuthMethod::Basic(u.clone(), p.clone()),
        tls: &EsTlsVerification::SkipVerify,
        ..Default::default()
      },
    )
    .await
    .unwrap();

    ElasticsearchProvider::new(
      "https://url:9200",
      EsOptions {
        auth: EsAuthMethod::Basic(u.clone(), p.clone()),
        tls: &EsTlsVerification::CaCertChain(cert.as_bytes().to_vec()),
        ..Default::default()
      },
    )
    .await
    .unwrap();

    #[cfg(feature = "aws")]
    ElasticsearchProvider::new(
      "http://url:9200",
      EsOptions {
        auth: EsAuthMethod::AwsIam(super::AwsService::Serverless),
        ..Default::default()
      },
    )
    .await
    .unwrap();
  }

  #[tokio::test]
  async fn es_builder_default_index_name() {
    let provider = ElasticsearchProvider::new("http://url:9200", EsOptions { ..Default::default() }).await.unwrap();

    assert_eq!(provider.index_prefix, "yente");
    assert_eq!(provider.main_index, "yente-entities");
    assert_eq!(provider.state.read().unwrap().scoped_index, None);
  }

  #[tokio::test]
  async fn es_builder_custom_index_name() {
    let provider = ElasticsearchProvider::new(
      "http://url:9200",
      EsOptions {
        index_name: Some("custom".to_string()),
        ..Default::default()
      },
    )
    .await
    .unwrap();

    assert_eq!(provider.index_prefix, "custom");
    assert_eq!(provider.main_index, "custom-entities");
    assert_eq!(provider.state.read().unwrap().scoped_index, None);
  }

  fn provider_with_prefix(prefix: &str) -> ElasticsearchProvider {
    ElasticsearchProvider {
      es: OpenSearch::default(),
      index_prefix: prefix.to_string(),
      main_index: format!("{prefix}-entities"),
      state: Arc::new(RwLock::new(IndexState {
        ready: false,
        index_version: IndexVersion::V4,
        scoped_index: None,
      })),
    }
  }

  #[test]
  fn alias_names_use_prefix() {
    let provider = provider_with_prefix("mydata");

    assert_eq!(provider.main_index, "mydata-entities");
    assert_eq!(provider.scoped_alias_name(), "mydata-motiva-scoped-entities");
  }

  #[test]
  fn alias_names_default_prefix() {
    let provider = provider_with_prefix("yente");

    assert_eq!(provider.main_index, "yente-entities");
    assert_eq!(provider.scoped_alias_name(), "yente-motiva-scoped-entities");
  }
}
