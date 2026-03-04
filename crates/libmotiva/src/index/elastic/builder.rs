use crate::index::elastic::config::EsOptions;
use crate::index::elastic::{DEFAULT_INDEX_PREFIX, SCOPED_INDEX_SUFFIX};
use crate::{error::MotivaError, index::elastic::config::IndexVersion, prelude::ElasticsearchProvider};
use anyhow::Context;
use elasticsearch::cert::{Certificate, CertificateValidation};
use elasticsearch::http::Url;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::indices::IndicesGetAliasParts;
use elasticsearch::{Elasticsearch, auth::Credentials};
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

      let transport = transport.build().context("could not build index client")?;

      match options.auth {
        EsAuthMethod::Basic(username, password) => transport.set_auth(Credentials::Basic(username, password)),
        EsAuthMethod::Bearer(token) => transport.set_auth(Credentials::Bearer(token)),
        EsAuthMethod::ApiKey(client_id, client_secret) => transport.set_auth(Credentials::ApiKey(client_id, client_secret)),
        EsAuthMethod::EncodedApiKey(api_key) => transport.set_auth(Credentials::EncodedApiKey(api_key)),
        _ => {}
      }

      Elasticsearch::new(transport)
    };

    let index_prefix = options.index_name.unwrap_or_else(|| DEFAULT_INDEX_PREFIX.to_string());

    let mut provider = ElasticsearchProvider {
      es,
      index_prefix: index_prefix.clone(),
      index_version: IndexVersion::V4,
      main_index: format!("{}-entities", index_prefix),
      scoped_index: None,
    };

    if options.index_version.is_none() {
      provider.index_version = provider.detect_index_version().await?;
    }

    provider.detect_index().await;

    Ok(provider)
  }

  async fn detect_index(&mut self) {
    let alias = self
      .es
      .indices()
      .get_alias(IndicesGetAliasParts::Index(&[&self.scoped_alias_name()]))
      .send()
      .await
      .map(|resp| resp.status_code())
      .unwrap_or(StatusCode::NOT_FOUND);

    if alias == StatusCode::OK {
      self.scoped_index = Some(self.scoped_alias_name());
    }
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
  /// API key
  EncodedApiKey(String),
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
  use crate::index::elastic::builder::EsTlsVerification;
  use crate::index::elastic::config::EsOptions;
  use crate::{
    index::elastic::config::IndexVersion,
    prelude::{ElasticsearchProvider, EsAuthMethod},
  };
  use elasticsearch::Elasticsearch;

  #[tokio::test]
  async fn es_builder() {
    let (u, p) = ("secret".to_string(), "secret".to_string());
    let cert = "-----BEGIN CERTIFICATE-----\nMFAwRgIBADADBgEAMAAwHhcNNTAwMTAxMDAwMDAwWhcNNDkxMjMxMjM1OTU5WjAAMBgwCwYJKoZIhvcNAQEBAwkAMAYCAQACAQAwAwYBAAMBAA==\n-----END CERTIFICATE-----";

    ElasticsearchProvider::new(
      "http://url:9200",
      EsOptions {
        index_version: Some(IndexVersion::V4),
        ..Default::default()
      },
    )
    .await
    .unwrap();

    ElasticsearchProvider::new(
      "http://url:9200",
      EsOptions {
        auth: EsAuthMethod::Basic(u.clone(), p.clone()),
        index_version: Some(IndexVersion::V4),
        ..Default::default()
      },
    )
    .await
    .unwrap();

    ElasticsearchProvider::new(
      "http://url:9200",
      EsOptions {
        auth: EsAuthMethod::Bearer(p.clone()),
        index_version: Some(IndexVersion::V4),
        ..Default::default()
      },
    )
    .await
    .unwrap();

    ElasticsearchProvider::new(
      "http://url:9200",
      EsOptions {
        auth: EsAuthMethod::ApiKey(u.clone(), p.clone()),
        index_version: Some(IndexVersion::V4),
        ..Default::default()
      },
    )
    .await
    .unwrap();

    ElasticsearchProvider::new(
      "http://url:9200",
      EsOptions {
        auth: EsAuthMethod::EncodedApiKey(p.clone()),
        index_version: Some(IndexVersion::V4),
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
        index_version: Some(IndexVersion::V4),
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
        index_version: Some(IndexVersion::V4),
        ..Default::default()
      },
    )
    .await
    .unwrap();
  }

  #[tokio::test]
  async fn es_builder_default_index_name() {
    let provider = ElasticsearchProvider::new(
      "http://url:9200",
      EsOptions {
        index_version: Some(IndexVersion::V4),
        ..Default::default()
      },
    )
    .await
    .unwrap();

    assert_eq!(provider.index_prefix, "yente");
    assert_eq!(provider.main_index, "yente-entities");
    assert_eq!(provider.scoped_index, None);
  }

  #[tokio::test]
  async fn es_builder_custom_index_name() {
    let provider = ElasticsearchProvider::new(
      "http://url:9200",
      EsOptions {
        index_name: Some("custom".to_string()),
        index_version: Some(IndexVersion::V4),
        ..Default::default()
      },
    )
    .await
    .unwrap();

    assert_eq!(provider.index_prefix, "custom");
    assert_eq!(provider.main_index, "custom-entities");
    assert_eq!(provider.scoped_index, None);
  }

  #[test]
  fn alias_names_use_prefix() {
    let provider = ElasticsearchProvider {
      es: Elasticsearch::default(),
      index_version: IndexVersion::V4,
      index_prefix: "mydata".to_string(),
      main_index: "mydata-entities".to_string(),
      scoped_index: None,
    };

    assert_eq!(provider.main_index, "mydata-entities");
    assert_eq!(provider.scoped_alias_name(), "mydata-motiva-scoped-entities");
  }

  #[test]
  fn alias_names_default_prefix() {
    let provider = ElasticsearchProvider {
      es: Elasticsearch::default(),
      index_version: IndexVersion::V4,
      index_prefix: "yente".to_string(),
      main_index: "yente-entities".to_string(),
      scoped_index: None,
    };

    assert_eq!(provider.main_index, "yente-entities");
    assert_eq!(provider.scoped_alias_name(), "yente-motiva-scoped-entities");
  }
}
