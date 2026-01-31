use crate::{error::MotivaError, index::elastic::version::IndexVersion, prelude::ElasticsearchProvider};
use anyhow::Context;
use elasticsearch::cert::{Certificate, CertificateValidation};
use elasticsearch::http::Url;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::{Elasticsearch, auth::Credentials};

impl ElasticsearchProvider {
  pub async fn new(url: &str, auth: EsAuthMethod, tls_option: &EsTlsVerification, version: Option<IndexVersion>) -> Result<ElasticsearchProvider, MotivaError> {
    let es = {
      let parsed_url = Url::parse(url).context("invalid index URL")?;
      let transport_builder = TransportBuilder::new(SingleNodeConnectionPool::new(parsed_url));

      let transport = match tls_option {
        EsTlsVerification::Default => transport_builder,
        EsTlsVerification::SkipVerify => transport_builder.cert_validation(CertificateValidation::None),
        EsTlsVerification::CaCertChain(pem) => transport_builder.cert_validation(CertificateValidation::Full(Certificate::from_pem(pem)?)),
      };

      let transport = transport.build().context("could not build index client")?;

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

#[cfg(test)]
mod tests {
  use crate::index::elastic::builder::EsTlsVerification;
  use crate::{
    index::elastic::version::IndexVersion,
    prelude::{ElasticsearchProvider, EsAuthMethod},
  };

  #[tokio::test]
  async fn es_builder() {
    let (u, p) = ("secret".to_string(), "secret".to_string());
    let cert = "-----BEGIN CERTIFICATE-----\nMFAwRgIBADADBgEAMAAwHhcNNTAwMTAxMDAwMDAwWhcNNDkxMjMxMjM1OTU5WjAAMBgwCwYJKoZIhvcNAQEBAwkAMAYCAQACAQAwAwYBAAMBAA==\n-----END CERTIFICATE-----";

    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::None, &EsTlsVerification::Default, Some(IndexVersion::V4))
      .await
      .unwrap();
    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::Basic(u.clone(), p.clone()), &EsTlsVerification::Default, Some(IndexVersion::V4))
      .await
      .unwrap();
    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::Bearer(p.clone()), &EsTlsVerification::Default, Some(IndexVersion::V4))
      .await
      .unwrap();
    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::ApiKey(u.clone(), p.clone()), &EsTlsVerification::Default, Some(IndexVersion::V4))
      .await
      .unwrap();
    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::EncodedApiKey(p.clone()), &EsTlsVerification::Default, Some(IndexVersion::V4))
      .await
      .unwrap();
    ElasticsearchProvider::new("https://url:9200", EsAuthMethod::Basic(u.clone(), p.clone()), &EsTlsVerification::SkipVerify, Some(IndexVersion::V4))
      .await
      .unwrap();
    ElasticsearchProvider::new(
      "https://url:9200",
      EsAuthMethod::Basic(u.clone(), p.clone()),
      &EsTlsVerification::CaCertChain(cert.as_bytes().to_vec()),
      Some(IndexVersion::V4),
    )
    .await
    .unwrap();
  }
}
