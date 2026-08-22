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
          let iam = aws_config::defaults(BehaviorVersion::latest()).region(region).load().await;
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

  #[cfg(feature = "aws")]
  mod aws_sigv4 {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use aws_credential_types::Credentials;
    use aws_sigv4::http_request::{PayloadChecksumKind, SignableBody, SignableRequest, SigningParams, SigningSettings, sign};
    use aws_sigv4::sign::v4;
    use aws_smithy_runtime_api::client::identity::Identity;
    use jiff::civil::DateTime;
    use jiff::tz::TimeZone;
    use serde_json::json;
    use wiremock::{
      Mock, MockServer, Request, ResponseTemplate,
      matchers::{method, path},
    };

    use crate::index::elastic::builder::AwsService;
    use crate::index::elastic::config::EsOptions;
    use crate::prelude::{ElasticsearchProvider, EsAuthMethod};

    const ACCESS_KEY: &str = "AKIDEXAMPLE";
    const SECRET_KEY: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    const REGION: &str = "us-east-1";
    const EMPTY_PAYLOAD_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const SIGNED_HEADERS: &str = "accept;content-type;host;x-amz-content-sha256;x-amz-date";

    const AWS_ENV: [(&str, Option<&str>); 9] = [
      ("AWS_REGION", Some(REGION)),
      ("AWS_DEFAULT_REGION", Some(REGION)),
      ("AWS_ACCESS_KEY_ID", Some(ACCESS_KEY)),
      ("AWS_SECRET_ACCESS_KEY", Some(SECRET_KEY)),
      ("AWS_SESSION_TOKEN", None),
      ("AWS_PROFILE", None),
      ("AWS_EC2_METADATA_DISABLED", Some("true")),
      ("AWS_CONFIG_FILE", Some("/nonexistent/motiva/config")),
      ("AWS_SHARED_CREDENTIALS_FILE", Some("/nonexistent/motiva/credentials")),
    ];

    async fn request(service: AwsService) -> (String, Request) {
      let server = MockServer::start().await;

      Mock::given(method("GET"))
        .and(path("/yente-entities/_mapping"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "yente-entities": { "mappings": { "_source": { "excludes": ["name_keys"] } } }
        })))
        .mount(&server)
        .await;

      ElasticsearchProvider::new(
        &server.uri(),
        EsOptions {
          auth: EsAuthMethod::AwsIam(service),
          ..Default::default()
        },
      )
      .await
      .unwrap();

      let request = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .find(|request| request.url.path() == "/yente-entities/_mapping")
        .expect("no request reached the mock server, signing failed");

      (server.uri(), request)
    }

    fn header<'r>(request: &'r Request, name: &str) -> &'r str {
      request.headers.get(name).expect("missing header").to_str().unwrap()
    }

    fn field<'a>(authorization: &'a str, name: &str) -> &'a str {
      authorization
        .split(", ")
        .find_map(|part| part.trim_start_matches("AWS4-HMAC-SHA256 ").strip_prefix(&format!("{name}=")))
        .expect("missing authorization header field")
    }

    fn parse_amz_date(date: &str) -> SystemTime {
      let seconds = DateTime::strptime("%Y%m%dT%H%M%SZ", date)
        .unwrap_or_else(|err| panic!("x-amz-date {date} is not a valid timestamp: {err}"))
        .to_zoned(TimeZone::UTC)
        .unwrap()
        .timestamp()
        .as_second();

      UNIX_EPOCH + Duration::from_secs(seconds as u64)
    }

    fn expected_signature(base_url: &str, request: &Request, headers: &str, signed_at: SystemTime, service: &str) -> String {
      let identity = Identity::new(Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "motiva-test"), None);

      #[allow(clippy::field_reassign_with_default, reason = "SigningSettings is #[non_exhaustive]")]
      let settings = {
        let mut settings = SigningSettings::default();
        settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
        settings
      };

      let params = SigningParams::V4(
        v4::SigningParams::builder()
          .identity(&identity)
          .name(service)
          .region(REGION)
          .time(signed_at)
          .settings(settings)
          .build()
          .unwrap(),
      );

      let headers = headers.split(';').map(|name| (name, header(request, name))).collect::<Vec<_>>();
      let uri = format!("{base_url}{}", request.url.path());
      let signable = SignableRequest::new(request.method.as_str(), uri.as_str(), headers.into_iter(), SignableBody::Bytes(&[])).unwrap();

      sign(signable, &params).unwrap().into_parts().1
    }

    async fn assert_request_is_signed(service: AwsService, expected_service: &str) {
      let (base_url, request) = request(service).await;

      let authorization = header(&request, "authorization");
      let date = header(&request, "x-amz-date");
      let signed_at = parse_amz_date(date);
      let drift = SystemTime::now().duration_since(signed_at).unwrap_or_else(|err| err.duration());

      assert!(drift < Duration::from_secs(300), "x-amz-date {date} is {drift:?} away from now");

      assert!(authorization.starts_with("AWS4-HMAC-SHA256 "), "unexpected signature algorithm: {authorization}");
      assert_eq!(header(&request, "x-amz-content-sha256"), EMPTY_PAYLOAD_SHA256);
      assert_eq!(field(authorization, "Credential"), format!("{ACCESS_KEY}/{}/{REGION}/{expected_service}/aws4_request", &date[..8]));
      assert_eq!(field(authorization, "SignedHeaders"), SIGNED_HEADERS);

      assert_eq!(field(authorization, "Signature"), expected_signature(&base_url, &request, SIGNED_HEADERS, signed_at, expected_service));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn signs_requests_for_managed_service() {
      temp_env::async_with_vars(AWS_ENV, assert_request_is_signed(AwsService::Service, "es")).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn signs_requests_for_serverless() {
      temp_env::async_with_vars(AWS_ENV, assert_request_is_signed(AwsService::Serverless, "aoss")).await;
    }
  }
}
