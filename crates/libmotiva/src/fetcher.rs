use std::{collections::HashMap, fs::File};

use anyhow::Context;
use reqwest::header;

use crate::{
  Catalog,
  catalog::{Manifest, OPENSANCTIONS_CATALOG_URL},
};

pub trait CatalogFetcher: Clone + Default + Send + Sync + 'static {
  fn fetch_manifest(&self) -> impl Future<Output = anyhow::Result<Manifest>> + Send;
  fn fetch_catalog(&self, url: &str, auth_token: Option<&str>) -> impl Future<Output = anyhow::Result<Catalog>> + Send;
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum ManifestProtocol {
  #[default]
  Http,
  LocalFile,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum ManifestFormat {
  #[default]
  Json,
  Yaml,
}

#[derive(Clone, Default)]
pub struct HttpCatalogFetcher {
  pub manifest_url: Option<String>,
  pub protocol: ManifestProtocol,
  pub format: ManifestFormat,
}

impl HttpCatalogFetcher {
  pub fn from_manifest_url(url: Option<String>) -> anyhow::Result<Self> {
    let (protocol, format) = match &url {
      Some(url) => {
        let protocol = if url.starts_with("http://") || url.starts_with("https://") {
          ManifestProtocol::Http
        } else {
          ManifestProtocol::LocalFile
        };

        let format = if url.ends_with(".json") {
          ManifestFormat::Json
        } else if url.ends_with(".yaml") || url.ends_with(".yml") {
          ManifestFormat::Yaml
        } else {
          anyhow::bail!("unsupported file format for manifest file");
        };

        (protocol, format)
      }

      None => (ManifestProtocol::Http, ManifestFormat::Json),
    };

    Ok(Self { protocol, format, manifest_url: url })
  }
}

impl CatalogFetcher for HttpCatalogFetcher {
  async fn fetch_manifest(&self) -> anyhow::Result<Manifest> {
    match &self.manifest_url {
      Some(url) => match self.protocol {
        ManifestProtocol::Http => self.fetch_http(url).await,
        ManifestProtocol::LocalFile => self.fetch_local_file(url).await,
      },

      None => Ok(Manifest::default()),
    }
  }

  async fn fetch_catalog(&self, url: &str, auth_token: Option<&str>) -> anyhow::Result<Catalog> {
    let client = reqwest::Client::default();

    match auth_token {
      Some(token) => Ok(client.get(url).header(header::AUTHORIZATION, format!("Token {token}")).send().await?.json::<Catalog>().await?),
      None => Ok(client.get(url).send().await?.json::<Catalog>().await?),
    }
  }
}

impl HttpCatalogFetcher {
  async fn fetch_http(&self, url: &str) -> anyhow::Result<Manifest> {
    let response = reqwest::get(url).await.context("could not reach manifest location")?;

    match self.format {
      ManifestFormat::Json => response.json().await.context("invalid manifest file"),
      ManifestFormat::Yaml => {
        tracing::warn!("using a YAML manifest is deprecated, support will be removed in a future version, use JSON instead");

        serde_yaml::from_str(&response.text().await?).context("could not parse local manifest file as YAML")
      }
    }
  }

  async fn fetch_local_file(&self, url: &str) -> anyhow::Result<Manifest> {
    let file = File::open(url)?;

    match self.format {
      ManifestFormat::Json => serde_json::from_reader(file).context("could not parse local manifest file as JSON"),
      ManifestFormat::Yaml => {
        tracing::warn!("using a YAML manifest is deprecated, support will be removed in a future version, use JSON instead");

        serde_yaml::from_reader(file).context("could not parse local manifest file as YAML")
      }
    }
  }
}

#[derive(Clone)]
pub struct TestFetcher {
  pub manifest: Manifest,
  pub catalogs: HashMap<String, Catalog>,
}

impl Default for TestFetcher {
  fn default() -> Self {
    let catalogs = {
      let mut m = HashMap::default();

      m.insert(OPENSANCTIONS_CATALOG_URL.to_string(), Catalog::default());
      m
    };

    Self {
      manifest: Manifest::default(),
      catalogs,
    }
  }
}

impl CatalogFetcher for TestFetcher {
  async fn fetch_manifest(&self) -> anyhow::Result<Manifest> {
    Ok(self.manifest.clone())
  }

  async fn fetch_catalog(&self, url: &str, _: Option<&str>) -> anyhow::Result<Catalog> {
    self.catalogs.get(url).ok_or_else(|| anyhow::anyhow!("unknown catalog url")).cloned()
  }
}

#[cfg(test)]
mod tests {
  use wiremock::{Mock, MockServer, ResponseTemplate, matchers::*};

  use crate::{
    CatalogFetcher, HttpCatalogFetcher,
    catalog::OPENSANCTIONS_CATALOG_URL,
    fetcher::{ManifestFormat, ManifestProtocol},
  };

  #[test]
  fn detect_protocol_and_format() {
    let tests = &[
      ("http://domain.tld/manifest.json", ManifestProtocol::Http, ManifestFormat::Json),
      ("http://domain.tld/manifest.yaml", ManifestProtocol::Http, ManifestFormat::Yaml),
      ("http://domain.tld/manifest.yml", ManifestProtocol::Http, ManifestFormat::Yaml),
      ("manifest.json", ManifestProtocol::LocalFile, ManifestFormat::Json),
      ("/path/manifest.json", ManifestProtocol::LocalFile, ManifestFormat::Json),
    ];

    for (url, protocol, format) in tests {
      let fetcher = HttpCatalogFetcher::from_manifest_url(Some(url.to_string())).unwrap();

      assert_eq!(fetcher.protocol, *protocol);
      assert_eq!(fetcher.format, *format);
    }
  }

  #[tokio::test]
  async fn default_manifest() {
    let manifest = HttpCatalogFetcher::from_manifest_url(None).unwrap().fetch_manifest().await.unwrap();

    assert_eq!(manifest.catalogs[0].url, OPENSANCTIONS_CATALOG_URL);
    assert_eq!(manifest.catalogs[0].scope.as_deref(), Some("default"));
    assert_eq!(manifest.catalogs[0].resource_name.as_deref(), Some("entities.ftm.json"));
  }

  #[test]
  fn invalid_extension() {
    assert!(HttpCatalogFetcher::from_manifest_url(Some("http://domain.tld/manifest.jpg".to_string())).is_err());
  }

  #[tokio::test]
  async fn valid_local_file_json() {
    std::fs::write(
      "/tmp/motiva-manifest.json",
      r#"{"catalogs":[{"url": "http://myurl.tld","scope":"myscope","resource_name":"ents.json"}]}"#,
    )
    .unwrap();

    let manifest = HttpCatalogFetcher::from_manifest_url(Some("/tmp/motiva-manifest.json".to_string()))
      .unwrap()
      .fetch_manifest()
      .await
      .unwrap();

    assert_eq!(manifest.catalogs[0].url, "http://myurl.tld");
    assert_eq!(manifest.catalogs[0].scope.as_deref(), Some("myscope"));
    assert_eq!(manifest.catalogs[0].resource_name.as_deref(), Some("ents.json"));
  }

  #[tokio::test]
  async fn valid_local_file_yaml() {
    for ext in ["yml", "yaml"] {
      std::fs::write(
        &format!("/tmp/motiva-manifest.{ext}"),
        r#"
      catalogs:
        - url: http://myurl.tld
          scope: myscope
          resource_name: ents.json
      "#,
      )
      .unwrap();

      let manifest = HttpCatalogFetcher::from_manifest_url(Some("/tmp/motiva-manifest.yml".to_string()))
        .unwrap()
        .fetch_manifest()
        .await
        .unwrap();

      assert_eq!(manifest.catalogs[0].url, "http://myurl.tld");
      assert_eq!(manifest.catalogs[0].scope.as_deref(), Some("myscope"));
      assert_eq!(manifest.catalogs[0].resource_name.as_deref(), Some("ents.json"));
    }
  }

  #[tokio::test]
  async fn valid_http_json() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
      .and(path("/manifest.json"))
      .respond_with(ResponseTemplate::new(200).set_body_raw(r#"{"catalogs":[{"url": "http://myurl.tld","scope":"myscope","resource_name":"ents.json"}]}"#, "application/json"))
      .mount(&mock)
      .await;

    let manifest = HttpCatalogFetcher::from_manifest_url(Some(format!("{}/manifest.json", mock.uri())))
      .unwrap()
      .fetch_manifest()
      .await
      .unwrap();

    assert_eq!(manifest.catalogs[0].url, "http://myurl.tld");
    assert_eq!(manifest.catalogs[0].scope.as_deref(), Some("myscope"));
    assert_eq!(manifest.catalogs[0].resource_name.as_deref(), Some("ents.json"));
  }

  #[tokio::test]
  async fn valid_authed_http_json() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
      .and(path("/catalog.json"))
      .and(header("authorization", "Token helloworld"))
      .respond_with(ResponseTemplate::new(200).set_body_raw(
        r#"{"datasets": [
          {
            "name": "thecatalog",
            "title": "The Catalog",
            "version": "20260110175500-abc"
          }
        ]}"#,
        "application/json",
      ))
      .mount(&mock)
      .await;

    let catalog = HttpCatalogFetcher::from_manifest_url(Some(format!("{}/manifest.json", mock.uri())))
      .unwrap()
      .fetch_catalog(&format!("{}/catalog.json", mock.uri()), Some("helloworld"))
      .await
      .unwrap();

    assert_eq!(catalog.datasets.len(), 1);
    assert_eq!(catalog.datasets[0].name, "thecatalog");
  }

  #[tokio::test]
  async fn valid_http_yaml() {
    let mock = MockServer::start().await;

    Mock::given(method("GET"))
      .and(path("/manifest.yml"))
      .respond_with(ResponseTemplate::new(200).set_body_raw(
        r#"
        catalogs:
          - url: http://myurl.tld
            scope: myscope
            resource_name: ents.json
        "#,
        "application/json",
      ))
      .mount(&mock)
      .await;

    let manifest = HttpCatalogFetcher::from_manifest_url(Some(format!("{}/manifest.yml", mock.uri())))
      .unwrap()
      .fetch_manifest()
      .await
      .unwrap();

    assert_eq!(manifest.catalogs[0].url, "http://myurl.tld");
    assert_eq!(manifest.catalogs[0].scope.as_deref(), Some("myscope"));
    assert_eq!(manifest.catalogs[0].resource_name.as_deref(), Some("ents.json"));
  }
}
