use elasticsearch::{Elasticsearch, auth::Credentials, http::transport::Transport};

use crate::{error::MotivaError, prelude::ElasticsearchProvider};

impl ElasticsearchProvider {
  pub fn new(url: &str, auth: EsAuthMethod) -> Result<ElasticsearchProvider, MotivaError> {
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

    Ok(ElasticsearchProvider { es })
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
  use crate::prelude::{ElasticsearchProvider, EsAuthMethod};

  #[test]
  fn es_builder() {
    let (u, p) = ("secret".to_string(), "secret".to_string());

    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::None).unwrap();
    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::Basic(u.clone(), p.clone())).unwrap();
    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::Bearer(p.clone())).unwrap();
    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::ApiKey(u.clone(), p.clone())).unwrap();
    ElasticsearchProvider::new("http://url:9200", EsAuthMethod::EncodedApiKey(p.clone())).unwrap();
  }
}
