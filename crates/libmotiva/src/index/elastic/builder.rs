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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EsAuthMethod {
  None,
  Basic(String, String),
  Bearer(String),
  ApiKey(String, String),
  EncodedApiKey(String),
}
