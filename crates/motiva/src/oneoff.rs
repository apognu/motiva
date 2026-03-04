use std::env;

use libmotiva::ElasticsearchProvider;
use serde_json::json;

pub async fn create_scoped_index(provider: &ElasticsearchProvider) -> Result<(), anyhow::Error> {
  let mut query = json!({
    "bool": {
      "must": [
        { "terms": { "schema": ["Person", "LegalEntity", "Organization", "Company", "Airplane", "Vessel"] } },
        { "term": { "topics": "sanction" } }
      ]
    }
  });

  if let Ok(custom) = env::var("SCOPED_INDEX_QUERY") {
    query = serde_json::from_str(&custom)?;
  }

  let result = libmotiva::create_scoped_index(provider, query).await;

  if let Err(ref err) = result {
    tracing::error!(error = ?err, "encountered error while creating scoped index");
  }

  result
}
