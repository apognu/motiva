use std::{env, io::Write};

use jiff::Timestamp;
use libmotiva::ElasticsearchProvider;
use serde_json::json;

pub fn version() -> Result<(), anyhow::Error> {
  use crate::build::*;
  use crate::git_version;

  let out = std::io::stdout();
  let mut out = out.lock();

  let features = match CARGO_FEATURES {
    "" => "(none)",
    features => features,
  };

  let build_time = Timestamp::from_second(BUILD_TIMESTAMP).unwrap().strftime("%Y-%m-%d");

  writeln!(out, "motiva {} ({}) © 2025 {}", git_version(), build_time, env!("CARGO_PKG_AUTHORS"))?;
  writeln!(out)?;
  writeln!(out, "Repository: {}", env!("CARGO_PKG_REPOSITORY"))?;
  writeln!(out, "Compiled features: {features}")?;

  Ok(())
}

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
