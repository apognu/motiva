use std::{borrow::Cow, collections::HashMap};

use ahash::RandomState;
use libmotiva::prelude::*;
use serde::{Deserialize, Serialize, Serializer};
use serde_inline_default::serde_inline_default;
use validator::{Validate, ValidationError};

#[serde_inline_default]
#[derive(Clone, Debug, Deserialize)]
pub struct GetEntityParams {
  #[serde_inline_default(true)]
  pub nested: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, Validate)]
pub(crate) struct Payload {
  #[validate(nested)]
  pub queries: HashMap<String, SearchEntity, RandomState>,
  #[serde(default)]
  #[validate(custom(function = "validate_weights"))]
  pub weights: HashMap<String, f64>,

  // Some query parameters are duplicated in the request body to overcome URL size limitations
  #[serde(default, skip_serializing)]
  pub params: PayloadParams,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, Validate)]
pub(crate) struct PayloadParams {
  #[serde(default)]
  pub include_datasets: Option<Vec<String>>,
  #[serde(default)]
  pub exclude_datasets: Option<Vec<String>>,
  #[serde(default)]
  pub exclude_entity_ids: Option<Vec<String>>,
}

#[derive(Default, Serialize)]
pub(super) struct MatchResponse {
  pub responses: HashMap<String, MatchResults, RandomState>,
  pub limit: usize,
}

#[derive(Default, Serialize)]
pub(super) struct MatchResults {
  pub status: u16,
  pub results: Vec<MatchHit>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub total: Option<MatchTotal>,
}

#[derive(Default, Serialize)]
pub(super) struct MatchTotal {
  pub relation: &'static str,
  pub value: usize,
}

#[derive(Serialize)]
pub(super) struct MatchHit {
  #[serde(flatten)]
  pub entity: Entity,

  #[serde(rename = "match")]
  pub match_: bool,
  #[serde(serialize_with = "serialize_score")]
  pub score: f64,
}

#[derive(Serialize)]
pub struct Algorithms {
  pub algorithms: Vec<AlgorithmDescription>,
  pub best: &'static str,
  pub default: &'static str,
}

#[derive(Serialize)]
pub struct AlgorithmDescription {
  pub name: &'static str,
}

#[derive(Serialize)]
pub struct Version {
  pub motiva: String,
  pub index: String,
}

fn validate_weights(weights: &HashMap<String, f64>) -> Result<(), ValidationError> {
  for (k, v) in weights {
    if !(&-1.0..=&1.0).contains(&v) {
      return Err(ValidationError {
        message: Some(Cow::Owned(format!("weight value for {k} is outside [-1.0,1.0] ({v})"))),
        code: Cow::Borrowed(""),
        params: Default::default(),
      });
    }
  }

  Ok(())
}

#[cfg(test)]
mod tests {
  use std::collections::HashMap;

  #[test]
  fn validate_weights() {
    let mut weights = HashMap::new();
    weights.insert("valid".into(), 0.4);
    weights.insert("invalid".into(), -1.2);

    assert!(super::validate_weights(&weights).is_err());

    weights.clear();
    weights.insert("valid1".into(), 0.4);
    weights.insert("valid2".into(), -0.7);

    assert!(super::validate_weights(&weights).is_ok());
  }
}

fn serialize_score<S>(score: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
  S: Serializer,
{
  serializer.serialize_f64(format_score(*score))
}
