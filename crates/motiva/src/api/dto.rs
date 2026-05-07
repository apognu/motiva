use std::collections::HashMap;

use ahash::RandomState;
use libmotiva::prelude::*;
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;
use validator::Validate;

#[serde_inline_default]
#[derive(Clone, Debug, Deserialize)]
pub struct GetEntityParams {
  #[serde_inline_default(true)]
  pub nested: bool,
}

#[derive(Clone, Debug, Deserialize, Validate)]
pub(crate) struct Payload {
  #[validate(nested, length(min = 1, message = "at least one query must be provided"))]
  pub queries: HashMap<String, SearchEntity, RandomState>,

  // Some query parameters are duplicated in the request body to overcome URL size limitations
  #[serde(default)]
  pub params: PayloadParams,
}

#[derive(Clone, Debug, Default, Deserialize, Validate)]
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
