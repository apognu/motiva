use std::collections::HashMap;

use ahash::RandomState;
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;
use validator::Validate;

use crate::model::{Entity, SearchEntity};

#[derive(Clone, Copy, Debug, Deserialize)]
pub enum Algorithm {
  #[serde(rename = "name-based")]
  NameBased,
  #[serde(rename = "name-qualified")]
  NameQualified,
  #[serde(rename = "logic-v1")]
  LogicV1,
}

#[serde_inline_default]
#[derive(Clone, Debug, Deserialize)]
pub struct MatchParams {
  #[serde(skip_deserializing)]
  pub scope: String,
  #[serde_inline_default(5)]
  pub limit: usize,
  #[serde_inline_default(0.7)]
  pub threshold: f64,
  #[serde_inline_default(0.5)]
  pub cutoff: f64,
  #[serde_inline_default(Algorithm::LogicV1)]
  pub algorithm: Algorithm,
  pub topics: Option<Vec<String>>,
  #[serde(default)]
  pub include_dataset: Vec<String>,
  #[serde(default)]
  pub exclude_dataset: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Validate)]
pub(super) struct Payload {
  #[validate(nested, length(min = 1, message = "at least one query must be provided"))]
  pub queries: HashMap<String, SearchEntity, RandomState>,
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
