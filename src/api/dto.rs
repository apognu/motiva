use std::collections::HashMap;

use serde::{Deserialize, Serialize};
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

#[derive(Clone, Debug, Deserialize)]
pub struct MatchParams {
  pub limit: Option<usize>,
  pub threshold: Option<f64>,
  pub cutoff: Option<f64>,
  pub algorithm: Option<Algorithm>,
  pub topics: Option<String>,
  pub include_dataset: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, Validate)]
pub(super) struct Payload {
  #[validate(nested, length(min = 1, message = "at least one query must be provided"))]
  pub queries: HashMap<String, SearchEntity>,
}

#[derive(Default, Serialize)]
pub(super) struct MatchResponse {
  pub responses: HashMap<String, MatchResults>,
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
