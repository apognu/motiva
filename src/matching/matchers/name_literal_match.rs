use macros::scoring_feature;

use crate::{
  matching::{Feature, utils},
  model::{Entity, HasProperties, SearchEntity},
};

#[scoring_feature(NameLiteralMatch, name = "name_literal_match")]
fn score_feature(&self, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  let lhs_names = utils::clean_names(&lhs.names_and_aliases());
  let rhs_names = utils::clean_names(&rhs.names_and_aliases());

  if !lhs_names.is_disjoint(&rhs_names) { 1.0 } else { 0.0 }
}
