use std::collections::HashSet;

use macros::scoring_feature;

use crate::{
  matching::{Feature, extractors},
  model::{Entity, HasProperties, SearchEntity},
};

#[scoring_feature(NameLiteralMatch, name = "name_literal_match")]
fn score_feature(&self, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  let lhs_names = extractors::clean_names(lhs.names_and_aliases().iter()).collect::<HashSet<_>>();
  let rhs_names = extractors::clean_names(rhs.names_and_aliases().iter()).collect::<HashSet<_>>();

  if !lhs_names.is_disjoint(&rhs_names) { 1.0 } else { 0.0 }
}
