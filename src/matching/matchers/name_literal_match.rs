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

  if lhs_names.is_disjoint(&rhs_names) { 0.0 } else { 1.0 }
}

#[cfg(test)]
mod tests {
  use crate::tests::{e, se};

  use super::Feature;

  #[test]
  fn name_literal_match() {
    let lhs = se("Person").properties(&[("name", &["Donald Trump"]), ("alias", &["Orange man"])]).call();
    let rhs = e("Person").properties(&[("name", &["Donald Trump"]), ("alias", &["Orange man"])]).call();

    assert_eq!(super::NameLiteralMatch.score_feature(&lhs, &rhs), 1.0);

    let lhs = se("Person").properties(&[("name", &["Donald Trump"]), ("alias", &["Orange man"])]).call();
    let rhs = e("Person").properties(&[("name", &["Donald Duck"]), ("alias", &["POTUS"])]).call();

    assert_eq!(super::NameLiteralMatch.score_feature(&lhs, &rhs), 0.0);
  }
}
