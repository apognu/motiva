use bumpalo::{
  Bump,
  collections::{CollectIn, Vec},
};
use itertools::Itertools;
use macros::scoring_feature;

use crate::{
  matching::{
    Feature,
    comparers::is_disjoint,
    extractors::{self},
  },
  model::{Entity, HasProperties, SearchEntity},
};

#[scoring_feature(NameLiteralMatch, name = "name_literal_match")]
fn score_feature(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  let lhs_names = extractors::clean_literal_names(lhs.names_and_aliases().iter()).unique().collect_in::<Vec<_>>(bump);
  let rhs_names = extractors::clean_literal_names(rhs.names_and_aliases().iter()).unique().collect_in::<Vec<_>>(bump);

  if is_disjoint(&lhs_names, &rhs_names) { 0.0 } else { 1.0 }
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;

  use crate::tests::{e, se};

  use super::Feature;

  #[test]
  fn name_literal_match() {
    let lhs = se("Person").properties(&[("name", &["Donald Trump"]), ("alias", &["Orange man"])]).call();
    let rhs = e("Person").properties(&[("name", &["Donald Trump"]), ("alias", &["Orange man"])]).call();

    assert_eq!(super::NameLiteralMatch.score_feature(&Bump::new(), &lhs, &rhs), 1.0);

    let lhs = se("Person").properties(&[("name", &["Donald Trump"]), ("alias", &["Orange man"])]).call();
    let rhs = e("Person").properties(&[("name", &["Donald Duck"]), ("alias", &["POTUS"])]).call();

    assert_eq!(super::NameLiteralMatch.score_feature(&Bump::new(), &lhs, &rhs), 0.0);
  }
}
