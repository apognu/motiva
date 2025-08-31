use bumpalo::{
  Bump,
  collections::{CollectIn, Vec},
};
use itertools::Itertools;
use macros::scoring_feature;
use strsim::levenshtein;

use crate::{
  matching::{
    Feature,
    comparers::is_disjoint,
    extractors::{self},
  },
  model::{Entity, HasProperties, SearchEntity},
};

#[scoring_feature(OrgIdMismatch, name = "orgid_disjoint")]
fn score_feature(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  if !lhs.schema.is_a("Organization") || !rhs.schema.is_a("Organization") {
    return 0.0;
  }

  let lhs = lhs.gather(&["registrationNumber", "taxNumber", "leiCode", "innCode", "bicCode", "ogrnCode"]);

  if lhs.is_empty() {
    return 0.0;
  }

  let rhs = rhs.gather(&["registrationNumber", "taxNumber", "leiCode", "innCode", "bicCode", "orgnCode"]);

  if rhs.is_empty() {
    return 0.0;
  }

  let lhs = extractors::clean_names(lhs.iter()).collect_in::<Vec<_>>(bump);
  let rhs = extractors::clean_names(rhs.iter()).collect_in::<Vec<_>>(bump);

  if lhs.is_empty() || rhs.is_empty() {
    return 0.0;
  }

  if !is_disjoint(&lhs, &rhs) {
    return 0.0;
  }

  1.0
    - lhs
      .into_iter()
      .cartesian_product(rhs.iter())
      .map(|(lhs, rhs)| {
        let distance = levenshtein(&lhs, rhs) as f64;
        let ratio = 1.0 - (distance / lhs.len().max(rhs.len()) as f64);

        if ratio > 0.7 { ratio } else { 0.0 }
      })
      .max_by(f64::total_cmp)
      .unwrap_or(1.0)
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;
  use float_cmp::approx_eq;

  use crate::{
    model::{Entity, SearchEntity},
    tests::python::nomenklatura_comparer,
  };

  use super::Feature;

  #[test]
  fn orgid_disjoint() {
    let lhs = SearchEntity::builder("Organization").properties(&[("registrationNumber", &["FR12-34"])]).build();
    let rhs = Entity::builder("Organization").properties(&[("registrationNumber", &["FR-1234"])]).build();

    assert_eq!(super::OrgIdMismatch.score_feature(&Bump::new(), &lhs, &rhs), 0.0);

    let lhs = SearchEntity::builder("Organization").properties(&[("registrationNumber", &["FR12-34"])]).build();
    let rhs = Entity::builder("Organization").properties(&[("registrationNumber", &["UK-4321"])]).build();

    assert_eq!(super::OrgIdMismatch.score_feature(&Bump::new(), &lhs, &rhs), 1.0);

    let lhs = SearchEntity::builder("Company").properties(&[("registrationNumber", &["FR1234567890"])]).build();
    let rhs = Entity::builder("Organization").properties(&[("registrationNumber", &["FR-1134567-890"])]).build();

    assert!(approx_eq!(f64, super::OrgIdMismatch.score_feature(&Bump::new(), &lhs, &rhs), 0.08, epsilon = 0.01));
  }

  #[test]
  #[serial_test::serial]
  fn against_nomenklatura() {
    pyo3::prepare_freethreaded_python();

    let lhs = SearchEntity::builder("Company").properties(&[("registrationNumber", &["FR1234567890"])]).build();
    let rhs = Entity::builder("Organization").properties(&[("registrationNumber", &["FR-1134567-890"])]).build();

    let nscore = nomenklatura_comparer("name_based.misc", "orgid_disjoint", &lhs, &rhs).unwrap();

    assert!(approx_eq!(f64, nscore, super::OrgIdMismatch.score_feature(&Bump::new(), &lhs, &rhs), epsilon = 0.01));
  }
}
