use std::collections::HashSet;

use itertools::Itertools;
use macros::scoring_feature;
use strsim::levenshtein;

use crate::{
  matching::{Feature, extractors},
  model::{Entity, HasProperties, SearchEntity},
};

#[scoring_feature(OrgIdMismatch, name = "orgid_disjoint")]
fn score_feature(&self, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  if !lhs.schema.is_a("Organization") || !rhs.schema.is_a("Organization") {
    return 0.0;
  }

  let lhs = lhs.gather(&["registrationNumber", "taxNumber"]);

  if lhs.is_empty() {
    return 0.0;
  }

  let rhs = rhs.gather(&["registrationNumber", "taxNumber"]);

  if rhs.is_empty() {
    return 0.0;
  }

  let lhs = extractors::clean_names(lhs.iter()).collect::<Vec<_>>();
  let rhs = extractors::clean_names(rhs.iter()).collect::<Vec<_>>();

  if lhs.is_empty() || rhs.is_empty() {
    return 0.0;
  }

  if !lhs.iter().collect::<HashSet<_>>().is_disjoint(&rhs.iter().collect()) {
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
      .max_by(|lhs, rhs| lhs.total_cmp(rhs))
      .unwrap_or(1.0)
}

#[cfg(test)]
mod tests {
  use float_cmp::approx_eq;

  use crate::tests::{e, python::nomenklatura_comparer, se};

  use super::Feature;

  #[test]
  fn orgid_disjoint() {
    let lhs = se("Organization").properties(&[("registrationNumber", &["FR12-34"])]).call();
    let rhs = e("Organization").properties(&[("registrationNumber", &["FR-1234"])]).call();

    assert_eq!(super::OrgIdMismatch.score_feature(&lhs, &rhs), 0.0);

    let lhs = se("Organization").properties(&[("registrationNumber", &["FR12-34"])]).call();
    let rhs = e("Organization").properties(&[("registrationNumber", &["UK-4321"])]).call();

    assert_eq!(super::OrgIdMismatch.score_feature(&lhs, &rhs), 1.0);

    let lhs = se("Company").properties(&[("registrationNumber", &["FR1234567890"])]).call();
    let rhs = e("Organization").properties(&[("registrationNumber", &["FR-1134567-890"])]).call();

    assert!(approx_eq!(f64, super::OrgIdMismatch.score_feature(&lhs, &rhs), 0.08, epsilon = 0.01));
  }

  #[test]
  #[serial_test::serial]
  fn against_nomenklatura() {
    pyo3::prepare_freethreaded_python();

    let lhs = se("Company").properties(&[("registrationNumber", &["FR1234567890"])]).call();
    let rhs = e("Organization").properties(&[("registrationNumber", &["FR-1134567-890"])]).call();

    let nscore = nomenklatura_comparer("name_based.misc", "orgid_disjoint", &lhs, &rhs).unwrap();

    assert!(approx_eq!(f64, nscore, super::OrgIdMismatch.score_feature(&lhs, &rhs), epsilon = 0.01));
  }
}
