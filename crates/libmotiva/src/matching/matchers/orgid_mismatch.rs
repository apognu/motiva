use bumpalo::{
  Bump,
  collections::{CollectIn, Vec},
};
use compact_str::CompactString;
use itertools::Itertools;
use libmotiva_macros::scoring_feature;
use strsim::levenshtein;

use crate::{
  matching::{
    Detail, Feature, ScoreResult,
    comparers::is_disjoint,
    extractors::{self},
    matchers::NO_DATA,
  },
  model::{Entity, HasProperties, SearchEntity, format_score},
};

#[scoring_feature(OrgIdMismatch, name = "orgid_disjoint")]
fn score(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity, explain: bool) -> ScoreResult {
  if !lhs.schema.is_a("Organization") || !rhs.schema.is_a("Organization") {
    return (0.0, explain.then_some(Detail::Note("not an organization"))).into();
  }

  let lhs = lhs.props(&["registrationNumber", "taxNumber", "leiCode", "innCode", "bicCode", "ogrnCode"]);

  if lhs.is_empty() {
    return (0.0, explain.then_some(Detail::Note("no organization identifiers to compare"))).into();
  }

  let rhs = rhs.props(&["registrationNumber", "taxNumber", "leiCode", "innCode", "bicCode", "orgnCode"]);

  if rhs.is_empty() {
    return (0.0, explain.then_some(Detail::Note("no organization identifiers to compare"))).into();
  }

  let lhs = extractors::normalize_identifiers(lhs.iter()).collect_in::<Vec<_>>(bump);
  let rhs = extractors::normalize_identifiers(rhs.iter()).collect_in::<Vec<_>>(bump);

  if lhs.is_empty() || rhs.is_empty() {
    return (0.0, explain.then_some(Detail::Note(NO_DATA))).into();
  }

  if !is_disjoint(&lhs, &rhs) {
    return (0.0, explain.then_some(Detail::Note("organization identifiers overlap"))).into();
  }

  let mut best_ratio = 0.0f64;
  let mut best_pair: Option<(CompactString, CompactString)> = None;

  for (l, r) in lhs.iter().cartesian_product(rhs.iter()) {
    let distance = levenshtein(l, r) as f64;
    let ratio = 1.0 - (distance / l.len().max(r.len()) as f64);
    let ratio = if ratio > 0.7 { ratio } else { 0.0 };

    if ratio > best_ratio {
      best_ratio = ratio;

      if explain {
        best_pair = Some((l.as_str().into(), r.as_str().into()));
      }
    }
  }

  let detail = explain.then(|| match best_pair {
    Some((lhs, rhs)) if best_ratio > 0.0 => Detail::Fuzzy {
      lhs,
      rhs,
      score: format_score(best_ratio),
    },
    _ => Detail::Note("organization identifiers are disjoint"),
  });

  (1.0 - best_ratio, detail).into()
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;
  use float_cmp::approx_eq;
  use pyo3::Python;

  use crate::{
    model::{Entity, SearchEntity},
    tests::python::nomenklatura_comparer,
  };

  use super::Feature;

  #[test]
  fn orgid_disjoint() {
    let lhs = SearchEntity::builder("Organization").properties(&[("registrationNumber", &["FR12-34"])]).build();
    let rhs = Entity::builder("Organization").properties(&[("registrationNumber", &["FR-1234"])]).build();

    assert_eq!(super::OrgIdMismatch.score_scalar(&Bump::new(), &lhs, &rhs), 0.0);

    let lhs = SearchEntity::builder("Organization").properties(&[("registrationNumber", &["FR12-34"])]).build();
    let rhs = Entity::builder("Organization").properties(&[("registrationNumber", &["UK-4321"])]).build();

    assert_eq!(super::OrgIdMismatch.score_scalar(&Bump::new(), &lhs, &rhs), 1.0);

    let lhs = SearchEntity::builder("Company").properties(&[("registrationNumber", &["FR1234567890"])]).build();
    let rhs = Entity::builder("Organization").properties(&[("registrationNumber", &["FR-1134567-890"])]).build();

    assert!(approx_eq!(f64, super::OrgIdMismatch.score_scalar(&Bump::new(), &lhs, &rhs), 0.08, epsilon = 0.01));
  }

  #[test]
  fn orgid_disjoint_uses_closest_pair() {
    let lhs = SearchEntity::builder("Organization").properties(&[("registrationNumber", &["AAAAAAAAAA", "ZZZZZZZZZZ"])]).build();
    let rhs = Entity::builder("Organization").properties(&[("registrationNumber", &["AAAAAAAAAB", "QQQQQQQQQQ"])]).build();

    assert!(approx_eq!(f64, super::OrgIdMismatch.score_scalar(&Bump::new(), &lhs, &rhs), 0.1, epsilon = 0.001));
  }

  #[test]
  fn orgid_disjoint_details() {
    fn detail(lhs: &[(&str, &[&str])], rhs: &[(&str, &[&str])]) -> String {
      let lhs = SearchEntity::builder("Organization").properties(lhs).build();
      let rhs = Entity::builder("Organization").properties(rhs).build();

      super::OrgIdMismatch.score(&Bump::new(), &lhs, &rhs, true).1.unwrap().to_string()
    }

    // Not an organization.
    let lhs = SearchEntity::builder("Person").properties(&[("registrationNumber", &["FR12"])]).build();
    let rhs = Entity::builder("Person").properties(&[("registrationNumber", &["FR12"])]).build();
    assert_eq!(super::OrgIdMismatch.score(&Bump::new(), &lhs, &rhs, true).1.unwrap().to_string(), "not an organization");

    // No identifiers on one side.
    assert_eq!(detail(&[], &[("registrationNumber", &["FR12"])]), "no organization identifiers to compare");
    assert_eq!(detail(&[("registrationNumber", &["FR12"])], &[]), "no organization identifiers to compare");

    // Overlapping identifiers.
    assert_eq!(
      detail(&[("registrationNumber", &["FR12-34"])], &[("registrationNumber", &["FR-1234"])]),
      "organization identifiers overlap"
    );

    // Disjoint and far apart.
    assert_eq!(
      detail(&[("registrationNumber", &["FR12-34"])], &[("registrationNumber", &["UK-4321"])]),
      "organization identifiers are disjoint"
    );

    // Disjoint but close: the nearest pair is surfaced.
    let close = detail(&[("registrationNumber", &["FR1234567890"])], &[("registrationNumber", &["FR-1134567-890"])]);
    assert!(close.contains(" ~= "), "expected a fuzzy pair: {close}");
  }

  #[test]
  #[serial_test::serial]
  fn against_nomenklatura() {
    Python::initialize();

    let lhs = SearchEntity::builder("Company").properties(&[("registrationNumber", &["FR1234567890"])]).build();
    let rhs = Entity::builder("Organization").properties(&[("registrationNumber", &["FR-1134567-890"])]).build();

    let nscore = nomenklatura_comparer("name_based.misc", "orgid_disjoint", &lhs, &rhs).unwrap();

    assert!(approx_eq!(f64, nscore, super::OrgIdMismatch.score_scalar(&Bump::new(), &lhs, &rhs), epsilon = 0.01));
  }
}
