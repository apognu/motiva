use bumpalo::{
  Bump,
  collections::{CollectIn, Vec},
};
use itertools::Itertools;
use libmotiva_macros::scoring_feature;
use strsim::jaro_winkler;

use crate::{
  matching::{
    Feature,
    comparers::{align_name_parts, is_levenshtein_plausible},
    extractors,
  },
  model::{Entity, HasProperties, SearchEntity},
};

#[scoring_feature(JaroNameParts, name = "jaro_name_parts")]
fn score_feature(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  if lhs.name_parts_flat.is_empty() {
    return 0.0;
  }

  let rhs_parts = extractors::name_parts_flat(rhs.prop_group("name").iter()).collect_in::<Vec<_>>(bump);

  if rhs_parts.is_empty() {
    return 0.0;
  }

  let mut similarities = Vec::with_capacity_in(lhs.name_parts_flat.len(), bump);

  for part in &lhs.name_parts_flat {
    let mut best = 0.0f64;

    for other in &rhs_parts {
      let similarity = jaro_winkler(part, other);

      if similarity > 0.6 {
        best = best.max(similarity);

        if best >= 1.0 {
          break;
        }
      }
    }

    similarities.push(best);
  }

  similarities.iter().sum::<f64>() / similarities.len() as f64
}

#[scoring_feature(PersonNameJaroWinkler, name = "person_name_jaro_winkler")]
fn score_feature(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  if !lhs.schema.is_a("Person") && !rhs.schema.is_a("Person") {
    return 0.0;
  }

  let lhs_names = &lhs.name_parts;
  let rhs_names = extractors::name_parts(rhs.prop_group("name").iter()).collect_in::<Vec<_>>(bump);

  let mut score = 0.0f64;

  for (lhs_parts, rhs_parts) in lhs_names.iter().cartesian_product(rhs_names.iter()) {
    let lhs_len: usize = lhs_parts.iter().map(|s| s.len()).sum();
    let rhs_len: usize = rhs_parts.iter().map(|s| s.len()).sum();

    if lhs_len > 0 && rhs_len > 0 {
      let len_ratio = lhs_len.min(rhs_len) as f64 / lhs_len.max(rhs_len) as f64;

      if len_ratio >= 0.5 {
        let lhs_joined = lhs_parts.join("");
        let rhs_joined = rhs_parts.join("");

        if is_levenshtein_plausible(&lhs_joined, &rhs_joined) {
          score = score.max(jaro_winkler(&lhs_joined, &rhs_joined).powi(lhs_joined.len() as i32));

          if score >= 1.0 {
            return 1.0;
          }
        }
      }
    }

    score = score.max(align_name_parts(lhs_parts, rhs_parts));

    if score >= 1.0 {
      return 1.0;
    }
  }

  score
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;
  use float_cmp::approx_eq;
  use pyo3::Python;

  use crate::{
    matching::Feature,
    model::{Entity, SearchEntity},
    tests::python::nomenklatura_comparer,
  };

  #[test]
  fn jaro_name_parts_empty() {
    let lhs = SearchEntity::builder("Organization").properties(&[("name", &[""])]).build();
    let rhs = Entity::builder("Company").properties(&[("name", &["bob"])]).build();
    let score = super::PersonNameJaroWinkler.score_feature(&Bump::new(), &lhs, &rhs);

    assert_eq!(score, 0.0);

    let lhs = SearchEntity::builder("Organization").properties(&[("name", &["bob"])]).build();
    let rhs = Entity::builder("Company").properties(&[("name", &[""])]).build();
    let score = super::PersonNameJaroWinkler.score_feature(&Bump::new(), &lhs, &rhs);

    assert_eq!(score, 0.0);
  }

  #[test]
  fn jaro_winkler_schema_mismatch() {
    let lhs = SearchEntity::builder("Organization").properties(&[("name", &["bob"])]).build();
    let rhs = Entity::builder("Company").properties(&[("name", &["bob"])]).build();
    let score = super::PersonNameJaroWinkler.score_feature(&Bump::new(), &lhs, &rhs);

    assert_eq!(score, 0.0);
  }

  #[test]
  #[serial_test::serial]
  fn jaro_name_parts_against_nomenklatura() {
    Python::initialize();

    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Fladymir Poutin"])]).build();

    let nscore = nomenklatura_comparer("name_based.names", "jaro_name_parts", &lhs, &rhs).unwrap();

    assert!(approx_eq!(f64, nscore, super::JaroNameParts.score_feature(&Bump::new(), &lhs, &rhs), epsilon = 0.01));
  }

  #[test]
  #[serial_test::serial]
  fn person_name_jaro_winkler_against_nomenklatura() {
    Python::initialize();

    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Fladymir Poutin"])]).build();

    let nscore = nomenklatura_comparer("compare.names", "person_name_jaro_winkler", &lhs, &rhs).unwrap();

    assert!(approx_eq!(f64, nscore, super::PersonNameJaroWinkler.score_feature(&Bump::new(), &lhs, &rhs), epsilon = 0.01));
  }
}
