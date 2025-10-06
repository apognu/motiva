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
  let mut similarities = Vec::with_capacity_in(lhs.name_parts.len(), bump);

  for part in &lhs.name_parts {
    let mut best = 0.0f64;

    for other in extractors::name_parts_flat(rhs.names_and_aliases().iter()) {
      let similarity = match jaro_winkler(part, &other) {
        score if score > 0.6 => score,
        _ => 0.0,
      };

      if similarity >= 0.5 {
        best = best.max(similarity);
      }
    }

    similarities.push(best);
  }

  similarities.iter().sum::<f64>() / 1.0f64.max(similarities.len() as f64)
}

#[scoring_feature(PersonNameJaroWinkler, name = "person_name_jaro_winkler")]
fn score_feature(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  if !lhs.schema.is_a("Person") && !rhs.schema.is_a("Person") {
    return 0.0;
  }

  let lhs_names = extractors::name_parts(lhs.names_and_aliases().iter()).collect_in::<Vec<_>>(bump);
  let rhs_names = extractors::name_parts(rhs.names_and_aliases().iter()).collect_in::<Vec<_>>(bump);

  let mut score = 0.0f64;

  for (lhs_parts, rhs_parts) in lhs_names.into_iter().cartesian_product(rhs_names.iter()) {
    let lhs_joined = lhs_parts.join("");
    let rhs_joined = rhs_parts.join("");

    if is_levenshtein_plausible(&lhs_joined, &rhs_joined) {
      score = score.max(jaro_winkler(&lhs_joined, &rhs_joined).powi(lhs_joined.len() as i32));
    }

    score = score.max(align_name_parts(&lhs_parts, rhs_parts));
  }

  score
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;
  use float_cmp::approx_eq;

  use crate::{
    matching::Feature,
    model::{Entity, SearchEntity},
    tests::python::nomenklatura_comparer,
  };

  #[test]
  #[serial_test::serial]
  fn jaro_name_parts_against_nomenklatura() {
    pyo3::prepare_freethreaded_python();

    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Fladymir Poutin"])]).build();

    let nscore = nomenklatura_comparer("name_based.names", "jaro_name_parts", &lhs, &rhs).unwrap();

    assert!(approx_eq!(f64, nscore, super::JaroNameParts.score_feature(&Bump::new(), &lhs, &rhs), epsilon = 0.01));
  }

  #[test]
  #[serial_test::serial]
  fn person_name_jaro_winkler_against_nomenklatura() {
    pyo3::prepare_freethreaded_python();

    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Fladymir Poutin"])]).build();

    let nscore = nomenklatura_comparer("compare.names", "person_name_jaro_winkler", &lhs, &rhs).unwrap();

    assert!(approx_eq!(f64, nscore, super::PersonNameJaroWinkler.score_feature(&Bump::new(), &lhs, &rhs), epsilon = 0.01));
  }
}
