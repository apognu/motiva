use std::collections::HashSet;

use ahash::RandomState;
use bumpalo::{
  Bump,
  collections::{CollectIn, Vec},
};
use itertools::Itertools;
use libmotiva_macros::scoring_feature;

use crate::{
  matching::{
    Detail, Feature, ScoreResult,
    comparers::levenshtein_similarity,
    extractors,
    replacers::{self, addresses::ADDRESS_FORMS, ordinals::ORDINALS},
  },
  model::{Entity, HasProperties, SearchEntity},
};

#[scoring_feature(AddressEntityMatch, name = "address_entity_match")]
fn score(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity, explain: bool) -> ScoreResult {
  #[inline]
  fn overlap_detail(overlap: &[&String]) -> Detail {
    Detail::Labeled("address overlap", overlap.iter().map(|token| token.as_str()).sorted().join(", ").into())
  }

  if !lhs.schema.is_a("Address") || !rhs.schema.is_a("Address") {
    return (0.0, explain.then_some(Detail::Note("not an address"))).into();
  }

  let lhs_props = lhs.props(&["full"]);
  let lhs_addresses = extractors::clean_address_parts(lhs_props.iter()).map(|address| {
    replacers::replace(&ORDINALS.0, &ORDINALS.1, &replacers::remove(&ADDRESS_FORMS.0, &address))
      .split_whitespace()
      .map(str::to_string)
      .unique()
      .collect::<HashSet<_, RandomState>>()
  });

  let rhs_props = rhs.props(&["full"]);
  let rhs_addresses = extractors::clean_address_parts(rhs_props.iter()).map(|address| {
    replacers::replace(&ORDINALS.0, &ORDINALS.1, &replacers::remove(&ADDRESS_FORMS.0, &address))
      .split_whitespace()
      .map(str::to_string)
      .unique()
      .collect::<HashSet<_, RandomState>>()
  });

  let mut max_score = 0.0f64;
  let mut best_overlap: Option<Detail> = None;

  for (lhs, rhs) in lhs_addresses.cartesian_product(rhs_addresses) {
    if lhs.is_empty() || rhs.is_empty() {
      continue;
    }

    let overlap = lhs.intersection(&rhs).collect_in::<Vec<_>>(bump);
    let overlap_size = overlap.len();

    if overlap_size == lhs.len() || overlap_size == rhs.len() {
      return (1.0, explain.then(|| overlap_detail(&overlap))).into();
    }

    let lhs_remainder: std::vec::Vec<_> = lhs.iter().filter(|word| !overlap.contains(word)).sorted().collect();
    let rhs_remainder: std::vec::Vec<_> = rhs.iter().filter(|word| !overlap.contains(word)).sorted().collect();

    let lhs_remainder_str = lhs_remainder.iter().join(" ");
    let rhs_remainder_str = rhs_remainder.iter().join(" ");
    let levenshtein_max_edits = lhs_remainder_str.len().max(rhs_remainder_str.len());

    let score = levenshtein_similarity(&lhs_remainder_str, &rhs_remainder_str, levenshtein_max_edits);
    let remainder_len = lhs_remainder.len().max(rhs_remainder.len());
    let score = (overlap.len() as f64 + (remainder_len as f64 * score)) / (remainder_len + overlap.len()) as f64;

    if score >= 1.0 {
      return (1.0, explain.then(|| overlap_detail(&overlap))).into();
    }

    if score > max_score {
      max_score = score;

      if explain {
        best_overlap = Some(if overlap.is_empty() { Detail::Note("no address overlap") } else { overlap_detail(&overlap) });
      }
    }
  }

  let detail = explain.then(|| best_overlap.unwrap_or(Detail::Note("no address overlap")));

  (max_score, detail).into()
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;
  use float_cmp::approx_eq;

  use crate::{
    matching::Feature,
    model::{Entity, SearchEntity},
  };

  #[test]
  fn address_entity_match() {
    let lhs = SearchEntity::builder("Address").properties(&[("full", &["No.3, Chabanais avenue, 103-222, Los Angeles"])]).build();
    let rhs = Entity::builder("Address").properties(&[("full", &["3 Chabanais ave, 103222, Los Angeles"])]).build();

    assert!(approx_eq!(f64, super::AddressEntityMatch.score_scalar(&Bump::new(), &lhs, &rhs), 0.95, epsilon = 0.01));
  }

  #[test]
  fn address_entity_match_details() {
    fn detail(lhs: &str, rhs: &str) -> String {
      let lhs = SearchEntity::builder("Address").properties(&[("full", &[lhs])]).build();
      let rhs = Entity::builder("Address").properties(&[("full", &[rhs])]).build();

      super::AddressEntityMatch.score(&Bump::new(), &lhs, &rhs, true).1.unwrap().to_string()
    }

    // Not an address.
    let lhs = SearchEntity::builder("Person").properties(&[("full", &["x"])]).build();
    let rhs = Entity::builder("Person").properties(&[("full", &["x"])]).build();
    assert_eq!(super::AddressEntityMatch.score(&Bump::new(), &lhs, &rhs, true).1.unwrap().to_string(), "not an address");

    // Full containment and partial overlap both surface the shared tokens.
    assert!(detail("Chabanais", "Chabanais Avenue").starts_with("address overlap: "));
    assert!(detail("No.3, Chabanais avenue, 103-222, Los Angeles", "3 Chabanais ave, 103222, Los Angeles").starts_with("address overlap: "));

    // No overlap.
    assert_eq!(detail("Zzzz", "Qqqq"), "no address overlap");
  }
}
