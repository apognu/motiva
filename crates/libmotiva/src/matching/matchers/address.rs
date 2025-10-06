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
    Feature,
    comparers::levenshtein_similarity,
    extractors,
    replacers::{self, addresses::ADDRESS_FORMS, ordinals::ORDINALS},
  },
  model::{Entity, HasProperties, SearchEntity},
};

#[scoring_feature(AddressEntityMatch, name = "address_entity_match")]
fn score_feature(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  if !lhs.schema.is_a("Address") || !rhs.schema.is_a("Address") {
    return 0.0;
  }

  let lhs_addresses = extractors::clean_address_parts(lhs.property("full").iter()).map(|address| {
    replacers::replace(&ORDINALS.0, &ORDINALS.1, &replacers::replace(&ADDRESS_FORMS.0, &ADDRESS_FORMS.1, &address))
      .split_whitespace()
      .map(str::to_string)
      .unique()
      .collect::<HashSet<_, RandomState>>()
  });

  let rhs_addresses = extractors::clean_address_parts(rhs.property("full").iter()).map(|address| {
    replacers::replace(&ORDINALS.0, &ORDINALS.1, &replacers::replace(&ADDRESS_FORMS.0, &ADDRESS_FORMS.1, &address))
      .split_whitespace()
      .map(str::to_string)
      .unique()
      .collect::<HashSet<_, RandomState>>()
  });

  let mut max_score = 0.0f64;

  for (lhs, rhs) in lhs_addresses.cartesian_product(rhs_addresses) {
    if lhs.is_empty() || rhs.is_empty() {
      continue;
    }

    let overlap = lhs.intersection(&rhs).collect_in::<Vec<_>>(bump);
    let overlap_size = overlap.len();

    if overlap_size == lhs.len() || overlap_size == rhs.len() {
      return 1.0;
    }

    let lhs_remainder = lhs.iter().filter(|word| !overlap.contains(word)).sorted();
    let rhs_remainder = rhs.iter().filter(|word| !overlap.contains(word)).sorted();

    let lhs_remainder_str = lhs_remainder.clone().join(" ");
    let rhs_remainder_str = rhs_remainder.clone().join(" ");
    let levenshtein_max_edits = lhs_remainder_str.len().max(rhs_remainder_str.len());

    let score = levenshtein_similarity(&lhs_remainder_str, &rhs_remainder_str, levenshtein_max_edits);
    let remainder_len = lhs_remainder.len().max(rhs_remainder.len());
    let score = (overlap.len() as f64 + (remainder_len as f64 * score)) / (remainder_len + overlap.len()) as f64;

    max_score = score.max(max_score);
  }

  max_score
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
    let lhs = SearchEntity::builder("Address").properties(&[("full", &["No.3, New York avenue, 103-222, New York City"])]).build();
    let rhs = Entity::builder("Address").properties(&[("full", &["3 New York ave, 103222, New York City"])]).build();

    assert!(approx_eq!(f64, super::AddressEntityMatch.score_feature(&Bump::new(), &lhs, &rhs), 0.9, epsilon = 0.01));
  }
}
