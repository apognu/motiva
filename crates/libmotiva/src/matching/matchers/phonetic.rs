use bumpalo::{
  Bump,
  collections::{CollectIn, Vec},
};
use itertools::Itertools;
use libmotiva_macros::scoring_feature;

use crate::{
  matching::{Feature, comparers::compare_name_phonetic_tuples, extractors},
  model::{Entity, HasProperties, SearchEntity},
};

#[scoring_feature(PersonNamePhoneticMatch, name = "person_name_phonetic_match")]
fn score_feature(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  if !lhs.schema.is_a("Person") && !rhs.schema.is_a("Person") {
    return 0.0;
  }

  let lhs_names = &lhs.clean_names;
  let rhs_names = extractors::clean_names(rhs.prop_group("name").iter()).collect_in::<Vec<_>>(bump);

  let lhs_phone = extractors::phonetic_names_tuples(lhs_names.iter());
  let rhs_phone = extractors::phonetic_names_tuples(rhs_names.iter());

  let mut score = 0.0f64;

  for (ls, rs) in lhs_phone.iter().cartesian_product(rhs_phone.iter()) {
    let mut matched = 0;
    let mut used = vec![false; rs.len()];

    for (l_name, l_phone) in ls {
      for (idx, (r_name, r_phone)) in rs.iter().enumerate() {
        if !used[idx] && compare_name_phonetic_tuples((l_name, l_phone.as_deref()), (r_name, r_phone.as_deref())) {
          matched += 1;
          used[idx] = true;
          break;
        }
      }
    }

    score = score.max(matched as f64 / ls.len() as f64);

    if score >= 1.0 {
      return 1.0;
    }
  }

  score
}
