use bumpalo::{
  Bump,
  collections::{CollectIn, Vec},
};
use itertools::Itertools;
use libmotiva_macros::scoring_feature;
use rphonetic::Metaphone;

use crate::{
  matching::{Feature, comparers::compare_name_phonetic_tuples, extractors},
  model::{Entity, HasProperties, SearchEntity},
};

#[scoring_feature(PersonNamePhoneticMatch, name = "person_name_phonetic_match")]
fn score_feature(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  if !lhs.schema.is_a("Person") && !rhs.schema.is_a("Person") {
    return 0.0;
  }

  let lhs_names = extractors::clean_names(lhs.names_and_aliases().iter()).collect_in::<Vec<_>>(bump);
  let rhs_names = extractors::clean_names(rhs.names_and_aliases().iter()).collect_in::<Vec<_>>(bump);

  let metaphone = Metaphone::new(None);
  let lhs_phone = extractors::phonetic_names_tuples(&metaphone, lhs_names.iter());
  let rhs_phone = extractors::phonetic_names_tuples(&metaphone, rhs_names.iter());

  let mut score = 0.0f64;

  lhs_phone.iter().cartesian_product(rhs_phone.iter()).for_each(|(ls, rs)| {
    let mut matched = 0;
    let mut comp = rs.clone();

    for (l_name, l_phone) in ls {
      for (r_name, r_phone) in comp.iter() {
        if compare_name_phonetic_tuples((l_name, l_phone.as_deref()), (r_name, r_phone.as_deref())) {
          matched += 1;

          if let Some(index) = comp.iter().position(|x| *x == (r_name.clone(), r_phone.clone())) {
            comp.remove(index);
          }

          break;
        }
      }
    }

    score = score.max(matched as f64 / ls.len() as f64);
  });

  score
}
