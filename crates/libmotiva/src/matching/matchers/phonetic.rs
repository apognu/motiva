use bumpalo::{
  Bump,
  collections::{CollectIn, Vec},
};
use itertools::Itertools;
use libmotiva_macros::scoring_feature;

use crate::{
  matching::{CodedPair, Detail, Feature, comparers::compare_name_phonetic_tuples, extractors},
  model::{Entity, HasProperties, PropertyFilter, SearchEntity},
};

#[scoring_feature(PersonNamePhoneticMatch, name = "person_name_phonetic_match")]
fn score(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity, explain: bool) -> (f64, Option<Detail>) {
  if !lhs.schema.is_a("Person") && !rhs.schema.is_a("Person") {
    return (0.0, explain.then_some(Detail::Note("not a person")));
  }

  let lhs_names = &lhs.clean_names;
  let rhs_names = extractors::clean_names(rhs.prop_group("name", PropertyFilter::All).iter()).collect_in::<Vec<_>>(bump);

  let lhs_phone = extractors::phonetic_names_tuples(lhs_names.iter());
  let rhs_phone = extractors::phonetic_names_tuples(rhs_names.iter());

  let mut score = 0.0f64;
  let mut best_matches: std::vec::Vec<CodedPair> = std::vec::Vec::new();

  for (ls, rs) in lhs_phone.iter().cartesian_product(rhs_phone.iter()) {
    let mut matched = 0;
    let mut used = vec![false; rs.len()];
    let mut combo_matches = std::vec::Vec::new();

    for (l_name, l_phone) in ls {
      for (idx, (r_name, r_phone)) in rs.iter().enumerate() {
        if !used[idx] && compare_name_phonetic_tuples((l_name, l_phone.as_deref()), (r_name, r_phone.as_deref())) {
          matched += 1;
          used[idx] = true;

          if explain {
            combo_matches.push(CodedPair {
              lhs: l_name.as_str().into(),
              lhs_code: l_phone.as_deref().unwrap_or_default().into(),
              rhs: r_name.as_str().into(),
              rhs_code: r_phone.as_deref().unwrap_or_default().into(),
            });
          }

          break;
        }
      }
    }

    let combo_score = matched as f64 / ls.len() as f64;

    if combo_score > score {
      score = combo_score;

      if explain {
        best_matches = combo_matches;
      }
    }

    if score >= 1.0 {
      break;
    }
  }

  let detail = explain.then(|| {
    if best_matches.is_empty() {
      Detail::Note("no phonetic match")
    } else {
      Detail::CodedList(best_matches)
    }
  });

  (score, detail)
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;

  use crate::{
    matching::Feature,
    model::{Entity, SearchEntity},
  };

  #[test]
  fn person_name_phonetic_match_details() {
    fn detail(lhs: &SearchEntity, rhs: &Entity) -> Option<String> {
      super::PersonNamePhoneticMatch.score(&Bump::new(), lhs, rhs, true).1.map(|detail| detail.to_string())
    }

    let lhs = SearchEntity::builder("Company").properties(&[("name", &["Vladimir Putin"])]).build();
    let rhs = Entity::builder("Company").properties(&[("name", &["Vladimir Putin"])]).build();
    assert_eq!(detail(&lhs, &rhs).as_deref(), Some("not a person"));

    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Vladymir Poutin"])]).build();
    let matched = detail(&lhs, &rhs).unwrap();
    assert!(matched.contains(" [") && matched.contains("] ~= "), "unexpected phonetic detail: {matched}");

    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Washington"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Zeppelin"])]).build();
    assert_eq!(detail(&lhs, &rhs).as_deref(), Some("no phonetic match"));
  }
}
