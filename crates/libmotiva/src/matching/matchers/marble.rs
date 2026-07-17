use alcs::FuzzyStrstr;
use bumpalo::Bump;
use libmotiva_macros::scoring_feature;

use crate::{
  Entity, HasProperties, SearchEntity,
  matching::{
    Feature, extractors,
    replacers::{self, company_types::ORG_TYPES, stopwords::STOPWORDS},
  },
  model::PropertyFilter,
};

fn fingerprint_name(name: &str) -> String {
  let output = replacers::replace(&STOPWORDS.0, &STOPWORDS.1, name);
  let output = replacers::replace(&ORG_TYPES.0, &ORG_TYPES.1, &output);

  output.trim().to_string()
}

#[scoring_feature(LongestCommonSubsequence, name = "longest_common_subsequence")]
fn score_feature(&self, _bump: &Bump, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  #[inline]
  fn coverage(matched: &str, full: &str) -> f64 {
    let full = full.chars().count();

    if full == 0 { 0.0 } else { matched.chars().count() as f64 / full as f64 }
  }

  let lhs_names = lhs.prop_group("name", PropertyFilter::All);
  let rhs_names = rhs.prop_group("name", PropertyFilter::All);

  let lhs_names = extractors::index_name_keys(lhs_names.iter()).map(|name| fingerprint_name(&name)).collect::<Vec<_>>();
  let rhs_names = extractors::index_name_keys(rhs_names.iter());

  let mut max = 0.0f64;

  for rhs_name in rhs_names {
    let rname = fingerprint_name(&rhs_name);

    for lname in &lhs_names {
      if let Some((score, matched)) = rname.fuzzy_find_str(lname, 0.6) {
        max = max.max(score as f64 * coverage(matched, &rname));
      }
    }
  }

  max
}

#[cfg(test)]
mod tests {
  use crate::{
    matching::{Feature, matchers::jaro_winkler::PersonNameJaroWinkler},
    model::{Entity, SearchEntity},
  };

  use bumpalo::Bump;

  #[test]
  fn longest_common_subsequence() {
    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Samir Kamil AlAssad"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Samer Kamel Al Asad"])]).build();

    // Sanity check that PersonNameJaroWinkler had a very bad scoring for this
    assert!(PersonNameJaroWinkler.score_feature(&Bump::new(), &lhs, &rhs) < 0.3);
    assert!(super::LongestCommonSubsequence.score_feature(&Bump::new(), &lhs, &rhs) > 0.8);
  }

  #[test]
  fn fills_jaro_winkler_gaps() {
    let cases = [
      ("Abdul Aziz", "Abdelaziz"),
      ("Abdul Rahman", "Abdurrahman"),
      ("Mohammed Reza", "Mohammadreza"),
      ("Hafez Al Assad", "Hafiz Alasad"),
    ];

    for (l, r) in cases {
      let lhs = SearchEntity::builder("Person").properties(&[("name", &[l])]).build();
      let rhs = Entity::builder("Person").properties(&[("name", &[r])]).build();

      assert!(PersonNameJaroWinkler.score_feature(&Bump::new(), &lhs, &rhs) < 0.7);
      assert!(super::LongestCommonSubsequence.score_feature(&Bump::new(), &lhs, &rhs) > 0.8);
    }
  }
}
