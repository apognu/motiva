use bumpalo::{
  Bump,
  collections::{CollectIn, Vec},
};
use compact_str::CompactString;
use itertools::Itertools;
use libmotiva_macros::scoring_feature;
use strsim::jaro_winkler;

use crate::{
  matching::{
    Detail, Feature,
    comparers::{align_name_parts, is_levenshtein_plausible},
    extractors,
    matchers::NO_DATA,
  },
  model::{Entity, HasProperties, PropertyFilter, SearchEntity, format_score},
};

#[scoring_feature(JaroNameParts, name = "jaro_name_parts")]
fn score(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity, explain: bool) -> (f64, Option<Detail>) {
  if lhs.name_parts_flat.is_empty() {
    return (0.0, explain.then_some(Detail::Note(NO_DATA)));
  }

  let rhs_parts = extractors::name_parts_flat(rhs.prop_group("name", PropertyFilter::All).iter()).collect_in::<Vec<_>>(bump);

  if rhs_parts.is_empty() {
    return (0.0, explain.then_some(Detail::Note(NO_DATA)));
  }

  let mut similarities = Vec::with_capacity_in(lhs.name_parts_flat.len(), bump);
  let mut details: Option<(CompactString, CompactString, f64)> = None;

  for part in &lhs.name_parts_flat {
    let mut best = 0.0f64;
    let mut best_other = None;

    for other in &rhs_parts {
      let similarity = jaro_winkler(part, other);

      if similarity > 0.6 && similarity > best {
        best = similarity;

        if explain {
          best_other = Some(other);
        }

        if best >= 1.0 {
          break;
        }
      }
    }

    similarities.push(best);

    if let Some(other) = best_other
      && details.as_ref().is_none_or(|(_, _, best_so_far)| best > *best_so_far)
    {
      details = Some((part.as_str().into(), other.as_str().into(), best));
    }
  }

  let score = similarities.iter().sum::<f64>() / similarities.len() as f64;

  let detail = explain.then(|| match details {
    Some((lhs, rhs, similarity)) if similarity >= 0.999 => Detail::Equal(lhs, rhs),
    Some((lhs, rhs, similarity)) => Detail::Fuzzy {
      lhs,
      rhs,
      score: format_score(similarity),
    },
    None => Detail::Note("no matching name parts"),
  });

  (score, detail)
}

pub struct PersonNameJaroWinkler;

impl Feature for PersonNameJaroWinkler {
  fn name(&self) -> &'static str {
    "person_name_jaro_winkler"
  }

  #[tracing::instrument(level = "trace", name = "person_name_jaro_winkler", skip_all, fields(feature = "person_name_jaro_winkler", entity_id = rhs.id))]
  fn score(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity, explain: bool) -> (f64, Option<Detail>) {
    if !lhs.schema.is_a("Person") && !rhs.schema.is_a("Person") {
      return (0.0, explain.then_some(Detail::Note("not a person")));
    }

    let lhs_names = &lhs.name_parts;
    let rhs_names = extractors::name_parts(rhs.prop_group("name", PropertyFilter::All).iter()).collect_in::<Vec<_>>(bump);

    let mut score = 0.0f64;
    let mut details: Option<(CompactString, CompactString)> = None;

    for (lhs_parts, rhs_parts) in lhs_names.iter().cartesian_product(rhs_names.iter()) {
      let lhs_len: usize = lhs_parts.iter().map(|s| s.len()).sum();
      let rhs_len: usize = rhs_parts.iter().map(|s| s.len()).sum();

      let mut pair_score = 0.0f64;

      if lhs_len > 0 && rhs_len > 0 {
        let len_ratio = lhs_len.min(rhs_len) as f64 / lhs_len.max(rhs_len) as f64;

        if len_ratio >= 0.5 {
          let lhs_joined = lhs_parts.join("");
          let rhs_joined = rhs_parts.join("");

          if is_levenshtein_plausible(&lhs_joined, &rhs_joined) {
            pair_score = jaro_winkler(&lhs_joined, &rhs_joined).powi(lhs_joined.len() as i32);
          }
        }
      }

      pair_score = pair_score.max(align_name_parts(lhs_parts, rhs_parts));

      if pair_score > score {
        score = pair_score;

        if explain {
          details = Some((CompactString::from(lhs_parts.join("").as_str()), CompactString::from(rhs_parts.join("").as_str())));
        }
      }

      if score >= 1.0 {
        break;
      }
    }

    let detail = explain.then(|| match details {
      Some((lhs, rhs)) if score >= 0.999 => Detail::Equal(lhs, rhs),
      Some((lhs, rhs)) => Detail::Fuzzy { lhs, rhs, score: format_score(score) },
      None => Detail::Note(NO_DATA),
    });

    (score, detail)
  }
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
    let score = super::PersonNameJaroWinkler.score_scalar(&Bump::new(), &lhs, &rhs);

    assert_eq!(score, 0.0);

    let lhs = SearchEntity::builder("Organization").properties(&[("name", &["bob"])]).build();
    let rhs = Entity::builder("Company").properties(&[("name", &[""])]).build();
    let score = super::PersonNameJaroWinkler.score_scalar(&Bump::new(), &lhs, &rhs);

    assert_eq!(score, 0.0);
  }

  #[test]
  fn jaro_winkler_schema_mismatch() {
    let lhs = SearchEntity::builder("Organization").properties(&[("name", &["bob"])]).build();
    let rhs = Entity::builder("Company").properties(&[("name", &["bob"])]).build();
    let score = super::PersonNameJaroWinkler.score_scalar(&Bump::new(), &lhs, &rhs);

    assert_eq!(score, 0.0);
  }

  #[test]
  fn jaro_name_parts_details() {
    fn detail(lhs: &str, rhs: &str) -> String {
      let lhs = SearchEntity::builder("Person").properties(&[("name", &[lhs])]).build();
      let rhs = Entity::builder("Person").properties(&[("name", &[rhs])]).build();

      super::JaroNameParts.score(&Bump::new(), &lhs, &rhs, true).1.unwrap().to_string()
    }

    assert_eq!(detail("", "Vladimir"), "no data to match against");
    assert_eq!(detail("Vladimir", "Vladimir"), "vladimir == vladimir");
    assert!(detail("Vladimir", "Vladymir").contains(" ~= "));
    assert_eq!(detail("Vladimir", "Zzzzzzz"), "no matching name parts");
  }

  #[test]
  fn person_name_jaro_winkler_details() {
    fn detail(schema: &str, lhs: &str, rhs: &str) -> String {
      let lhs = SearchEntity::builder(schema).properties(&[("name", &[lhs])]).build();
      let rhs = Entity::builder(schema).properties(&[("name", &[rhs])]).build();

      super::PersonNameJaroWinkler.score(&Bump::new(), &lhs, &rhs, true).1.unwrap().to_string()
    }

    assert_eq!(detail("Company", "Vladimir Putin", "Vladimir Putin"), "not a person");
    assert!(detail("Person", "Vladimir Putin", "Vladimir Putin").contains(" == "));
    assert!(detail("Person", "Vladimir Putin", "Vladymir Putln").contains(" ~= "));
    assert_eq!(detail("Person", "Aaaa", "Zzzz"), "no data to match against");
  }

  #[test]
  #[serial_test::serial]
  fn jaro_name_parts_against_nomenklatura() {
    Python::initialize();

    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Fladymir Poutin"])]).build();

    let nscore = nomenklatura_comparer("name_based.names", "jaro_name_parts", &lhs, &rhs).unwrap();

    assert!(approx_eq!(f64, nscore, super::JaroNameParts.score_scalar(&Bump::new(), &lhs, &rhs), epsilon = 0.01));
  }

  #[test]
  #[serial_test::serial]
  fn person_name_jaro_winkler_against_nomenklatura() {
    Python::initialize();

    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Fladymir Poutin"])]).build();

    let nscore = nomenklatura_comparer("compare.names", "person_name_jaro_winkler", &lhs, &rhs).unwrap();

    assert!(approx_eq!(f64, nscore, super::PersonNameJaroWinkler.score_scalar(&Bump::new(), &lhs, &rhs), epsilon = 0.01));
  }
}
