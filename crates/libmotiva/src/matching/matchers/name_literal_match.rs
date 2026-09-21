use bumpalo::{
  Bump,
  collections::{CollectIn, Vec},
};
use compact_str::CompactString;
use itertools::Itertools;

use crate::{
  matching::{
    Candidate, Detail, Feature, ScoreResult,
    extractors::{self},
  },
  model::{Entity, HasProperties, PropertyFilter, SearchEntity},
};

pub struct NameLiteralMatch;

impl NameLiteralMatch {
  fn shared_name<'a>(lhs_names: &'a [String], rhs_names: &[String]) -> Option<&'a String> {
    lhs_names.iter().find(|name| rhs_names.contains(name))
  }
}

impl Feature for NameLiteralMatch {
  fn name(&self) -> &'static str {
    "name_literal_match"
  }

  #[tracing::instrument(level = "trace", name = "name_literal_match", skip_all, fields(feature = "name_literal_match", entity_id = rhs.id))]
  fn score(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity, explain: bool) -> ScoreResult {
    let lhs_names = extractors::clean_literal_names(lhs.prop_group("name", PropertyFilter::All).iter()).unique().collect_in::<Vec<_>>(bump);
    let rhs_candidates = rhs.prop_group("name", PropertyFilter::All);
    let rhs_names = rhs_candidates
      .iter()
      .flat_map(|value| extractors::clean_literal_names(std::iter::once(value)))
      .unique()
      .collect_in::<Vec<_>>(bump);

    match Self::shared_name(&lhs_names, &rhs_names) {
      Some(name) => {
        let candidate = explain
          .then(|| {
            rhs_candidates.iter().find_map(|value| {
              extractors::clean_literal_names(std::iter::once(value))
                .any(|cleaned| cleaned == *name)
                .then(|| Candidate::new(value.field, value.value))
            })
          })
          .flatten();
        (1.0, explain.then(|| Detail::Equal(CompactString::from(name.as_str()), CompactString::from(name.as_str()))), candidate).into()
      }
      None => (0.0, explain.then_some(Detail::Note("no literal name match"))).into(),
    }
  }
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;

  use crate::{
    matching::ScoreResult,
    model::{Entity, SearchEntity},
  };

  use super::Feature;

  #[test]
  fn name_literal_match() {
    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Donald Trump"]), ("alias", &["Orange man"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Donald Trump"]), ("alias", &["Orange man"])]).build();

    assert_eq!(super::NameLiteralMatch.score_scalar(&Bump::new(), &lhs, &rhs), 1.0);

    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Donald Trump"]), ("alias", &["Orange man"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Donald Duck"]), ("alias", &["POTUS"])]).build();

    assert_eq!(super::NameLiteralMatch.score_scalar(&Bump::new(), &lhs, &rhs), 0.0);
  }

  #[test]
  fn name_literal_match_preserves_candidate_provenance() {
    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Donald Trump"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Someone Else"]), ("alias", &["DONALD TRUMP!!!"])]).build();

    let ScoreResult(score, _, candidate) = super::NameLiteralMatch.score(&Bump::new(), &lhs, &rhs, true);
    let candidate = candidate.unwrap();

    assert_eq!(score, 1.0);
    assert_eq!(candidate.field, "alias");
    assert_eq!(candidate.value, "DONALD TRUMP!!!");

    let ScoreResult(_, _, candidate) = super::NameLiteralMatch.score(&Bump::new(), &lhs, &rhs, false);
    assert!(candidate.is_none());
  }
}
