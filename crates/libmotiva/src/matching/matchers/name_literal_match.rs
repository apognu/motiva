use bumpalo::{
  Bump,
  collections::{CollectIn, Vec},
};
use compact_str::CompactString;
use itertools::Itertools;

use crate::{
  matching::{
    Detail, Feature, ScoreResult,
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
    let rhs_names = extractors::clean_literal_names(rhs.prop_group("name", PropertyFilter::All).iter()).unique().collect_in::<Vec<_>>(bump);

    match Self::shared_name(&lhs_names, &rhs_names) {
      Some(name) => (1.0, explain.then(|| Detail::Equal(CompactString::from(name.as_str()), CompactString::from(name.as_str())))).into(),
      None => (0.0, explain.then_some(Detail::Note("no literal name match"))).into(),
    }
  }
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;

  use crate::model::{Entity, SearchEntity};

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
}
