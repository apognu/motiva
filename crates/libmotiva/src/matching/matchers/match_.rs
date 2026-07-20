use std::borrow::Cow;

use bumpalo::{
  Bump,
  collections::{CollectIn, Vec},
};
use itertools::Itertools;
use libmotiva_macros::scoring_feature;

use crate::{
  matching::{Detail, Feature, ScoreResult, extractors, matchers::NO_DATA},
  model::{Entity, HasProperties, PropertyFilter, SearchEntity},
};

pub(crate) type MatchExtractor<'e> = &'e (dyn Fn(&'_ dyn HasProperties) -> Cow<[String]> + Send + Sync);

pub(crate) struct SimpleMatch<'e> {
  name: &'static str,
  extractor: MatchExtractor<'e>,
}

impl<'e> SimpleMatch<'e> {
  pub(crate) fn new(name: &'static str, extractor: MatchExtractor<'e>) -> &'static Self {
    Box::leak(Box::new(SimpleMatch { name, extractor }))
  }
}

impl<'e> Feature for SimpleMatch<'e> {
  fn name(&self) -> &'static str {
    self.name
  }

  fn score(&self, _bump: &Bump, lhs: &SearchEntity, rhs: &Entity, explain: bool) -> ScoreResult {
    let lhs_names = (self.extractor)(lhs);
    let rhs_names = (self.extractor)(rhs);

    if lhs_names.is_empty() || rhs_names.is_empty() {
      return (0.0, explain.then_some(Detail::Note(NO_DATA))).into();
    }

    let matched = lhs_names.iter().any(|value| rhs_names.contains(value));

    let detail = explain.then(|| {
      if !matched {
        return Detail::Note("no match");
      }

      let shared = lhs_names.iter().filter(|value| rhs_names.contains(value)).map(String::as_str).unique().join(", ");

      Detail::Labeled("matched", shared.into())
    });

    (if matched { 1.0 } else { 0.0 }, detail).into()
  }
}

#[scoring_feature(WeakAliasMatch, name = "weak_alias_match")]
fn score(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity, explain: bool) -> ScoreResult {
  let lhs_names = extractors::clean_names_light(lhs.prop_group("name", PropertyFilter::All).iter()).collect_in::<Vec<_>>(bump);
  let rhs_names = extractors::clean_names_light(rhs.props(&["weakAlias", "abbreviation"]).iter()).collect_in::<Vec<_>>(bump);

  if lhs_names.is_empty() || rhs_names.is_empty() {
    return (0.0, explain.then_some(Detail::Note(NO_DATA))).into();
  }

  match lhs_names.iter().find(|name| rhs_names.contains(name)) {
    Some(alias) => (1.0, explain.then(|| Detail::Labeled("matched weak alias", alias.as_str().into()))).into(),
    None => (0.0, explain.then_some(Detail::Note("no weak alias match"))).into(),
  }
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;

  use crate::{
    Entity, Feature, SearchEntity,
    matching::{
      ScoreResult,
      matchers::match_::{SimpleMatch, WeakAliasMatch},
    },
  };

  #[test]
  fn weak_alias_match() {
    let lhs = SearchEntity::builder("Company").properties(&[("name", &["bob"])]).build();
    let rhs = Entity::builder("Company").properties(&[("weakAlias", &["joe", "bob"])]).build();

    let score = WeakAliasMatch.score_scalar(&Bump::new(), &lhs, &rhs);

    assert_eq!(score, 1.0);

    let lhs = SearchEntity::builder("Company").properties(&[("name", &["bill"])]).build();
    let rhs = Entity::builder("Company").properties(&[("weakAlias", &["joe", "bob"])]).build();

    let score = WeakAliasMatch.score_scalar(&Bump::new(), &lhs, &rhs);

    assert_eq!(score, 0.0);
  }

  #[test]
  fn weak_alias_match_details() {
    fn detail(lhs: &[&str], rhs: &[&str]) -> String {
      let lhs = SearchEntity::builder("Company").properties(&[("name", lhs)]).build();
      let rhs = Entity::builder("Company").properties(&[("weakAlias", rhs)]).build();

      WeakAliasMatch.score(&Bump::new(), &lhs, &rhs, true).1.unwrap().to_string()
    }

    assert_eq!(detail(&["bob"], &["joe", "bob"]), "matched weak alias: bob");
    assert_eq!(detail(&["bill"], &["joe", "bob"]), "no weak alias match");

    // No aliases to compare on the candidate side.
    let lhs = SearchEntity::builder("Company").properties(&[("name", &["bob"])]).build();
    let rhs = Entity::builder("Company").properties(&[]).build();
    assert_eq!(WeakAliasMatch.score(&Bump::new(), &lhs, &rhs, true).1.unwrap().to_string(), "no data to match against");
  }

  #[test]
  fn simple_match() {
    let matcher = SimpleMatch::new("", &|e| e.props(&["id"]));

    let lhs = SearchEntity::builder("Company").properties(&[("id", &["12345"])]).build();
    let rhs = Entity::builder("Company").properties(&[("id", &["1234"])]).build();

    assert_eq!(matcher.score_scalar(&Bump::new(), &lhs, &rhs), 0.0);

    let lhs = SearchEntity::builder("Company").properties(&[("id", &["1234"])]).build();
    let rhs = Entity::builder("Company").properties(&[("id", &["1234"])]).build();

    assert_eq!(matcher.score_scalar(&Bump::new(), &lhs, &rhs), 1.0);
  }

  #[test]
  fn simple_match_details() {
    let matcher = SimpleMatch::new("", &|e| e.props(&["id"]));

    let lhs = SearchEntity::builder("Company").properties(&[("id", &["a", "b", "c"])]).build();
    let rhs = Entity::builder("Company").properties(&[("id", &["b", "c", "d"])]).build();

    let ScoreResult(score, detail) = matcher.score(&Bump::new(), &lhs, &rhs, true);
    assert_eq!(score, 1.0);
    assert_eq!(detail.unwrap().to_string(), "matched: b, c");
  }
}
