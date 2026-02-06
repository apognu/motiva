use std::borrow::Cow;

use bumpalo::{
  Bump,
  collections::{CollectIn, Vec},
};
use libmotiva_macros::scoring_feature;

use crate::{
  matching::{Feature, comparers::is_disjoint, extractors},
  model::{Entity, HasProperties, SearchEntity},
};

pub(crate) type MatchExtractor<'e> = &'e (dyn Fn(&'_ dyn HasProperties) -> Cow<[String]> + Send + Sync);
type MismatchMatcher = Option<fn(lhs: &[String], rhs: &[String]) -> f64>;

pub(crate) struct SimpleMatch<'e> {
  name: &'static str,
  extractor: MatchExtractor<'e>,
  matcher: MismatchMatcher,
}

impl<'e> SimpleMatch<'e> {
  pub(crate) fn new(name: &'static str, extractor: MatchExtractor<'e>, matcher: MismatchMatcher) -> &'static Self {
    Box::leak(Box::new(SimpleMatch { name, extractor, matcher }))
  }
}

impl<'e> Feature for SimpleMatch<'e> {
  fn name(&self) -> &'static str {
    self.name
  }

  fn score_feature(&self, _bump: &Bump, lhs: &SearchEntity, rhs: &Entity) -> f64 {
    let lhs_names = (self.extractor)(lhs);
    let rhs_names = (self.extractor)(rhs);

    if lhs_names.is_empty() || rhs_names.is_empty() {
      return 0.0;
    }

    match self.matcher {
      Some(func) => (func)(&lhs_names, &rhs_names),

      None => match is_disjoint(&lhs_names, &rhs_names) {
        false => 1.0,
        true => 0.0,
      },
    }
  }
}

#[scoring_feature(WeakAliasMatch, name = "weak_alias_match")]
fn score_feature(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  let lhs_names = extractors::clean_names_light(lhs.prop_group("name").iter()).collect_in::<Vec<_>>(bump);
  let rhs_names = extractors::clean_names_light(rhs.props(&["weakAlias", "abbreviation"]).iter()).collect_in::<Vec<_>>(bump);

  if lhs_names.is_empty() || rhs_names.is_empty() {
    return 0.0;
  }

  match is_disjoint(&lhs_names, &rhs_names) {
    false => 1.0,
    true => 0.0,
  }
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;

  use crate::{
    Entity, Feature, SearchEntity,
    matching::matchers::match_::{SimpleMatch, WeakAliasMatch},
  };

  #[test]
  fn weak_alias_match() {
    let lhs = SearchEntity::builder("Company").properties(&[("name", &["bob"])]).build();
    let rhs = Entity::builder("Company").properties(&[("weakAlias", &["joe", "bob"])]).build();

    let score = WeakAliasMatch.score_feature(&Bump::new(), &lhs, &rhs);

    assert_eq!(score, 1.0);

    let lhs = SearchEntity::builder("Company").properties(&[("name", &["bill"])]).build();
    let rhs = Entity::builder("Company").properties(&[("weakAlias", &["joe", "bob"])]).build();

    let score = WeakAliasMatch.score_feature(&Bump::new(), &lhs, &rhs);

    assert_eq!(score, 0.0);
  }

  #[test]
  fn simple_match() {
    let lhs = SearchEntity::builder("Company").properties(&[("id", &["12345"])]).build();
    let rhs = Entity::builder("Company").properties(&[("id", &["1234"])]).build();

    let matcher = SimpleMatch::new("", &|e| e.props(&["id"]), None);

    assert_eq!(matcher.score_feature(&Bump::new(), &lhs, &rhs), 0.0);

    let lhs = SearchEntity::builder("Company").properties(&[("id", &["1234"])]).build();
    let rhs = Entity::builder("Company").properties(&[("id", &["1234"])]).build();

    let matcher = SimpleMatch::new("", &|e| e.props(&["id"]), None);

    assert_eq!(matcher.score_feature(&Bump::new(), &lhs, &rhs), 1.0);
  }

  #[test]
  fn simple_match_with_custom_matcher() {
    let lhs = SearchEntity::builder("Company").properties(&[("id", &["1234"])]).build();
    let rhs = Entity::builder("Company").properties(&[("id", &["1234"])]).build();

    fn match_quarter(_: &[String], _: &[String]) -> f64 {
      0.25
    }

    fn match_three_quarter(_: &[String], _: &[String]) -> f64 {
      0.75
    }

    let matcher = SimpleMatch::new("", &|e| e.props(&["id"]), Some(match_quarter));

    assert_eq!(matcher.score_feature(&Bump::new(), &lhs, &rhs), 0.25);

    let matcher = SimpleMatch::new("", &|e| e.props(&["id"]), Some(match_three_quarter));

    assert_eq!(matcher.score_feature(&Bump::new(), &lhs, &rhs), 0.75);
  }
}
