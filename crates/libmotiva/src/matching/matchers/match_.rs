use std::borrow::Cow;

use bumpalo::Bump;

use crate::{
  matching::{Feature, comparers::is_disjoint},
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
