use std::collections::HashSet;

use bumpalo::{
  Bump,
  collections::{CollectIn, Vec},
};
use compact_str::CompactString;
use macros::scoring_feature;
use tracing::instrument;

use crate::{
  matching::{
    Feature,
    comparers::{is_disjoint, is_disjoint_chars},
    extractors::{self, extract_numbers},
  },
  model::{Entity, HasProperties, SearchEntity},
};

type MismatchExtractor<'e> = &'e (dyn Fn(&'_ dyn HasProperties) -> &[String] + Send + Sync);
type MismatchMatcher = Option<fn(bump: &Bump, lhs: &[String], rhs: &[String]) -> f64>;

pub struct SimpleMismatch<'e> {
  name: &'static str,
  extractor: MismatchExtractor<'e>,
  matcher: MismatchMatcher,
}

impl<'e> SimpleMismatch<'e> {
  pub fn new(name: &'static str, extractor: MismatchExtractor<'e>, matcher: MismatchMatcher) -> Self {
    SimpleMismatch { name, extractor, matcher }
  }
}

impl<'e> Feature<'e> for SimpleMismatch<'e> {
  fn name(&self) -> &'static str {
    self.name
  }

  #[instrument(level = "trace", name = "simple_mismatch", skip_all, fields(entity_id = rhs.id, mismatch = self.name))]
  fn score_feature(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity) -> f64 {
    let lhs = (self.extractor)(lhs);

    if lhs.is_empty() {
      return 0.0;
    }

    let rhs = (self.extractor)(rhs);

    if rhs.is_empty() {
      return 0.0;
    }

    match self.matcher {
      Some(func) => (func)(bump, lhs, rhs),

      None => match is_disjoint(lhs, rhs) {
        true => 1.0,
        false => 0.0,
      },
    }
  }
}

#[scoring_feature(NumbersMismatch, name = "numbers_mismatch")]
fn score_feature(&self, _bump: &Bump, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  let (lhs_numbers, rhs_numbers) = match lhs.schema.is_a("Address") {
    true => (
      HashSet::<String>::from_iter(extract_numbers(lhs.property("full").iter()).map(ToOwned::to_owned)),
      HashSet::<String>::from_iter(extract_numbers(rhs.property("full").iter()).map(ToOwned::to_owned)),
    ),
    false => (
      HashSet::<String>::from_iter(extract_numbers(lhs.names_and_aliases()).map(ToOwned::to_owned)),
      HashSet::<String>::from_iter(extract_numbers(rhs.names_and_aliases()).map(ToOwned::to_owned)),
    ),
  };

  let base = lhs_numbers.len().min(rhs_numbers.len());
  let mismatches = lhs_numbers.difference(&rhs_numbers).count();

  mismatches as f64 / base.max(1) as f64
}

pub fn dob_year_disjoint<S: AsRef<str>>(bump: &Bump, lhs: &[S], rhs: &[S]) -> f64 {
  let lhs_years = lhs.iter().map(|d| d.as_ref().chars().take(4).collect::<CompactString>()).collect_in::<Vec<_>>(bump);
  let rhs_years = rhs.iter().map(|d| d.as_ref().chars().take(4).collect::<CompactString>()).collect_in::<Vec<_>>(bump);

  match is_disjoint(&lhs_years, &rhs_years) {
    true => 1.0,
    false => 0.0,
  }
}

pub fn dob_day_disjoint<S: AsRef<str>>(bump: &Bump, lhs: &[S], rhs: &[S]) -> f64 {
  if dob_year_disjoint(bump, lhs, rhs) > 0.0 {
    return 1.0;
  }

  let lhs_months = lhs.iter().map(|d| d.as_ref().chars().skip(5).collect::<std::vec::Vec<char>>()).collect_in::<Vec<_>>(bump);
  let rhs_months = rhs.iter().map(|d| d.as_ref().chars().skip(5).collect::<std::vec::Vec<char>>()).collect_in::<Vec<_>>(bump);

  if !is_disjoint_chars(&lhs_months, &rhs_months) {
    return 0.0;
  }

  let lhs_flipped = lhs_months.into_iter().filter(|d| d.len() == 5).map(extractors::flip_date).collect_in::<Vec<_>>(bump);

  if !is_disjoint_chars(&lhs_flipped, &rhs_months) {
    return 0.5;
  }

  1.0
}

#[cfg(test)]
mod tests {
  use crate::{
    matching::Feature,
    tests::{e, se},
  };

  use bumpalo::Bump;

  #[test]
  fn dob_year_disjoint() {
    assert_eq!(super::dob_year_disjoint(&Bump::new(), &["1988-07-22"], &["1989-07-22"]), 1.0);
    assert_eq!(super::dob_year_disjoint(&Bump::new(), &["2022-07-22"], &["2022-07-22"]), 0.0);
  }

  #[test]
  fn dob_day_disjoint() {
    assert_eq!(super::dob_day_disjoint(&Bump::new(), &["2022-07-22"], &["2022-07-22"]), 0.0);

    assert_eq!(super::dob_day_disjoint(&Bump::new(), &["2022-01-02"], &["2022-10-11"]), 1.0);
    assert_eq!(super::dob_day_disjoint(&Bump::new(), &["2022-01-02"], &["2022-01-02"]), 0.0);
    assert_eq!(super::dob_day_disjoint(&Bump::new(), &["2022-01-02"], &["2022-02-01"]), 0.5);
    assert_eq!(super::dob_day_disjoint(&Bump::new(), &["1987-07-20", "2022-01-02"], &["2022-03-04", "1987-20-07"]), 0.5);
  }

  #[test]
  fn numbers_mismatch() {
    let lhs = se("Person").properties(&[("name", &["123 Limited", "The answer is 42"])]).call();
    let rhs = e("Person").properties(&[("name", &["The 123 Name", "Avenue 4123"])]).call();

    assert_eq!(super::NumbersMismatch.score_feature(&Bump::new(), &lhs, &rhs), 0.5);
  }
}
