use std::collections::HashSet;

use compact_str::CompactString;
use tracing::instrument;

use crate::{
  matching::{Feature, extractors},
  model::{Entity, HasProperties, SearchEntity},
};

type MismatchExtractor<'e> = &'e (dyn Fn(&'_ dyn HasProperties) -> &[String] + Send + Sync);
type MismatchMatcher = Option<fn(lhs: &[String], rhs: &[String]) -> f64>;

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

  #[instrument(level = "trace", name = "simple_mismatch", skip_all, fields(mismatch = self.name))]
  fn score_feature(&self, lhs: &SearchEntity, rhs: &Entity) -> f64 {
    let lhs = (self.extractor)(lhs);

    if lhs.is_empty() {
      return 0.0;
    }

    let rhs = (self.extractor)(rhs);

    if rhs.is_empty() {
      return 0.0;
    }

    match self.matcher {
      Some(func) => (func)(lhs, rhs),

      None => match extractors::is_disjoint(lhs, rhs) {
        true => 1.0,
        false => 0.0,
      },
    }
  }
}

pub fn dob_year_disjoint<S: AsRef<str>>(lhs: &[S], rhs: &[S]) -> f64 {
  let lhs_years = lhs.iter().map(|d| d.as_ref().chars().take(4).collect::<CompactString>()).collect::<HashSet<_>>();
  let rhs_years = rhs.iter().map(|d| d.as_ref().chars().take(4).collect::<CompactString>()).collect::<HashSet<_>>();

  match lhs_years.is_disjoint(&rhs_years) {
    true => 1.0,
    false => 0.0,
  }
}

pub fn dob_day_disjoint<S: AsRef<str>>(lhs: &[S], rhs: &[S]) -> f64 {
  if dob_year_disjoint(lhs, rhs) > 0.0 {
    return 1.0;
  }

  let lhs_months = lhs.iter().map(|d| d.as_ref().chars().skip(5).collect::<Vec<char>>()).collect::<HashSet<_>>();
  let rhs_months = rhs.iter().map(|d| d.as_ref().chars().skip(5).collect::<Vec<char>>()).collect::<HashSet<_>>();

  if !lhs_months.is_disjoint(&rhs_months) {
    return 0.0;
  }

  let lhs_flipped = lhs_months.into_iter().filter(|d| d.len() == 5).map(extractors::flip_date).collect::<HashSet<_>>();

  if !lhs_flipped.is_disjoint(&rhs_months) {
    return 0.5;
  }

  1.0
}

#[cfg(test)]
mod tests {
  #[test]
  fn dob_year_disjoint() {
    assert_eq!(super::dob_year_disjoint(&["1988-07-22"], &["1989-07-22"]), 1.0);
    assert_eq!(super::dob_year_disjoint(&["2022-07-22"], &["2022-07-22"]), 0.0);
  }

  #[test]
  fn dob_day_disjoint() {
    assert_eq!(super::dob_day_disjoint(&["2022-07-22"], &["2022-07-22"]), 0.0);

    assert_eq!(super::dob_day_disjoint(&["2022-01-02"], &["2022-10-11"]), 1.0);
    assert_eq!(super::dob_day_disjoint(&["2022-01-02"], &["2022-01-02"]), 0.0);
    assert_eq!(super::dob_day_disjoint(&["2022-01-02"], &["2022-02-01"]), 0.5);
    assert_eq!(super::dob_day_disjoint(&["1987-07-20", "2022-01-02"], &["2022-03-04", "1987-20-07"]), 0.5);
  }
}
