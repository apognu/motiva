use ahash::HashSet;

use bumpalo::{
  Bump,
  collections::{CollectIn, Vec},
};
use compact_str::CompactString;
use itertools::Itertools;
use jiff::{
  SpanTotal, Unit,
  civil::{Date, date},
};
use libmotiva_macros::scoring_feature;
use tracing::instrument;

use crate::{
  matching::{
    Feature,
    comparers::{is_disjoint, is_disjoint_chars},
    extractors::{self, extract_numbers},
    matchers::match_::MatchExtractor,
  },
  model::{Entity, HasProperties, PropertyFilter, SearchEntity},
};

type MismatchMatcher = Option<fn(bump: &Bump, lhs: &[String], rhs: &[String]) -> f64>;

pub(crate) struct SimpleMismatch<'e> {
  name: &'static str,
  extractor: MatchExtractor<'e>,
  matcher: MismatchMatcher,
}

impl<'e> SimpleMismatch<'e> {
  pub(crate) fn new(name: &'static str, extractor: MatchExtractor<'e>, matcher: MismatchMatcher) -> &'static Self {
    Box::leak(Box::new(SimpleMismatch { name, extractor, matcher }))
  }
}

impl<'e> Feature for SimpleMismatch<'e> {
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
      Some(func) => (func)(bump, lhs.as_ref(), rhs.as_ref()),

      None => match is_disjoint(lhs.as_ref(), rhs.as_ref()) {
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
      HashSet::<String>::from_iter(extract_numbers(lhs.props(&["full"]).iter()).map(ToOwned::to_owned)),
      HashSet::<String>::from_iter(extract_numbers(rhs.props(&["full"]).iter()).map(ToOwned::to_owned)),
    ),
    false => (
      HashSet::<String>::from_iter(extract_numbers(lhs.prop_group("name", PropertyFilter::All).iter()).map(ToOwned::to_owned)),
      HashSet::<String>::from_iter(extract_numbers(rhs.prop_group("name", PropertyFilter::All).iter()).map(ToOwned::to_owned)),
    ),
  };

  let base = lhs_numbers.len().min(rhs_numbers.len());
  let mismatches = lhs_numbers.difference(&rhs_numbers).count();

  mismatches as f64 / base.max(1) as f64
}

pub(crate) fn dob_progressive_match<S: AsRef<str>>(lhs: &[S], rhs: &[S]) -> f64 {
  const YEAR_EPSILON: u64 = 1;
  const MONTH_EPSILON: f64 = 3.0;
  const DAY_THRESHOLDS: (f64, f64) = (1.0, 60.0);

  fn compare_year(lhs: u64, rhs: u64) -> f64 {
    let diff = lhs.abs_diff(rhs);

    if diff == 0 {
      return 1.0;
    }
    if diff <= YEAR_EPSILON {
      return 1.0 / (diff + 1) as f64;
    }

    0.0
  }

  fn compare_months(diff: f64) -> f64 {
    match diff.abs() {
      diff if diff > MONTH_EPSILON => 0.0,
      diff => 1.0 / diff.max(1.0),
    }
  }

  fn compare_days(diff: f64) -> f64 {
    match diff.abs() {
      diff if diff <= DAY_THRESHOLDS.0 => 1.0,
      diff if diff > DAY_THRESHOLDS.1 => 0.0,
      diff => (DAY_THRESHOLDS.1 - diff) / (DAY_THRESHOLDS.1 - DAY_THRESHOLDS.0),
    }
  }

  let mut max = 0.0f64;

  for (lhs, rhs) in lhs.iter().cartesian_product(rhs) {
    let (lhs, rhs) = (lhs.as_ref(), rhs.as_ref());

    if !lhs.is_ascii() || !rhs.is_ascii() {
      continue;
    }

    match (lhs.len(), rhs.len()) {
      (4, other) | (other, 4) if other >= 4 => {
        let Ok(lhs) = lhs[..4].parse::<u64>() else { continue };
        let Ok(rhs) = rhs[..4].parse::<u64>() else { continue };

        max = max.max(compare_year(lhs, rhs));
      }

      (7, other) | (other, 7) if other >= 7 => {
        let mut ldate: std::vec::Vec<char> = lhs.chars().chain(['-', '0', '1']).collect();
        let mut rdate: std::vec::Vec<char> = rhs.chars().chain(['-', '0', '1']).collect();

        ldate[4] = '-';
        rdate[4] = '-';

        let Ok(ldate) = ldate.into_iter().take(10).collect::<String>().parse::<Date>() else { continue };
        let Ok(rdate) = rdate.into_iter().take(10).collect::<String>().parse::<Date>() else { continue };

        if let Ok(diff) = (ldate - rdate).total((Unit::Month, date(2026, 1, 1))) {
          max = max.max(compare_months(diff));
        }
      }

      (10, other) | (other, 10) if other >= 10 => {
        let mut ldate: std::vec::Vec<char> = lhs.chars().collect();
        let mut rdate: std::vec::Vec<char> = rhs.chars().collect();

        (ldate[4], ldate[7]) = ('-', '-');
        (rdate[4], rdate[7]) = ('-', '-');

        let Ok(ldate) = ldate.into_iter().collect::<String>().parse::<Date>() else { continue };
        let Ok(rdate) = rdate.into_iter().collect::<String>().parse::<Date>() else { continue };

        if let Ok(diff) = (ldate - rdate).total(SpanTotal::from(Unit::Day).days_are_24_hours()) {
          max = max.max(compare_days(diff));
        }
      }

      _ => {}
    }
  }

  max
}

pub(crate) fn dob_year_disjoint<S: AsRef<str>>(bump: &Bump, lhs: &[S], rhs: &[S]) -> f64 {
  // A date of birth is intrinsically invalid if it is not plain ASCII; such
  // values are skipped so they neither match nor trigger a mismatch penalty.
  let lhs_years = lhs
    .iter()
    .filter(|d| d.as_ref().is_ascii())
    .map(|d| d.as_ref().chars().take(4).collect::<CompactString>())
    .collect_in::<Vec<_>>(bump);
  let rhs_years = rhs
    .iter()
    .filter(|d| d.as_ref().is_ascii())
    .map(|d| d.as_ref().chars().take(4).collect::<CompactString>())
    .collect_in::<Vec<_>>(bump);

  if lhs_years.is_empty() || rhs_years.is_empty() {
    return 0.0;
  }

  match is_disjoint(&lhs_years, &rhs_years) {
    true => 1.0,
    false => 0.0,
  }
}

pub(crate) fn dob_day_disjoint<S: AsRef<str>>(bump: &Bump, lhs: &[S], rhs: &[S]) -> f64 {
  // Non-ASCII dates are intrinsically invalid and are skipped; requiring ASCII
  // also makes the byte length a valid proxy for the character count.
  let lhs_months = lhs.iter().filter(|d| d.as_ref().is_ascii() && d.as_ref().len() >= 10).map(extract_month_day).collect_in::<Vec<_>>(bump);
  let rhs_months = rhs.iter().filter(|d| d.as_ref().is_ascii() && d.as_ref().len() >= 10).map(extract_month_day).collect_in::<Vec<_>>(bump);

  if lhs_months.is_empty() || rhs_months.is_empty() {
    return 0.0;
  }

  if dob_year_disjoint(bump, lhs, rhs) == 1.0 {
    return 1.0;
  }

  if !is_disjoint_chars(&lhs_months, &rhs_months) {
    return 0.0;
  }

  let lhs_flipped = lhs_months.into_iter().filter(|d| d.len() == 4).map(extractors::flip_date).collect_in::<Vec<_>>(bump);

  if !is_disjoint_chars(&lhs_flipped, &rhs_months) {
    return 0.5;
  }

  1.0
}

fn extract_month_day<S: AsRef<str>>(date: S) -> std::vec::Vec<char> {
  date.as_ref().chars().skip(5).enumerate().filter(|(idx, _)| idx != &2).map(|(_, c)| c).collect::<std::vec::Vec<char>>()
}

#[cfg(test)]
mod tests {
  use crate::{
    matching::Feature,
    model::{Entity, SearchEntity},
  };

  use bumpalo::Bump;

  #[test]
  fn dob_year_disjoint() {
    assert_eq!(super::dob_year_disjoint(&Bump::new(), &["1988-07-22"], &["1989-07-22"]), 1.0);
    assert_eq!(super::dob_year_disjoint(&Bump::new(), &["1988/07/22"], &["1989x07x22"]), 1.0);
    assert_eq!(super::dob_year_disjoint(&Bump::new(), &["2022-07-22"], &["2022-07-22"]), 0.0);
    assert_eq!(super::dob_year_disjoint(&Bump::new(), &["2022x07x22"], &["2022+07+22"]), 0.0);

    assert_eq!(super::dob_year_disjoint(&Bump::new(), &["1988💃07💃22"], &["1989-07-22"]), 0.0);
    assert_eq!(super::dob_year_disjoint(&Bump::new(), &["1988💃07💃22", "1988-07-22"], &["1989-07-22"]), 1.0);
  }

  #[test]
  fn dob_day_disjoint() {
    assert_eq!(super::dob_day_disjoint(&Bump::new(), &["2022-07-22"], &["2022-07-22"]), 0.0);
    assert_eq!(super::dob_day_disjoint(&Bump::new(), &["2022/07/22"], &["2022x07x22"]), 0.0);

    assert_eq!(super::dob_day_disjoint(&Bump::new(), &["2022-01-02"], &["2022-10-11"]), 1.0);
    assert_eq!(super::dob_day_disjoint(&Bump::new(), &["2022-01-02"], &["2022-01-02"]), 0.0);
    assert_eq!(super::dob_day_disjoint(&Bump::new(), &["2022-01-02"], &["2022-02-01"]), 0.5);
    assert_eq!(super::dob_day_disjoint(&Bump::new(), &["1987-07-20", "2022-01-02"], &["2022-03-04", "1987-20-07"]), 0.5);
    assert_eq!(super::dob_day_disjoint(&Bump::new(), &["1987/07/20", "2022o01o02"], &["2022*03*04", "1987*20*07"]), 0.5);

    assert_eq!(super::dob_day_disjoint(&Bump::new(), &["1987-07-20"], &["1987💃20💃07"]), 0.0);
    assert_eq!(super::dob_day_disjoint(&Bump::new(), &["1987-07-20"], &["1987💃20💃07", "1987-07-20"]), 0.0);
    assert_eq!(super::dob_day_disjoint(&Bump::new(), &["1987-07-20"], &["1987💃20💃07", "1987-07-21"]), 1.0);
  }

  #[test]
  fn dob_progressive_match() {
    // Year level (YEAR_EPSILON = 1): exact => 1.0, one year off => 1/(diff+1), beyond => 0.0.
    assert_eq!(super::dob_progressive_match(&["1988"], &["1988"]), 1.0);
    assert_eq!(super::dob_progressive_match(&["1988"], &["1989"]), 0.5);
    assert_eq!(super::dob_progressive_match(&["1988"], &["1990"]), 0.0);

    // Day level (DAY_THRESHOLDS = (1.0, 60.0)): (60 - diff) / 59, clamped to [0, 1].
    assert_eq!(super::dob_progressive_match(&["1988-07-22"], &["1988-07-22"]), 1.0);
    assert!((super::dob_progressive_match(&["1988-07-22"], &["1988-07-27"]) - (60.0 - 5.0) / (60.0 - 1.0)).abs() < 1e-9);
    assert!((super::dob_progressive_match(&["1988-07-22"], &["1988-07-29"]) - (60.0 - 7.0) / (60.0 - 1.0)).abs() < 1e-9);
    assert_eq!(super::dob_progressive_match(&["1988-07-22"], &["1990-07-22"]), 0.0);

    // Day-level interpolation is symmetric.
    let interpolated = super::dob_progressive_match(&["2021-01-01"], &["2021-02-10"]);
    assert!((interpolated - (60.0 - 40.0) / (60.0 - 1.0)).abs() < 1e-9);
    assert_eq!(super::dob_progressive_match(&["2021-02-10"], &["2021-01-01"]), interpolated);

    // Non-dash separators are normalized before parsing.
    assert_eq!(super::dob_progressive_match(&["1988/07/22"], &["1988.07.22"]), 1.0);

    // The best pair across the cartesian product wins.
    assert!((super::dob_progressive_match(&["1970-01-01", "1988-07-22"], &["1988-07-24"]) - (60.0 - 2.0) / (60.0 - 1.0)).abs() < 1e-9);

    // Comparing mixed precisions collapses to the coarser (shortest) level; the finer operand's extra digits are ignored.
    assert_eq!(super::dob_progressive_match(&["1988"], &["1988-06"]), 1.0);
    assert_eq!(super::dob_progressive_match(&["1989"], &["1988-06"]), 0.5);
    assert_eq!(super::dob_progressive_match(&["1988"], &["1988-12-31"]), 1.0);
    assert_eq!(super::dob_progressive_match(&["1988-07"], &["1988-07-31"]), 1.0);
    assert!((super::dob_progressive_match(&["1988-07-01"], &["1988-07-31"]) - (60.0 - 30.0) / (60.0 - 1.0)).abs() < 1e-9);

    // Month level (MONTH_EPSILON = 3.0): a span beyond three months is a mismatch.
    assert_eq!(super::dob_progressive_match(&["1990-05"], &["1988-07"]), 0.0);

    // Empty inputs and unparseable dates score 0.0.
    assert_eq!(super::dob_progressive_match::<&str>(&[], &["1988"]), 0.0);
    assert_eq!(super::dob_progressive_match::<&str>(&["1988"], &[]), 0.0);
    assert_eq!(super::dob_progressive_match(&["1988-99-99"], &["1988-07-22"]), 0.0);
    assert!((super::dob_progressive_match(&["1988-99-99", "1988-07-22"], &["1988-07-24"]) - (60.0 - 2.0) / (60.0 - 1.0)).abs() < 1e-9);

    // Non-ASCII inputs
    assert_eq!(super::dob_progressive_match(&["a💃xx"], &["1988"]), 0.0);
    assert_eq!(super::dob_progressive_match(&["1988-07-22"], &["💃💃💃"]), 0.0);
    assert_eq!(super::dob_progressive_match(&["1988💃07💃22"], &["1988-07-22"]), 0.0);
    assert_eq!(super::dob_progressive_match(&["💃💃💃", "1988"], &["1988"]), 1.0);
  }

  #[test]
  fn numbers_mismatch() {
    let lhs = SearchEntity::builder("Person").properties(&[("name", &["123 Limited", "The answer is 42"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["The 123 Name", "Avenue 4123"])]).build();

    assert_eq!(super::NumbersMismatch.score_feature(&Bump::new(), &lhs, &rhs), 0.5);
  }
}
