use bumpalo::Bump;
use compact_str::CompactString;
use itertools::Itertools;
use jiff::{
  RoundMode, Unit,
  civil::{Date, DateDifference},
};

use crate::{
  matching::{Detail, Feature, ScoreResult, matchers::NO_DATA},
  model::{Entity, HasProperties, SearchEntity},
};

/// Matches birth dates progressively (year, then month, then day), allowing for
/// approximate dates and month/day transpositions.
pub(crate) struct DobProgressiveMatch;

impl Feature for DobProgressiveMatch {
  fn name(&self) -> &'static str {
    "dob_progressive_match"
  }

  fn score(&self, _bump: &Bump, lhs: &SearchEntity, rhs: &Entity, explain: bool) -> ScoreResult {
    let lhs_dates = lhs.props(&["birthDate"]);
    let rhs_dates = rhs.props(&["birthDate"]);

    if lhs_dates.is_empty() || rhs_dates.is_empty() {
      return (0.0, explain.then_some(Detail::Note(NO_DATA))).into();
    }

    let (score, best) = dob_progressive(&lhs_dates[..], &rhs_dates[..], explain);

    let detail = explain.then(|| match best {
      Some(best) => best.into_detail(),
      None => Detail::Note("no comparable birth dates"),
    });

    (score, detail).into()
  }
}

#[derive(Clone, Copy)]
enum DobUnit {
  Year,
  Month,
  Day,
}

impl DobUnit {
  fn label(self) -> &'static str {
    match self {
      DobUnit::Year => "year",
      DobUnit::Month => "month",
      DobUnit::Day => "day",
    }
  }
}

struct DobMatch {
  lhs: CompactString,
  rhs: CompactString,
  unit: DobUnit,
  diff: i64,
  swapped: bool,
}

impl DobMatch {
  fn into_detail(self) -> Detail {
    if self.diff == 0 && !self.swapped {
      return Detail::Equal(self.lhs, self.rhs);
    }

    let note = match (self.diff, self.swapped) {
      (0, _) => CompactString::const_new("day/month swapped"),
      (diff, false) => format!("{diff} {}{} apart", self.unit.label(), if diff == 1 { "" } else { "s" }).into(),
      (diff, true) => format!("{diff} {}{} apart, day/month swapped", self.unit.label(), if diff == 1 { "" } else { "s" }).into(),
    };

    Detail::Approximate { lhs: self.lhs, rhs: self.rhs, note }
  }
}

fn dob_progressive<S: AsRef<str>>(lhs: &[S], rhs: &[S], explain: bool) -> (f64, Option<DobMatch>) {
  const YEAR_EPSILON: u64 = 1;
  const MONTH_EPSILON: i32 = 3;
  const DAY_THRESHOLDS: (i32, i32) = (1, 60);
  const SWAPPED_FACTOR: f64 = 0.5;

  #[inline]
  fn compare_year(lhs: u64, rhs: u64) -> f64 {
    let diff = lhs.abs_diff(rhs);

    if diff == 0 {
      return 1.0;
    }
    if diff <= YEAR_EPSILON {
      return 1.0 / (diff + 1) as f64;
    }

    -1.0
  }

  #[inline]
  fn compare_months(diff: i32) -> f64 {
    match diff.abs() {
      0 => 1.0,
      diff if diff >= MONTH_EPSILON => -1.0,
      diff => 1.0 - (1.0 * (diff as f64 / MONTH_EPSILON as f64)),
    }
  }

  #[inline]
  fn compare_days(diff: i32) -> f64 {
    match diff.abs() {
      diff if diff <= DAY_THRESHOLDS.0 => 1.0,
      diff if diff >= DAY_THRESHOLDS.1 => -1.0,
      diff => (DAY_THRESHOLDS.1 - diff) as f64 / (DAY_THRESHOLDS.1 - DAY_THRESHOLDS.0) as f64,
    }
  }

  #[inline]
  fn swap_month_day(date: &str) -> Option<CompactString> {
    if date.len() != 10 || !date.is_ascii() {
      return None;
    }

    let reversed = date.as_bytes();

    // Skip work if month == day
    if reversed[5..7] == reversed[8..10] {
      return None;
    }

    let mut out = [b'-'; 10];
    out[0..4].copy_from_slice(&reversed[0..4]);
    out[5..7].copy_from_slice(&reversed[8..10]);
    out[8..10].copy_from_slice(&reversed[5..7]);

    let swapped = std::str::from_utf8(&out).ok()?;

    // Check if valid date
    swapped.parse::<Date>().ok()?;

    Some(CompactString::from(swapped))
  }

  #[inline]
  fn score_pair(lhs: &str, rhs: &str) -> (f64, Option<(DobUnit, i64)>) {
    if !lhs.is_ascii() || !rhs.is_ascii() {
      return (0.0, None);
    }

    match (lhs.len(), rhs.len()) {
      (4, other) | (other, 4) if other >= 4 => {
        let Ok(lhs) = lhs[..4].parse::<u64>() else { return (0.0, None) };
        let Ok(rhs) = rhs[..4].parse::<u64>() else { return (0.0, None) };

        (compare_year(lhs, rhs), Some((DobUnit::Year, lhs.abs_diff(rhs) as i64)))
      }

      (7, other) | (other, 7) if other >= 7 => {
        let mut ldate: std::vec::Vec<char> = lhs.chars().take(7).chain(['-', '1', '5']).collect();
        let mut rdate: std::vec::Vec<char> = rhs.chars().take(7).chain(['-', '1', '5']).collect();

        ldate[4] = '-';
        rdate[4] = '-';

        let Ok(ldate) = ldate.into_iter().take(10).collect::<CompactString>().parse::<Date>() else {
          return (0.0, None);
        };
        let Ok(rdate) = rdate.into_iter().take(10).collect::<CompactString>().parse::<Date>() else {
          return (0.0, None);
        };

        match ldate.min(rdate).until(DateDifference::new(ldate.max(rdate)).smallest(Unit::Month).mode(RoundMode::HalfExpand)) {
          Ok(diff) => (compare_months(diff.get_months()), Some((DobUnit::Month, diff.get_months().unsigned_abs() as i64))),
          Err(_) => (0.0, None),
        }
      }

      (10, other) | (other, 10) if other >= 10 => {
        let mut ldate: std::vec::Vec<char> = lhs.chars().collect();
        let mut rdate: std::vec::Vec<char> = rhs.chars().collect();

        (ldate[4], ldate[7]) = ('-', '-');
        (rdate[4], rdate[7]) = ('-', '-');

        let Ok(ldate) = ldate.into_iter().collect::<CompactString>().parse::<Date>() else {
          return (0.0, None);
        };
        let Ok(rdate) = rdate.into_iter().collect::<CompactString>().parse::<Date>() else {
          return (0.0, None);
        };

        match ldate.min(rdate).until(DateDifference::new(ldate.max(rdate)).smallest(Unit::Day).mode(RoundMode::HalfExpand)) {
          Ok(diff) => (compare_days(diff.get_days()), Some((DobUnit::Day, diff.get_days().unsigned_abs() as i64))),
          Err(_) => (0.0, None),
        }
      }

      _ => (0.0, None),
    }
  }

  let mut max = f64::MIN;
  let mut best: Option<DobMatch> = None;

  for (lhs, rhs) in lhs.iter().cartesian_product(rhs) {
    let (lhs, rhs) = (lhs.as_ref(), rhs.as_ref());

    let (score, comparison) = score_pair(lhs, rhs);

    if score > max {
      max = score;

      if explain {
        best = comparison.map(|(unit, diff)| DobMatch {
          lhs: lhs.into(),
          rhs: rhs.into(),
          unit,
          diff,
          swapped: false,
        });
      }
    }

    if let Some(lswap) = swap_month_day(lhs).as_deref() {
      let (score, comparison) = score_pair(lswap, rhs);

      if score > 0.0 {
        let weighted = score * SWAPPED_FACTOR;

        if weighted > max {
          max = weighted;

          if explain {
            best = comparison.map(|(unit, diff)| DobMatch {
              lhs: lhs.into(),
              rhs: rhs.into(),
              unit,
              diff,
              swapped: true,
            });
          }
        }
      }
    }
  }

  if max == f64::MIN {
    return (0.0, None);
  }

  (max, best)
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;
  use float_cmp::assert_approx_eq;

  use crate::{
    matching::{Feature, matchers::NO_DATA},
    model::{Entity, SearchEntity},
  };

  #[test]
  fn dob_progressive_details() {
    fn detail(lhs: &str, rhs: &str) -> String {
      let l = SearchEntity::builder("Person").properties(&[("birthDate", &[lhs])]).build();
      let r = Entity::builder("Person").properties(&[("birthDate", &[rhs])]).build();

      super::DobProgressiveMatch.score(&Bump::new(), &l, &r, true).1.unwrap().to_string()
    }

    assert_eq!(detail("1988-07-22", "1988-07-22"), "1988-07-22 == 1988-07-22");
    assert_eq!(detail("1988-07-22", "1988-07-27"), "1988-07-22 ~= 1988-07-27 (5 days apart)");
    assert_eq!(detail("1988", "1989"), "1988 ~= 1989 (1 year apart)");
    assert_eq!(detail("1988-05", "1988-06"), "1988-05 ~= 1988-06 (1 month apart)");
    assert_eq!(detail("1988-07-05", "1988-05-07"), "1988-07-05 ~= 1988-05-07 (day/month swapped)");
    assert_eq!(detail("1988", "1990"), "1988 ~= 1990 (2 years apart)");

    // No date on one side.
    let l = SearchEntity::builder("Person").properties(&[]).build();
    let r = Entity::builder("Person").properties(&[("birthDate", &["1988"])]).build();

    assert_eq!(super::DobProgressiveMatch.score(&Bump::new(), &l, &r, true).1.unwrap().to_string(), NO_DATA);
  }

  fn match_bare(lhs: &[&str], rhs: &[&str]) -> f64 {
    super::dob_progressive(lhs, rhs, false).0
  }

  #[test]
  fn dob_progressive() {
    // Year level (YEAR_EPSILON = 1): exact => 1.0, one year off => 1/(diff+1), beyond => -1.0 (active mismatch).
    assert_eq!(match_bare(&["1988"], &["1988"]), 1.0);
    assert_eq!(match_bare(&["1988"], &["1989"]), 0.5);
    assert_eq!(match_bare(&["1988"], &["1990"]), -1.0);

    // Day level (DAY_THRESHOLDS = (1.0, 60.0)): (60 - diff) / 59 within range, -1.0 beyond.
    assert_eq!(match_bare(&["1988-07-22"], &["1988-07-22"]), 1.0);
    assert_eq!(match_bare(&["1988-07-22"], &["1988-07-27"]), 55.0 / 59.0);
    assert_eq!(match_bare(&["1988-07-22"], &["1988-07-29"]), 53.0 / 59.0);
    assert_eq!(match_bare(&["1988-07-22"], &["1990-07-22"]), -1.0);

    // Day-level interpolation is symmetric.
    let symmetric = match_bare(&["2021-01-01"], &["2021-02-10"]);
    assert_eq!(symmetric, 20.0 / 59.0);
    assert_eq!(match_bare(&["2021-02-10"], &["2021-01-01"]), symmetric);

    // Non-dash separators are normalized before parsing.
    assert_eq!(match_bare(&["1988/07/22"], &["1988.07.22"]), 1.0);

    // The best pair across the cartesian product wins.
    assert_eq!(match_bare(&["1970-01-01", "1988-07-22"], &["1988-07-24"]), 58.0 / 59.0);

    // Comparing mixed precisions collapses to the coarser (shortest) level; the finer operand's extra digits are ignored.
    assert_eq!(match_bare(&["1988"], &["1988-06"]), 1.0);
    assert_eq!(match_bare(&["1989"], &["1988-06"]), 0.5);
    assert_eq!(match_bare(&["1988"], &["1988-12-31"]), 1.0);
    assert_eq!(match_bare(&["1988-07"], &["1988-07-31"]), 1.0);
    assert_eq!(match_bare(&["1988-07-01"], &["1988-07-31"]), 30.0 / 59.0);

    // Month level (MONTH_EPSILON = 3.0): a span beyond three months is an active mismatch.
    assert_eq!(match_bare(&["1988-04"], &["1988-07"]), -1.0);
    assert_approx_eq!(f64, match_bare(&["1988-05"], &["1988-06"]), 2.0 / 3.0);
    assert_approx_eq!(f64, match_bare(&["1988-05"], &["1988-07"]), 1.0 / 3.0);
    assert_eq!(match_bare(&["1988-05"], &["1988-08", "1988-05-31"]), 1.0);
    assert_approx_eq!(f64, match_bare(&["1988-12"], &["1989-01"]), 2.0 / 3.0);
    assert_eq!(match_bare(&["1988-12"], &["1989-12"]), -1.0);

    // No dates can be compared (empty inputs or unparseable values) => 0.0, never a penalty.
    assert_eq!(match_bare(&[], &["1988"]), 0.0);
    assert_eq!(match_bare(&["1988"], &[]), 0.0);
    assert_eq!(match_bare(&[], &[]), 0.0);
    assert_eq!(match_bare(&[""], &[""]), 0.0);
    assert_eq!(match_bare(&["1988-99-99"], &["1988-07-22"]), 0.0);
    assert_eq!(match_bare(&["1988-99-99", "1988-07-22"], &["1988-07-24"]), 58.0 / 59.0);

    // Swapping month and day (left-hand side only; a flipped match is weighted by 0.5).
    assert_eq!(match_bare(&["1988-07-05"], &["1988-05-07"]), 0.5);
    assert_eq!(match_bare(&["1988-05-11"], &["1988-11"]), 0.5);
    assert_eq!(match_bare(&["1988-07-22"], &["1988-22-07"]), 0.0);

    // A flipped interpretation only ever boosts toward a match; it must never soften a
    // genuine mismatch penalty, even when the left-hand side has a valid transposition.
    assert_eq!(match_bare(&["1988-07-05"], &["1995-07-05"]), -1.0);
    assert_eq!(match_bare(&["1988-07-05"], &["1988-11-30"]), -1.0);
    // A discounted flip does not override a stronger direct match.
    assert_eq!(match_bare(&["1988-07-11"], &["1988-07-13"]), 58.0 / 59.0);

    // Non-ASCII inputs
    assert_eq!(match_bare(&["a💃xx"], &["1988"]), 0.0);
    assert_eq!(match_bare(&["1988-07-22"], &["💃💃💃"]), 0.0);
    assert_eq!(match_bare(&["1988💃07💃22"], &["1988-07-22"]), 0.0);
    assert_eq!(match_bare(&["💃💃💃", "1988"], &["1988"]), 1.0);

    // Invalid
    assert_eq!(match_bare(&["1988/01/31"], &["1988/31/31"]), 0.0);
    assert_eq!(match_bare(&["1988/31/31"], &["1988/01/31"]), 0.0);
  }
}
