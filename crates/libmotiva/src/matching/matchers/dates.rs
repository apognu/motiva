use compact_str::CompactString;
use itertools::Itertools;
use jiff::{
  RoundMode, Unit,
  civil::{Date, DateDifference},
};

pub(crate) fn dob_progressive_match<S: AsRef<str>>(lhs: &[S], rhs: &[S]) -> f64 {
  const YEAR_EPSILON: u64 = 1;
  const MONTH_EPSILON: i32 = 3;
  const DAY_THRESHOLDS: (i32, i32) = (1, 60);

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
  fn score_pair(lhs: &str, rhs: &str) -> f64 {
    if !lhs.is_ascii() || !rhs.is_ascii() {
      return 0.0;
    }

    match (lhs.len(), rhs.len()) {
      (4, other) | (other, 4) if other >= 4 => {
        let Ok(lhs) = lhs[..4].parse::<u64>() else { return 0.0 };
        let Ok(rhs) = rhs[..4].parse::<u64>() else { return 0.0 };

        compare_year(lhs, rhs)
      }

      (7, other) | (other, 7) if other >= 7 => {
        let mut ldate: std::vec::Vec<char> = lhs.chars().take(7).chain(['-', '1', '5']).collect();
        let mut rdate: std::vec::Vec<char> = rhs.chars().take(7).chain(['-', '1', '5']).collect();

        ldate[4] = '-';
        rdate[4] = '-';

        let Ok(ldate) = ldate.into_iter().take(10).collect::<CompactString>().parse::<Date>() else {
          return 0.0;
        };
        let Ok(rdate) = rdate.into_iter().take(10).collect::<CompactString>().parse::<Date>() else {
          return 0.0;
        };

        ldate
          .min(rdate)
          .until(DateDifference::new(ldate.max(rdate)).smallest(Unit::Month).mode(RoundMode::HalfExpand))
          .map(|diff| compare_months(diff.get_months()))
          .unwrap_or(0.0)
      }

      (10, other) | (other, 10) if other >= 10 => {
        let mut ldate: std::vec::Vec<char> = lhs.chars().collect();
        let mut rdate: std::vec::Vec<char> = rhs.chars().collect();

        (ldate[4], ldate[7]) = ('-', '-');
        (rdate[4], rdate[7]) = ('-', '-');

        let Ok(ldate) = ldate.into_iter().collect::<CompactString>().parse::<Date>() else { return 0.0 };
        let Ok(rdate) = rdate.into_iter().collect::<CompactString>().parse::<Date>() else { return 0.0 };

        ldate
          .min(rdate)
          .until(DateDifference::new(ldate.max(rdate)).smallest(Unit::Day).mode(RoundMode::HalfExpand))
          .map(|diff| compare_days(diff.get_days()))
          .unwrap_or(0.0)
      }

      _ => 0.0,
    }
  }

  let mut max = f64::MIN;

  for (lhs, rhs) in lhs.iter().cartesian_product(rhs) {
    let (lhs, rhs) = (lhs.as_ref(), rhs.as_ref());
    let lswap = swap_month_day(lhs);

    max = max.max(score_pair(lhs, rhs));

    if let Some(lswap) = lswap.as_deref() {
      let score = score_pair(lswap, rhs);

      if score > 0.0 {
        max = max.max(score * 0.5);
      }
    }
  }

  if max == f64::MIN {
    return 0.0;
  }

  max
}

#[cfg(test)]
mod tests {
  use float_cmp::assert_approx_eq;

  #[test]
  fn dob_progressive_match() {
    // Year level (YEAR_EPSILON = 1): exact => 1.0, one year off => 1/(diff+1), beyond => -1.0 (active mismatch).
    assert_eq!(super::dob_progressive_match(&["1988"], &["1988"]), 1.0);
    assert_eq!(super::dob_progressive_match(&["1988"], &["1989"]), 0.5);
    assert_eq!(super::dob_progressive_match(&["1988"], &["1990"]), -1.0);

    // Day level (DAY_THRESHOLDS = (1.0, 60.0)): (60 - diff) / 59 within range, -1.0 beyond.
    assert_eq!(super::dob_progressive_match(&["1988-07-22"], &["1988-07-22"]), 1.0);
    assert_eq!(super::dob_progressive_match(&["1988-07-22"], &["1988-07-27"]), 55.0 / 59.0);
    assert_eq!(super::dob_progressive_match(&["1988-07-22"], &["1988-07-29"]), 53.0 / 59.0);
    assert_eq!(super::dob_progressive_match(&["1988-07-22"], &["1990-07-22"]), -1.0);

    // Day-level interpolation is symmetric.
    let symmetric = super::dob_progressive_match(&["2021-01-01"], &["2021-02-10"]);
    assert_eq!(symmetric, 20.0 / 59.0);
    assert_eq!(super::dob_progressive_match(&["2021-02-10"], &["2021-01-01"]), symmetric);

    // Non-dash separators are normalized before parsing.
    assert_eq!(super::dob_progressive_match(&["1988/07/22"], &["1988.07.22"]), 1.0);

    // The best pair across the cartesian product wins.
    assert_eq!(super::dob_progressive_match(&["1970-01-01", "1988-07-22"], &["1988-07-24"]), 58.0 / 59.0);

    // Comparing mixed precisions collapses to the coarser (shortest) level; the finer operand's extra digits are ignored.
    assert_eq!(super::dob_progressive_match(&["1988"], &["1988-06"]), 1.0);
    assert_eq!(super::dob_progressive_match(&["1989"], &["1988-06"]), 0.5);
    assert_eq!(super::dob_progressive_match(&["1988"], &["1988-12-31"]), 1.0);
    assert_eq!(super::dob_progressive_match(&["1988-07"], &["1988-07-31"]), 1.0);
    assert_eq!(super::dob_progressive_match(&["1988-07-01"], &["1988-07-31"]), 30.0 / 59.0);

    // Month level (MONTH_EPSILON = 3.0): a span beyond three months is an active mismatch.
    assert_eq!(super::dob_progressive_match(&["1988-04"], &["1988-07"]), -1.0);
    assert_approx_eq!(f64, super::dob_progressive_match(&["1988-05"], &["1988-06"]), 2.0 / 3.0);
    assert_approx_eq!(f64, super::dob_progressive_match(&["1988-05"], &["1988-07"]), 1.0 / 3.0);
    assert_eq!(super::dob_progressive_match(&["1988-05"], &["1988-08", "1988-05-31"]), 1.0);
    assert_approx_eq!(f64, super::dob_progressive_match(&["1988-12"], &["1989-01"]), 2.0 / 3.0);
    assert_eq!(super::dob_progressive_match(&["1988-12"], &["1989-12"]), -1.0);

    // No dates can be compared (empty inputs or unparseable values) => 0.0, never a penalty.
    assert_eq!(super::dob_progressive_match::<&str>(&[], &["1988"]), 0.0);
    assert_eq!(super::dob_progressive_match::<&str>(&["1988"], &[]), 0.0);
    assert_eq!(super::dob_progressive_match::<&str>(&[], &[]), 0.0);
    assert_eq!(super::dob_progressive_match::<&str>(&[""], &[""]), 0.0);
    assert_eq!(super::dob_progressive_match(&["1988-99-99"], &["1988-07-22"]), 0.0);
    assert_eq!(super::dob_progressive_match(&["1988-99-99", "1988-07-22"], &["1988-07-24"]), 58.0 / 59.0);

    // Swapping month and day (left-hand side only; a flipped match is weighted by 0.5).
    assert_eq!(super::dob_progressive_match(&["1988-07-05"], &["1988-05-07"]), 0.5);
    assert_eq!(super::dob_progressive_match(&["1988-05-11"], &["1988-11"]), 0.5);
    assert_eq!(super::dob_progressive_match(&["1988-07-22"], &["1988-22-07"]), 0.0);

    // A flipped interpretation only ever boosts toward a match; it must never soften a
    // genuine mismatch penalty, even when the left-hand side has a valid transposition.
    assert_eq!(super::dob_progressive_match(&["1988-07-05"], &["1995-07-05"]), -1.0);
    assert_eq!(super::dob_progressive_match(&["1988-07-05"], &["1988-11-30"]), -1.0);
    // A discounted flip does not override a stronger direct match.
    assert_eq!(super::dob_progressive_match(&["1988-07-11"], &["1988-07-13"]), 58.0 / 59.0);

    // Non-ASCII inputs
    assert_eq!(super::dob_progressive_match(&["a💃xx"], &["1988"]), 0.0);
    assert_eq!(super::dob_progressive_match(&["1988-07-22"], &["💃💃💃"]), 0.0);
    assert_eq!(super::dob_progressive_match(&["1988💃07💃22"], &["1988-07-22"]), 0.0);
    assert_eq!(super::dob_progressive_match(&["💃💃💃", "1988"], &["1988"]), 1.0);
  }
}
