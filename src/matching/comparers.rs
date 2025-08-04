use std::{borrow::Borrow, cmp::Ordering, collections::HashMap};

use ahash::{HashMapExt, RandomState};
use itertools::Itertools;
use strsim::{jaro_winkler, levenshtein};

pub fn is_disjoint<'s, S>(a: &[S], b: &[S]) -> bool
where
  S: Borrow<str> + Ord + Clone + 's,
{
  let (mut shorter, longer) = if a.len() < b.len() { (a.to_vec(), b) } else { (b.to_vec(), a) };

  shorter.sort_unstable();

  for x in longer {
    if shorter.binary_search(x).is_ok() {
      return false;
    }
  }

  true
}

pub fn is_disjoint_chars(a: &[Vec<char>], b: &[Vec<char>]) -> bool {
  let (mut shorter, longer) = if a.len() < b.len() { (a.to_vec(), b) } else { (b.to_vec(), a) };

  shorter.sort_unstable();

  for x in longer {
    if shorter.binary_search(x).is_ok() {
      return false;
    }
  }

  true
}

pub fn compare_name_phonetic_tuples((l_name, l_phone): (&str, Option<&str>), (r_name, r_phone): (&str, Option<&str>)) -> bool {
  if l_phone.is_none() || r_phone.is_none() {
    return l_name == r_name;
  }

  if l_phone == r_phone {
    return is_levenshtein_plausible(l_name, r_name);
  }

  false
}

pub fn is_levenshtein_plausible(lhs: &str, rhs: &str) -> bool {
  let pct = (lhs.len().min(rhs.len()) as f32 * 0.2).ceil();
  let threshold = 4.min(pct as usize);

  levenshtein(&lhs.to_lowercase(), &rhs.to_lowercase()) <= threshold
}

pub fn default_levenshtein_similarity(lhs: &str, rhs: &str) -> f64 {
  levenshtein_similarity(lhs, rhs, 4)
}

pub fn levenshtein_similarity(lhs: &str, rhs: &str, max_edits: usize) -> f64 {
  if lhs.is_empty() || rhs.is_empty() {
    return 0.0;
  }

  let pct_edits = (lhs.len().min(rhs.len()) as f64 * 0.2).ceil();
  let max_edits = (max_edits as f64).min(pct_edits);

  if (lhs.len() as isize - rhs.len() as isize).abs() > max_edits as isize {
    return 0.0;
  }

  let distance = levenshtein(lhs, rhs) as f64;

  if distance > max_edits {
    return 0.0;
  }

  1.0 - (distance / lhs.len().max(rhs.len()) as f64)
}

pub fn align_name_parts<'s, S>(query: &[S], result: &[S]) -> f64
where
  S: Borrow<str> + 's,
{
  if query.is_empty() || result.is_empty() {
    return 0.0;
  }

  let mut query_counts = count_parts(query);
  let mut result_counts = count_parts(result);

  let mut scores = query_counts
    .keys()
    .cartesian_product(result_counts.keys())
    .filter_map(|(&qn, &rn)| {
      let score = jaro_winkler(qn, rn);

      if score > 0.0 && is_levenshtein_plausible(qn, rn) { Some((qn, rn, score)) } else { None }
    })
    .collect::<Vec<_>>();

  scores.sort_unstable_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(Ordering::Equal));

  let mut final_score = 1.0;
  let mut pairs: Vec<(&str, &str)> = Vec::with_capacity(query.len());

  for (qn, rn, score) in scores {
    let q_count = query_counts.get_mut(qn).unwrap();
    let r_count = result_counts.get_mut(rn).unwrap();

    while *q_count > 0 && *r_count > 0 {
      *q_count -= 1;
      *r_count -= 1;
      final_score *= score;

      pairs.push((qn, rn));
    }
  }

  if pairs.len() < query.len() {
    return 0.0;
  }

  pairs.reverse();

  let query_aligned = pairs.iter().map(|p| p.0).join(" ");
  let result_aligned = pairs.iter().map(|p| p.1).join(" ");

  if !is_levenshtein_plausible(&query_aligned, &result_aligned) {
    return 0.0;
  }

  final_score
}

#[inline(always)]
fn count_parts<'s, S: Borrow<str> + 's>(parts: &'s [S]) -> HashMap<&'s str, usize, RandomState> {
  let mut counts = HashMap::<_, _, RandomState>::with_capacity(parts.len());
  for part in parts {
    *counts.entry(part.borrow()).or_default() += 1;
  }
  counts
}

#[cfg(test)]
mod tests {
  use float_cmp::assert_approx_eq;

  use crate::tests::python::nomenklatura_str_list;

  #[test]
  fn is_levenshtein_plausible() {
    assert!(super::is_levenshtein_plausible("Martin", "Jardin"));
    assert!(!super::is_levenshtein_plausible("John", "Nicolas"));
  }

  #[test]
  #[serial_test::serial]
  fn align_name_parts() {
    pyo3::prepare_freethreaded_python();

    let data: &[(&[&str], &[&str])] = &[
      (&["vladimir", "putin"], &["vladimir", "vladimirovich", "putin"]),
      (&["mohamed", "laha"], &["khalil", "ibrahim", "mohamed", "achar", "foudail", "taha"]),
    ];

    for (lhs, rhs) in data {
      let score = super::align_name_parts(lhs, rhs);
      let nscore = nomenklatura_str_list("compare.names", "_align_name_parts", lhs, rhs).unwrap();

      assert_approx_eq!(f64, score, nscore, epsilon = 0.01);
    }
  }
}
