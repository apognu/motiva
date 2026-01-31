use std::{borrow::Borrow, cmp::Ordering};

use ahash::{HashMap, HashSet, RandomState};
use itertools::Itertools;
use strsim::{jaro_winkler, levenshtein};

#[inline]
pub(crate) fn is_disjoint<'s, S>(lhs: &[S], rhs: &[S]) -> bool
where
  S: Borrow<str> + Ord + Clone + 's,
{
  let (bigger, smaller) = if lhs.len() > rhs.len() { (&lhs, &rhs) } else { (&rhs, &lhs) };

  if bigger.len() > 5 {
    let set = smaller.iter().map(|s| s.borrow()).collect::<HashSet<_>>();

    return bigger.iter().all(|b| !set.contains(b.borrow()));
  }

  for a in lhs {
    for b in rhs {
      if a == b {
        return false;
      }
    }
  }

  true
}

#[inline]
pub(crate) fn is_disjoint_chars(lhs: &[Vec<char>], rhs: &[Vec<char>]) -> bool {
  for a in lhs {
    for b in rhs {
      if a == b {
        return false;
      }
    }
  }

  true
}

#[inline]
pub(crate) fn compare_name_phonetic_tuples((l_name, l_phone): (&str, Option<&str>), (r_name, r_phone): (&str, Option<&str>)) -> bool {
  if l_phone.is_none() || r_phone.is_none() {
    return l_name == r_name;
  }

  if l_phone == r_phone {
    return is_levenshtein_plausible(l_name, r_name);
  }

  false
}

#[inline]
pub(crate) fn is_levenshtein_plausible(lhs: &str, rhs: &str) -> bool {
  if lhs == rhs {
    return true;
  }

  let pct = (lhs.len().min(rhs.len()) as f32 * 0.2).ceil();
  let threshold = 4.min(pct as usize);

  levenshtein(&lhs.to_lowercase(), &rhs.to_lowercase()) <= threshold
}

#[inline]
pub(crate) fn default_levenshtein_similarity(lhs: &str, rhs: &str) -> f64 {
  levenshtein_similarity(lhs, rhs, 4)
}

pub(crate) fn levenshtein_similarity(lhs: &str, rhs: &str, max_edits: usize) -> f64 {
  if lhs.is_empty() || rhs.is_empty() {
    return 0.0;
  }
  if lhs == rhs {
    return 1.0;
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

pub(crate) fn align_name_parts<'s, S>(query: &[S], result: &[S]) -> f64
where
  S: Borrow<str> + 's,
{
  if query.is_empty() || result.is_empty() {
    return 0.0;
  }

  let mut query_counts = count_parts(query);
  let mut result_counts = count_parts(result);

  let mut scores = query_counts
    .iter()
    .cartesian_product(result_counts.iter())
    .filter_map(|((qn, _), (rn, _))| {
      let score = jaro_winkler(qn, rn);

      if score > 0.0 && is_levenshtein_plausible(qn, rn) { Some((*qn, *rn, score)) } else { None }
    })
    .collect::<Vec<_>>();

  scores.sort_unstable_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(Ordering::Equal));

  let mut final_score = 1.0;
  let mut pairs: Vec<(&str, &str)> = Vec::with_capacity(query.len());

  for (qn, rn, score) in scores {
    if let Some(q_entry) = query_counts.iter_mut().find(|(s, _)| *s == qn)
      && let Some(r_entry) = result_counts.iter_mut().find(|(s, _)| *s == rn)
    {
      while q_entry.1 > 0 && r_entry.1 > 0 {
        q_entry.1 -= 1;
        r_entry.1 -= 1;
        final_score *= score;

        pairs.push((qn, rn));
      }
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
fn count_parts<'s, S: Borrow<str> + 's>(parts: &'s [S]) -> Vec<(&'s str, usize)> {
  let mut map: HashMap<&str, usize> = HashMap::with_capacity_and_hasher(parts.len(), RandomState::default());

  for part in parts {
    *map.entry(part.borrow()).or_insert(0) += 1;
  }

  map.into_iter().collect()
}

#[cfg(test)]
mod tests {
  use float_cmp::assert_approx_eq;
  use pyo3::Python;

  use crate::tests::python::nomenklatura_str_list;

  #[test]
  fn is_disjoint() {
    assert!(super::is_disjoint(&["a", "b", "c"], &["d", "e"]));
    assert!(super::is_disjoint(&["a", "b", "c"], &["d", "e", "f", "g"]));
    assert!(!super::is_disjoint(&["a", "b", "c"], &["d", "c", "f", "g"]));
    assert!(super::is_disjoint(&["a", "b", "c", "d", "e", "f"], &["g"]));
    assert!(!super::is_disjoint(&["a", "b", "c", "d", "e", "f"], &["d"]));
  }

  #[test]
  fn count_parts() {
    let counts = super::count_parts(&["a", "a", "b", "c", "a", "c", "b"]);

    assert!(counts.contains(&("a", 3)));
    assert!(counts.contains(&("b", 2)));
    assert!(counts.contains(&("c", 2)));
  }

  #[test]
  fn is_levenshtein_plausible() {
    assert!(super::is_levenshtein_plausible("Martin", "Jardin"));
    assert!(!super::is_levenshtein_plausible("John", "Nicolas"));
  }

  #[test]
  #[serial_test::serial]
  fn align_name_parts() {
    Python::initialize();

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
