use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use strsim::{jaro_winkler, levenshtein};

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

// TODO: rewrite
pub fn align_name_parts(query: &[String], result: &[String]) -> f64 {
  if query.is_empty() || result.is_empty() {
    return 0.0;
  }

  // Use HashSets to get unique name parts, like Python's `set()`.
  let query_set: HashSet<String> = query.iter().cloned().collect();
  let result_set: HashSet<String> = result.iter().cloned().collect();

  // 1. Compute all pairwise scores for name parts.
  // Using `itertools::cartesian_product` for clean iteration.
  let mut scores = HashMap::new();
  for (qn, rn) in query_set.iter().cartesian_product(result_set.iter()) {
    let score = jaro_winkler(qn, rn);
    // Filter pairs that are not plausible matches.
    if score > 0.0 && is_levenshtein_plausible(qn, rn) {
      scores.insert((qn, rn), score);
    }
  }

  // To avoid mutating the input slices (which is un-idiomatic and often impossible),
  // we use frequency counters for the original lists.
  let mut query_counts: HashMap<&str, usize> = HashMap::new();
  for part in query {
    *query_counts.entry(part).or_insert(0) += 1;
  }

  let mut result_counts: HashMap<&str, usize> = HashMap::new();
  for part in result {
    *result_counts.entry(part).or_insert(0) += 1;
  }

  // 2. Find the best pairing for each name part by score.
  // First, sort the scores in descending order.
  let mut sorted_scores: Vec<_> = scores.into_iter().collect();
  sorted_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

  let mut pairs: Vec<(String, String)> = Vec::new();
  let mut total_score = 1.0;
  let original_query_len = query.len();

  for ((qn, rn), score) in sorted_scores {
    // Check if both parts are still available to be matched.
    let q_count = query_counts.entry(qn).or_insert(0);
    let r_count = result_counts.entry(rn).or_insert(0);

    // One name part can only be used once per match, but can appear multiple times
    // if it's in the input lists multiple times.
    while *q_count > 0 && *r_count > 0 {
      // "Use" up one instance of each part.
      *q_count -= 1;
      *r_count -= 1;

      total_score *= score;
      pairs.push((qn.clone(), rn.clone()));
    }
  }

  // 3. Assume there should be at least one candidate for each query name part.
  if pairs.len() < original_query_len {
    return 0.0;
  }

  // 4. Final plausibility check on the concatenated aligned strings.
  // Weakest evidence first to bias Jaro-Winkler for lower scores on imperfect matches.
  pairs.reverse(); // In-place reverse is efficient.

  let query_aligned: String = pairs.iter().map(|p| p.0.to_string()).collect();
  let result_aligned: String = pairs.iter().map(|p| p.1.to_string()).collect();

  if !is_levenshtein_plausible(&query_aligned, &result_aligned) {
    return 0.0;
  }

  // Return the multiplicative score.
  total_score

  // The original code had a commented-out alternative return.
  // If you prefer that, the Rust equivalent would be:
  // jaro_winkler(&query_aligned, &result_aligned)
}
