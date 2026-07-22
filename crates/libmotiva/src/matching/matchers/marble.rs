use bumpalo::Bump;
use compact_str::CompactString;
use libmotiva_macros::scoring_feature;

use crate::{
  Entity, HasProperties, SearchEntity,
  matching::{
    Detail, Feature, ScoreResult, extractors,
    replacers::{self, company_types::ORG_TYPES, stopwords::STOPWORDS},
  },
  model::{PropertyFilter, format_score},
};

fn fingerprint_name(name: &str) -> String {
  let output = replacers::replace(&STOPWORDS.0, &STOPWORDS.1, name);
  let output = replacers::replace(&ORG_TYPES.0, &ORG_TYPES.1, &output);

  output.trim().to_string()
}

#[scoring_feature(LongestCommonSubsequence, name = "longest_common_subsequence")]
fn score(&self, _bump: &Bump, lhs: &SearchEntity, rhs: &Entity, explain: bool) -> ScoreResult {
  let lhs_names = lhs.prop_group("name", PropertyFilter::All);
  let rhs_names = rhs.prop_group("name", PropertyFilter::All);

  let lhs_names = extractors::index_name_keys(lhs_names.iter())
    .map(|name| fingerprint_name(&name).chars().collect::<Vec<char>>())
    .collect::<Vec<_>>();

  let mut max = 0.0f64;
  let mut best: Option<(CompactString, CompactString, CompactString)> = None;

  for rhs_name in extractors::index_name_keys(rhs_names.iter()) {
    let rname = fingerprint_name(&rhs_name).chars().collect::<Vec<char>>();

    for lname in &lhs_names {
      let longest = lname.len().max(rname.len());

      if longest == 0 {
        continue;
      }

      let (length, matched) = lcs(lname, &rname, explain);
      let combined = length as f64 / longest as f64;

      if combined > max {
        max = combined;

        if explain {
          let matched = matched.unwrap_or_default();
          best = Some((lname.iter().collect(), rname.iter().collect(), matched.into()));
        }
      }
    }
  }

  let detail = explain.then(|| match best {
    Some((lhs, rhs, _)) if max >= 0.999 => Detail::Equal(lhs, rhs),
    Some((lhs, rhs, matched)) => Detail::Subsequence {
      lhs,
      rhs,
      matched,
      score: format_score(max),
    },
    None => Detail::Note("no common subsequence"),
  });

  (max, detail).into()
}

/// Longest common subsequence of `a` and `b`.
///
/// Returns the LCS length, and — only when `reconstruct` is set — the matched
/// characters themselves (used for the scoring explanation). A standard O(n·m)
/// dynamic program over a single flat table; the backtrack that materializes
/// the sequence runs only when a caller asks for it.
fn lcs(a: &[char], b: &[char], reconstruct: bool) -> (usize, Option<String>) {
  let (na, nb) = (a.len(), b.len());

  if na == 0 || nb == 0 {
    return (0, reconstruct.then(String::new));
  }

  // table[i * stride + j] = LCS length of a[..i] and b[..j].
  let stride = nb + 1;
  let mut table = vec![0usize; (na + 1) * stride];

  for i in 1..=na {
    for j in 1..=nb {
      table[i * stride + j] = if a[i - 1] == b[j - 1] {
        table[(i - 1) * stride + j - 1] + 1
      } else {
        table[(i - 1) * stride + j].max(table[i * stride + j - 1])
      };
    }
  }

  let len = table[na * stride + nb];

  if !reconstruct {
    return (len, None);
  }

  // Backtrack from the bottom-right corner, collecting matched characters in
  // reverse order.
  let mut matched = Vec::with_capacity(len);
  let (mut i, mut j) = (na, nb);

  while i > 0 && j > 0 {
    if a[i - 1] == b[j - 1] {
      matched.push(a[i - 1]);
      i -= 1;
      j -= 1;
    } else if table[(i - 1) * stride + j] >= table[i * stride + j - 1] {
      i -= 1;
    } else {
      j -= 1;
    }
  }

  (len, Some(matched.iter().rev().collect()))
}

#[cfg(test)]
mod tests {
  use crate::{
    matching::{Feature, ScoreResult, matchers::jaro_winkler::PersonNameJaroWinkler},
    model::{Entity, SearchEntity},
  };

  use bumpalo::Bump;

  #[test]
  fn longest_common_subsequence() {
    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Samir Kamil AlAssad"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Samer Kamel Al Asad"])]).build();

    // Sanity check that PersonNameJaroWinkler had a very bad scoring for this
    assert!(PersonNameJaroWinkler.score_scalar(&Bump::new(), &lhs, &rhs) < 0.3);
    assert!(super::LongestCommonSubsequence.score_scalar(&Bump::new(), &lhs, &rhs) > 0.8);
  }

  #[test]
  fn longest_common_subsequence_detail() {
    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Samir Kamil AlAsad"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Samer Kamal Al-Assad"])]).build();

    let ScoreResult(score, detail) = super::LongestCommonSubsequence.score(&Bump::new(), &lhs, &rhs, true);
    let detail = detail.unwrap().to_string();

    assert!(score > 0.8 && score < 1.0, "score={score}");
    assert_eq!(detail, "alasadkamilsamir ~= alassadkamalsamer = 0.824 (matched: alasadkamlsamr)");
  }

  #[test]
  fn lcs_reconstructs_subsequence() {
    fn is_subsequence(needle: &str, haystack: &str) -> bool {
      let mut chars = haystack.chars();
      needle.chars().all(|c| chars.any(|h| h == c))
    }

    let a = "abcbdab".chars().collect::<Vec<_>>();
    let b = "bdcaba".chars().collect::<Vec<_>>();

    let (length, matched) = super::lcs(&a, &b, true);
    assert_eq!(length, 4);

    let matched = matched.unwrap();
    assert_eq!(matched.chars().count(), 4, "matched={matched}");
    assert!(is_subsequence(&matched, "abcbdab"), "matched={matched}");
    assert!(is_subsequence(&matched, "bdcaba"), "matched={matched}");

    // Without reconstruction we still get the length but no string.
    assert_eq!(super::lcs(&a, &b, false), (4, None));
  }

  #[test]
  fn fills_jaro_winkler_gaps() {
    let cases = [
      ("Abdul Aziz", "Abdelaziz"),
      ("Abdul Rahman", "Abdurrahman"),
      ("Mohammed Reza", "Mohammadreza"),
      ("Hafez Al Assad", "Hafiz Alasad"),
    ];

    for (l, r) in cases {
      let lhs = SearchEntity::builder("Person").properties(&[("name", &[l])]).build();
      let rhs = Entity::builder("Person").properties(&[("name", &[r])]).build();

      assert!(PersonNameJaroWinkler.score_scalar(&Bump::new(), &lhs, &rhs) < 0.7);
      assert!(super::LongestCommonSubsequence.score_scalar(&Bump::new(), &lhs, &rhs) > 0.8);
    }
  }
}
