use bumpalo::Bump;
use compact_str::CompactString;
use itertools::Itertools;
use libmotiva_macros::scoring_feature;

use crate::{
  matching::{
    Detail, Feature,
    comparers::{default_levenshtein_similarity, levenshtein_similarity},
    extractors::{clean_names, tokenize_clean_names},
    replacers::{self, company_types::ORG_TYPES, stopwords::STOPWORDS},
  },
  model::{Entity, HasProperties, PropertyFilter, SearchEntity, format_score},
};

#[scoring_feature(NameFingerprintLevenshtein, name = "name_fingerprint_levenshtein")]
fn score(&self, _bump: &Bump, lhs: &SearchEntity, rhs: &Entity, explain: bool) -> (f64, Option<Detail>) {
  let (score, best) = name_fingerprint_levenshtein(lhs, rhs, explain);

  let detail = explain.then(|| match best {
    Some((lhs, rhs)) if score >= 0.999 => Detail::Equal(lhs, rhs),
    Some((lhs, rhs)) => Detail::Fuzzy { lhs, rhs, score: format_score(score) },
    None if lhs.schema.is_a("Person") || rhs.schema.is_a("Person") => Detail::Note("not an organization"),
    None => Detail::Note("no name fingerprint match"),
  });

  (score, detail)
}

fn fingerprint_name(name: &str) -> String {
  let output = replacers::replace(&STOPWORDS.0, &STOPWORDS.1, name);
  let output = replacers::replace(&ORG_TYPES.0, &ORG_TYPES.1, &output);

  output.trim().to_string()
}

fn pair_score(qn: &str, rn: &str) -> f64 {
  let mut score = default_levenshtein_similarity(qn, rn);

  let (qfp, rfp) = (fingerprint_name(qn), fingerprint_name(rn));

  if qfp.chars().any(|c| !c.is_whitespace()) && rfp.chars().any(|c| !c.is_whitespace()) {
    let qfp_no_spaces = qfp.chars().filter(|c| !c.is_whitespace()).collect::<String>();
    let rfp_no_spaces = rfp.chars().filter(|c| !c.is_whitespace()).collect::<String>();

    score = score.max(default_levenshtein_similarity(&qfp_no_spaces, &rfp_no_spaces));
  }

  let qtokens: Vec<_> = tokenize_clean_names(std::iter::once(&qfp)).collect();
  let rtokens: Vec<_> = tokenize_clean_names(std::iter::once(&rfp)).collect();

  if qtokens.is_empty() || rtokens.is_empty() {
    return score;
  }

  let mut token_scores: Vec<_> = Vec::with_capacity(qtokens.len() * rtokens.len());
  for (qi, q) in qtokens.iter().enumerate() {
    for (ri, r) in rtokens.iter().enumerate() {
      token_scores.push(((qi, ri), levenshtein_similarity(q, r, 4)));
    }
  }

  token_scores.sort_unstable_by(|&(_, s1), &(_, s2)| s1.partial_cmp(&s2).unwrap_or(std::cmp::Ordering::Equal).reverse());

  let mut aligned_q = String::new();
  let mut aligned_r = String::new();

  let mut used_q = vec![false; qtokens.len()];
  let mut used_r = vec![false; rtokens.len()];

  for ((qi, ri), _) in token_scores {
    if !used_q[qi] && !used_r[ri] {
      used_q[qi] = true;
      used_r[ri] = true;

      aligned_q.push_str(&qtokens[qi]);
      aligned_r.push_str(&rtokens[ri]);
    }
  }

  if used_q.iter().any(|&u| !u) {
    return score;
  }

  score.max(default_levenshtein_similarity(&aligned_q, &aligned_r))
}

pub(crate) fn name_fingerprint_levenshtein(lhs: &SearchEntity, rhs: &Entity, explain: bool) -> (f64, Option<(CompactString, CompactString)>) {
  if lhs.schema.is_a("Person") || rhs.schema.is_a("Person") {
    return (0.0, None);
  }

  let qiter = lhs.prop_group("name", PropertyFilter::All);
  let riter = rhs.prop_group("name", PropertyFilter::All);

  let query_names = clean_names(qiter.iter()).filter(|word| word.len() >= 2);
  let result_names = clean_names(riter.iter()).filter(|word| word.len() >= 2);

  let mut max = 0.0f64;
  let mut best: Option<(CompactString, CompactString)> = None;

  for (qn, rn) in query_names.cartesian_product(result_names) {
    let score = pair_score(&qn, &rn);

    if score > max {
      max = score;

      if explain {
        best = Some((qn.as_str().into(), rn.as_str().into()));
      }
    }
  }

  (max, best)
}

#[cfg(test)]
mod tests {
  use float_cmp::approx_eq;
  use pyo3::Python;

  use crate::{
    model::{Entity, SearchEntity},
    tests::python::nomenklatura_comparer,
  };

  #[test]
  fn fingerprint_name() {
    assert_eq!(
      super::fingerprint_name("ACME Inc. Comandita por Acciones General Partnership Anything Free Zone Co. andelslag"),
      "ACME Inc. sca  Partnership Anything Free Zone Co. anl"
    );
  }

  #[test]
  fn name_fingerprint_levenshtein_details() {
    use bumpalo::Bump;

    use crate::matching::Feature;

    fn detail(lhs: &SearchEntity, rhs: &Entity) -> Option<String> {
      super::NameFingerprintLevenshtein.score(&Bump::new(), lhs, rhs, true).1.map(|detail| detail.to_string())
    }

    // Exact fingerprint match.
    let lhs = SearchEntity::builder("Company").properties(&[("name", &["Google LLC"])]).build();
    let rhs = Entity::builder("Company").properties(&[("name", &["Google LLC"])]).build();
    assert_eq!(detail(&lhs, &rhs).as_deref(), Some("google llc == google llc"));

    // Fuzzy fingerprint match.
    let lhs = SearchEntity::builder("Company").properties(&[("name", &["Google LLC"])]).build();
    let rhs = Entity::builder("Company").properties(&[("name", &["Gooogle LLC"])]).build();
    let fuzzy = detail(&lhs, &rhs).unwrap();
    assert!(fuzzy.contains(" ~= "), "expected a fuzzy match: {fuzzy}");

    // Not an organization.
    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Google"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Google"])]).build();
    assert_eq!(detail(&lhs, &rhs).as_deref(), Some("not an organization"));

    // No comparable names (too short to fingerprint).
    let lhs = SearchEntity::builder("Company").properties(&[("name", &["A"])]).build();
    let rhs = Entity::builder("Company").properties(&[("name", &["B"])]).build();
    assert_eq!(detail(&lhs, &rhs).as_deref(), Some("no name fingerprint match"));
  }

  #[test]
  #[serial_test::serial]
  fn name_fingerprint_levenshtein() {
    Python::initialize();

    let lhs = SearchEntity::builder("Company").properties(&[("name", &["AGoogle LLC"])]).build();
    let rhs = Entity::builder("Company").properties(&[("name", &["Gooogle LIMITED LIABILITY COMPANY"])]).build();

    let nscore = nomenklatura_comparer("compare.names", "name_fingerprint_levenshtein", &lhs, &rhs).unwrap();

    assert!(approx_eq!(f64, nscore, super::name_fingerprint_levenshtein(&lhs, &rhs, false).0, epsilon = 0.01));
  }
}
