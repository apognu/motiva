use bumpalo::Bump;
use itertools::Itertools;
use libmotiva_macros::scoring_feature;

use crate::{
  matching::{
    Feature,
    comparers::{default_levenshtein_similarity, levenshtein_similarity},
    extractors::{clean_names, tokenize_clean_names},
    replacers::{self, company_types::ORG_TYPES, stopwords::STOPWORDS},
  },
  model::{Entity, HasProperties, SearchEntity},
};

#[scoring_feature(NameFingerprintLevenshtein, name = "name_fingerprint_levenshtein")]
fn score_feature(&self, _bump: &Bump, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  name_fingerprint_levenshtein(lhs, rhs)
}

fn fingerprint_name(name: &str) -> String {
  let output = replacers::replace(&STOPWORDS.0, &STOPWORDS.1, name);
  let output = replacers::replace(&ORG_TYPES.0, &ORG_TYPES.1, &output);
  // This was not supposed to be here.
  // let output = replacers::replace(&ORG_SYMBOLS.0, &ORG_SYMBOLS.1, &output);

  output.trim().to_string()
}

pub(crate) fn name_fingerprint_levenshtein(lhs: &SearchEntity, rhs: &Entity) -> f64 {
  if lhs.schema.is_a("Person") || rhs.schema.is_a("Person") {
    return 0.0;
  }

  let qiter = lhs.names_and_aliases();
  let riter = rhs.names_and_aliases();

  let query_names = clean_names(qiter.iter()).filter(|word| word.len() >= 2);
  let result_names = clean_names(riter.iter()).filter(|word| word.len() >= 2);

  query_names
    .cartesian_product(result_names)
    .map(|(qn, rn)| {
      let mut score = default_levenshtein_similarity(&qn, &rn);

      let (qfp, rfp) = (fingerprint_name(&qn), fingerprint_name(&rn));
      let qfp_no_spaces = qfp.chars().filter(|c| !c.is_whitespace()).collect::<String>();
      let rfp_no_spaces = rfp.chars().filter(|c| !c.is_whitespace()).collect::<String>();

      if !qfp_no_spaces.is_empty() && !rfp_no_spaces.is_empty() {
        score = score.max(default_levenshtein_similarity(&qfp_no_spaces, &rfp_no_spaces));
      }

      let qtokens: Vec<_> = tokenize_clean_names(std::iter::once(&qfp)).collect();
      let rtokens: Vec<_> = tokenize_clean_names(std::iter::once(&rfp)).collect();

      if qtokens.is_empty() || rtokens.is_empty() {
        return score;
      }

      let mut token_scores: Vec<_> = qtokens.iter().cartesian_product(rtokens.iter()).map(|(q, r)| ((q, r), levenshtein_similarity(q, r, 4))).collect();

      token_scores.sort_unstable_by(|&(_, s1), &(_, s2)| s1.partial_cmp(&s2).unwrap_or(std::cmp::Ordering::Equal).reverse());

      let mut aligned_q = String::new();
      let mut aligned_r = String::new();

      let mut used_q = vec![false; qtokens.len()];
      let mut used_r = vec![false; rtokens.len()];

      for ((q, r), _) in token_scores {
        let q_idx = qtokens.iter().position(|t| t == q && !used_q[qtokens.iter().position(|x| x == q).unwrap()]);
        let r_idx = rtokens.iter().position(|t| t == r && !used_r[rtokens.iter().position(|x| x == r).unwrap()]);

        if let (Some(qi), Some(ri)) = (q_idx, r_idx) {
          used_q[qi] = true;
          used_r[ri] = true;
          aligned_q.push_str(q);
          aligned_r.push_str(r);
        }
      }

      if used_q.iter().any(|&u| !u) {
        return score;
      }

      score.max(default_levenshtein_similarity(&aligned_q, &aligned_r))
    })
    .fold(0.0, f64::max)
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
  #[serial_test::serial]
  fn name_fingerprint_levenshtein() {
    Python::initialize();

    let lhs = SearchEntity::builder("Company").properties(&[("name", &["AGoogle LLC"])]).build();
    let rhs = Entity::builder("Company").properties(&[("name", &["Gooogle LIMITED LIABILITY COMPANY"])]).build();

    let nscore = nomenklatura_comparer("compare.names", "name_fingerprint_levenshtein", &lhs, &rhs).unwrap();

    assert!(approx_eq!(f64, nscore, super::name_fingerprint_levenshtein(&lhs, &rhs), epsilon = 0.01));
  }
}
