use std::cmp::Reverse;

use itertools::Itertools;
use macros::scoring_feature;
use strsim::levenshtein;

use crate::{
  matching::{
    Feature,
    comparers::default_levenshtein_similarity,
    extractors::{clean_names, tokenize_clean_names},
    replacers::{self, company_types::ORG_TYPES},
  },
  model::{Entity, HasProperties, SearchEntity},
};

#[scoring_feature(NameFingerprintLevenshtein, name = "name_fingerprint_levenshtein")]
fn score_feature(&self, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  name_fingerprint_levenshtein(lhs, rhs)
}

fn fingerprint_name(name: &str) -> String {
  fn is_word_boundary(c: char) -> bool {
    !c.is_alphanumeric()
  }

  replacers::replace(&ORG_TYPES.0, &ORG_TYPES.1, name)
}

pub fn name_fingerprint_levenshtein(lhs: &SearchEntity, rhs: &Entity) -> f64 {
  if lhs.schema.is_a("Person") || rhs.schema.is_a("Person") {
    return 0.0;
  }

  let qiter = lhs.names_and_aliases();
  let riter = rhs.names_and_aliases();

  let query_names = clean_names(qiter.iter());
  let result_names = clean_names(riter.iter());

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

      let mut token_scores: Vec<_> = qtokens.iter().cartesian_product(rtokens.iter()).map(|(q, r)| ((q, r), levenshtein(q, r))).collect();

      token_scores.sort_unstable_by_key(|&(_, score)| Reverse(score));

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

      if used_q.iter().any(|&u| !u) || used_r.iter().any(|&u| !u) {
        return score;
      }

      score.max(default_levenshtein_similarity(&aligned_q, &aligned_r))
    })
    .fold(0.0, f64::max)
}

#[cfg(test)]
mod tests {
  use float_cmp::approx_eq;

  use crate::tests::{e, python::nomenklatura_comparer, se};

  #[test]
  fn fingerprint_name() {
    assert_eq!(
      super::fingerprint_name("ACME Inc. Comandita por Acciones General Partnership Anything Free Zone Co. andelslag"),
      "ACME llc jsc gp Anything llc coop"
    );
  }

  #[serial_test::serial]
  #[test]
  fn name_fingerprint_levenshtein() {
    pyo3::prepare_freethreaded_python();

    let lhs = se("Company").properties(&[("name", &["AGoogle LLC"])]).call();
    let rhs = e("Company").properties(&[("name", &["Gooogle SAS"])]).call();

    let nscore = nomenklatura_comparer("compare.names", "name_fingerprint_levenshtein", &lhs, &rhs).unwrap();

    assert!(approx_eq!(f64, nscore, super::name_fingerprint_levenshtein(&lhs, &rhs), epsilon = 0.01));
  }
}
