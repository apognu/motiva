use std::collections::HashSet;

use macros::scoring_feature;
use rphonetic::{Encoder, Soundex};

use crate::{
  matching::{Feature, extractors},
  model::{Entity, HasProperties, SearchEntity},
};

#[scoring_feature(SoundexNameParts, name = "soundex_name_parts")]
fn score_feature(&self, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  let soundex = Soundex::default();
  let mut similarities = Vec::new();

  let parts = extractors::name_parts_flat(rhs.names().iter()).collect::<HashSet<String>>();
  let rhs_soundexes = parts.iter().map(|s| soundex.encode(s)).collect::<Vec<_>>();

  for part in &lhs.name_parts {
    let lhs_soundex = soundex.encode(part);

    similarities.push(if rhs_soundexes.contains(&lhs_soundex) { 1.0 } else { 0.0 });
  }

  similarities.iter().sum::<f64>() / 1.0f64.max(similarities.len() as f64)
}

#[cfg(test)]
mod tests {
  use float_cmp::approx_eq;

  use crate::{
    matching::Feature,
    tests::{e, python::nomenklatura_comparer, se},
  };

  #[test]
  #[serial_test::serial]
  fn against_nomenklatura() {
    pyo3::prepare_freethreaded_python();

    let lhs = se("Person").properties(&[("name", &["Vladimir Putin", "Vladimir Putin"])]).call();
    let rhs = e("Person").properties(&[("name", &["Vladymire Poutine"])]).call();

    let nscore = nomenklatura_comparer("logic_v1.phonetic", "name_soundex_match", &lhs, &rhs).unwrap();

    assert!(approx_eq!(f64, nscore, super::SoundexNameParts.score_feature(&lhs, &rhs), epsilon = 0.01));
  }
}
