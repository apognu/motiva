use bumpalo::{
  Bump,
  collections::{CollectIn, Vec},
};
use itertools::Itertools;
use libmotiva_macros::scoring_feature;
use rphonetic::{Encoder, Soundex};

use crate::{
  matching::{Feature, extractors},
  model::{Entity, HasProperties, SearchEntity},
};

#[scoring_feature(SoundexNameParts, name = "soundex_name_parts")]
fn score_feature(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  let soundex = Soundex::default();
  let mut similarities = Vec::with_capacity_in(lhs.name_parts.len(), bump);

  let rhs_soundexes = extractors::name_parts_flat(rhs.names_and_aliases().iter())
    .unique()
    .map(|s| soundex.encode(&s.to_string()))
    .collect_in::<Vec<_>>(bump);

  for part in &lhs.name_parts {
    let lhs_soundex = soundex.encode(part);

    similarities.push(if rhs_soundexes.contains(&lhs_soundex) { 1.0 } else { 0.0 });
  }

  similarities.iter().sum::<f64>() / 1.0f64.max(similarities.len() as f64)
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;
  use float_cmp::approx_eq;
  use pyo3::Python;

  use crate::{
    matching::Feature,
    model::{Entity, SearchEntity},
    tests::python::nomenklatura_comparer,
  };

  #[test]
  #[serial_test::serial]
  fn against_nomenklatura() {
    Python::initialize();

    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin", "Vladimir Putin"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Vladymire Poutine"])]).build();

    let nscore = nomenklatura_comparer("logic_v1.phonetic", "name_soundex_match", &lhs, &rhs).unwrap();

    assert!(approx_eq!(f64, nscore, super::SoundexNameParts.score_feature(&Bump::new(), &lhs, &rhs), epsilon = 0.01));
  }
}
