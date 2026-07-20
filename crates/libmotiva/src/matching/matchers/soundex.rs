use std::sync::LazyLock;

use bumpalo::{
  Bump,
  collections::{CollectIn, Vec},
};
use itertools::Itertools;
use libmotiva_macros::scoring_feature;
use rphonetic::{Encoder, Soundex};

use crate::{
  matching::{CodedPair, Detail, Feature, ScoreResult, extractors},
  model::{Entity, HasProperties, PropertyFilter, SearchEntity},
};

static SOUNDEX: LazyLock<Soundex> = LazyLock::new(Soundex::default);

#[scoring_feature(SoundexNameParts, name = "soundex_name_parts")]
fn score(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity, explain: bool) -> ScoreResult {
  let mut similarities = Vec::with_capacity_in(lhs.name_parts_flat.len(), bump);

  let rhs_soundexes = extractors::name_parts_flat(rhs.prop_group("name", PropertyFilter::All).iter())
    .unique()
    .map(|part| {
      let code = SOUNDEX.encode(&part);
      (part, code)
    })
    .collect_in::<Vec<_>>(bump);

  let mut best_match: Option<CodedPair> = None;

  for part in &lhs.name_parts_flat {
    let lhs_soundex = SOUNDEX.encode(part);
    let matched = rhs_soundexes.iter().find(|(_, code)| code == &lhs_soundex);

    similarities.push(if matched.is_some() { 1.0 } else { 0.0 });

    if explain
      && best_match.is_none()
      && let Some((rhs_part, rhs_code)) = matched
    {
      best_match = Some(CodedPair {
        lhs: part.as_str().into(),
        lhs_code: lhs_soundex.as_str().into(),
        rhs: rhs_part.as_str().into(),
        rhs_code: rhs_code.as_str().into(),
      });
    }
  }

  let score = similarities.iter().sum::<f64>() / 1.0f64.max(similarities.len() as f64);

  let detail = explain.then(|| match best_match {
    Some(pair) => Detail::Coded(pair),
    None => Detail::Note("no soundex match"),
  });

  (score, detail).into()
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
  fn soundex_name_parts_details() {
    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Vladymir Poutin"])]).build();

    let detail = super::SoundexNameParts.score(&Bump::new(), &lhs, &rhs, true).1.unwrap().to_string();
    assert!(detail.contains(" [") && detail.contains("] ~= "), "unexpected soundex detail: {detail}");

    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Washington"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Zeppelin"])]).build();

    let detail = super::SoundexNameParts.score(&Bump::new(), &lhs, &rhs, true).1.unwrap().to_string();
    assert_eq!(detail, "no soundex match");
  }

  #[test]
  #[serial_test::serial]
  fn against_nomenklatura() {
    Python::initialize();

    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin", "Vladimir Putin"])]).build();
    let rhs = Entity::builder("Person").properties(&[("name", &["Vladymire Poutine"])]).build();

    let nscore = nomenklatura_comparer("logic_v1.phonetic", "name_soundex_match", &lhs, &rhs).unwrap();

    assert!(approx_eq!(f64, nscore, super::SoundexNameParts.score_scalar(&Bump::new(), &lhs, &rhs), epsilon = 0.01));
  }
}
