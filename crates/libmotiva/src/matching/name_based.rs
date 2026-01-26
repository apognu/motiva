use bumpalo::Bump;
use tracing::instrument;

use crate::{
  matching::{
    Feature, MatchingAlgorithm,
    matchers::{jaro_winkler::JaroNameParts, soundex::SoundexNameParts},
    run_features,
  },
  model::{Entity, SearchEntity},
};

/// Simple matching algorithm using name similarity
pub struct NameBased;

const FEATURES: &[(&dyn Feature, f64)] = &[(&SoundexNameParts, 0.5), (&JaroNameParts, 0.5)];

impl MatchingAlgorithm for NameBased {
  fn name() -> &'static str {
    "name-based"
  }

  #[instrument(name = "score_hit", skip_all)]
  fn score(bump: &Bump, lhs: &SearchEntity, rhs: &Entity, cutoff: f64) -> (f64, Vec<(&'static str, f64)>) {
    if !rhs.schema.is_a(lhs.schema.as_str()) {
      return (0.0, vec![]);
    }

    let mut results = Vec::with_capacity(FEATURES.len());
    let score = run_features(bump, lhs, rhs, 0.0, cutoff, FEATURES.iter(), &mut results);

    (score.clamp(0.0, 1.0), results)
  }
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;
  use float_cmp::approx_eq;
  use pyo3::Python;

  use crate::{
    matching::{Algorithm, MatchingAlgorithm, name_based::NameBased},
    model::{Entity, SearchEntity},
    tests::python::nomenklatura_score,
  };

  #[test]
  fn name() {
    assert_eq!(NameBased::name(), "name-based");
  }

  #[test]
  fn incompatible_schemas() {
    let e1 = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();
    let e2 = Entity::builder("Company").properties(&[("name", &["Vladimir Putin"])]).build();

    let (score, _) = NameBased::score(&Bump::new(), &e1, &e2, 0.0);

    assert_eq!(score, 0.0);
  }

  #[test]
  fn name_based() {
    let e1 = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();
    let e2 = Entity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();

    let (score, _) = NameBased::score(&Bump::new(), &e1, &e2, 0.0);

    assert_eq!(score, 1.0);
  }

  #[test]
  #[serial_test::serial]
  fn against_nomenklatura() {
    Python::initialize();

    let query = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();

    let results = vec![
      Entity::builder("Person")
        .id("Q7747")
        .properties(&[("name", &["PUTIN, Vladimir Vladimirovich", "Владимир Владимирович Путин", "Vladimir Vladimirovich Putin"])])
        .build(),
      Entity::builder("Person")
        .id("NK-5dEHMo3SqLdUgnTVvTtejp")
        .properties(&[("name", &["Vladimir Nikitovich Skoch", "SKOCH, Vladimir Nikitovich", "Владимир Никитович Скоч"])])
        .build(),
      Entity::builder("Person")
        .id("NK-8bMT7hixpkpiKCpEHUupAp")
        .properties(&[("name", &["POLIN, Vladimir Anatolevich", "Владимир Анатольевич Полин", "Vladimir Anatolevich Polin"])])
        .build(),
      Entity::builder("Person")
        .id("Q108898811")
        .properties(&[("name", &["PLYAKIN, Vladimir Vladimirovich", "Vladimir Vladimirovich Plyakin", "Владимир Владимирович Плякин"])])
        .build(),
    ];

    let nscores = nomenklatura_score(Algorithm::NameBased, &query, results.clone()).unwrap();

    for (index, (_, nscore)) in nscores.into_iter().enumerate() {
      let (score, _) = NameBased::score(&Bump::new(), &query, results.get(index).unwrap(), 0.0);

      assert!(approx_eq!(f64, score, nscore, epsilon = 0.05));
    }
  }
}
