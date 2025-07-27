use tracing::instrument;

use crate::{
  matching::{
    Feature, MatchingAlgorithm,
    matchers::{jaro_winkler::JaroNameParts, soundex::SoundexNameParts},
    run_features,
  },
  model::{Entity, SearchEntity},
};

pub struct NameBased;

impl MatchingAlgorithm for NameBased {
  fn name() -> &'static str {
    "name-based"
  }

  #[instrument(name = "score_hit", skip_all)]
  fn score(lhs: &SearchEntity, rhs: &Entity) -> (f64, Vec<(&'static str, f64)>) {
    let features: &[(&dyn Feature, f64)] = &[(&SoundexNameParts, 0.5), (&JaroNameParts, 0.5)];
    let mut results = Vec::with_capacity(features.len());
    let score = run_features(lhs, rhs, 0.0, features, &mut results);

    (score, results)
  }
}

#[cfg(test)]
mod tests {
  use float_cmp::approx_eq;

  use crate::{
    api::dto::Algorithm,
    matching::{MatchingAlgorithm, name_based::NameBased},
    tests::{e, python::nomenklatura_score, se},
  };

  #[test]
  fn name_based() {
    let e1 = se("Person").properties(&[("name", &["Vladimir Putin"])]).call();
    let e2 = e("Person").properties(&[("name", &["Vladimir Putin"])]).call();

    let (score, _) = NameBased::score(&e1, &e2);

    assert_eq!(score, 1.0);
  }

  #[test]
  fn against_nomenklatura() {
    pyo3::prepare_freethreaded_python();

    let query = se("Person").properties(&[("name", &["Vladimir Putin"])]).call();

    let results = vec![
      e("Person")
        .id("Q7747")
        .properties(&[("name", &["PUTIN, Vladimir Vladimirovich", "Владимир Владимирович Путин", "Vladimir Vladimirovich Putin"])])
        .call(),
      e("Person")
        .id("NK-5dEHMo3SqLdUgnTVvTtejp")
        .properties(&[("name", &["Vladimir Nikitovich Skoch", "SKOCH, Vladimir Nikitovich", "Владимир Никитович Скоч"])])
        .call(),
      e("Person")
        .id("NK-8bMT7hixpkpiKCpEHUupAp")
        .properties(&[("name", &["POLIN, Vladimir Anatolevich", "Владимир Анатольевич Полин", "Vladimir Anatolevich Polin"])])
        .call(),
      e("Person")
        .id("Q108898811")
        .properties(&[("name", &["PLYAKIN, Vladimir Vladimirovich", "Vladimir Vladimirovich Plyakin", "Владимир Владимирович Плякин"])])
        .call(),
    ];

    let nomenklatura_scores = nomenklatura_score(Algorithm::NameBased, &query, results.clone()).unwrap();

    for (index, (_, nomenklatura_score)) in nomenklatura_scores.into_iter().enumerate() {
      let (score, _) = NameBased::score(&query, results.get(index).unwrap());

      assert!(approx_eq!(f64, score, nomenklatura_score, epsilon = 0.05));
    }
  }
}
