use tracing::instrument;

use crate::{
  matching::{
    Feature, MatchingAlgorithm,
    matchers::{
      jaro_winkler::JaroNameParts,
      mismatch::{SimpleMismatch, dob_day_disjoint, dob_year_disjoint},
      orgid_mismatch::OrgIdMismatch,
      soundex::SoundexNameParts,
    },
    run_features,
  },
  model::{Entity, SearchEntity},
};

pub struct NameQualified;

impl MatchingAlgorithm for NameQualified {
  fn name() -> &'static str {
    "name-qualified"
  }

  #[instrument(name = "score_hit", skip_all, fields(algorithm = Self::name(), entity_id = rhs.id))]
  fn score(lhs: &SearchEntity, rhs: &Entity) -> (f64, Vec<(&'static str, f64)>) {
    let features: &[(&dyn Feature, f64)] = &[
      (&SoundexNameParts, 0.5),
      (&JaroNameParts, 0.5),
      (&SimpleMismatch::new("country_disjoint", &|e| e.property("country"), None), -0.1),
      (&SimpleMismatch::new("dob_year_disjoint", &|e| e.property("birthDate"), Some(dob_year_disjoint)), -0.1),
      (&SimpleMismatch::new("dob_day_disjoint", &|e| e.property("birthDate"), Some(dob_day_disjoint)), -0.15),
      (&SimpleMismatch::new("gender_disjoint", &|e| e.property("gender"), None), -0.1),
      (&OrgIdMismatch, -0.1),
    ];

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
    matching::{MatchingAlgorithm, name_qualified::NameQualified},
    tests::{e, python::nomenklatura_score, se},
  };

  #[test]
  fn putin_against_nomenklatura() {
    pyo3::prepare_freethreaded_python();

    let queries = vec![
      se("Person").properties(&[("name", &["Владимир Путин"])]).call(),
      se("Person").properties(&[("name", &["Vladimir Putin"])]).call(),
      se("Person").properties(&[("name", &["Vladimir Putin"]), ("birthDate", &["1952-07-10"])]).call(),
      se("Person").properties(&[("name", &["Vladimir Putin"]), ("birthDate", &["1987-04-20"])]).call(),
      se("Person").properties(&[("name", &["Vladimir Putin"]), ("birthDate", &["1952-04-20"])]).call(),
      se("Person")
        .properties(&[("name", &["Vladimir Putin"]), ("birthDate", &["1982-07-10"]), ("gender", &["female"])])
        .call(),
      se("Person")
        .properties(&[("name", &["Vladimir Putin"]), ("birthDate", &["1982-07-10"]), ("gender", &["female"]), ("country", &["fr"])])
        .call(),
    ];

    let results = vec![
      e("Person")
        .id("Q7747")
        .properties(&[
          ("name", &["PUTIN, Vladimir Vladimirovich", "Владимир Владимирович Путин", "Vladimir Vladimirovich Putin"]),
          ("birthDate", &["1952-10-07"]),
          ("gender", &["male"]),
          ("country", &["ru"]),
        ])
        .call(),
    ];

    for query in queries {
      let nomenklatura_scores = nomenklatura_score(Algorithm::NameQualified, &query, results.clone()).unwrap();

      for (index, (_, nomenklatura_score)) in nomenklatura_scores.into_iter().enumerate() {
        let (score, _) = NameQualified::score(&query, results.get(index).unwrap());

        assert!(approx_eq!(f64, score, nomenklatura_score, epsilon = 0.05));
      }
    }
  }
}
