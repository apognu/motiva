use bumpalo::Bump;
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

/// Simple matching algorithm using name similarity, and penalty for disjoint attributes
pub struct NameQualified;

impl MatchingAlgorithm for NameQualified {
  fn name() -> &'static str {
    "name-qualified"
  }

  #[instrument(name = "score_hit", skip_all, fields(algorithm = Self::name(), entity_id = rhs.id))]
  fn score(bump: &Bump, lhs: &SearchEntity, rhs: &Entity, cutoff: f64) -> (f64, Vec<(&'static str, f64)>) {
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
    let score = run_features(bump, lhs, rhs, 0.0, cutoff, features, &mut results);

    (score.clamp(0.0, 1.0), results)
  }
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;
  use float_cmp::approx_eq;

  use crate::{
    matching::{Algorithm, MatchingAlgorithm, name_qualified::NameQualified},
    model::{Entity, SearchEntity},
    tests::python::nomenklatura_score,
  };

  #[test]
  #[serial_test::serial]
  fn person_against_nomenklatura() {
    pyo3::prepare_freethreaded_python();

    let queries = vec![
      SearchEntity::builder("Person").properties(&[("name", &["Владимир Путин"])]).build(),
      SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build(),
      SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"]), ("birthDate", &["1952-07-10"])]).build(),
      SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"]), ("birthDate", &["1987-04-20"])]).build(),
      SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"]), ("birthDate", &["1952-04-20"])]).build(),
      SearchEntity::builder("Person")
        .properties(&[("name", &["Vladimir Putin"]), ("birthDate", &["1982-07-10"]), ("gender", &["female"])])
        .build(),
      SearchEntity::builder("Person")
        .properties(&[("name", &["Vladimir Putin"]), ("birthDate", &["1982-07-10"]), ("gender", &["female"]), ("country", &["fr"])])
        .build(),
      SearchEntity::builder("Person").properties(&[("name", &["Beyonce Knowles"])]).build(),
      SearchEntity::builder("Person").properties(&[("name", &["Beyoncé Knowles"])]).build(),
      SearchEntity::builder("Person").properties(&[("name", &["Beyoncé"]), ("birthDate", &["1981-09-04"])]).build(),
      SearchEntity::builder("Person").properties(&[("name", &["Beyoncé"]), ("birthDate", &["1981-09-05"])]).build(),
      SearchEntity::builder("Person")
        .properties(&[("name", &["Beyoncé"]), ("birthDate", &["1981-09-04"]), ("gender", &["female"])])
        .build(),
      SearchEntity::builder("Person")
        .properties(&[("name", &["Beyoncé"]), ("birthDate", &["1981-09-04"]), ("gender", &["male"])])
        .build(),
      SearchEntity::builder("Person")
        .properties(&[("name", &["Beyoncé"]), ("birthDate", &["1981-09-04"]), ("country", &["us"])])
        .build(),
      SearchEntity::builder("Person")
        .properties(&[("name", &["Beyoncé"]), ("birthDate", &["1981-09-04"]), ("country", &["fr"])])
        .build(),
      SearchEntity::builder("Person").properties(&[("name", &["Elon Musk"])]).build(),
      SearchEntity::builder("Person").properties(&[("name", &["Elon Musc"])]).build(),
      SearchEntity::builder("Person").properties(&[("name", &["Elon Musk"]), ("birthDate", &["1971-06-28"])]).build(),
      SearchEntity::builder("Person").properties(&[("name", &["Elon Musk"]), ("birthDate", &["1971-06-29"])]).build(),
      SearchEntity::builder("Person")
        .properties(&[("name", &["Elon Musk"]), ("birthDate", &["1971-06-28"]), ("gender", &["male"])])
        .build(),
      SearchEntity::builder("Person")
        .properties(&[("name", &["Elon Musk"]), ("birthDate", &["1971-06-28"]), ("gender", &["female"])])
        .build(),
      SearchEntity::builder("Person")
        .properties(&[("name", &["Elon Musk"]), ("birthDate", &["1971-06-28"]), ("country", &["za"])])
        .build(),
      SearchEntity::builder("Person")
        .properties(&[("name", &["Elon Musk"]), ("birthDate", &["1971-06-28"]), ("country", &["us"])])
        .build(),
    ];

    let results = vec![
      Entity::builder("Person")
        .id("Q7747")
        .properties(&[
          ("name", &["Fladymir Poutin"]),
          ("alias", &["Vladou", "Poutine"]),
          ("birthDate", &["1952-10-07"]),
          ("gender", &["male"]),
          ("country", &["ru"]),
        ])
        .build(),
      Entity::builder("Person")
        .id("Q3123")
        .properties(&[
          ("name", &["Beyoncé Knowles"]),
          ("alias", &["Beyoncé"]),
          ("birthDate", &["1981-09-04"]),
          ("gender", &["female"]),
          ("country", &["us"]),
        ])
        .build(),
      Entity::builder("Person")
        .id("Q317521")
        .properties(&[
          ("name", &["Elon Musk"]),
          ("alias", &["Elon"]),
          ("birthDate", &["1971-06-28"]),
          ("gender", &["male"]),
          ("country", &["za"]),
        ])
        .build(),
    ];

    for query in queries {
      let nscores = nomenklatura_score(Algorithm::NameQualified, &query, results.clone()).unwrap();

      for (index, (_, nscore)) in nscores.into_iter().enumerate() {
        let (score, _) = NameQualified::score(&Bump::new(), &query, results.get(index).unwrap(), 0.0);

        assert!(
          approx_eq!(f64, score, nscore, epsilon = 0.01),
          "score mistmatch {score} vs {nscore}: {query:?} / {:?}",
          results.get(index)
        );
      }
    }
  }

  #[test]
  #[serial_test::serial]
  fn company_against_nomenklatura() {
    pyo3::prepare_freethreaded_python();

    let queries = vec![
      SearchEntity::builder("Company").properties(&[("name", &["Google"]), ("registrationNumber", &["FR12"])]).build(),
      SearchEntity::builder("Company").properties(&[("name", &["Gooogle"]), ("registrationNumber", &["US1234"])]).build(),
      SearchEntity::builder("Company").properties(&[("name", &["Google"]), ("registrationNumber", &["US1234"])]).build(),
    ];

    let results = vec![Entity::builder("Company").id("TEST").properties(&[("name", &["Google"]), ("registrationNumber", &["US-1234"])]).build()];

    for query in queries {
      let nscores = nomenklatura_score(Algorithm::NameQualified, &query, results.clone()).unwrap();

      for (index, (_, nscore)) in nscores.into_iter().enumerate() {
        let (score, _) = NameQualified::score(&Bump::new(), &query, results.get(index).unwrap(), 0.0);

        assert!(approx_eq!(f64, score, nscore, epsilon = 0.01));
      }
    }
  }
}
