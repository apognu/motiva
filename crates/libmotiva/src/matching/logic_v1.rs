use std::time::Instant;

use bumpalo::Bump;
use tracing::instrument;

use crate::matching::{
  Feature, MatchingAlgorithm,
  matchers::{
    address::AddressEntityMatch,
    crypto_wallet::CryptoWalletMatch,
    identifier::IdentifierMatch,
    jaro_winkler::PersonNameJaroWinkler,
    match_::SimpleMatch,
    mismatch::{NumbersMismatch, SimpleMismatch, dob_day_disjoint, dob_year_disjoint},
    name_fingerprint_levenshtein::NameFingerprintLevenshtein,
    name_literal_match::NameLiteralMatch,
    orgid_mismatch::OrgIdMismatch,
    phonetic::PersonNamePhoneticMatch,
  },
  run_features,
  validators::{validate_bic, validate_imo_mmsi, validate_inn, validate_isin, validate_ogrn},
};

/// Default matching algorithm
pub struct LogicV1;

impl MatchingAlgorithm for LogicV1 {
  fn name() -> &'static str {
    "logic-v1"
  }

  #[instrument(name = "score_hit", skip_all, fields(entity_id = rhs.id))]
  fn score(bump: &Bump, lhs: &crate::model::SearchEntity, rhs: &crate::model::Entity, cutoff: f64) -> (f64, Vec<(&'static str, f64)>) {
    if !rhs.schema.is_a(lhs.schema.as_str()) {
      return (0.0, vec![]);
    }

    let features: &[(&dyn Feature, f64)] = &[
      (&NameLiteralMatch, 1.0),
      (&PersonNameJaroWinkler, 0.8),
      (&PersonNamePhoneticMatch, 0.9),
      (&NameFingerprintLevenshtein, 0.9),
      // TODO: The weight of those two features are 0.0 by default, so until we
      // implement a way to customize weights, there is no use implementing
      // them:
      //
      //  - name_metaphone_match
      //  - name_soundex_match
      (&AddressEntityMatch, 0.98),
      (&CryptoWalletMatch, 0.98),
      (&IdentifierMatch::new("isin_security_match", &["isin"], Some(validate_isin)), 0.98),
      (&IdentifierMatch::new("lei_code_match", &["leiCode"], Some(lei::validate)), 0.95),
      (&IdentifierMatch::new("ogrn_code_match", &["ogrnCode"], Some(validate_ogrn)), 0.95),
      (&IdentifierMatch::new("vessel_imo_mmsi_match", &["imoNumber", "mmsi"], Some(validate_imo_mmsi)), 0.95),
      (&IdentifierMatch::new("inn_code_match", &["innCode"], Some(validate_inn)), 0.95),
      (&IdentifierMatch::new("bic_code_match", &["bicCode"], Some(validate_bic)), 0.95),
      (
        &SimpleMatch::new(
          "identifier_match",
          &|e| e.props(&["registrationNumber", "taxNumber", "leiCode", "innCode", "bicCode", "ogrnCode", "imoNumber", "mmsi"]),
          None,
        ),
        0.85,
      ), // TODO: add cleaning
      (&SimpleMatch::new("weak_alias_match", &|e| e.props(&["weakAlias", "name"]), None), 0.8),
    ];

    let qualifiers: &[(&dyn Feature, f64)] = &[
      (&SimpleMismatch::new("country_mismatch", &|e| e.props(&["country", "nationality", "citizenship"]), None), -0.2),
      (&SimpleMismatch::new("last_name_mismatch", &|e| e.props(&["lastName"]), None), -0.2),
      (&SimpleMismatch::new("dob_year_disjoint", &|e| e.props(&["birthDate"]), Some(dob_year_disjoint)), -0.15),
      (&SimpleMismatch::new("dob_day_disjoint", &|e| e.props(&["birthDate"]), Some(dob_day_disjoint)), -0.2),
      (&SimpleMismatch::new("gender_mismatch", &|e| e.props(&["gender"]), None), -0.2),
      (&OrgIdMismatch, -0.2),
      (&NumbersMismatch, -0.1),
    ];

    let mut results = Vec::with_capacity(features.len() + qualifiers.len());
    let mut score = 0.0f64;

    for (func, weight) in features {
      let then = Instant::now();
      let feature_score = func.score_feature(bump, lhs, rhs);

      results.push((func.name(), feature_score));

      if (feature_score * weight) > score {
        score = feature_score * weight;
      }

      tracing::debug!(feature = func.name(), score = feature_score, latency = ?then.elapsed(), "computed feature score");
    }

    let score = run_features(bump, lhs, rhs, cutoff, score, qualifiers, &mut results);

    (score.clamp(0.0, 1.0), results)
  }
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;
  use float_cmp::approx_eq;
  use itertools::Itertools;
  use pyo3::Python;

  use crate::{
    matching::{Algorithm, Feature, MatchingAlgorithm, logic_v1::LogicV1},
    model::{Entity, SearchEntity},
    tests::python::nomenklatura_score,
  };

  #[test]
  fn logic_v1_person() {
    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Bob Putain"])]).build();
    let rhs = Entity::builder("Person")
      .properties(&[("name", &["PUTIN vladimir vladimirovich", "PUTIN, Vladimir Vladimirovich", "Владимир Путин", "Vladimyr Bob Phutain"])])
      .build();

    let (score, features) = super::LogicV1::score(&Bump::new(), &lhs, &rhs, 0.0);

    assert!(approx_eq!(f64, score, 0.72, epsilon = 0.01));
    assert!(approx_eq!(
      f64,
      features.iter().filter(|(name, _)| *name == "person_name_jaro_winkler").map(|(_, score)| *score).next().unwrap(),
      0.9,
      epsilon = 0.01
    ));
    assert!(features.iter().contains(&("person_name_phonetic_match", 2.0 / 3.0)));
  }

  #[test]
  fn logic_v1_company() {
    let lhs = SearchEntity::builder("Company")
      .properties(&[("name", &["Google LLC"]), ("leiCode", &["529900T8BM49AURSDO55"]), ("ogrnCode", &["2022200525818"])])
      .build();
    let rhs = Entity::builder("Company")
      .properties(&[
        ("name", &["Gogole LIMITED LIABILITY COMPANY"]),
        ("leiCode", &["LEI1234"]),
        ("innCode", &["529900T8BM49AURSDO55", "2022200525818"]),
      ])
      .build();

    let (score, features) = super::LogicV1::score(&Bump::new(), &lhs, &rhs, 0.0);

    assert_eq!(score, 0.95);
    assert!(features.iter().contains(&("name_fingerprint_levenshtein", 7.0 / 9.0)));
    assert!(features.iter().contains(&("lei_code_match", 1.0)));
    assert!(features.iter().contains(&("ogrn_code_match", 1.0)));
  }

  #[test]
  fn logic_v1_vessel() {
    let lhs = SearchEntity::builder("Vessel").properties(&[("mmsi", &["366123456"])]).build();
    let rhs = Entity::builder("Vessel").properties(&[("imoNumber", &["366123456"])]).build();

    let (score, features) = super::LogicV1::score(&Bump::new(), &lhs, &rhs, 0.0);

    assert_eq!(score, 0.95);
    assert!(features.iter().contains(&("vessel_imo_mmsi_match", 1.0)));
  }

  #[test]
  fn person_name_jaro_winkler() {
    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Putin"])]).build();
    let rhs = Entity::builder("Person")
      .properties(&[("name", &["PUTIN vladimir vladimirovich", "PUTIN, Vladimir Vladimirovich", "Владимир Путин"])])
      .build();

    assert_eq!(super::PersonNameJaroWinkler.score_feature(&Bump::new(), &lhs, &rhs), 1.0);
  }

  #[test]
  fn person_name_phonetic_match() {
    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vlodimir Bob Putain"])]).build();
    let rhs = Entity::builder("Person")
      .properties(&[("name", &["PUTIN vladimir vladimirovich", "PUTIN, Vladimir Vladimirovich", "Владимир Путин"])])
      .build();

    assert!(approx_eq!(f64, super::PersonNamePhoneticMatch.score_feature(&Bump::new(), &lhs, &rhs), 2.0 / 3.0));
  }

  #[test]
  #[serial_test::serial]
  fn against_nomenklatura() {
    Python::initialize();

    let queries = vec![
      SearchEntity::builder("Person").properties(&[("name", &["Fladimir Poutine"]), ("gender", &["female"])]).build(),
      SearchEntity::builder("Person")
        .properties(&[("name", &["Fladimir Poutine"]), ("gender", &["female"]), ("country", &["cn"])])
        .build(),
      SearchEntity::builder("Company").properties(&[("name", &["Google"]), ("leiCode", &["529900T8BM49AURSDO55"])]).build(),
      SearchEntity::builder("Vessel").properties(&[("name", &["Titanic"]), ("imoNumber", &["IMO8712345"])]).build(),
      SearchEntity::builder("Address").properties(&[("full", &["No.3, New York avenue, 103-222, New York City"])]).build(),
    ];

    let results = vec![
      Entity::builder("Person")
        .id("Q7747")
        .properties(&[("name", &["Vladimir Putin"]), ("gender", &["male"]), ("country", &["ru"])])
        .build(),
      Entity::builder("Person")
        .id("Q7748")
        .properties(&[("name", &["Barack Hussein Obama"]), ("gender", &["female"]), ("country", &["us"])])
        .build(),
      Entity::builder("Company").properties(&[("name", &["Gooogle"]), ("innCode", &["529900T8BM49AURSDO55"])]).build(),
      Entity::builder("Vessel").properties(&[("name", &["Titanic"]), ("mssi", &["IMO8712345"])]).build(),
      Entity::builder("Address").properties(&[("full", &["3 New York ave, 103222, New York City"])]).build(),
    ];

    for query in queries {
      let nscores = nomenklatura_score(Algorithm::LogicV1, &query, results.clone()).unwrap();

      for (index, (_, nscore)) in nscores.into_iter().enumerate() {
        let (score, _) = LogicV1::score(&Bump::new(), &query, results.get(index).unwrap(), 0.0);

        assert!(approx_eq!(f64, score, nscore, epsilon = 0.01));
      }
    }
  }
}
