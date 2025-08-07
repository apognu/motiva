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
  validators::{validate_bic, validate_inn, validate_isin, validate_mmsi, validate_ogrn},
};

// WIP
pub struct LogicV1;

impl MatchingAlgorithm for LogicV1 {
  fn name() -> &'static str {
    "logic-v1"
  }

  #[instrument(name = "score_hit", skip_all)]
  fn score(bump: &Bump, lhs: &crate::model::SearchEntity, rhs: &crate::model::Entity, cutoff: f64) -> (f64, Vec<(&'static str, f64)>) {
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
      (&IdentifierMatch::new("isin_security_match", &["isin"], Some(validate_isin)), 0.98), // TODO: Only for Security
      (&IdentifierMatch::new("lei_code_match", &["leiCode"], Some(lei::validate)), 0.95),
      (&IdentifierMatch::new("ogrn_code_match", &["ogrnCode"], Some(validate_ogrn)), 0.95),
      (&IdentifierMatch::new("vessel_imo_mmsi_match", &["imoNumer", "mmsi"], Some(validate_mmsi)), 0.95), // TODO: Only for Vessel
      (&IdentifierMatch::new("inn_code_match", &["innCode"], Some(validate_inn)), 0.95),
      (&IdentifierMatch::new("bic_code_match", &["bicCode"], Some(validate_bic)), 0.95),
      (&SimpleMatch::new("identifier_match", &|e| e.gather(&["registrationNumber", "taxNumber"]), None), 0.85), // TODO: add cleaning
      (&SimpleMatch::new("weak_alias_match", &|e| e.gather(&["weakAlias", "name"]), None), 0.8),
    ];

    let qualifiers: &[(&dyn Feature, f64)] = &[
      (&SimpleMismatch::new("country_mismatch", &|e| e.property("country"), None), -0.2),
      (&SimpleMismatch::new("last_name_mismatch", &|e| e.property("lastName"), None), -0.2),
      (&SimpleMismatch::new("dob_year_disjoint", &|e| e.property("birthDate"), Some(dob_year_disjoint)), -0.2),
      (&SimpleMismatch::new("dob_day_disjoint", &|e| e.property("birthDate"), Some(dob_day_disjoint)), -0.2),
      (&SimpleMismatch::new("gender_mismatch", &|e| e.property("gender"), None), -0.2),
      (&OrgIdMismatch, -0.2),
      (&NumbersMismatch, -0.1),
    ];

    let mut results = Vec::with_capacity(features.len());
    let mut score = 0.0f64;

    for (func, weight) in features {
      let feature_score = func.score_feature(bump, lhs, rhs);

      results.push((func.name(), feature_score));

      tracing::debug!(feature = func.name(), score = feature_score, "computed feature score");

      if (feature_score * weight) > score {
        score = feature_score * weight;
      }
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

  use crate::{
    api::dto::Algorithm,
    matching::{Feature, MatchingAlgorithm, logic_v1::LogicV1},
    tests::{e, python::nomenklatura_score, se},
  };

  #[test]
  fn logic_v1_person() {
    let lhs = se("Person").properties(&[("name", &["Vladimir Bob Putain"])]).call();
    let rhs = e("Person")
      .properties(&[("name", &["PUTIN vladimir vladimirovich", "PUTIN, Vladimir Vladimirovich", "Владимир Путин", "Vladimyr Bob Phutain"])])
      .call();

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
    let lhs = se("Company")
      .properties(&[("name", &["Google LLC"]), ("leiCode", &["529900T8BM49AURSDO55"]), ("ogrnCode", &["2022200525818"])])
      .call();
    let rhs = e("Company")
      .properties(&[("name", &["Gogole SAS"]), ("leiCode", &["LEI1234"]), ("innCode", &["529900T8BM49AURSDO55", "2022200525818"])])
      .call();

    let (score, features) = super::LogicV1::score(&Bump::new(), &lhs, &rhs, 0.0);

    assert_eq!(score, 0.95);
    assert!(features.iter().contains(&("name_fingerprint_levenshtein", 7.0 / 9.0)));
    assert!(features.iter().contains(&("lei_code_match", 1.0)));
    assert!(features.iter().contains(&("ogrn_code_match", 1.0)));
  }

  #[test]
  fn logic_v1_vessel() {
    let lhs = se("Vessel").properties(&[("mmsi", &["366123456"])]).call();
    let rhs = e("Vessel").properties(&[("imoNumber", &["366123456"])]).call();

    let (score, features) = super::LogicV1::score(&Bump::new(), &lhs, &rhs, 0.0);

    assert_eq!(score, 0.95);
    assert!(features.iter().contains(&("vessel_imo_mmsi_match", 1.0)));
  }

  #[test]
  fn person_name_jaro_winkler() {
    let lhs = se("Person").properties(&[("name", &["Vladimir Putin"])]).call();
    let rhs = e("Person")
      .properties(&[("name", &["PUTIN vladimir vladimirovich", "PUTIN, Vladimir Vladimirovich", "Владимир Путин"])])
      .call();

    assert_eq!(super::PersonNameJaroWinkler.score_feature(&Bump::new(), &lhs, &rhs), 1.0);
  }

  #[test]
  fn person_name_phonetic_match() {
    let lhs = se("Person").properties(&[("name", &["Vlodimir Bob Putain"])]).call();
    let rhs = e("Person")
      .properties(&[("name", &["PUTIN vladimir vladimirovich", "PUTIN, Vladimir Vladimirovich", "Владимир Путин"])])
      .call();

    assert!(approx_eq!(f64, super::PersonNamePhoneticMatch.score_feature(&Bump::new(), &lhs, &rhs), 2.0 / 3.0));
  }

  #[serial_test::serial]
  #[test]
  fn against_nomenklatura() {
    pyo3::prepare_freethreaded_python();

    let queries = vec![
      se("Person").properties(&[("name", &["Fladimir Poutine"]), ("gender", &["female"])]).call(),
      se("Person").properties(&[("name", &["Fladimir Poutine"]), ("gender", &["female"]), ("country", &["cn"])]).call(),
      se("Company").properties(&[("name", &["Google"]), ("leiCode", &["529900T8BM49AURSDO55"])]).call(),
      se("Vessel").properties(&[("name", &["Titanic"]), ("imoNumber", &["IMO8712345"])]).call(),
      se("Address").properties(&[("full", &["No.3, New York avenue, 103-222, New York City"])]).call(),
    ];

    let results = vec![
      e("Person").id("Q7747").properties(&[("name", &["Vladimir Putin"]), ("gender", &["male"]), ("country", &["ru"])]).call(),
      e("Person")
        .id("Q7748")
        .properties(&[("name", &["Barack Hussein Obama"]), ("gender", &["female"]), ("country", &["us"])])
        .call(),
      e("Company").properties(&[("name", &["Gooogle"]), ("innCode", &["529900T8BM49AURSDO55"])]).call(),
      e("Vessel").properties(&[("name", &["Titanic"]), ("mssi", &["IMO8712345"])]).call(),
      e("Address").properties(&[("full", &["3 New York ave, 103222, New York City"])]).call(),
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
