use std::sync::LazyLock;

use bumpalo::Bump;
use tracing::instrument;

use crate::{
  matching::{
    Feature, MatchingAlgorithm,
    logic_v1::logic_v1,
    matchers::{
      address::AddressEntityMatch,
      crypto_wallet::CryptoWalletMatch,
      identifier::IdentifierMatch,
      jaro_winkler::PersonNameJaroWinkler,
      marble::LongestCommonSubsequence,
      match_::{SimpleMatch, WeakAliasMatch},
      mismatch::{NumbersMismatch, SimpleMismatch, dob_day_disjoint, dob_progressive_match, dob_year_disjoint},
      name_fingerprint_levenshtein::NameFingerprintLevenshtein,
      name_literal_match::NameLiteralMatch,
      orgid_mismatch::OrgIdMismatch,
      phonetic::PersonNamePhoneticMatch,
    },
    validators::{validate_bic, validate_imo_mmsi, validate_inn, validate_isin, validate_ogrn},
  },
  model::PropertyFilter,
};

/// Default matching algorithm
pub struct MarbleV0;

static FEATURES: LazyLock<Vec<(&'static dyn Feature, f64)>> = LazyLock::new(|| {
  vec![
    (&NameLiteralMatch, 1.0),
    (&PersonNameJaroWinkler, 0.8),
    (&PersonNamePhoneticMatch, 0.9),
    (&NameFingerprintLevenshtein, 0.9),
    (&LongestCommonSubsequence, 0.8),
    // TODO: The weight of those two features are 0.0 by default, so until we
    // implement a way to customize weights, there is no use implementing
    // them:
    //
    //  - name_metaphone_match
    //  - name_soundex_match
    (&AddressEntityMatch, 0.98),
    (&CryptoWalletMatch, 0.98),
    (IdentifierMatch::new("isin_security_match", &["isin"], Some(validate_isin)), 0.98),
    (IdentifierMatch::new("lei_code_match", &["leiCode"], Some(lei::validate)), 0.95),
    (IdentifierMatch::new("ogrn_code_match", &["ogrnCode"], Some(validate_ogrn)), 0.95),
    (IdentifierMatch::new("vessel_imo_mmsi_match", &["imoNumber", "mmsi"], Some(validate_imo_mmsi)), 0.95),
    (IdentifierMatch::new("inn_code_match", &["innCode"], Some(validate_inn)), 0.95),
    (IdentifierMatch::new("bic_code_match", &["bicCode"], Some(validate_bic)), 0.95),
    (SimpleMatch::new("identifier_match", &|e| e.prop_group("identifier", PropertyFilter::All), None), 0.85), // TODO: add cleaning
    (&WeakAliasMatch, 0.8),
  ]
});

static QUALIFIERS: LazyLock<Vec<(&'static dyn Feature, f64)>> = LazyLock::new(|| {
  vec![
    (SimpleMatch::new("country_match", &|e| e.prop_group("country", PropertyFilter::All), None), 0.1),
    (SimpleMatch::new("dob_progressive_match", &|e| e.props(&["birthDate"]), Some(dob_progressive_match)), 0.15),
  ]
});

static DISQUALIFIERS: LazyLock<Vec<(&'static dyn Feature, f64)>> = LazyLock::new(|| {
  vec![
    (SimpleMismatch::new("country_mismatch", &|e| e.prop_group("country", PropertyFilter::All), None), -0.2),
    (SimpleMismatch::new("last_name_mismatch", &|e| e.props(&["lastName"]), None), -0.2),
    (SimpleMismatch::new("dob_year_disjoint", &|e| e.props(&["birthDate"]), Some(dob_year_disjoint)), -0.15),
    (SimpleMismatch::new("dob_day_disjoint", &|e| e.props(&["birthDate"]), Some(dob_day_disjoint)), -0.2),
    (SimpleMismatch::new("gender_mismatch", &|e| e.props(&["gender"]), None), -0.2),
    (SimpleMismatch::new("identifier_mismatch", &|e| e.prop_group("identifier", PropertyFilter::All), None), -0.3),
    (&OrgIdMismatch, -0.2),
    (&NumbersMismatch, -0.1),
  ]
});

impl MatchingAlgorithm for MarbleV0 {
  fn name() -> &'static str {
    "marble-v0"
  }

  #[instrument(name = "score_hit", skip_all, fields(entity_id = rhs.id))]
  fn score(bump: &Bump, lhs: &crate::model::SearchEntity, rhs: &crate::model::Entity, cutoff: f64) -> (f64, Vec<(&'static str, f64)>) {
    logic_v1(bump, lhs, rhs, cutoff, &FEATURES, &QUALIFIERS, &DISQUALIFIERS)
  }
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;
  use float_cmp::approx_eq;
  use itertools::Itertools;

  use crate::{
    matching::{Feature, MatchingAlgorithm},
    model::{Entity, SearchEntity},
  };

  #[test]
  fn marble_v0_person() {
    let lhs = SearchEntity::builder("Person").properties(&[("name", &["Vladimir Bob Putain"])]).build();
    let rhs = Entity::builder("Person")
      .properties(&[("name", &["PUTIN vladimir vladimirovich", "PUTIN, Vladimir Vladimirovich", "Владимир Путин", "Vladimyr Bob Phutain"])])
      .build();

    let (score, features) = super::MarbleV0::score(&Bump::new(), &lhs, &rhs, 0.0);

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
  fn marble_v0_company() {
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

    let (score, features) = super::MarbleV0::score(&Bump::new(), &lhs, &rhs, 0.0);

    assert_eq!(score, 0.95);
    assert!(features.iter().contains(&("name_fingerprint_levenshtein", 7.0 / 9.0)));
    assert!(features.iter().contains(&("lei_code_match", 1.0)));
    assert!(features.iter().contains(&("ogrn_code_match", 1.0)));
  }

  #[test]
  fn marble_v0_vessel() {
    let lhs = SearchEntity::builder("Vessel").properties(&[("mmsi", &["366123456"])]).build();
    let rhs = Entity::builder("Vessel").properties(&[("imoNumber", &["366123456"])]).build();

    let (score, features) = super::MarbleV0::score(&Bump::new(), &lhs, &rhs, 0.0);

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
  fn marble_v0_features() {
    let lhs = SearchEntity::builder("Person")
      .properties(&[("name", &["Samir Kamil AlAssad"]), ("country", &["sy"]), ("birthDate", &["1980-06-15"]), ("passportNumber", &["X111"])])
      .build();
    let rhs = Entity::builder("Person")
      .properties(&[("name", &["Samer Kamel Al Asad"]), ("country", &["sy"]), ("birthDate", &["1980-06-15"]), ("passportNumber", &["Y999"])])
      .build();

    let (_, features) = super::MarbleV0::score(&Bump::new(), &lhs, &rhs, 0.0);
    let feature_score = |name: &str| features.iter().find(|(n, _)| *n == name).map(|(_, score)| *score);

    assert!(feature_score("longest_common_subsequence").is_some_and(|score| score > 0.8));
    assert_eq!(feature_score("country_match"), Some(1.0));
    assert_eq!(feature_score("dob_progressive_match"), Some(1.0));
    assert_eq!(feature_score("identifier_mismatch"), Some(1.0));
  }
}
