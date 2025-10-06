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
          &|e| e.gather(&["registrationNumber", "taxNumber", "leiCode", "innCode", "bicCode", "ogrnCode", "imoNumber", "mmsi"]),
          None,
        ),
        0.85,
      ), // TODO: add cleaning
      (&SimpleMatch::new("weak_alias_match", &|e| e.gather(&["weakAlias", "name"]), None), 0.8),
    ];

    let qualifiers: &[(&dyn Feature, f64)] = &[
      (&SimpleMismatch::new("country_mismatch", &|e| e.property("country"), None), -0.2),
      (&SimpleMismatch::new("last_name_mismatch", &|e| e.property("lastName"), None), -0.2),
      (&SimpleMismatch::new("dob_year_disjoint", &|e| e.property("birthDate"), Some(dob_year_disjoint)), -0.15),
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
    pyo3::prepare_freethreaded_python();

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

  #[test]
  #[ignore]
  fn extensive_entity_matching_test() {
    pyo3::prepare_freethreaded_python();

    let queries = vec![
      SearchEntity::builder("Person").properties(&[("name", &["John Fitzgerald Kennedy"]), ("country", &["us"])]).build(),
      SearchEntity::builder("Person").properties(&[("name", &["Владимир Путин"]), ("gender", &["male"])]).build(),
      SearchEntity::builder("Person").properties(&[("name", &["محمد بن سلمان آل سعود"]), ("name", &["MBS"])]).build(),
      SearchEntity::builder("Person").properties(&[("name", &["Barak Obama"]), ("country", &["us"])]).build(),
      SearchEntity::builder("Person")
        .properties(&[("name", &["Angela Merkel"]), ("gender", &["male"]), ("country", &["de"])])
        .build(),
      SearchEntity::builder("Person").properties(&[("name", &["Einstein"])]).build(),
      SearchEntity::builder("Person").properties(&[("name", &["Frida Kahlo"]), ("birthDate", &["1907"])]).build(),
      SearchEntity::builder("Person").properties(&[("name", &["The Rock"]), ("gender", &["male"])]).build(),
      SearchEntity::builder("Person")
        .properties(&[("name", &["Osama bin Laden"]), ("name", &["Usama bin Mohammed bin Awad bin Ladin"])])
        .build(),
      SearchEntity::builder("Person").properties(&[("name", &["Yassir Arafat"]), ("country", &["ps"])]).build(),
      SearchEntity::builder("Person").properties(&[("name", &["Xi Jinping"]), ("country", &["cn"])]).build(),
      SearchEntity::builder("Person").properties(&[("name", &["J. R. R. Tolkien"]), ("birthDate", &["1892-01-03"])]).build(),
      SearchEntity::builder("Person").properties(&[("name", &["Sherlock Holmes"]), ("nationality", &["gb"])]).build(),
      SearchEntity::builder("Person")
        .properties(&[("name", &["Marie Curie"]), ("gender", &["male"]), ("birthDate", &["1900"])])
        .build(),
      SearchEntity::builder("Person").properties(&[("name", &["Ford"])]).build(),
      SearchEntity::builder("Company")
        .properties(&[("name", &["Apple Inc."]), ("leiCode", &["HWUPKR0MPOU8FGXBT394"])])
        .build(),
      SearchEntity::builder("Company").properties(&[("name", &["Сбербанк"]), ("innCode", &["7707083893"])]).build(),
      SearchEntity::builder("Company")
        .properties(&[("name", &["Gooogle LLC"]), ("leiCode", &["5493001KJTIIGC8Y1R12"])])
        .build(),
      SearchEntity::builder("Company")
        .properties(&[("name", &["Microsoft Corporation"]), ("leiCode", &["THIS-IS-WRONG"])])
        .build(),
      SearchEntity::builder("Company").properties(&[("name", &["IBM"]), ("country", &["us"])]).build(),
      SearchEntity::builder("Company").properties(&[("ogrnCode", &["1027700067328"])]).build(),
      SearchEntity::builder("Company").properties(&[("name", &["General Electric"])]).build(),
      SearchEntity::builder("Company").properties(&[("name", &["株式会社日立製作所"])]).build(),
      SearchEntity::builder("Company").properties(&[("name", &["Enron Corp."]), ("country", &["us"])]).build(),
      SearchEntity::builder("Company").properties(&[("name", &["Gazprom PAO"]), ("country", &["ru"])]).build(),
      SearchEntity::builder("Company")
        .properties(&[("name", &["Rosneft"]), ("innCode", &["7706107510"]), ("leiCode", &["WRONG-LEI"])])
        .build(),
      SearchEntity::builder("Company")
        .properties(&[("name", &["Berkshire Hathaway"]), ("leiCode", &["5493003G524S322TA861"])])
        .build(),
      SearchEntity::builder("Company").properties(&[("name", &["The Coca-Cola Company (Global)"])]).build(),
      SearchEntity::builder("Company").properties(&[("name", &["Toyota Motor"]), ("taxNumber", &["1201040105787"])]).build(),
      SearchEntity::builder("Company").properties(&[("name", &["Global Trade Solutions Ltd"])]).build(),
      SearchEntity::builder("Vessel").properties(&[("name", &["Ever Given"]), ("imoNumber", &["9811000"])]).build(),
      SearchEntity::builder("Vessel").properties(&[("name", &["Queen Mary 2"])]).build(),
      SearchEntity::builder("Vessel").properties(&[("imoNumber", &["IMO9241061"])]).build(),
      SearchEntity::builder("Vessel").properties(&[("mmsi", &["311000538"])]).build(),
      SearchEntity::builder("Vessel").properties(&[("name", &["Titnic"]), ("imoNumber", &["491414"])]).build(),
      SearchEntity::builder("Vessel").properties(&[("name", &["Symphony of the Seas"]), ("imoNumber", &["0000000"])]).build(),
      SearchEntity::builder("Vessel").properties(&[("name", &["Seawise Giant"])]).build(),
      SearchEntity::builder("Vessel")
        .properties(&[("name", &["CMA CGM Jacques Saadé"]), ("imoNumber", &["9839179"]), ("mmsi", &["228403000"])])
        .build(),
      SearchEntity::builder("Address")
        .properties(&[("full", &["1600 Pennsylvania Avenue NW, Washington, D.C. 20500, USA"])])
        .build(),
      SearchEntity::builder("Address").properties(&[("full", &["10 Downing St, London, UK"])]).build(),
      SearchEntity::builder("Address").properties(&[("full", &["221B Baker Stret, London"])]).build(),
      SearchEntity::builder("Address").properties(&[("full", &["الكرملين، موسكو، روسيا، 103132"])]).build(),
      SearchEntity::builder("Address").properties(&[("full", &["Eiffel Tower, Paris"])]).build(),
      SearchEntity::builder("Address").properties(&[("full", &["Apartment 42, 1 Hacker Way, Menlo Park, CA"])]).build(),
      SearchEntity::builder("Address").properties(&[("full", &["Wall Street, New York"])]).build(),
      SearchEntity::builder("CryptoWallet")
        .properties(&[("publicKey", &["bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq"])])
        .build(),
      SearchEntity::builder("CryptoWallet")
        .properties(&[("publicKey", &["0x742d35Cc6634C0532925a3b844Bc454e4438f44e"])])
        .build(),
      SearchEntity::builder("CryptoWallet")
        .properties(&[("publicKey", &["0x742d35Cc6634C0532925a3b844Bc454e4438f44f"])])
        .build(),
      SearchEntity::builder("CryptoWallet").properties(&[("publicKey", &["THIS-IS-NOT-A-WALLET-ADDRESS"])]).build(),
      SearchEntity::builder("CryptoWallet").properties(&[("publicKey", &["0x12345"])]).build(),
    ];

    let candidates = vec![
      Entity::builder("Person")
        .id("Q9696")
        .properties(&[("name", &["John F. Kennedy"]), ("country", &["us"]), ("gender", &["male"])])
        .build(),
      Entity::builder("Person")
        .id("Q7747")
        .properties(&[("name", &["Vladimir Putin"]), ("gender", &["male"]), ("country", &["ru"])])
        .build(),
      Entity::builder("Person").id("Q3099982").properties(&[("name", &["Mohammed bin Salman"]), ("country", &["sa"])]).build(),
      Entity::builder("Person")
        .id("Q76")
        .properties(&[("name", &["Barack Obama"]), ("birthDate", &["1961"]), ("country", &["us"])])
        .build(),
      Entity::builder("Person")
        .id("Q567")
        .properties(&[("name", &["Angela Dorothea Merkel"]), ("gender", &["female"]), ("country", &["de"])])
        .build(),
      Entity::builder("Person")
        .id("Q937")
        .properties(&[("name", &["Albert Einstein"]), ("gender", &["male"]), ("birthDate", &["1879"])])
        .build(),
      Entity::builder("Person")
        .id("Q5588")
        .properties(&[("name", &["Frida Kahlo de Rivera"]), ("birthDate", &["1907-07-06"])])
        .build(),
      Entity::builder("Person")
        .id("Q10738")
        .properties(&[("name", &["Dwayne Johnson"]), ("name", &["The Rock"]), ("gender", &["male"])])
        .build(),
      Entity::builder("Person")
        .id("Q1317")
        .properties(&[("name", &["Osama bin Laden"]), ("country", &["sa"]), ("status", &["deceased"])])
        .build(),
      Entity::builder("Person").id("Q34211").properties(&[("name", &["Yasser Arafat"]), ("country", &["ps"])]).build(),
      Entity::builder("Person").id("Q15031").properties(&[("name", &["习近平"]), ("country", &["cn"])]).build(),
      Entity::builder("Person")
        .id("Q892")
        .properties(&[("name", &["John Ronald Reuel Tolkien"]), ("birthDate", &["1892-01-03"])])
        .build(),
      Entity::builder("Person")
        .id("Q35610")
        .properties(&[("name", &["Arthur Conan Doyle"]), ("nationality", &["gb"])])
        .build(),
      Entity::builder("Person")
        .id("Q7186")
        .properties(&[("name", &["Marie Skłodowska Curie"]), ("gender", &["female"]), ("birthDate", &["1867-11-07"])])
        .build(),
      Entity::builder("Person").id("Q82333").properties(&[("name", &["Henry Ford"]), ("gender", &["male"])]).build(),
      Entity::builder("Company")
        .id("NK-aV5xM")
        .properties(&[("name", &["Apple Inc."]), ("leiCode", &["HWUPKR0MPOU8FGXBT394"]), ("country", &["us"])])
        .build(),
      Entity::builder("Company")
        .id("NK-9s8aF")
        .properties(&[("name", &["Sberbank of Russia"]), ("innCode", &["7707083893"]), ("country", &["ru"])])
        .build(),
      Entity::builder("Company")
        .id("NK-2aZ5h")
        .properties(&[("name", &["Google LLC"]), ("leiCode", &["5493001KJTIIGC8Y1R12"])])
        .build(),
      Entity::builder("Company")
        .id("NK-fS7uW")
        .properties(&[("name", &["Microsoft Corporation"]), ("leiCode", &["549300336W2JAR332I34"])])
        .build(),
      Entity::builder("Company")
        .id("NK-cK4vL")
        .properties(&[("name", &["International Business Machines Corporation"]), ("alias", &["IBM"])])
        .build(),
      Entity::builder("Company")
        .id("NK-pB6gX")
        .properties(&[("name", &["Gazprom"]), ("ogrnCode", &["1027700067328"]), ("country", &["ru"])])
        .build(),
      Entity::builder("Company")
        .id("NK-tJ8kE")
        .properties(&[("name", &["General Electric Company"]), ("leiCode", &["EVI5T9JDE1V64T22FD69"])])
        .build(),
      Entity::builder("Company").id("NK-mN5yR").properties(&[("name", &["Hitachi, Ltd."]), ("country", &["jp"])]).build(),
      Entity::builder("Company")
        .id("NK-yH2wD")
        .properties(&[("name", &["Chevron Corporation"]), ("country", &["us"])])
        .build(),
      Entity::builder("Company")
        .id("NK-pB6gX-2")
        .properties(&[("name", &["Public Joint Stock Company Gazprom"]), ("country", &["ru"])])
        .build(),
      Entity::builder("Company")
        .id("NK-vC3oZ")
        .properties(&[("name", &["Rosneft Oil Company"]), ("innCode", &["7706107510"]), ("leiCode", &["253400JT3CD521J45262"])])
        .build(),
      Entity::builder("Company")
        .id("NK-rG9sB")
        .properties(&[("name", &["Berkshire Hathaway Inc."]), ("leiCode", &["5493003G524S322TA861"])])
        .build(),
      Entity::builder("Company")
        .id("NK-aF4xP")
        .properties(&[("name", &["The Coca-Cola Company"]), ("country", &["us"])])
        .build(),
      Entity::builder("Company")
        .id("NK-uL7iQ")
        .properties(&[("name", &["Toyota Motor Corporation"]), ("leiCode", &["549300P4B9R13GCSL880"]), ("country", &["jp"])])
        .build(),
      Entity::builder("Company").id("NK-bH5zK").properties(&[("name", &["Maersk Line"]), ("country", &["dk"])]).build(),
      Entity::builder("Vessel")
        .id("Q65063544")
        .properties(&[("name", &["Ever Given"]), ("imoNumber", &["IMO9811000"])])
        .build(),
      Entity::builder("Vessel")
        .id("Q502933")
        .properties(&[("name", &["RMS Queen Mary 2"]), ("imoNumber", &["IMO9241061"])])
        .build(),
      Entity::builder("Vessel").id("Q502933-2").properties(&[("name", &["APL England"])]).build(),
      Entity::builder("Vessel")
        .id("Q60775394")
        .properties(&[("name", &["Wonder of the Seas"]), ("mmsi", &["311000538"])])
        .build(),
      Entity::builder("Vessel").id("Q25173").properties(&[("name", &["RMS Titanic"]), ("imoNumber", &["491414"])]).build(),
      Entity::builder("Vessel")
        .id("Q28404283")
        .properties(&[("name", &["Symphony of the Seas"]), ("imoNumber", &["9744001"])])
        .build(),
      Entity::builder("Vessel")
        .id("Q642392")
        .properties(&[("name", &["Knock Nevis"]), ("pastName", &["Seawise Giant"])])
        .build(),
      Entity::builder("Vessel")
        .id("Q65087796")
        .properties(&[("name", &["CMA CGM Jacques Saadé"]), ("imoNumber", &["9839179"]), ("mmsi", &["228403000"])])
        .build(),
      Entity::builder("Address")
        .id("ADDR_US_WH")
        .properties(&[("full", &["The White House, 1600 Pennsylvania Ave NW, Washington, DC 20500"])])
        .build(),
      Entity::builder("Address")
        .id("ADDR_UK_DS")
        .properties(&[("full", &["10 Downing Street, London, SW1A 2AA, United Kingdom"])])
        .build(),
      Entity::builder("Address").id("ADDR_UK_BH").properties(&[("full", &["221B Baker Street, London"])]).build(),
      Entity::builder("Address").id("ADDR_RU_KR").properties(&[("full", &["Moscow Kremlin, Moscow, Russia, 103132"])]).build(),
      Entity::builder("Address")
        .id("ADDR_FR_ET")
        .properties(&[("full", &["Champ de Mars, 5 Avenue Anatole France, 75007 Paris, France"])])
        .build(),
      Entity::builder("Address")
        .id("ADDR_US_FB")
        .properties(&[("full", &["1 Hacker Way, Menlo Park, CA 94025, USA"])])
        .build(),
      Entity::builder("Address")
        .id("ADDR_US_WS")
        .properties(&[("full", &["11 Wall Street, New York, NY 10005, USA"])])
        .build(),
      Entity::builder("CryptoWallet")
        .id("BTC_DONATE_1")
        .properties(&[("publicKey", &["bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq"])])
        .build(),
      Entity::builder("CryptoWallet")
        .id("ETH_KRAKEN_10")
        .properties(&[("publicKey", &["0x742d35Cc6634C0532925a3b844Bc454e4438f44e"])])
        .build(),
      Entity::builder("CryptoWallet")
        .id("ETH_BINANCE_1")
        .properties(&[("publicKey", &["0x28C6c06298d514Db089934071355E5743bf21d60"])])
        .build(),
      Entity::builder("CryptoWallet")
        .id("BTC_SATOSHI_GEN")
        .properties(&[("publicKey", &["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"])])
        .build(),
      Entity::builder("CryptoWallet")
        .id("XRP_RANDOM_1")
        .properties(&[("publicKey", &["rEb8TK3gBgk5oZkwcONkWIb4n57_w4Xhnp"])])
        .build(),
    ];

    for query in queries {
      let nscores = nomenklatura_score(Algorithm::LogicV1, &query, candidates.clone()).unwrap();

      for (index, (_, nscore)) in nscores.into_iter().enumerate() {
        let candidate = candidates.get(index).unwrap();
        let (score, _) = LogicV1::score(&Bump::new(), &query, candidate, 0.0);

        assert!(
          approx_eq!(f64, score, nscore, epsilon = 0.01),
          "score mistmatch {score} vs {nscore}: {query:?} / {:?}",
          candidates.get(index)
        );
      }
    }
  }
}
