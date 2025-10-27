#[cfg(test)]
mod tests {
  use bumpalo::Bump;
  use float_cmp::approx_eq;
  use pyo3::Python;

  use crate::{Algorithm, Entity, LogicV1, MatchingAlgorithm, SearchEntity, tests::python::nomenklatura_score};

  #[test]
  #[ignore = "comprehensive, slow test"]
  fn extensive_entity_matching_test() {
    Python::initialize();

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
