use std::sync::LazyLock;

use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use serde::Deserialize;

use crate::matching::replacers::Fingerprints;

pub(crate) static ORG_TYPES: LazyLock<(AhoCorasick, Vec<String>)> = LazyLock::new(|| {
  let file = Fingerprints::get("types/types.yml").expect("could not read org types dictionary");
  let dictionary = serde_yaml::from_slice::<OrgTypeDictionary>(&file.data).expect("could not unmarshal org type dictionary");

  let mut patterns = Vec::new();
  let mut replacements = Vec::new();

  for item in dictionary.types {
    if let Some(main) = &item.main {
      for alias in item.forms {
        patterns.push(alias.to_lowercase());
        replacements.push(main.to_lowercase());
      }
    }
  }

  (
    AhoCorasickBuilder::new().match_kind(MatchKind::LeftmostLongest).ascii_case_insensitive(true).build(patterns).unwrap(),
    replacements,
  )
});

#[derive(Deserialize)]
struct OrgTypeDictionary {
  types: Vec<OrgTypeDictionaryEntry>,
}

#[derive(Deserialize)]
struct OrgTypeDictionaryEntry {
  main: Option<String>,
  forms: Vec<String>,
}
