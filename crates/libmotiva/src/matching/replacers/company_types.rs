use std::sync::LazyLock;

use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use serde::Deserialize;

use crate::matching::replacers::Dictionaries;

pub(crate) static ORG_TYPES: LazyLock<(AhoCorasick, Vec<String>)> = LazyLock::new(|| {
  let file = Dictionaries::get("names/org_types.yml").expect("could not read org types dictionary");
  let dictionary = serde_yaml::from_slice::<OrgTypeDictionary>(&file.data).expect("could not unmarshal org type dictionary");

  let mut patterns = Vec::new();
  let mut replacements = Vec::new();

  for item in dictionary.types {
    if let Some(display) = &item.display {
      for alias in item.aliases {
        patterns.push(alias.to_lowercase());
        replacements.push(display.to_lowercase());
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
  display: Option<String>,
  aliases: Vec<String>,
}
