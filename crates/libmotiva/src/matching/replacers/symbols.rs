use std::{collections::HashMap, sync::LazyLock};

use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use serde::Deserialize;

use crate::matching::replacers::Dictionaries;

#[allow(dead_code)]
pub(crate) static ORG_SYMBOLS: LazyLock<(AhoCorasick, Vec<String>)> = LazyLock::new(|| {
  let file = Dictionaries::get("names/symbols.yml").expect("could not read org symbols dictionary");
  let dictionary = serde_yaml::from_slice::<OrgSymbolDictionary>(&file.data).expect("could not unmarshal org symbols dictionary");

  let mut patterns = Vec::new();
  let mut replacements = Vec::new();

  for (key, items) in dictionary.org_symbols {
    for item in items {
      patterns.push(item.to_lowercase());
      replacements.push(key.clone());
    }
  }

  (
    AhoCorasickBuilder::new().match_kind(MatchKind::LeftmostLongest).ascii_case_insensitive(true).build(patterns).unwrap(),
    replacements,
  )
});

#[derive(Deserialize)]
struct OrgSymbolDictionary {
  org_symbols: HashMap<String, Vec<String>>,
}

#[cfg(test)]
mod tests {
  use super::ORG_SYMBOLS;
  use crate::matching::replacers::replace;

  #[test]
  fn org_symbols_dictionary_builds() {
    assert!(!ORG_SYMBOLS.1.is_empty());
    assert_eq!(ORG_SYMBOLS.0.patterns_len(), ORG_SYMBOLS.1.len());
  }

  #[test]
  fn org_symbols_replaces_variants() {
    assert_eq!(replace(&ORG_SYMBOLS.0, &ORG_SYMBOLS.1, "company"), "co");
  }
}
