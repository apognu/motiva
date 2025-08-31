use std::sync::LazyLock;

use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use serde::Deserialize;

use crate::matching::replacers::Dictionaries;

pub(crate) static STOPWORDS: LazyLock<(AhoCorasick, Vec<String>)> = LazyLock::new(|| {
  let file = Dictionaries::get("names/stopwords.yml").expect("could not read stopwords dictionary");
  let dictionary = serde_yaml::from_slice::<OrgSymbolDictionary>(&file.data).expect("could not unmarshal stopwords dictionary");

  let mut patterns = Vec::new();
  let mut replacements = Vec::new();

  for item in dictionary.person_name_prefixes {
    patterns.push(item.to_lowercase());
    replacements.push(String::new());
  }

  (
    AhoCorasickBuilder::new().match_kind(MatchKind::LeftmostLongest).ascii_case_insensitive(true).build(patterns).unwrap(),
    replacements,
  )
});

#[derive(Deserialize)]
struct OrgSymbolDictionary {
  #[serde(rename = "PERSON_NAME_PREFIXES")]
  person_name_prefixes: Vec<String>,
}
