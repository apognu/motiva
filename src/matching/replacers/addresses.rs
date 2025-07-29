use std::{collections::HashMap, sync::LazyLock};

use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use serde::Deserialize;

use crate::matching::replacers::Dictionaries;

pub static ADDRESS_FORMS: LazyLock<(AhoCorasick, Vec<&'static str>)> = LazyLock::new(|| {
  let file = Dictionaries::get("addresses/forms.yml").expect("could not read address forms dictionary");
  let dictionary = serde_yaml::from_slice::<AddressFormDictionary>(&file.data).expect("could not unmarshal org type dictionary");

  let mut patterns = Vec::new();
  let mut replacements = Vec::new();

  for (key, items) in dictionary.forms {
    patterns.push(key.to_lowercase());
    replacements.push(" ");

    for item in items {
      patterns.push(item.to_lowercase());
      replacements.push(" ");
    }
  }

  (
    AhoCorasickBuilder::new().match_kind(MatchKind::LeftmostLongest).ascii_case_insensitive(true).build(patterns).unwrap(),
    replacements,
  )
});

#[derive(Deserialize)]
pub struct AddressFormDictionary {
  forms: HashMap<String, Vec<String>>,
}
