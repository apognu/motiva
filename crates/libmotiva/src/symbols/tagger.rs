use std::{collections::HashMap, io::BufReader, sync::LazyLock};

use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use serde_jsonlines::JsonLinesReader;

use crate::matching::{
  extractors::tokenize_names,
  replacers::{Dictionaries, RigourData},
};

use super::{Symbol, SymbolCategory};

pub(crate) struct Tagger {
  automaton: AhoCorasick,
  symbols: Vec<Vec<Symbol>>,
}

impl Tagger {
  fn new(mapping: HashMap<String, Vec<Symbol>>) -> Self {
    let mut patterns = Vec::new();
    let mut symbols = Vec::new();

    for (pattern, syms) in mapping {
      if pattern.is_empty() {
        continue;
      }
      patterns.push(pattern);
      symbols.push(syms);
    }

    let automaton = AhoCorasickBuilder::new()
      .match_kind(MatchKind::LeftmostLongest)
      .ascii_case_insensitive(true)
      .build(patterns)
      .expect("failed to build Aho-Corasick automaton");

    Tagger { automaton, symbols }
  }

  pub(crate) fn tag(&self, text: &str) -> Vec<(String, Option<Symbol>)> {
    let mut results = Vec::new();

    let tokens: Vec<String> = tokenize_names([text].iter()).next().unwrap_or_default();

    if tokens.is_empty() {
      return results;
    }

    for token in &tokens {
      results.push((unaccent::unaccent(token), None));
    }

    let normalized = unaccent::unaccent(tokens.join(" "));

    for mat in self.automaton.find_iter(&normalized) {
      let pattern_index = mat.pattern().as_usize();
      let matched_text = &normalized[mat.start()..mat.end()];

      if self.is_token_boundary(&normalized, mat.start(), mat.end()) {
        for symbol in &self.symbols[pattern_index] {
          results.push((matched_text.to_string(), Some(symbol.clone())));
        }
      }
    }

    results
  }

  fn is_token_boundary(&self, text: &str, start: usize, end: usize) -> bool {
    let start_ok = start == 0 || text[..start].chars().last().is_some_and(|c| c.is_whitespace());
    let end_ok = end == text.len() || text[end..].chars().next().is_some_and(|c| c.is_whitespace());
    start_ok && end_ok
  }
}

pub static ORG_TAGGER: LazyLock<Tagger> = LazyLock::new(|| {
  let mut mapping: HashMap<String, Vec<Symbol>> = HashMap::new();

  add_org_symbols(&mut mapping);
  // TODO: org domains are .py only
  add_org_types(&mut mapping);
  add_org_territories(&mut mapping);
  add_ordinals(&mut mapping);

  Tagger::new(mapping)
});

pub static PERSON_TAGGER: LazyLock<Tagger> = LazyLock::new(|| {
  let mut mapping: HashMap<String, Vec<Symbol>> = HashMap::new();

  add_ordinals(&mut mapping);
  add_person_names(&mut mapping);
  add_person_symbols(&mut mapping);

  Tagger::new(mapping)
});

fn add_org_symbols(mapping: &mut HashMap<String, Vec<Symbol>>) {
  use crate::matching::replacers::Dictionaries;
  use serde::Deserialize;
  use std::collections::HashMap as StdHashMap;

  #[derive(Deserialize)]
  struct SymbolsDictionary {
    org_symbols: Option<StdHashMap<String, Vec<String>>>,
  }

  let Some(file) = Dictionaries::get("names/symbols.yml") else {
    return;
  };

  let Ok(dictionary) = serde_yaml::from_slice::<SymbolsDictionary>(&file.data) else {
    return;
  };

  if let Some(person_symbols) = dictionary.org_symbols {
    for (key, values) in person_symbols {
      let symbol = Symbol::new(SymbolCategory::Symbol, key.to_uppercase());
      for value in values {
        let normalized = value.to_lowercase();
        mapping.entry(normalized).or_default().push(symbol.clone());
      }
    }
  }
}

fn add_org_types(mapping: &mut HashMap<String, Vec<Symbol>>) {
  use serde::Deserialize;

  #[derive(Deserialize)]
  struct OrgTypeDictionary {
    types: Vec<OrgTypeDictionaryEntry>,
  }

  #[derive(Deserialize)]
  struct OrgTypeDictionaryEntry {
    generic: Option<String>,
    compare: Option<String>,
    aliases: Vec<String>,
  }

  let file = Dictionaries::get("names/org_types.yml").expect("could not read org types dictionary");
  let dictionary: OrgTypeDictionary = serde_yaml::from_slice(&file.data).expect("could not unmarshal org type dictionary");

  for item in dictionary.types {
    let Some(main) = item.generic else {
      continue;
    };

    let symbol = Symbol::new(SymbolCategory::OrgClass, main.to_uppercase());
    let normalized = tokenize_names([main].iter()).next().unwrap().join("");

    mapping.entry(normalized).or_default().push(symbol.clone());

    if let Some(compare) = item.compare {
      let normalized = tokenize_names([compare].iter()).next().unwrap().join("");

      mapping.entry(normalized).or_default().push(symbol.clone());
    }

    for form in item.aliases {
      let normalized = tokenize_names([form].iter()).next().unwrap().join("");

      mapping.entry(normalized).or_default().push(symbol.clone());
    }
  }
}

fn add_org_territories(mapping: &mut HashMap<String, Vec<Symbol>>) {
  use serde::Deserialize;

  #[derive(Deserialize)]
  struct OrgTerritory {
    code: String,
    #[serde(default)]
    names_strong: Vec<String>,
    name: String,
    full_name: String,
  }

  let file = RigourData::get("territories/data.jsonl").expect("could not read org territories dictionary");
  let dictionary = JsonLinesReader::new(BufReader::new(file.data.as_ref()))
    .read_all::<OrgTerritory>()
    .collect::<Result<Vec<_>, _>>()
    .expect("could not read org territories dictionary");

  for item in dictionary {
    let symbol = Symbol::new(SymbolCategory::Location, item.code);

    for name in item.names_strong.into_iter().chain([item.full_name, item.name].into_iter()) {
      let normalized = tokenize_names([name].iter()).next().unwrap().join("");

      mapping.entry(normalized).or_default().push(symbol.clone());
    }
  }
}

fn add_ordinals(mapping: &mut HashMap<String, Vec<Symbol>>) {
  use crate::matching::replacers::Dictionaries;
  use serde::Deserialize;
  use std::collections::HashMap as StdHashMap;

  #[derive(Deserialize)]
  struct OrdinalsDictionary {
    ordinals: StdHashMap<usize, Vec<String>>,
  }

  let file = Dictionaries::get("text/ordinals.yml").expect("could not read ordinals dictionary");
  let dictionary: OrdinalsDictionary = serde_yaml::from_slice(&file.data).expect("could not unmarshal ordinals dictionary");

  for (key, items) in dictionary.ordinals {
    let symbol = Symbol::new(SymbolCategory::Numeric, key.to_string());
    let key_str = key.to_string();
    mapping.entry(key_str).or_default().push(symbol.clone());

    for item in items {
      let normalized = item.to_lowercase();
      mapping.entry(normalized).or_default().push(symbol.clone());
    }
  }
}

fn add_person_names(mapping: &mut HashMap<String, Vec<Symbol>>) {
  use crate::matching::replacers::RigourData;

  let Some(file) = RigourData::get("names/persons.txt") else {
    return;
  };

  let content = String::from_utf8_lossy(&file.data);

  for line in content.lines() {
    if line.trim().is_empty() || line.starts_with('#') {
      continue;
    }

    if let Some((names_part, qid_part)) = line.split_once(" => ") {
      let qid = qid_part[1..].trim();
      let symbol = Symbol::new(SymbolCategory::Name, qid);

      for name in names_part.split(',') {
        let normalized = unaccent::unaccent(name.trim().to_lowercase());
        if !normalized.is_empty() {
          mapping.entry(normalized).or_default().push(symbol.clone());
        }
      }
    }
  }
}

fn add_person_symbols(mapping: &mut HashMap<String, Vec<Symbol>>) {
  use crate::matching::replacers::Dictionaries;
  use serde::Deserialize;
  use std::collections::HashMap as StdHashMap;

  #[derive(Deserialize)]
  struct SymbolsDictionary {
    person_symbols: Option<StdHashMap<String, Vec<String>>>,
    person_names: Option<StdHashMap<String, Vec<String>>>,
    person_nick: Option<StdHashMap<String, Vec<String>>>,
  }

  let Some(file) = Dictionaries::get("names/symbols.yml") else {
    return;
  };

  let Ok(dictionary) = serde_yaml::from_slice::<SymbolsDictionary>(&file.data) else {
    return;
  };

  if let Some(person_symbols) = dictionary.person_symbols {
    for (key, values) in person_symbols {
      let symbol = Symbol::new(SymbolCategory::Symbol, key.to_uppercase());
      for value in values {
        let normalized = value.to_lowercase();
        mapping.entry(normalized).or_default().push(symbol.clone());
      }
    }
  }

  if let Some(person_names) = dictionary.person_names {
    for (key, values) in person_names {
      let symbol = Symbol::new(SymbolCategory::Name, key.to_uppercase());
      for value in values {
        let normalized = value.to_lowercase();
        mapping.entry(normalized).or_default().push(symbol.clone());
      }
    }
  }

  if let Some(person_nick) = dictionary.person_nick {
    for (key, values) in person_nick {
      let symbol = Symbol::new(SymbolCategory::Nick, key.to_uppercase());
      for value in values {
        let normalized = value.to_lowercase();
        mapping.entry(normalized).or_default().push(symbol.clone());
      }
    }
  }
}
