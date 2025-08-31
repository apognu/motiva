use std::{
  collections::{HashMap, HashSet},
  sync::LazyLock,
};

use ahash::RandomState;
use rust_embed::Embed;
use serde::Deserialize;

#[derive(Embed)]
#[folder = "../../assets/followthemoney/followthemoney/schema"]
struct Schemas;

pub static SCHEMAS: LazyLock<HashMap<String, FtmSchema, RandomState>> = LazyLock::new(|| {
  tracing::debug!("building schemas");

  let mut schemas = Schemas::iter()
    .map(|filename| {
      let file = Schemas::get(filename.as_ref()).expect("invalid schema");
      let content = std::str::from_utf8(&file.data).expect("invalid schema");
      let schema = serde_yaml::from_str::<HashMap<String, FtmSchema>>(content).expect("invalid schema");

      schema.into_iter().next().expect("schema does not contain schema")
    })
    .collect::<HashMap<String, FtmSchema, RandomState>>();

  let schemas_clone = schemas.clone();
  let mut children_map: HashMap<&str, Vec<&str>> = HashMap::default();

  for (name, schema) in &schemas_clone {
    schemas.get_mut(name).unwrap().matchable_chain = resolve_schemas(&schemas, name, true).unwrap_or_default();
    schemas.get_mut(name).unwrap().parents = resolve_schemas(&schemas, name, false).unwrap_or_default();

    for parent in &schema.extends {
      children_map.entry(parent).or_default().push(name);
    }
  }

  for name in schemas_clone.keys() {
    let mut descendants: HashSet<&str> = HashSet::default();
    let mut stack: Vec<&str> = Vec::default();

    if let Some(children) = children_map.get(name.as_str()) {
      stack.extend(children);
    }

    while let Some(node) = stack.pop() {
      if descendants.insert(node)
        && let Some(children) = children_map.get(&node)
      {
        stack.extend(children.clone());
      }
    }

    schemas.get_mut(name).unwrap().descendants = descendants.into_iter().map(String::from).collect();
  }

  schemas
});

fn resolve_schemas(schemas: &HashMap<String, FtmSchema, RandomState>, schema: &str, if_matchable: bool) -> Option<Vec<String>> {
  let mut out = Vec::with_capacity(8);

  if let Some(def) = schemas.get(schema) {
    if if_matchable && schema != "Thing" && !def.matchable {
      return None;
    }

    if !if_matchable || def.matchable || schema == "Thing" {
      out.push(schema.to_string());
    }

    for parent in &def.extends {
      out.extend(resolve_schemas(schemas, parent, false)?);
    }
  }

  Some(out)
}

#[derive(Clone, Debug, Deserialize)]
pub struct FtmSchema {
  #[serde(default)]
  pub extends: Vec<String>,
  pub matchable: bool,
  #[serde(default)]
  pub caption: Vec<String>,
  #[serde(default)]
  pub properties: HashMap<String, FtmProperty, RandomState>,

  #[serde(skip)]
  pub matchable_chain: Vec<String>,
  #[serde(skip)]
  pub parents: Vec<String>,
  #[serde(skip)]
  pub descendants: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct FtmProperty {
  #[serde(default, rename = "type")]
  pub _type: String,
  #[serde(default = "c_true")]
  pub matchable: bool,
  #[serde(default)]
  pub reverse: Option<FtmReverseField>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct FtmReverseField {
  pub name: String,
}

const fn c_true() -> bool {
  true
}

#[cfg(test)]
mod tests {
  #[test]
  fn resolve_schemas() {
    assert_eq!(super::resolve_schemas(&super::SCHEMAS, "Thing", true).as_ref(), Some(&vec!["Thing".into()]));

    assert_eq!(
      super::resolve_schemas(&super::SCHEMAS, "Person", true).as_ref(),
      Some(&vec!["Person".into(), "LegalEntity".into(), "Thing".into()])
    );

    assert_eq!(super::resolve_schemas(&super::SCHEMAS, "Event", true).as_ref(), None);

    assert_eq!(
      super::resolve_schemas(&super::SCHEMAS, "Event", false).as_ref(),
      Some(&vec!["Event".into(), "Interval".into(), "Analyzable".into(), "Thing".into()])
    );
  }
}
