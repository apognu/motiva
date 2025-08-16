#[cfg(test)]
pub mod python;

use std::collections::HashMap;

use ahash::RandomState;

use crate::model::{Entity, Properties, Schema, SearchEntity};

#[bon::builder]
pub fn e(#[builder(start_fn)] schema: &str, id: Option<&str>, properties: &[(&str, &[&str])]) -> Entity {
  let mut props: HashMap<_, _, RandomState> = HashMap::default();

  for (prop, values) in properties {
    props.insert(prop.to_string(), values.iter().map(|s| s.to_string()).collect::<Vec<_>>());
  }

  Entity {
    schema: Schema::from(schema),
    id: id.map(ToOwned::to_owned).unwrap_or_default(),
    caption: String::new(),
    properties: Properties { strings: props, ..Default::default() },
    ..Default::default()
  }
}

#[bon::builder]
pub fn se(#[builder(start_fn)] schema: &str, properties: &[(&str, &[&str])]) -> SearchEntity {
  let mut props: HashMap<_, _, RandomState> = HashMap::default();

  for (prop, values) in properties {
    props.insert(prop.to_string(), values.iter().map(|s| s.to_string()).collect::<Vec<_>>());
  }

  let mut entity = SearchEntity {
    schema: Schema::from(schema),
    properties: props,
    name_parts: Default::default(),
  };

  entity.precompute();
  entity
}
