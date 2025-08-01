use std::collections::{HashMap, HashSet};

use jiff::civil::DateTime;
use serde::{Deserialize, Serialize, Serializer, ser::SerializeMap};
use validator::Validate;

use crate::{
  index::EsEntity,
  matching::extractors,
  schemas::{FtmProperty, SCHEMAS},
};

pub const EMPTY: [String; 0] = [];

pub trait HasProperties {
  fn names(&self) -> &[String];
  fn names_and_aliases(&self) -> Vec<String>;
  fn property(&self, key: &str) -> &[String];
  fn gather(&self, keys: &[&str]) -> Vec<String>;
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Schema(String);

impl Schema {
  pub fn from(name: &str) -> Schema {
    Schema(name.to_string())
  }

  pub fn as_str(&self) -> &str {
    &self.0
  }

  pub fn is_a(&self, schema: &str) -> bool {
    if self.0 == schema {
      return true;
    }

    let Some(asked) = SCHEMAS.get(schema) else {
      return false;
    };

    asked.descendants.iter().any(|s| s == &self.0)
  }

  pub fn properties(&self) -> Option<Vec<(String, FtmProperty)>> {
    let Some(schema) = SCHEMAS.get(self.as_str()) else {
      return None;
    };

    Some(
      schema
        .parents
        .iter()
        .filter_map(|s| match SCHEMAS.get(s) {
          Some(schema) => Some(schema.properties.clone()),
          None => None,
        })
        .flatten()
        .collect::<Vec<_>>(),
    )
  }
}

#[derive(Clone, Debug, Deserialize, Serialize, Validate)]
pub struct SearchEntity {
  pub schema: Schema,
  #[validate(length(min = 1, message = "at least one property must be given"))]
  pub properties: HashMap<String, Vec<String>>,

  // Those attributes will be precomputed when receiving the request to skip the computation for every matching entity.
  #[serde(skip)]
  pub name_parts: HashSet<String>,
}

impl SearchEntity {
  pub fn precompute(&mut self) {
    self.name_parts = extractors::name_parts_flat(self.property("name").iter()).collect();
  }
}

impl HasProperties for SearchEntity {
  fn names(&self) -> &[String] {
    match self.properties.get("name") {
      Some(names) => names,
      None => &EMPTY,
    }
  }

  fn names_and_aliases(&self) -> Vec<String> {
    let names = self.property("name");
    let names = names.iter().chain(self.property("alias").iter());

    names.cloned().collect()
  }

  fn property(&self, key: &str) -> &[String] {
    match self.properties.get(key) {
      Some(values) => values,
      None => &EMPTY,
    }
  }

  fn gather(&self, keys: &[&str]) -> Vec<String> {
    let mut values = Vec::with_capacity(keys.len());

    for key in keys {
      values.extend(self.property(key).iter().cloned());
    }

    values
  }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(bound(deserialize = "'de: 'static"))]
pub struct Entity {
  pub id: String,
  pub caption: String,
  pub schema: Schema,
  pub datasets: Vec<String>,
  pub referents: Vec<String>,
  pub target: bool,

  pub first_seen: DateTime,
  pub last_seen: DateTime,
  pub last_change: DateTime,

  pub properties: Properties,

  #[serde(serialize_with = "features_to_map")]
  pub features: Vec<(&'static str, f64)>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(bound(deserialize = "'de: 'static"))]
pub struct Properties {
  #[serde(flatten)]
  pub strings: HashMap<String, Vec<String>>,
  #[serde(flatten)]
  pub entities: HashMap<String, serde_json::Value>,
}

fn features_to_map<S: Serializer>(input: &[(&'static str, f64)], ser: S) -> Result<S::Ok, S::Error> {
  let mut map = ser.serialize_map(Some(input.len()))?;
  for (k, v) in input {
    map.serialize_entry(k, &v)?;
  }
  map.end()
}

impl From<EsEntity> for Entity {
  fn from(entity: EsEntity) -> Self {
    let caption = entity.caption().to_string();

    Self {
      id: entity.id,
      caption,
      schema: entity._source.schema,
      datasets: entity._source.datasets,
      referents: entity._source.referents,
      target: entity._source.target,
      first_seen: entity._source.first_seen,
      last_seen: entity._source.last_seen,
      last_change: entity._source.last_change,
      properties: Properties {
        strings: entity._source.properties,
        ..Default::default()
      },
      ..Default::default()
    }
  }
}

impl HasProperties for Entity {
  fn names(&self) -> &[String] {
    match self.properties.strings.get("name") {
      Some(names) => names,
      None => &EMPTY,
    }
  }

  fn names_and_aliases(&self) -> Vec<String> {
    let names = self.property("name");
    let names = names.iter().chain(self.property("alias").iter());

    names.cloned().collect()
  }

  fn property(&self, key: &str) -> &[String] {
    match self.properties.strings.get(key) {
      Some(values) => values,
      None => &EMPTY,
    }
  }

  fn gather(&self, keys: &[&str]) -> Vec<String> {
    let mut values = Vec::with_capacity(keys.len());

    for key in keys {
      values.extend(self.property(key).iter().cloned());
    }

    values
  }
}

#[cfg(test)]
mod tests {
  use crate::tests::e;

  #[test]
  fn entity_is_a() {
    let entity = e("Company").properties(&[]).call();

    assert!(entity.schema.is_a("Organization"));
  }
}
