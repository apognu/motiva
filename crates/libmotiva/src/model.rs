use std::{
  collections::{HashMap, HashSet},
  sync::{Arc, Mutex},
};

use ahash::RandomState;
use bon::bon;
use jiff::civil::DateTime;
use serde::{Deserialize, Serialize, Serializer, ser::SerializeMap};
use validator::Validate;

use crate::{
  index::EsEntity,
  matching::extractors,
  schemas::{FtmProperty, SCHEMAS},
};

const EMPTY: [String; 0] = [];

pub trait HasProperties {
  fn names_and_aliases(&self) -> Vec<String>;
  fn property(&self, key: &str) -> &[String];
  fn gather(&self, keys: &[&str]) -> Vec<String>;
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Schema(String);

impl Schema {
  pub(crate) fn from(name: &str) -> Schema {
    Schema(name.to_string())
  }

  pub fn as_str(&self) -> &str {
    &self.0
  }

  pub(crate) fn is_a(&self, schema: &str) -> bool {
    if self.0 == schema {
      return true;
    }

    let Some(asked) = SCHEMAS.get(schema) else {
      return false;
    };

    asked.descendants.iter().any(|s| s == &self.0)
  }

  pub fn properties(&self) -> Option<Vec<(String, FtmProperty)>> {
    let schema = SCHEMAS.get(self.as_str())?;

    Some(
      schema
        .parents
        .iter()
        .filter_map(|name| SCHEMAS.get(name).map(|schema| schema.properties.clone()))
        .flatten()
        .collect::<Vec<_>>(),
    )
  }

  pub fn property(&self, name: &str) -> Option<(String, FtmProperty)> {
    let schema = SCHEMAS.get(self.as_str())?;

    schema
      .matchable_chain
      .iter()
      .filter_map(|s| SCHEMAS.get(s).map(|schema| schema.properties.clone().into_iter().find(|(n, _)| n == name)))
      .next()?
  }
}

#[derive(Clone, Debug, Deserialize, Serialize, Validate)]
pub struct SearchEntity {
  pub schema: Schema,
  #[validate(length(min = 1, message = "at least one property must be given"))]
  pub properties: HashMap<String, Vec<String>, RandomState>,

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

#[bon]
impl SearchEntity {
  #[builder]
  pub fn builder(#[builder(start_fn)] schema: &str, properties: &[(&str, &[&str])]) -> SearchEntity {
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

  #[serde(skip_serializing_if = "Option::is_none")]
  pub first_seen: Option<DateTime>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub last_seen: Option<DateTime>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub last_change: Option<DateTime>,

  pub properties: Properties,

  #[serde(serialize_with = "features_to_map", skip_serializing_if = "Vec::is_empty")]
  pub features: Vec<(&'static str, f64)>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(bound(deserialize = "'de: 'static"))]
pub struct Properties {
  #[serde(flatten)]
  pub strings: HashMap<String, Vec<String>, RandomState>,
  // Arc<Mutex<T>> is used here because this struct is sent to threads, and must
  // be thread-safe, there should be no concurrency on this field.
  #[serde(flatten, skip_deserializing)]
  pub entities: HashMap<String, Vec<Arc<Mutex<Entity>>>, RandomState>,
}

fn features_to_map<S: Serializer>(input: &[(&'static str, f64)], ser: S) -> Result<S::Ok, S::Error> {
  if input.is_empty() {
    return ser.serialize_unit();
  }

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

#[bon]
impl Entity {
  #[builder]
  pub fn builder(#[builder(start_fn)] schema: &str, id: Option<&str>, properties: &[(&str, &[&str])]) -> Entity {
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
}

#[cfg(test)]
mod tests {
  use crate::model::Entity;

  #[test]
  fn entity_is_a() {
    let entity = Entity::builder("Company").properties(&[]).build();

    assert!(entity.schema.is_a("Organization"));
  }
}
