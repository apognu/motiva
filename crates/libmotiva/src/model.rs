use std::{
  borrow::Cow,
  collections::{HashMap, HashSet},
  sync::{Arc, Mutex},
};

use ahash::RandomState;
use bon::bon;
use itertools::Itertools;
use jiff::civil::DateTime;
use serde::{Deserialize, Serialize, Serializer, ser::SerializeMap};
use validator::Validate;

use crate::{
  matching::extractors,
  schemas::{FtmProperty, SCHEMAS, resolve_schemas},
};

const EMPTY: [String; 0] = [];

pub trait HasProperties {
  fn names_and_aliases(&self) -> Vec<String>;
  fn props(&self, keys: &[&str]) -> Cow<'_, [String]>;
  fn prop_group(&self, group: &str) -> Cow<'_, [String]>;
}

#[derive(Eq, PartialEq)]
pub(crate) enum ResolveSchemaLevel {
  Root,
  Deep,
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

  pub(crate) fn matchable_schemas(&self, level: ResolveSchemaLevel) -> Vec<String> {
    let mut out = Vec::with_capacity(8);
    let root = level == ResolveSchemaLevel::Root;

    if let Some(schema) = SCHEMAS.get(self.as_str()) {
      if root {
        out.extend(schema.descendants.clone());
      }

      if root || schema.matchable {
        out.push(self.as_str().to_string());
      }

      for parent in &schema.extends {
        out.extend(Schema::from(parent).matchable_schemas(ResolveSchemaLevel::Deep));
      }
    }

    out
  }

  pub(crate) fn can_match(&self, schema: &str) -> bool {
    Schema::from(schema).matchable_schemas(ResolveSchemaLevel::Root).iter().any(|s| s == &self.0)
  }

  pub(crate) fn is_edge(&self) -> bool {
    SCHEMAS.get(self.as_str()).and_then(|s| s.edge.as_ref()).is_some()
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

/// Search terms
#[derive(Clone, Debug, Deserialize, Serialize, Validate)]
pub struct SearchEntity {
  pub schema: Schema,
  #[validate(length(min = 1, message = "at least one property must be given"))]
  pub properties: HashMap<String, Vec<String>, RandomState>,

  // Those attributes will be precomputed when receiving the request to skip the computation for every matching entity.
  #[serde(skip)]
  pub(crate) name_parts: HashSet<String>,
}

impl SearchEntity {
  pub fn precompute(&mut self) {
    const MAX_NAME_COMBINATIONS: usize = 100;

    let props = [
      self.props(&["firstName"]),
      self.props(&["secondName"]),
      self.props(&["middleName"]),
      self.props(&["fatherName"]),
      self.props(&["lastName"]),
    ];

    let mut combined = HashSet::with_capacity_and_hasher(
      props.iter().map(|n| if n.is_empty() { 1 } else { n.len() }).product::<usize>().min(MAX_NAME_COMBINATIONS),
      RandomState::default(),
    );

    for combination in props.iter().map(|v| v.as_ref()).filter(|v| !v.is_empty()).multi_cartesian_product().take(MAX_NAME_COMBINATIONS) {
      if !combination.is_empty() {
        combined.insert(combination.iter().join(" "));
      }
    }

    let names = self.properties.entry("name".to_string()).or_default();
    names.reserve(combined.len());
    names.extend(combined);

    self.name_parts = extractors::name_parts_flat(self.props(&["name"]).iter()).collect();
  }
}

impl HasProperties for SearchEntity {
  fn names_and_aliases(&self) -> Vec<String> {
    let names = self.props(&["name"]);
    let aliases = self.props(&["alias"]);

    names.iter().chain(aliases.iter()).cloned().collect()
  }

  fn props(&self, keys: &[&str]) -> Cow<'_, [String]> {
    match keys.len() {
      0 => Cow::Borrowed(&EMPTY),

      1 => match self.properties.get(keys[0]) {
        Some(values) => Cow::Borrowed(values),
        None => Cow::Borrowed(&EMPTY),
      },

      _ => {
        let capacity: usize = keys.iter().filter_map(|key| self.properties.get(*key)).map(|v| v.len()).sum();
        let mut values = Vec::with_capacity(capacity);

        for key in keys {
          if let Some(prop_values) = self.properties.get(*key) {
            values.extend(prop_values.iter().cloned());
          }
        }

        Cow::Owned(values)
      }
    }
  }

  fn prop_group(&self, group: &str) -> Cow<'_, [String]> {
    let schemas = resolve_schemas(&SCHEMAS, self.schema.as_str(), false).unwrap_or_default();
    let mut keys = Vec::new();

    for (_, schema) in SCHEMAS.iter().filter(|(s, _)| schemas.contains(s)) {
      for (prop, _) in schema.properties.iter().filter(|(_, p)| p._type == group) {
        keys.push(prop.to_owned());
      }
    }

    let capacity: usize = keys.iter().filter_map(|key| self.properties.get(key)).map(|v| v.len()).sum();
    let mut values = Vec::with_capacity(capacity);

    for key in keys {
      if let Some(prop_values) = self.properties.get(&key) {
        values.extend(prop_values.iter().cloned());
      }
    }

    Cow::Owned(values)
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

/// An Entity returned from the index
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

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(bound(deserialize = "'de: 'static"))]
pub struct Properties {
  #[serde(flatten)]
  pub strings: HashMap<String, Vec<String>, RandomState>,
  // Arc<Mutex<T>> is used here because this struct is sent to threads, and must
  // be thread-safe, there should be no concurrency on this field.
  #[serde(flatten, skip_deserializing)]
  pub entities: HashMap<String, Vec<Arc<Mutex<Entity>>>, RandomState>,
}

// Custom serializer for output properties, since we might have duplicated keys
// after enrichment. We want to only serialize simple `strings` propeties if
// the key has not been expanded in the `entities` field.
impl Serialize for Properties {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut map = serializer.serialize_map(None)?;

    for (k, v) in &self.strings {
      if !self.entities.contains_key(k) {
        map.serialize_entry(k, v)?;
      }
    }
    for (k, v) in &self.entities {
      map.serialize_entry(k, v)?;
    }

    map.end()
  }
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

impl HasProperties for Entity {
  fn names_and_aliases(&self) -> Vec<String> {
    let names = self.props(&["name"]);
    let aliases = self.props(&["alias"]);

    let mut values = Vec::with_capacity(names.len() + aliases.len());
    values.extend_from_slice(&names);
    values.extend_from_slice(&aliases);
    values
  }

  fn props(&self, keys: &[&str]) -> Cow<'_, [String]> {
    match keys.len() {
      0 => Cow::Borrowed(&EMPTY),

      1 => match self.properties.strings.get(keys[0]) {
        Some(values) => Cow::Borrowed(values),
        None => Cow::Borrowed(&EMPTY),
      },

      _ => {
        let capacity: usize = keys.iter().filter_map(|key| self.properties.strings.get(*key)).map(|v| v.len()).sum();
        let mut values = Vec::with_capacity(capacity);

        for key in keys {
          if let Some(prop_values) = self.properties.strings.get(*key) {
            values.extend(prop_values.iter().cloned());
          }
        }

        Cow::Owned(values)
      }
    }
  }

  fn prop_group(&self, group: &str) -> Cow<'_, [String]> {
    let schemas = resolve_schemas(&SCHEMAS, self.schema.as_str(), false).unwrap_or_default();
    let mut keys = Vec::new();

    for (_, schema) in SCHEMAS.iter().filter(|(s, _)| schemas.contains(s)) {
      for (prop, _) in schema.properties.iter().filter(|(_, p)| p._type == group) {
        keys.push(prop);
      }
    }

    let capacity: usize = keys.iter().filter_map(|key| self.properties.strings.get(*key)).map(|v| v.len()).sum();
    let mut values = Vec::with_capacity(capacity);

    for key in keys {
      if let Some(prop_values) = self.properties.strings.get(key) {
        values.extend(prop_values.iter().cloned());
      }
    }

    Cow::Owned(values)
  }
}

#[bon]
impl Entity {
  #[builder]
  pub fn builder(#[builder(start_fn)] schema: &str, id: Option<&str>, #[builder(default)] properties: &[(&str, &[&str])]) -> Entity {
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
  use ahash::HashSet;

  use crate::{
    HasProperties, SearchEntity,
    model::{Entity, ResolveSchemaLevel, Schema},
  };

  #[test]
  fn entity_is_a() {
    let entity = Entity::builder("Company").properties(&[]).build();

    assert!(entity.schema.is_a("Organization"));
    assert!(!entity.schema.is_a("Nothing"));
    assert!(entity.schema.is_a("Thing"));

    let entity = Entity::builder("Thing").properties(&[]).build();

    assert!(!entity.schema.is_a("Person"));
  }

  #[test]
  fn schema_properties() {
    let schema = Schema::from("Person");
    let properties = schema.properties().unwrap();

    assert!(properties.iter().any(|(name, _)| name == "secondName"));
    assert!(properties.iter().any(|(name, _)| name == "topics"));
    assert!(properties.iter().any(|(name, _)| name == "socialSecurityNumber"));
    assert!(!properties.iter().any(|(name, _)| name == "mmsi"));
  }

  #[test]
  fn schema_property() {
    let schema = Schema::from("Person");

    assert!(schema.property("mmsi").is_none());

    let (name, prop) = schema.property("socialSecurityNumber").unwrap();

    assert_eq!(name, "socialSecurityNumber");
    assert!(prop.matchable);
    assert!(prop.reverse.is_none());

    let schema = Schema::from("LegalEntity");
    let (name, prop) = schema.property("parent").unwrap();

    assert_eq!(name, "parent");
    assert!(prop.reverse.is_some());
    assert_eq!(prop.reverse.unwrap().name, "subsidiaries");
  }

  #[test]
  fn schema_property_group() {
    let se = SearchEntity::builder("Person")
      .properties(&[
        ("vatCode", &["VAT"]),
        ("idNumber", &["ID"]),
        ("passportNumber", &["PN"]),
        ("socialSecurityNumber", &["SSN"]),
        ("country", &["COUNTRY"]),
        ("jurisdiction", &["JURIS"]),
        ("nationality", &["NAT"]),
        ("citizenship", &["CIT"]),
      ])
      .build();

    let identifiers = se.prop_group("identifier");
    let countries = se.prop_group("country");

    assert!(identifiers.as_ref().iter().any(|p| p == "VAT"));
    assert!(identifiers.as_ref().iter().any(|p| p == "ID"));
    assert!(identifiers.as_ref().iter().any(|p| p == "PN"));
    assert!(identifiers.as_ref().iter().any(|p| p == "SSN"));
    assert!(countries.as_ref().iter().any(|p| p == "COUNTRY"));
    assert!(countries.as_ref().iter().any(|p| p == "JURIS"));
    assert!(countries.as_ref().iter().any(|p| p == "NAT"));
    assert!(countries.as_ref().iter().any(|p| p == "CIT"));
  }

  #[test]
  fn precompute() {
    let se = SearchEntity::builder("Person")
      .properties(&[("firstName", &["Vladimir"]), ("fatherName", &["Vladimirovitch"]), ("lastName", &["Putin"])])
      .build();

    assert_eq!(se.names_and_aliases(), &["Vladimir Vladimirovitch Putin"]);
  }

  #[test]
  fn resolve_schema_chain() {
    assert_eq!(Schema::from("Person").matchable_schemas(ResolveSchemaLevel::Root), &["Person", "LegalEntity"]);
    assert_eq!(Schema::from("Company").matchable_schemas(ResolveSchemaLevel::Root), &["Company", "Organization", "LegalEntity"]);
    assert_eq!(Schema::from("Airplane").matchable_schemas(ResolveSchemaLevel::Root), &["Airplane"]);
    // assert!(Schema::from("Vehicle").matchable_schemas(ResolveSchemaLevel::Root).is_empty());
    assert_eq!(
      HashSet::from_iter(Schema::from("Thing").matchable_schemas(ResolveSchemaLevel::Root).iter()),
      HashSet::from_iter(
        [
          "Video".to_string(),
          "Table".to_string(),
          "CryptoWallet".to_string(),
          "Event".to_string(),
          "Pages".to_string(),
          "CallForTenders".to_string(),
          "License".to_string(),
          "Project".to_string(),
          "Note".to_string(),
          "Security".to_string(),
          "PlainText".to_string(),
          "Contract".to_string(),
          "Document".to_string(),
          "UserAccount".to_string(),
          "Email".to_string(),
          "Package".to_string(),
          "Audio".to_string(),
          "Person".to_string(),
          "Folder".to_string(),
          "Vessel".to_string(),
          "Message".to_string(),
          "Workbook".to_string(),
          "BankAccount".to_string(),
          "Vehicle".to_string(),
          "Image".to_string(),
          "HyperText".to_string(),
          "Address".to_string(),
          "RealEstate".to_string(),
          "Position".to_string(),
          "Article".to_string(),
          "Company".to_string(),
          "Asset".to_string(),
          "Airplane".to_string(),
          "LegalEntity".to_string(),
          "Organization".to_string(),
          "CourtCase".to_string(),
          "PublicBody".to_string(),
          "Trip".to_string(),
          "Thing".to_string(),
        ]
        .iter()
      )
    );
  }
}
