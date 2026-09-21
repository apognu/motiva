use std::{
  borrow::{Borrow, Cow},
  cmp::Ordering,
  collections::{HashMap, HashSet},
  fmt,
  ops::Deref,
  str::FromStr,
  sync::{Arc, Mutex},
};

use ahash::RandomState;
use bon::bon;
use celes::Country;
use itertools::Itertools;
use jiff::civil::DateTime;
use serde::{Deserialize, Serialize, Serializer, ser::SerializeMap};
use strsim::levenshtein;
use validator::Validate;

use crate::{
  matching::{
    Explanation,
    extractors::{self, clean_names},
  },
  schemas::{FtmProperty, SCHEMAS, resolve_schemas},
};

#[derive(Clone, Copy)]
pub enum PropertyFilter {
  All,
  Matchable,
}

/// A property value together with the field it came from.
///
/// Comparisons and ordering use the value only; `field` is provenance carried
/// alongside it for consumers such as scoring explanations.
#[derive(Clone, Copy, Debug)]
pub struct PropertyValue<'a> {
  pub field: &'a str,
  pub value: &'a str,
}

impl PropertyValue<'_> {
  pub const fn as_str(&self) -> &str {
    self.value
  }
}

impl PartialEq for PropertyValue<'_> {
  fn eq(&self, other: &Self) -> bool {
    self.value == other.value
  }
}

impl Eq for PropertyValue<'_> {}

impl PartialEq<&str> for PropertyValue<'_> {
  fn eq(&self, other: &&str) -> bool {
    self.value == *other
  }
}

impl PartialOrd for PropertyValue<'_> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for PropertyValue<'_> {
  fn cmp(&self, other: &Self) -> Ordering {
    self.value.cmp(other.value)
  }
}

impl Deref for PropertyValue<'_> {
  type Target = str;

  fn deref(&self) -> &Self::Target {
    self.value
  }
}

impl Borrow<str> for PropertyValue<'_> {
  fn borrow(&self) -> &str {
    self.value
  }
}

impl AsRef<str> for PropertyValue<'_> {
  fn as_ref(&self) -> &str {
    self.value
  }
}

impl fmt::Display for PropertyValue<'_> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.value)
  }
}

pub trait HasProperties {
  fn schema(&self) -> &Schema;
  fn props<'a>(&'a self, keys: &[&str]) -> Vec<PropertyValue<'a>>;

  fn prop_group(&self, group: &str, filter: PropertyFilter) -> Vec<PropertyValue<'_>> {
    let keys = property_group_keys(self.schema(), group, filter);
    self.props(&keys)
  }
}

fn property_group_keys(schema: &Schema, group: &str, filter: PropertyFilter) -> Vec<&'static str> {
  let schemas = resolve_schemas(&SCHEMAS, schema.as_str(), false).unwrap_or_default();
  let mut keys = SCHEMAS
    .iter()
    .filter(|(schema, _)| schemas.contains(schema))
    .flat_map(|(_, schema)| {
      schema.properties.iter().filter_map(|(name, property)| {
        let selected = match filter {
          PropertyFilter::All => property._type == group,
          PropertyFilter::Matchable => property._type == group && property.matchable,
        };
        selected.then_some(name.as_str())
      })
    })
    .collect::<Vec<_>>();

  keys.sort_unstable();
  keys.dedup();
  keys
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
      .find(Option::is_some)?
  }
}

#[derive(Clone, Debug, Deserialize)]
pub struct PayloadParams {
  pub include_datasets: Option<Vec<String>>,
  pub exclude_datasets: Option<Vec<String>>,
}

/// Search terms
#[derive(Clone, Debug, Deserialize, Serialize, Validate)]
pub struct SearchEntity {
  pub schema: Schema,
  pub properties: HashMap<String, Vec<String>, RandomState>,

  #[serde(default)]
  pub filters: Option<HashMap<String, Vec<Vec<String>>>>,
  #[serde(skip_serializing)]
  pub params: Option<PayloadParams>,

  // Those attributes will be precomputed when receiving the request to skip the computation for every matching entity.
  #[serde(skip)]
  pub(crate) clean_names: Vec<String>,
  #[serde(skip)]
  pub(crate) name_parts_flat: HashSet<String>,
  #[serde(skip)]
  pub(crate) name_parts: Vec<Vec<String>>,
}

impl SearchEntity {
  pub fn precompute(&mut self) {
    self.combine_names();

    self.clean_names = extractors::clean_names(self.prop_group("name", PropertyFilter::All).iter()).collect();
    self.name_parts = extractors::name_parts(self.prop_group("name", PropertyFilter::All).iter()).collect();
    self.name_parts_flat = extractors::name_parts_flat(self.prop_group("name", PropertyFilter::All).iter()).collect();

    for (prop, values) in &mut self.properties {
      let Some((_, p)) = self.schema.property(prop) else { continue };

      if p._type == "country" {
        *values = values
          .iter()
          .filter_map(|value| {
            let normalized = unaccent::unaccent(value).chars().filter(|c| c.is_alphabetic()).collect::<String>();

            Country::from_str(&normalized).ok().map(|c| c.alpha2.to_lowercase())
          })
          .collect();
      }
    }
  }

  pub fn combine_names(&mut self) {
    if self.prop_group("name", PropertyFilter::Matchable).len() > 20 {
      return;
    }

    let aliases = {
      let firstnames = self.props(&["firstName"]);
      let secondnames = self.props(&["secondName"]);
      let middlenames = self.props(&["middleName"]);
      let fathernames = self.props(&["fatherName"]);
      let mothernames = self.props(&["motherName"]);
      let lastnames = self.props(&["lastName"]);

      let combined = [
        firstnames.as_slice(),
        secondnames.as_slice(),
        middlenames.as_slice(),
        fathernames.as_slice(),
        mothernames.as_slice(),
        lastnames.as_slice(),
      ]
      .into_iter()
      .filter(|iter| !iter.is_empty())
      .multi_cartesian_product()
      .map(|names| names.iter().join(" "));

      let mut names = Vec::new();

      for name in combined {
        if !name.is_empty() {
          names.push(name);
        }
      }

      names
    };

    if !aliases.is_empty() {
      self.properties.entry("alias".into()).or_default().extend(aliases);
    }
  }

  pub fn pick_names(&self, count: usize) -> Cow<'_, [String]> {
    let names = self.prop_group("name", PropertyFilter::Matchable);

    if names.len() < count {
      return Cow::Owned(names.iter().map(|name| name.value.to_owned()).collect());
    }

    let mut picked = Vec::with_capacity(count);
    let processed = clean_names(names.iter()).collect::<Vec<_>>();

    // TODO: Centroid is **not** the longest name in the original Yente implementation
    if let Some(centroid) = names.iter().max_by_key(|name| name.len()) {
      picked.push(centroid.value.to_owned());
    }

    while picked.len() < count {
      let mut best: Option<String> = None;
      let mut max_distance = -1isize;

      for (index, candidate) in processed.iter().enumerate() {
        if picked.iter().any(|name| name == names[index].value) {
          continue;
        }

        let total: usize = picked.iter().map(|name| levenshtein(candidate, name)).sum();

        if total as isize > max_distance {
          max_distance = total as isize;
          best = Some(names[index].value.to_owned());
        }
      }

      match best {
        Some(best) => picked.push(best),
        None => break,
      }
    }

    Cow::Owned(picked)
  }
}

impl HasProperties for SearchEntity {
  fn schema(&self) -> &Schema {
    &self.schema
  }

  fn props<'a>(&'a self, keys: &[&str]) -> Vec<PropertyValue<'a>> {
    let mut keys = keys.to_vec();
    keys.sort_unstable();
    keys.dedup();

    keys
      .into_iter()
      .filter_map(|key| self.properties.get_key_value(key))
      .flat_map(|(field, values)| values.iter().map(move |value| PropertyValue { field, value }))
      .collect()
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
      filters: None,
      params: None,
      clean_names: Default::default(),
      name_parts: Default::default(),
      name_parts_flat: Default::default(),
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

  #[serde(serialize_with = "explanations_to_map", skip_serializing_if = "Vec::is_empty", skip_deserializing)]
  pub explanations: Vec<Explanation>,
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
    map.serialize_entry(k, &format_score(*v))?;
  }
  map.end()
}

fn explanations_to_map<S: Serializer>(input: &[crate::matching::Explanation], ser: S) -> Result<S::Ok, S::Error> {
  use crate::matching::Explanation;

  struct Rendered<'a>(&'a Explanation);

  impl Serialize for Rendered<'_> {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
      let scored = self.0.score != 0.0;

      let mut map = ser.serialize_map(Some((if scored { 3 } else { 1 }) + usize::from(self.0.candidate.is_some())))?;

      if scored {
        map.serialize_entry("score", &format_score(self.0.score))?;
        map.serialize_entry("weighted", &format_score(self.0.weighted))?;
      }

      map.serialize_entry("detail", &self.0.detail.to_string())?;

      if let Some(candidate) = &self.0.candidate {
        #[derive(Serialize)]
        struct RenderedCandidate<'a> {
          field: &'a str,
          value: &'a str,
        }

        map.serialize_entry(
          "candidate",
          &RenderedCandidate {
            field: candidate.field.as_str(),
            value: candidate.value.as_str(),
          },
        )?;
      }
      map.end()
    }
  }

  let mut map = ser.serialize_map(Some(input.len()))?;
  for explanation in input {
    map.serialize_entry(explanation.name, &Rendered(explanation))?;
  }
  map.end()
}

impl HasProperties for Entity {
  fn schema(&self) -> &Schema {
    &self.schema
  }

  fn props<'a>(&'a self, keys: &[&str]) -> Vec<PropertyValue<'a>> {
    let mut keys = keys.to_vec();
    keys.sort_unstable();
    keys.dedup();

    keys
      .into_iter()
      .filter_map(|key| self.properties.strings.get_key_value(key))
      .flat_map(|(field, values)| values.iter().map(move |value| PropertyValue { field, value }))
      .collect()
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

#[inline]
pub fn format_score(score: f64) -> f64 {
  const SCORE_DECIMALS: u32 = 3;
  const MULTIPLIER: f64 = 10u32.pow(SCORE_DECIMALS) as f64;

  (score * MULTIPLIER).round() / MULTIPLIER
}

#[cfg(test)]
mod tests {
  use ahash::HashSet;

  use crate::{
    HasProperties, SearchEntity,
    matching::{Detail, Explanation},
    model::{Entity, PropertyFilter, ResolveSchemaLevel, Schema},
  };

  #[test]
  fn explanations_serialize_to_map() {
    let mut entity = Entity::builder("Person").properties(&[]).build();
    entity.explanations = vec![
      Explanation {
        name: "identifier_match",
        score: 1.0,
        weighted: 0.851_2,
        detail: Detail::Labeled("matched identifier", "X123".into()),
        candidate: Some(crate::matching::Candidate::new("leiCode", "X123")),
      },
      Explanation {
        name: "dob_year_disjoint",
        score: 0.0,
        weighted: 0.0,
        detail: Detail::Note("no data to match against"),
        candidate: None,
      },
    ];

    let json = serde_json::to_value(&entity).unwrap();
    let explanations = &json["explanations"];

    assert_eq!(explanations["identifier_match"]["score"], 1.0);
    assert_eq!(explanations["identifier_match"]["weighted"], 0.851);
    assert_eq!(explanations["identifier_match"]["detail"], "matched identifier: X123");
    assert_eq!(explanations["identifier_match"]["candidate"]["field"], "leiCode");
    assert_eq!(explanations["identifier_match"]["candidate"]["value"], "X123");
    assert_eq!(explanations["dob_year_disjoint"]["detail"], "no data to match against");
    assert!(explanations["dob_year_disjoint"].get("candidate").is_none());

    let entity = Entity::builder("Person").properties(&[]).build();
    let json = serde_json::to_value(&entity).unwrap();
    assert!(json.get("explanations").is_none());
  }

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
        ("country", &["fr"]),
        ("jurisdiction", &["gb"]),
        ("nationality", &["ru"]),
        ("citizenship", &["ci"]),
      ])
      .build();

    let identifiers = se.prop_group("identifier", PropertyFilter::All);
    let countries = se.prop_group("country", PropertyFilter::All);

    assert!(identifiers.iter().any(|p| p.value == "VAT"));
    assert!(identifiers.iter().any(|p| p.field == "vatCode" && p.value == "VAT"));
    assert!(identifiers.iter().any(|p| p.value == "ID"));
    assert!(identifiers.iter().any(|p| p.value == "PN"));
    assert!(identifiers.iter().any(|p| p.value == "SSN"));
    assert!(countries.iter().any(|p| p.value == "fr"));
    assert!(countries.iter().any(|p| p.value == "gb"));
    assert!(countries.iter().any(|p| p.value == "ru"));
    assert!(countries.iter().any(|p| p.value == "ci"));
  }
  #[test]
  fn precompute() {
    let se = SearchEntity::builder("Person").properties(&[("name", &["vladimir-putin", "bar'ack obama", "バラク・オバマ"])]).build();

    assert_eq!(se.name_parts, [["vladimir", "putin"], ["barack", "obama"], ["baraku", "obama"]]);
    assert_eq!(
      se.name_parts_flat,
      std::collections::HashSet::from_iter(["vladimir", "putin", "barack", "obama", "baraku", "obama"].into_iter().map(String::from))
    );
    assert_eq!(se.clean_names, ["vladimir putin", "barack obama", "baraku obama"]);
  }

  #[test]
  fn precompute_name_parts_combinations() {
    let se = SearchEntity::builder("Person")
      .properties(&[("name", &["Joe Bob"]), ("firstName", &["Vladimir"]), ("lastName", &["Putin"])])
      .build();

    assert_eq!(se.props(&["name"])[0].value, "Joe Bob");
    assert_eq!(se.props(&["alias"])[0].value, "Vladimir Putin");
  }

  #[test]
  fn precompute_countries() {
    let mut se = SearchEntity::builder("Person")
      .properties(&[("citizenship", &["Whatever", "The Russian Federation", "fr", "GB", "RUS"])])
      .build();

    se.precompute();

    assert_eq!(
      HashSet::from_iter(se.properties.get("citizenship").unwrap().iter().cloned()),
      HashSet::from_iter(["ru", "fr", "gb"].into_iter().map(str::to_string)),
    );
  }

  #[test]
  fn pick_names() {
    let aliases = SearchEntity::builder("Person")
      .properties(&[("name", &["Vladimir Putin"]), ("alias", &["John Doe", "John  Doe", "J. Doe", "Jonathan Doe", "JD", "Mr. John Doe"])])
      .build();

    let names = aliases.pick_names(4);

    assert_eq!(names.as_ref(), &["Vladimir Putin", "John Doe", "JD", "Jonathan Doe"]);
  }

  #[test]
  fn resolve_schema_chain() {
    assert_eq!(Schema::from("Person").matchable_schemas(ResolveSchemaLevel::Root), &["Person", "LegalEntity"]);
    assert_eq!(Schema::from("Company").matchable_schemas(ResolveSchemaLevel::Root), &["Company", "Organization", "LegalEntity"]);
    assert_eq!(Schema::from("Airplane").matchable_schemas(ResolveSchemaLevel::Root), &["Airplane", "Vehicle"]);

    assert_eq!(
      HashSet::from_iter(Schema::from("Vehicle").matchable_schemas(ResolveSchemaLevel::Root).iter()),
      HashSet::from_iter(["Vessel".to_string(), "Airplane".to_string(), "Vehicle".to_string()].iter())
    );

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
