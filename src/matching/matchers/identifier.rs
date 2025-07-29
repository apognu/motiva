use itertools::Itertools;
use tracing::instrument;

use crate::{
  matching::{Feature, extractors::is_disjoint},
  model::{Entity, HasProperties, Schema, SearchEntity},
  schemas::{FtmProperty, SCHEMAS},
};

pub struct IdentifierMatch<'p> {
  name: &'static str,
  properties: &'p [&'p str],
  validator: Option<fn(&str) -> bool>,
}

impl<'p> IdentifierMatch<'p> {
  pub fn new(name: &'static str, properties: &'p [&'p str], validator: Option<fn(&str) -> bool>) -> Self {
    Self { name, properties, validator }
  }

  fn match_property(&self, schema: &Schema, lhs: &impl HasProperties, rhs: &impl HasProperties, property: &str) -> bool {
    let lhs_values = lhs.property(property);

    if lhs_values.is_empty() {
      return false;
    }

    if let Some(validator) = self.validator
      && lhs_values.iter().any(|code| !(validator)(code))
    {
      return false;
    }

    let Some(schema) = SCHEMAS.get(schema.as_str()) else {
      return false;
    };

    let mut schema_property: Option<FtmProperty> = None;
    let mut properties = Vec::new();

    'prop: for chain in &schema.parents {
      let Some(chain_schema) = SCHEMAS.get(chain) else {
        continue;
      };

      for (name, prop) in &chain_schema.properties {
        if name == property {
          schema_property = Some(prop.clone());
          break 'prop;
        }
      }
    }

    if schema_property.is_none() {
      return false;
    }

    for chain in &schema.parents {
      let Some(chain_schema) = SCHEMAS.get(chain) else {
        continue;
      };

      let rhs_properties = chain_schema
        .properties
        .iter()
        .filter(|(_, prop)| prop._type == schema_property.clone().unwrap()._type)
        .map(|(name, _)| name.as_str())
        .unique();

      properties.extend(rhs_properties);
    }

    let rhs_values = rhs.gather(&properties).into_iter().filter(|code| self.validator.map(|v| v(code)).unwrap_or(true)).collect::<Vec<_>>();

    !is_disjoint(lhs.property(property), &rhs_values)
  }
}

impl<'p> Feature<'p> for IdentifierMatch<'p> {
  fn name(&self) -> &'static str {
    self.name
  }

  #[instrument(level = "trace", name = "identifier_match", skip_all, fields(identifier = ?self.properties))]
  fn score_feature(&self, lhs: &SearchEntity, rhs: &Entity) -> f64 {
    for property in self.properties {
      if self.match_property(&lhs.schema, lhs, rhs, property) {
        return 1.0;
      }
      if self.match_property(&rhs.schema, rhs, lhs, property) {
        return 1.0;
      }
    }

    0.0
  }
}
