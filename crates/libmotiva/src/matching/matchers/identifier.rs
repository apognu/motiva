use bumpalo::{
  Bump,
  collections::{CollectIn, Vec},
};
use compact_str::CompactString;
use itertools::Itertools;
use tracing::instrument;

use crate::{
  matching::{Detail, Feature, ScoreResult},
  model::{Entity, HasProperties, Schema, SearchEntity},
  schemas::{FtmProperty, SCHEMAS},
};

pub(crate) struct IdentifierMatch<'p> {
  name: &'static str,
  properties: &'p [&'p str],
  validator: Option<fn(&str) -> bool>,
}

impl<'p> IdentifierMatch<'p> {
  pub(crate) fn new(name: &'static str, properties: &'p [&'p str], validator: Option<fn(&str) -> bool>) -> &'static Self {
    Box::leak(Box::new(Self { name, properties, validator }))
  }

  fn match_property(&self, bump: &Bump, schema: &Schema, lhs: &impl HasProperties, rhs: &impl HasProperties, property: &str) -> Option<CompactString> {
    let lhs_values = lhs.props(&[property]);

    if lhs_values.is_empty() {
      return None;
    }

    if let Some(validator) = self.validator
      && lhs_values.iter().any(|code| !(validator)(code))
    {
      return None;
    }

    let schema = SCHEMAS.get(schema.as_str())?;

    let mut schema_property: Option<FtmProperty> = None;
    let mut properties = Vec::new_in(bump);

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

    let schema_property = schema_property?;

    for chain in &schema.parents {
      let Some(chain_schema) = SCHEMAS.get(chain) else {
        continue;
      };

      let rhs_properties = chain_schema
        .properties
        .iter()
        .filter(|(_, prop)| prop._type == schema_property.clone()._type)
        .map(|(name, _)| name.as_str())
        .unique();

      properties.extend(rhs_properties);
    }

    let rhs_values = rhs
      .props(&properties)
      .into_owned()
      .into_iter()
      .filter(|code| self.validator.map(|v| v(code)).unwrap_or(true))
      .collect_in::<Vec<_>>(bump);

    lhs_values
      .iter()
      .find(|code| rhs_values.iter().any(|other| other == *code))
      .map(|code| CompactString::from(code.as_str()))
  }
}

impl<'p> Feature for IdentifierMatch<'p> {
  fn name(&self) -> &'static str {
    self.name
  }

  #[instrument(level = "trace", name = "identifier_match", skip_all, fields(entity_id = rhs.id, identifier = ?self.properties))]
  fn score(&self, bump: &Bump, lhs: &SearchEntity, rhs: &Entity, explain: bool) -> ScoreResult {
    let matched = self.properties.iter().find_map(|property| {
      self
        .match_property(bump, &lhs.schema, lhs, rhs, property)
        .or_else(|| self.match_property(bump, &rhs.schema, rhs, lhs, property))
    });

    match matched {
      Some(code) => (1.0, explain.then(|| Detail::Labeled("matched identifier", code))).into(),
      None => (0.0, explain.then_some(Detail::Note("no match on identifiers"))).into(),
    }
  }
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;

  use crate::{
    matching::{Feature, matchers::identifier::IdentifierMatch},
    model::{Entity, SearchEntity},
  };

  #[test]
  fn identifier_match_details() {
    let feature = IdentifierMatch::new("t", &["leiCode"], None);

    // Matched: the shared identifier is surfaced.
    let lhs = SearchEntity::builder("Company").properties(&[("leiCode", &["ABC123"])]).build();
    let rhs = Entity::builder("Company").properties(&[("leiCode", &["ABC123"])]).build();
    assert_eq!(feature.score(&Bump::new(), &lhs, &rhs, true).1.unwrap().to_string(), "matched identifier: ABC123");

    // No match.
    let lhs = SearchEntity::builder("Company").properties(&[("leiCode", &["ABC123"])]).build();
    let rhs = Entity::builder("Company").properties(&[("leiCode", &["XYZ789"])]).build();
    assert_eq!(feature.score(&Bump::new(), &lhs, &rhs, true).1.unwrap().to_string(), "no match on identifiers");
  }
}
