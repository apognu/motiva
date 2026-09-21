use bumpalo::Bump;
use compact_str::CompactString;
use itertools::Itertools;
use tracing::instrument;

use crate::{
  matching::{Candidate, Detail, Feature, ScoreResult},
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

  fn match_property(&self, schema: &Schema, lhs: &impl HasProperties, rhs: &impl HasProperties, property: &str, provenance_side: Option<bool>) -> Option<(CompactString, Option<Candidate>)> {
    let lhs_values = lhs.props(&[property]);

    if lhs_values.is_empty() {
      return None;
    }

    if let Some(validator) = self.validator
      && lhs_values.iter().any(|code| !(validator)(code.value))
    {
      return None;
    }

    let schema = SCHEMAS.get(schema.as_str())?;

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

    let rhs_values = rhs.props(&properties);
    let (matched, other) = lhs_values.iter().find_map(|code| {
      rhs_values
        .iter()
        .find(|other| other.value == code.value && self.validator.map(|v| v(other.value)).unwrap_or(true))
        .map(|other| (code, other))
    })?;

    let provenance = provenance_side.map(|candidate_on_rhs| {
      let value = if candidate_on_rhs { other } else { matched };
      Candidate::new(value.field, value.value)
    });

    Some((CompactString::from(matched.as_str()), provenance))
  }
}

impl<'p> Feature for IdentifierMatch<'p> {
  fn name(&self) -> &'static str {
    self.name
  }

  #[instrument(level = "trace", name = "identifier_match", skip_all, fields(entity_id = rhs.id, identifier = ?self.properties))]
  fn score(&self, _bump: &Bump, lhs: &SearchEntity, rhs: &Entity, explain: bool) -> ScoreResult {
    let matched = self.properties.iter().find_map(|property| {
      self
        .match_property(&lhs.schema, lhs, rhs, property, explain.then_some(true))
        .or_else(|| self.match_property(&rhs.schema, rhs, lhs, property, explain.then_some(false)))
    });

    match matched {
      Some((code, candidate)) => (1.0, explain.then(|| Detail::Labeled("matched identifier", code)), candidate).into(),
      None => (0.0, explain.then_some(Detail::Note("no match on identifiers"))).into(),
    }
  }
}

#[cfg(test)]
mod tests {
  use bumpalo::Bump;

  use crate::{
    matching::{Feature, ScoreResult, matchers::identifier::IdentifierMatch},
    model::{Entity, SearchEntity},
  };

  #[test]
  fn identifier_match_details() {
    let feature = IdentifierMatch::new("t", &["leiCode"], None);

    // Matched: the shared identifier is surfaced.
    let lhs = SearchEntity::builder("Company").properties(&[("leiCode", &["ABC123"])]).build();
    let rhs = Entity::builder("Company").properties(&[("leiCode", &["ABC123"])]).build();
    assert_eq!(feature.score(&Bump::new(), &lhs, &rhs, true).1.unwrap().to_string(), "matched identifier: ABC123");
    let ScoreResult(_, _, candidate) = feature.score(&Bump::new(), &lhs, &rhs, true);
    let candidate = candidate.unwrap();
    assert_eq!(candidate.field, "leiCode");
    assert_eq!(candidate.value, "ABC123");

    // No match.
    let lhs = SearchEntity::builder("Company").properties(&[("leiCode", &["ABC123"])]).build();
    let rhs = Entity::builder("Company").properties(&[("leiCode", &["XYZ789"])]).build();
    assert_eq!(feature.score(&Bump::new(), &lhs, &rhs, true).1.unwrap().to_string(), "no match on identifiers");
  }
}
