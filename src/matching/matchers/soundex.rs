use std::collections::HashSet;

use macros::scoring_feature;
use rphonetic::{Encoder, Soundex};

use crate::{
  matching::{Feature, utils},
  model::{Entity, HasProperties, SearchEntity},
};

#[scoring_feature(SoundexNameParts, name = "soundex_name_parts")]
fn score_feature(&self, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  let soundex = Soundex::default();
  let mut similarities = Vec::new();

  let parts = utils::name_parts(rhs.names()).collect::<HashSet<String>>();
  let rhs_soundexes = parts.iter().map(|s| soundex.encode(s)).collect::<Vec<_>>();

  for part in &lhs.name_parts {
    let lhs_soundex = soundex.encode(part);

    similarities.push(if rhs_soundexes.contains(&lhs_soundex) { 1.0 } else { 0.0 });
  }

  similarities.iter().sum::<f64>() / 1.0f64.max(similarities.len() as f64)
}
