use macros::scoring_feature;
use strsim::jaro_winkler;

use crate::{
  matching::{Feature, utils},
  model::{Entity, HasProperties, SearchEntity},
};

#[scoring_feature(JaroNameParts, name = "jaro_name_parts")]
fn score_feature(&self, lhs: &SearchEntity, rhs: &Entity) -> f64 {
  let mut similarities = Vec::new();

  for part in &lhs.name_parts {
    let mut best = 0.0f64;

    for other in utils::name_parts(rhs.names()) {
      let similarity = match jaro_winkler(part, &other) {
        score if score > 0.6 => score,
        _ => 0.0,
      };

      if similarity >= 0.5 {
        best = best.max(similarity);
      }
    }

    similarities.push(best);
  }

  similarities.iter().sum::<f64>() / 1.0f64.max(similarities.len() as f64)
}
