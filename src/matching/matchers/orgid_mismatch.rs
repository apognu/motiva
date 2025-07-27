use itertools::Itertools;
use strsim::levenshtein;

use crate::{
  matching::Feature,
  model::{Entity, HasProperties, SearchEntity},
};

#[derive(Default)]
pub struct OrgIdMismatch;

impl<'e> Feature<'e> for OrgIdMismatch {
  fn name(&self) -> &'static str {
    "orgid_disjoint"
  }

  fn score_feature(&self, lhs: &SearchEntity, rhs: &Entity) -> f64 {
    if lhs.schema.0 != "Organization" || rhs.schema.0 != "Organization" {
      return 0.0;
    }

    let lhs = lhs.gather(&["registrationNumber", "taxNumber"]);
    let rhs = rhs.gather(&["registrationNumber", "taxNumber"]);

    if lhs.is_empty() || rhs.is_empty() {
      return 0.0;
    }

    if !lhs.is_disjoint(&rhs) {
      return 0.0;
    }

    lhs.into_iter().cartesian_product(rhs.iter()).max_by(|(l1, l2), (r1, r2)| {
      let lev1 = levenshtein(l1, l2) as f64;
      let lev2 = levenshtein(r1, r2) as f64;
      let ratio1 = 1.0 - (lev1 / l1.len().max(l2.len()) as f64);
      let ratio2 = 1.0 - (lev2 / r1.len().max(r2.len()) as f64);

      (if ratio1 > 0.7 { ratio1 } else { 0.0 }).total_cmp(if ratio2 > 0.7 { &ratio2 } else { &0.0 })
    });

    0.0
  }
}
