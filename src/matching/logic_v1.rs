use tracing::instrument;

use crate::matching::{Feature, MatchingAlgorithm, matchers::name_literal_match::NameLiteralMatch, run_features};

pub struct LogicV1;

// WIP
impl MatchingAlgorithm for LogicV1 {
  fn name() -> &'static str {
    "logic-v1"
  }

  #[instrument(name = "score_hit", skip_all)]
  fn score(lhs: &crate::model::SearchEntity, rhs: &crate::model::Entity) -> (f64, Vec<(&'static str, f64)>) {
    let features: &[(&dyn Feature, f64)] = &[
      (&NameLiteralMatch, 1.0),
      // TODO: new features
      // (&SimpleMismatch::new("country_disjoint", &|e| e.property("country"), None), -0.2),
      // (&SimpleMismatch::new("last_name_mismatch", &|e| e.property("lastName"), None), -0.2),
      // (&SimpleMismatch::new("dob_year_disjoint", &|e| e.property("birthDate"), Some(dob_year_disjoint)), -0.2),
      // (&SimpleMismatch::new("dob_day_disjoint", &|e| e.property("birthDate"), Some(dob_day_disjoint)), -0.2),
      // (&SimpleMismatch::new("gender_mismatch", &|e| e.property("gender"), None), -0.2),
      // (&OrgIdMismatch, -0.2),
      // TODO: numbers_mismatch
    ];

    let mut results = Vec::with_capacity(features.len());
    let score = run_features(lhs, rhs, 0.0, features, &mut results);

    (score, results)
  }
}
