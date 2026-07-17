use std::fmt;

use compact_str::CompactString;

/// A single derived-representation match: a value and its derived code on each
/// side, e.g. `putin` / `PTN` against `poutine` / `PTN`.
#[derive(Clone, Debug)]
pub struct CodedPair {
  pub lhs: CompactString,
  pub lhs_code: CompactString,
  pub rhs: CompactString,
  pub rhs_code: CompactString,
}

/// Structured, allocation-light explanation of how a feature scored.
///
/// Only owns `&'static str`, [`CompactString`] (short values stay inline) and
/// `f64`, so it never borrows the scoring bump arena and is rendered to a
/// `String` only at serialization time (see [`fmt::Display`]).
#[derive(Clone, Debug, Default)]
pub enum Detail {
  /// No detail available; renders to an empty string.
  #[default]
  None,
  /// A static message, e.g. "no match on identifiers".
  Note(&'static str),
  /// A static label followed by a matched value, e.g. "matched identifier: X".
  Labeled(&'static str, CompactString),
  /// Two values that compared equal, e.g. "A == B".
  Equal(CompactString, CompactString),
  /// Two values that compared fuzzily, e.g. "A ~= B = 0.9".
  Fuzzy { lhs: CompactString, rhs: CompactString, score: f64 },
  /// Two values that matched approximately, with a human note about the gap,
  /// e.g. "1988-07-22 ~= 1988-07-27 (5 days apart)".
  Approximate { lhs: CompactString, rhs: CompactString, note: CompactString },
  /// A fuzzy substring match, showing the sequence that matched, e.g.
  /// "google llc ~= gooogle limited liability company = 0.9 (matched: googl)".
  Subsequence {
    lhs: CompactString,
    rhs: CompactString,
    matched: CompactString,
    score: f64,
  },
  /// Two values matched through a derived representation (a phonetic code, a
  /// soundex, ...), e.g. "Putin [PTN] ~= Poutine [PTN]".
  Coded(CodedPair),
  /// Several derived-representation matches that jointly produced a score,
  /// rendered comma-separated, e.g.
  /// "vladimir [FLTMR] ~= vladymir [FLTMR], putin [PTN] ~= poutine [PTN]".
  CodedList(Vec<CodedPair>),
}

impl fmt::Display for Detail {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Detail::None => Ok(()),
      Detail::Note(note) => f.write_str(note),
      Detail::Labeled(label, value) => write!(f, "{label}: {value}"),
      Detail::Equal(lhs, rhs) => write!(f, "{lhs} == {rhs}"),
      Detail::Fuzzy { lhs, rhs, score } => write!(f, "{lhs} ~= {rhs} = {score}"),
      Detail::Approximate { lhs, rhs, note } => write!(f, "{lhs} ~= {rhs} ({note})"),
      Detail::Subsequence { lhs, rhs, matched, score } => write!(f, "{lhs} ~= {rhs} = {score} (matched: {matched})"),
      Detail::Coded(pair) => write!(f, "{} [{}] ~= {} [{}]", pair.lhs, pair.lhs_code, pair.rhs, pair.rhs_code),
      Detail::CodedList(matches) => {
        for (index, pair) in matches.iter().enumerate() {
          if index > 0 {
            f.write_str(", ")?;
          }

          write!(f, "{} [{}] ~= {} [{}]", pair.lhs, pair.lhs_code, pair.rhs, pair.rhs_code)?;
        }

        Ok(())
      }
    }
  }
}

/// Per-feature result carried from a [`MatchingAlgorithm`](super::MatchingAlgorithm)
/// out to serialization.
#[derive(Clone, Debug)]
pub struct Explanation {
  pub name: &'static str,
  pub score: f64,
  pub weighted: f64,
  pub detail: Detail,
}

#[cfg(test)]
mod tests {
  use super::{CodedPair, Detail};

  #[test]
  fn detail_rendering() {
    assert_eq!(Detail::None.to_string(), "");
    assert_eq!(Detail::Note("no match on identifiers").to_string(), "no match on identifiers");
    assert_eq!(Detail::Labeled("matched identifier", "X123".into()).to_string(), "matched identifier: X123");
    assert_eq!(Detail::Equal("Bob Singer".into(), "Bob Singer".into()).to_string(), "Bob Singer == Bob Singer");
    assert_eq!(
      Detail::Fuzzy {
        lhs: "Bob Singer".into(),
        rhs: "Bobby Ringer".into(),
        score: 0.9
      }
      .to_string(),
      "Bob Singer ~= Bobby Ringer = 0.9"
    );
    assert_eq!(
      Detail::Approximate {
        lhs: "1988-07-22".into(),
        rhs: "1988-07-27".into(),
        note: "5 days apart".into()
      }
      .to_string(),
      "1988-07-22 ~= 1988-07-27 (5 days apart)"
    );
    assert_eq!(
      Detail::Subsequence {
        lhs: "google llc".into(),
        rhs: "gooogle limited liability company".into(),
        matched: "googl".into(),
        score: 0.9
      }
      .to_string(),
      "google llc ~= gooogle limited liability company = 0.9 (matched: googl)"
    );
    assert_eq!(
      Detail::Coded(CodedPair {
        lhs: "Putin".into(),
        lhs_code: "PTN".into(),
        rhs: "Poutine".into(),
        rhs_code: "PTN".into()
      })
      .to_string(),
      "Putin [PTN] ~= Poutine [PTN]"
    );
    assert_eq!(
      Detail::CodedList(vec![
        CodedPair {
          lhs: "Vladimir".into(),
          lhs_code: "FLTMR".into(),
          rhs: "Vladymir".into(),
          rhs_code: "FLTMR".into()
        },
        CodedPair {
          lhs: "Putin".into(),
          lhs_code: "PTN".into(),
          rhs: "Poutine".into(),
          rhs_code: "PTN".into()
        },
      ])
      .to_string(),
      "Vladimir [FLTMR] ~= Vladymir [FLTMR], Putin [PTN] ~= Poutine [PTN]"
    );
  }
}
